"""Append-only persistence: recording mutations, and replaying them at startup.

Records are RESP2 arrays, the same framing the wire protocol uses, so keys and
values carrying NUL or CRLF need no escaping and the file can be read with any
RESP tooling.

The recorded vocabulary is deliberately a *superset* of the client vocabulary:
commands whose meaning depends on when they ran are rewritten into an absolute
form, so `SET k v EX 60` is recorded as `SET k v PXAT <deadline>` and never
gains a fresh minute of life on reload. `PXAT` and `PEXPIREAT` exist only in
the file; no client can send them.

The writer never blocks on anything but the filesystem, and it stays on the
event loop: an fsync can stall the loop, which is the documented price of the
`always` policy and, once a second, of `everysec`.
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from contextlib import suppress
from enum import StrEnum
from pathlib import Path
from typing import BinaryIO, Final, NamedTuple, NoReturn, Protocol

from pyredis import resp
from pyredis.log import get_logger
from pyredis.store import KeyValueStore

_logger = get_logger(__name__)

CRLF: Final = b"\r\n"


class FsyncPolicy(StrEnum):
    """How hard the journal works to get bytes onto the platter.

    Every policy flushes to the operating system on each append, so killing the
    process loses nothing; the policy only decides how often the OS is told to
    put its page cache on disk, which is what a power failure takes away.
    """

    ALWAYS = "always"
    EVERYSEC = "everysec"
    NO = "no"


FSYNC_POLICIES: Final = tuple(policy.value for policy in FsyncPolicy)


class AofError(Exception):
    """Base class for append-only file problems."""


class PersistenceError(AofError):
    """A mutation could not be recorded."""


class AofCorruptError(AofError):
    """The append-only file holds something that is not a valid record."""

    def __init__(self, offset: int, reason: str) -> None:
        super().__init__(f"{reason} at byte offset {offset}")
        self.offset = offset


class Journal(Protocol):
    """Where mutations are recorded."""

    @property
    def failed(self) -> bool:
        """True once a write has failed and the journal has stopped trusting itself."""

    def append(self, record: bytes) -> None: ...

    def sync(self) -> None: ...

    def close(self) -> None: ...


class NullJournal:
    """The journal used when persistence is switched off: it records nothing."""

    @property
    def failed(self) -> bool:
        return False

    def append(self, record: bytes) -> None:
        return None

    def sync(self) -> None:
        return None

    def close(self) -> None:
        return None


#: Shared instance -- `NullJournal` holds no state.
NO_JOURNAL: Final = NullJournal()


class AppendOnlyFile:
    """A journal backed by a real file.

    Once any write fails the journal is **failed for good**: it records nothing
    further and rejects every attempt, so the file can never drift silently out
    of step with memory. Restarting the server is the only way back, which is a
    deliberate simplification -- there is no retry, no half-recovery, and no
    pretending persistence came back.
    """

    def __init__(self, path: Path, policy: FsyncPolicy) -> None:
        self._path = path
        self._policy = policy
        self._failed = False
        try:
            self._file: BinaryIO = path.open("ab")
        except OSError as exc:
            raise PersistenceError(f"cannot open {path}: {exc}") from exc

    @property
    def failed(self) -> bool:
        return self._failed

    def append(self, record: bytes) -> None:
        """Record one mutation, syncing first if the policy demands it."""
        if self._failed:
            raise PersistenceError("journal has failed; restart to recover")
        try:
            self._file.write(record)
            self._file.flush()
            if self._policy is FsyncPolicy.ALWAYS:
                os.fsync(self._file.fileno())
        except OSError as exc:
            self._fail("append", exc)

    def sync(self) -> None:
        """Force the operating system to commit what has been written."""
        if self._failed:
            return
        try:
            self._file.flush()
            os.fsync(self._file.fileno())
        except OSError as exc:
            self._fail("fsync", exc)

    def close(self) -> None:
        if not self._failed:
            with suppress(OSError):
                self._file.flush()
                os.fsync(self._file.fileno())
        with suppress(OSError):
            self._file.close()

    def _fail(self, action: str, exc: OSError) -> NoReturn:
        self._failed = True
        _logger.error(
            "persistence failed during %s to %s: %s -- writes will be rejected until restart",
            action,
            self._path,
            exc,
        )
        raise PersistenceError(f"{action} failed: {exc}") from exc


# --------------------------------------------------------------------------
# Reading a log back
# --------------------------------------------------------------------------


class Record(NamedTuple):
    offset: int
    parts: list[bytes]


class Scan(NamedTuple):
    records: list[Record]
    consumed: int  # bytes covered by complete records
    trailing: int  # bytes left over; 0 means the file ended cleanly


class LoadResult(NamedTuple):
    records: int
    keys: int
    expired: int
    repaired: int  # bytes discarded from a torn final record


def load(path: Path, store: KeyValueStore) -> LoadResult:
    """Replay `path` into `store`, repairing a torn final record if there is one.

    Raises:
        AofCorruptError: the file is damaged somewhere other than its tail, in
            which case nothing is applied and the caller should refuse to start.
        PersistenceError: the file exists but cannot be read or repaired.
    """
    if not path.exists():
        return LoadResult(0, 0, 0, 0)
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise PersistenceError(f"cannot read {path}: {exc}") from exc

    scanned = scan(data)
    for record in scanned.records:
        _apply(store, record)
    if scanned.trailing:
        _truncate(path, scanned.consumed)
    expired = store.drop_expired()
    return LoadResult(len(scanned.records), store.dbsize(), expired, scanned.trailing)


def scan(data: bytes) -> Scan:
    """Split `data` into records, stopping at the first incomplete one.

    A trailing *prefix* of a well-formed record is a torn write and is reported
    through `trailing`. Anything that could never become a valid record raises,
    because discarding it would silently drop every write that followed.
    """
    records: list[Record] = []
    offset = 0
    while offset < len(data):
        parts, next_offset = _read_record(data, offset)
        if parts is None:
            return Scan(records, offset, len(data) - offset)
        records.append(Record(offset, parts))
        offset = next_offset
    return Scan(records, offset, 0)


def _read_record(data: bytes, offset: int) -> tuple[list[bytes] | None, int]:
    header, cursor = _read_line(data, offset)
    if header is None:
        return None, offset
    if not header.startswith(b"*"):
        raise AofCorruptError(offset, f"expected '*', got '{resp.printable(header[:1])}'")

    count = _count(header[1:], offset, "invalid multibulk length")
    if count <= 0:
        raise AofCorruptError(offset, "invalid multibulk length")

    parts: list[bytes] = []
    for _ in range(count):
        line, after = _read_line(data, cursor)
        if line is None:
            return None, offset
        if not line.startswith(b"$"):
            raise AofCorruptError(cursor, f"expected '$', got '{resp.printable(line[:1])}'")
        length = _count(line[1:], cursor, "invalid bulk length")
        if length < 0:
            raise AofCorruptError(cursor, "invalid bulk length")
        end = after + length + len(CRLF)
        if end > len(data):
            return None, offset
        if data[after + length : end] != CRLF:
            raise AofCorruptError(cursor, "bulk string is not terminated")
        parts.append(data[after : after + length])
        cursor = end
    return parts, cursor


def _read_line(data: bytes, offset: int) -> tuple[bytes | None, int]:
    end = data.find(CRLF, offset)
    if end == -1:
        return None, offset
    return data[offset:end], end + len(CRLF)


def _count(raw: bytes, offset: int, reason: str) -> int:
    digits = raw[1:] if raw.startswith(b"-") else raw
    if not digits or not digits.isdigit():
        raise AofCorruptError(offset, reason)
    return int(raw)


def _truncate(path: Path, size: int) -> None:
    _logger.warning(
        "append-only file ends in a partial record; truncating %s to %d byte(s)", path, size
    )
    try:
        with path.open("r+b") as handle:
            handle.truncate(size)
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as exc:
        raise PersistenceError(f"cannot repair {path}: {exc}") from exc


def _apply(store: KeyValueStore, record: Record) -> None:
    """Apply one record through the store's recovery primitives.

    Every record is a statement of fact rather than a computation, so nothing
    here can fail on the data: replay is total.
    """
    parts = record.parts
    name = parts[0].upper()
    if name == b"SET":
        if len(parts) == 3:
            store.restore(parts[1], parts[2])
        elif len(parts) == 5 and parts[3].upper() == b"PXAT":
            deadline = _count(parts[4], record.offset, "malformed PXAT deadline")
            store.restore(parts[1], parts[2], deadline_ms=deadline)
        else:
            raise AofCorruptError(record.offset, "malformed SET record")
    elif name == b"PEXPIREAT":
        _require(len(parts) == 3, record.offset, "malformed PEXPIREAT record")
        store.set_deadline(parts[1], _count(parts[2], record.offset, "bad deadline"))
    elif name == b"DEL":
        _require(len(parts) >= 2, record.offset, "malformed DEL record")
        store.discard(*parts[1:])
    elif name == b"PERSIST":
        _require(len(parts) == 2, record.offset, "malformed PERSIST record")
        store.clear_deadline(parts[1])
    elif name == b"FLUSHDB":
        _require(len(parts) == 1, record.offset, "malformed FLUSHDB record")
        store.flushdb()
    else:
        raise AofCorruptError(record.offset, f"unknown record '{resp.printable(parts[0][:32])}'")


def _require(condition: bool, offset: int, reason: str) -> None:
    if not condition:
        raise AofCorruptError(offset, reason)


def encode_record(*parts: bytes) -> bytes:
    """Encode one AOF record."""
    return resp.encode_array(parts)


def encode_set(key: bytes, value: bytes, deadline_ms: int | None) -> bytes:
    """Encode the canonical form of a write, carrying its deadline if it has one."""
    if deadline_ms is None:
        return encode_record(b"SET", key, value)
    return encode_record(b"SET", key, value, b"PXAT", str(deadline_ms).encode("ascii"))


def encode_delete(keys: Sequence[bytes]) -> bytes:
    return encode_record(b"DEL", *keys)

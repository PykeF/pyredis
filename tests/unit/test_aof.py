from __future__ import annotations

import os
from pathlib import Path

import pytest

from pyredis.aof import (
    NO_JOURNAL,
    AofCorruptError,
    AppendOnlyFile,
    FsyncPolicy,
    PersistenceError,
    encode_delete,
    encode_record,
    encode_set,
    load,
    scan,
)
from pyredis.store import KeyValueStore

BINARY_KEY = b"\x00\xff key\r\n"
BINARY_VALUE = b"\x89PNG\r\n\x1a\n\x00\xfe"


class FakeClock:
    """A clock the tests move by hand, so no test ever waits in real time."""

    def __init__(self, now: int = 1_700_000_000_000) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now

    def advance(self, ms: int) -> None:
        self.now += ms


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def store(clock: FakeClock) -> KeyValueStore:
    return KeyValueStore(clock=clock)


# --------------------------------------------------------------------------
# Record encoding
# --------------------------------------------------------------------------


def test_encodes_a_record_as_a_resp_array() -> None:
    assert encode_record(b"SET", b"k", b"v") == b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"


def test_encodes_a_write_without_a_deadline() -> None:
    assert encode_set(b"k", b"v", None) == b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"


def test_encodes_a_write_with_an_absolute_deadline() -> None:
    assert encode_set(b"k", b"v", 1700000000000) == (
        b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$4\r\nPXAT\r\n$13\r\n1700000000000\r\n"
    )


def test_binary_keys_and_values_need_no_escaping() -> None:
    record = encode_set(BINARY_KEY, BINARY_VALUE, None)

    assert scan(record).records[0].parts == [b"SET", BINARY_KEY, BINARY_VALUE]


# --------------------------------------------------------------------------
# Scanning
# --------------------------------------------------------------------------


def test_scans_an_empty_file() -> None:
    assert scan(b"") == ([], 0, 0)


def test_scans_several_records() -> None:
    data = encode_set(b"a", b"1", None) + encode_record(b"DEL", b"a") + encode_record(b"FLUSHDB")

    scanned = scan(data)

    assert [record.parts for record in scanned.records] == [
        [b"SET", b"a", b"1"],
        [b"DEL", b"a"],
        [b"FLUSHDB"],
    ]
    assert scanned.trailing == 0
    assert scanned.consumed == len(data)


def test_records_carry_their_byte_offsets() -> None:
    first = encode_set(b"a", b"1", None)
    scanned = scan(first + encode_record(b"FLUSHDB"))

    assert [record.offset for record in scanned.records] == [0, len(first)]


@pytest.mark.parametrize("keep", range(1, 24))
def test_any_truncation_of_a_record_is_reported_as_trailing_bytes(keep: int) -> None:
    # Every partial write of a valid record must look like a torn tail, never
    # like corruption -- that is the difference between starting and refusing.
    complete = encode_set(b"a", b"1", None)
    data = encode_record(b"FLUSHDB") + complete[:keep]

    scanned = scan(data)

    assert len(scanned.records) == 1
    assert scanned.trailing == keep
    assert scanned.consumed == len(encode_record(b"FLUSHDB"))


@pytest.mark.parametrize(
    "tail",
    [
        pytest.param(b"+OK\r\n", id="not-an-array"),
        pytest.param(b"*abc\r\n", id="bad-multibulk-length"),
        pytest.param(b"*0\r\n", id="empty-array"),
        pytest.param(b"*-1\r\n", id="null-array"),
        pytest.param(b"*1\r\n+PING\r\n", id="element-is-not-a-bulk-string"),
        pytest.param(b"*1\r\n$xyz\r\n", id="bad-bulk-length"),
        pytest.param(b"*1\r\n$1\r\nabXX", id="unterminated-bulk-string"),
    ],
)
def test_corruption_is_reported_rather_than_skipped(tail: bytes) -> None:
    with pytest.raises(AofCorruptError):
        scan(encode_record(b"FLUSHDB") + tail)


def test_corruption_reports_the_byte_offset() -> None:
    prefix = encode_record(b"FLUSHDB")

    with pytest.raises(AofCorruptError) as caught:
        scan(prefix + b"+OK\r\n")

    assert caught.value.offset == len(prefix)


# --------------------------------------------------------------------------
# Replay
# --------------------------------------------------------------------------


def write_aof(path: Path, *records: bytes) -> Path:
    path.write_bytes(b"".join(records))
    return path


def test_replays_a_plain_write(tmp_path: Path, store: KeyValueStore) -> None:
    path = write_aof(tmp_path / "a.aof", encode_set(b"k", b"v", None))

    result = load(path, store)

    assert store.get(b"k") == b"v"
    assert result.records == 1
    assert result.keys == 1


def test_replays_records_in_order(tmp_path: Path, store: KeyValueStore) -> None:
    path = write_aof(
        tmp_path / "a.aof",
        encode_set(b"k", b"first", None),
        encode_set(b"k", b"second", None),
    )

    load(path, store)

    assert store.get(b"k") == b"second"


def test_replays_a_delete(tmp_path: Path, store: KeyValueStore) -> None:
    path = write_aof(
        tmp_path / "a.aof", encode_set(b"k", b"v", None), encode_delete([b"k"])
    )

    load(path, store)

    assert store.get(b"k") is None


def test_replays_a_flushdb(tmp_path: Path, store: KeyValueStore) -> None:
    path = write_aof(
        tmp_path / "a.aof",
        encode_set(b"a", b"1", None),
        encode_record(b"FLUSHDB"),
        encode_set(b"b", b"2", None),
    )

    load(path, store)

    assert store.get(b"a") is None
    assert store.get(b"b") == b"2"


def test_replays_binary_keys_and_values(tmp_path: Path, store: KeyValueStore) -> None:
    path = write_aof(tmp_path / "a.aof", encode_set(BINARY_KEY, BINARY_VALUE, None))

    load(path, store)

    assert store.get(BINARY_KEY) == BINARY_VALUE


def test_a_replayed_deadline_keeps_its_original_absolute_time(
    tmp_path: Path, store: KeyValueStore, clock: FakeClock
) -> None:
    # Recorded when 60s remained; 45s have passed since. The key must come back
    # with 15s left, not a fresh minute.
    deadline = clock.now + 60_000
    path = write_aof(tmp_path / "a.aof", encode_set(b"k", b"v", deadline))
    clock.advance(45_000)

    load(path, store)

    assert store.ttl(b"k") == (True, 15_000)


def test_a_deadline_that_passed_while_down_is_dropped(
    tmp_path: Path, store: KeyValueStore, clock: FakeClock
) -> None:
    path = write_aof(tmp_path / "a.aof", encode_set(b"k", b"v", clock.now + 1_000))
    clock.advance(10_000)

    result = load(path, store)

    assert store.get(b"k") is None
    assert result.expired == 1
    assert result.keys == 0


def test_pexpireat_is_applied_to_an_existing_key(
    tmp_path: Path, store: KeyValueStore, clock: FakeClock
) -> None:
    deadline = clock.now + 5_000
    path = write_aof(
        tmp_path / "a.aof",
        encode_set(b"k", b"v", None),
        encode_record(b"PEXPIREAT", b"k", str(deadline).encode()),
    )

    load(path, store)

    assert store.ttl(b"k") == (True, 5_000)


def test_persist_after_a_deadline_that_has_already_passed(
    tmp_path: Path, store: KeyValueStore, clock: FakeClock
) -> None:
    # The ordering case that forces expiry to stay switched off during replay:
    # evaluating the stale deadline as the SET is applied would delete the key
    # before the PERSIST that was supposed to save it.
    path = write_aof(
        tmp_path / "a.aof",
        encode_set(b"k", b"v", clock.now + 1_000),
        encode_record(b"PERSIST", b"k"),
    )
    clock.advance(10_000)

    load(path, store)

    assert store.get(b"k") == b"v"
    assert store.ttl(b"k") == (True, None)


def test_a_delete_after_an_expired_deadline_still_applies(
    tmp_path: Path, store: KeyValueStore, clock: FakeClock
) -> None:
    path = write_aof(
        tmp_path / "a.aof",
        encode_set(b"k", b"v", clock.now + 1_000),
        encode_delete([b"k"]),
        encode_set(b"k", b"fresh", None),
    )
    clock.advance(10_000)

    load(path, store)

    assert store.get(b"k") == b"fresh"


def test_a_missing_file_replays_as_an_empty_keyspace(
    tmp_path: Path, store: KeyValueStore
) -> None:
    result = load(tmp_path / "absent.aof", store)

    assert result == (0, 0, 0, 0)
    assert store.dbsize() == 0


@pytest.mark.parametrize(
    "record",
    [
        pytest.param(encode_record(b"SET", b"k"), id="set-missing-value"),
        pytest.param(encode_record(b"SET", b"k", b"v", b"FOO", b"1"), id="set-unknown-option"),
        pytest.param(encode_record(b"SET", b"k", b"v", b"PXAT", b"soon"), id="bad-deadline"),
        pytest.param(encode_record(b"PEXPIREAT", b"k"), id="pexpireat-missing-time"),
        pytest.param(encode_record(b"DEL"), id="del-without-keys"),
        pytest.param(encode_record(b"PERSIST"), id="persist-without-key"),
        pytest.param(encode_record(b"FLUSHDB", b"extra"), id="flushdb-with-argument"),
        pytest.param(encode_record(b"GET", b"k"), id="not-a-mutation"),
        pytest.param(encode_record(b"NONSENSE"), id="unknown-record"),
    ],
)
def test_a_malformed_record_is_corruption(
    tmp_path: Path, store: KeyValueStore, record: bytes
) -> None:
    path = write_aof(tmp_path / "a.aof", record)

    with pytest.raises(AofCorruptError):
        load(path, store)


# --------------------------------------------------------------------------
# Torn tails
# --------------------------------------------------------------------------


def test_a_torn_final_record_is_repaired(tmp_path: Path, store: KeyValueStore) -> None:
    complete = encode_set(b"a", b"1", None)
    torn = encode_set(b"b", b"2", None)[:9]
    path = write_aof(tmp_path / "a.aof", complete, torn)

    result = load(path, store)

    assert store.get(b"a") == b"1"
    assert store.get(b"b") is None
    assert result.repaired == len(torn)
    assert path.read_bytes() == complete


def test_a_repaired_file_can_be_appended_to_and_reloaded(
    tmp_path: Path, store: KeyValueStore
) -> None:
    path = write_aof(tmp_path / "a.aof", encode_set(b"a", b"1", None), b"*3\r\n$3\r\nSE")
    load(path, store)

    journal = AppendOnlyFile(path, FsyncPolicy.NO)
    journal.append(encode_set(b"b", b"2", None))
    journal.close()

    reloaded = KeyValueStore()
    load(path, reloaded)
    assert reloaded.get(b"a") == b"1"
    assert reloaded.get(b"b") == b"2"


# --------------------------------------------------------------------------
# Writing
# --------------------------------------------------------------------------


def test_append_writes_the_record_verbatim(tmp_path: Path) -> None:
    path = tmp_path / "a.aof"
    journal = AppendOnlyFile(path, FsyncPolicy.NO)

    journal.append(encode_set(b"k", b"v", None))
    journal.close()

    assert path.read_bytes() == encode_set(b"k", b"v", None)


def test_append_is_visible_before_close_on_every_policy(tmp_path: Path) -> None:
    # Each append flushes to the OS, so a killed process loses nothing even
    # when the policy never fsyncs.
    for policy in FsyncPolicy:
        path = tmp_path / f"{policy.value}.aof"
        journal = AppendOnlyFile(path, policy)
        journal.append(encode_set(b"k", b"v", None))

        assert path.read_bytes() == encode_set(b"k", b"v", None)
        journal.close()


def test_the_always_policy_syncs_on_every_append(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    synced: list[int] = []
    monkeypatch.setattr(os, "fsync", lambda fd: synced.append(fd))
    journal = AppendOnlyFile(tmp_path / "a.aof", FsyncPolicy.ALWAYS)

    journal.append(encode_set(b"k", b"v", None))
    journal.append(encode_set(b"k", b"w", None))

    assert len(synced) == 2


@pytest.mark.parametrize("policy", [FsyncPolicy.EVERYSEC, FsyncPolicy.NO])
def test_the_deferred_policies_do_not_sync_on_append(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, policy: FsyncPolicy
) -> None:
    synced: list[int] = []
    monkeypatch.setattr(os, "fsync", lambda fd: synced.append(fd))
    journal = AppendOnlyFile(tmp_path / "a.aof", policy)

    journal.append(encode_set(b"k", b"v", None))

    assert synced == []

    journal.sync()

    assert len(synced) == 1


def test_opening_an_unwritable_path_is_a_persistence_error(tmp_path: Path) -> None:
    with pytest.raises(PersistenceError):
        AppendOnlyFile(tmp_path / "missing-directory" / "a.aof", FsyncPolicy.NO)


# --------------------------------------------------------------------------
# The failed state
# --------------------------------------------------------------------------


def broken_fsync(fd: int) -> None:
    raise OSError(28, "No space left on device")


def test_a_failed_append_marks_the_journal_and_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = AppendOnlyFile(tmp_path / "a.aof", FsyncPolicy.ALWAYS)
    monkeypatch.setattr(os, "fsync", broken_fsync)

    with pytest.raises(PersistenceError):
        journal.append(encode_set(b"k", b"v", None))

    assert journal.failed is True


def test_a_failed_journal_refuses_every_later_append(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = AppendOnlyFile(tmp_path / "a.aof", FsyncPolicy.ALWAYS)
    monkeypatch.setattr(os, "fsync", broken_fsync)
    with pytest.raises(PersistenceError):
        journal.append(encode_set(b"k", b"v", None))

    monkeypatch.setattr(os, "fsync", os.fsync)  # the disk "recovers"

    with pytest.raises(PersistenceError):
        journal.append(encode_set(b"k", b"w", None))
    assert journal.failed is True, "a failed journal must never quietly recover"


def test_a_failed_sync_marks_the_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = AppendOnlyFile(tmp_path / "a.aof", FsyncPolicy.EVERYSEC)
    journal.append(encode_set(b"k", b"v", None))
    monkeypatch.setattr(os, "fsync", broken_fsync)

    with pytest.raises(PersistenceError):
        journal.sync()

    assert journal.failed is True


def test_syncing_a_failed_journal_is_quiet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = AppendOnlyFile(tmp_path / "a.aof", FsyncPolicy.EVERYSEC)
    monkeypatch.setattr(os, "fsync", broken_fsync)
    with pytest.raises(PersistenceError):
        journal.sync()

    journal.sync()  # no second exception; there is nothing left to report

    assert journal.failed is True


# --------------------------------------------------------------------------
# The disabled journal
# --------------------------------------------------------------------------


def test_the_null_journal_records_nothing_and_never_fails() -> None:
    NO_JOURNAL.append(encode_set(b"k", b"v", None))
    NO_JOURNAL.sync()
    NO_JOURNAL.close()

    assert NO_JOURNAL.failed is False

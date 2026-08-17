"""The in-memory keyspace: PyRedis' core key-value store.

Keys and values are `bytes` throughout, so any byte sequence -- including NUL
bytes and data that is not valid UTF-8 -- round-trips unchanged.

This module deliberately depends on nothing else in PyRedis and nothing in
`asyncio`. It knows nothing about RESP, sockets, configuration, or persistence;
it owns data semantics only. No method blocks or yields, so a single-threaded
caller gets atomic operations without locks.

Expiration deadlines are absolute Unix timestamps in integer milliseconds. They
are absolute so that a deadline keeps its meaning across a restart, which is
what a future append-only file has to persist; they are integers so boundary
comparisons and TTL arithmetic are exact. The cost of a wall clock is that a
system time jump moves every deadline with it.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable
from typing import Final, NamedTuple

#: Values are constrained to the signed 64-bit range Redis inherits from C.
#: Python integers are arbitrary-precision, so the bound is enforced here
#: explicitly rather than being a property of the machine word.
INT64_MIN: Final = -(2**63)
INT64_MAX: Final = 2**63 - 1

#: A canonical decimal integer: optional '-', then either a lone '0' or a
#: digit sequence with no leading zero. Rejects '+1', '01', '-0', whitespace,
#: and anything non-ASCII.
_INTEGER_RE: Final = re.compile(rb"0|-?[1-9][0-9]*")


def now_ms() -> int:
    """The production clock: Unix time in whole milliseconds."""
    return time.time_ns() // 1_000_000


class StoreError(Exception):
    """Base class for errors the store reports back to a client."""


class NotAnIntegerError(StoreError):
    """A value was needed as an integer but cannot be read as one."""

    def __init__(self) -> None:
        super().__init__("value is not an integer or out of range")


class IntegerOverflowError(StoreError):
    """The result of an arithmetic operation leaves the signed 64-bit range."""

    def __init__(self) -> None:
        super().__init__("increment or decrement would overflow")


class InvalidExpireError(StoreError):
    """A requested expiration is not usable.

    The caller supplies the command name in the client-facing message, since
    only it knows which command was being run.
    """

    def __init__(self) -> None:
        super().__init__("invalid expire time")


class TtlResult(NamedTuple):
    """The outcome of a TTL query, evaluated against a single clock reading.

    Both facts come back together because asking "does it exist?" and "when
    does it expire?" as two calls would read the clock twice, and a key that
    expired in between would be reported as living forever.
    """

    exists: bool
    remaining_ms: int | None  # None when the key has no expiration


class KeyValueStore:
    """A single PyRedis database: a flat mapping of byte keys to byte values."""

    __slots__ = ("_clock", "_data", "_expires", "_sweep_cursor")

    def __init__(self, clock: Callable[[], int] = now_ms) -> None:
        self._data: dict[bytes, bytes] = {}
        #: key -> absolute deadline in ms. Never holds a key absent from _data.
        self._expires: dict[bytes, int] = {}
        self._clock = clock
        #: Round-robin cursor over _expires, refilled once exhausted, so the
        #: active sweep covers every key without rescanning the keyspace each
        #: time it runs.
        self._sweep_cursor: list[bytes] = []

    def set(self, key: bytes, value: bytes, *, ttl_ms: int | None = None) -> None:
        """Store `value` under `key`, replacing any existing value.

        A plain `set` clears any deadline the key had. Passing `ttl_ms`
        installs a new one, replacing whatever was there.

        Raises:
            InvalidExpireError: `ttl_ms` is not positive, or the resulting
                deadline leaves the signed 64-bit range.
        """
        if ttl_ms is None:
            self._data[key] = value
            self._expires.pop(key, None)
            return

        deadline = self._deadline(ttl_ms)
        self._data[key] = value
        self._expires[key] = deadline

    def get(self, key: bytes) -> bytes | None:
        """Return the value stored under `key`, or `None` if it is not set.

        `None` is distinct from `b""`: a key explicitly set to the empty value
        exists and returns `b""`. An expired key reads as missing.
        """
        self._drop_if_expired(key)
        return self._data.get(key)

    def delete(self, *keys: bytes) -> int:
        """Remove `keys`, returning how many of them were actually present.

        Missing and expired keys are ignored rather than reported. A key
        repeated in `keys` counts once, because the later removals find
        nothing left. Deleting a key discards its deadline too.
        """
        removed = 0
        for key in keys:
            self._drop_if_expired(key)
            if key in self._data:
                self._remove(key)
                removed += 1
        return removed

    def exists(self, *keys: bytes) -> int:
        """Count how many of `keys` are set and unexpired.

        Repeats are counted every time, matching Redis: asking twice about one
        existing key answers 2.
        """
        present = 0
        for key in keys:
            self._drop_if_expired(key)
            if key in self._data:
                present += 1
        return present

    def incr(self, key: bytes) -> int:
        """Increment the integer at `key` by one and return the new value.

        A missing or expired key is treated as 0, so it is created holding 1.
        The result is written back in canonical decimal form regardless of how
        the previous value was spelled. An existing deadline is preserved.

        Raises:
            NotAnIntegerError: the stored value is not a canonical decimal
                integer within the signed 64-bit range.
            IntegerOverflowError: the result would leave that range.

        The stored value and its deadline are left untouched when either error
        is raised.
        """
        self._drop_if_expired(key)
        raw = self._data.get(key)
        current = 0 if raw is None else parse_int(raw)
        updated = current + 1
        if updated > INT64_MAX:
            raise IntegerOverflowError
        self._data[key] = str(updated).encode("ascii")
        return updated

    def expire(self, key: bytes, ttl_ms: int) -> bool:
        """Give `key` a deadline `ttl_ms` from now; return whether it applied.

        A non-positive `ttl_ms` deletes the key outright, as Redis does, and
        reports True if there was anything to delete. Returns False for a
        missing or already-expired key.

        Raises:
            InvalidExpireError: the resulting deadline leaves the signed
                64-bit range.
        """
        self._drop_if_expired(key)
        if key not in self._data:
            return False
        if ttl_ms <= 0:
            self._remove(key)
            return True
        self._expires[key] = self._deadline(ttl_ms)
        return True

    def ttl(self, key: bytes) -> TtlResult:
        """Report whether `key` exists and how long it has left."""
        now = self._clock()
        self._drop_if_expired(key, now)
        if key not in self._data:
            return TtlResult(exists=False, remaining_ms=None)
        deadline = self._expires.get(key)
        if deadline is None:
            return TtlResult(exists=True, remaining_ms=None)
        return TtlResult(exists=True, remaining_ms=deadline - now)

    def persist(self, key: bytes) -> bool:
        """Remove `key`'s deadline, keeping the key. True if one was removed."""
        self._drop_if_expired(key)
        if key not in self._data or key not in self._expires:
            return False
        del self._expires[key]
        return True

    def dbsize(self) -> int:
        """Return the number of live keys.

        Expired keys are collected first, so the count never includes a key
        that every other command would call missing. This deliberately differs
        from Redis, which reports keys it has not yet reclaimed. The cost is
        proportional to the number of keys carrying a deadline, not to the
        size of the keyspace.
        """
        for key in list(self._expires):
            self._drop_if_expired(key)
        return len(self._data)

    def flushdb(self) -> None:
        """Remove every key and every deadline. Safe on an empty store."""
        self._data.clear()
        self._expires.clear()
        self._sweep_cursor.clear()

    def sweep_expired(self, limit: int) -> int:
        """Examine at most `limit` keys with deadlines; drop the expired ones.

        Returns how many were removed. This is the active half of expiration:
        without it, a key that is written, given a deadline, and never read
        again would sit in memory forever. Work is bounded per call, and the
        cursor resumes where the previous call stopped, so repeated calls
        cover the whole keyspace without ever rescanning it in one go.

        Synchronous by design: the caller schedules it, and because it never
        yields, no command can observe a half-swept keyspace.
        """
        now = self._clock()
        removed = 0
        refilled = False
        for _ in range(limit):
            if not self._sweep_cursor:
                if refilled or not self._expires:
                    break
                self._sweep_cursor = list(self._expires)
                refilled = True
            key = self._sweep_cursor.pop()
            if key in self._expires and now >= self._expires[key]:
                self._remove(key)
                removed += 1
        return removed

    def _deadline(self, ttl_ms: int) -> int:
        if ttl_ms <= 0:
            raise InvalidExpireError
        deadline = self._clock() + ttl_ms
        if deadline > INT64_MAX:
            raise InvalidExpireError
        return deadline

    def _drop_if_expired(self, key: bytes, now: int | None = None) -> None:
        """Delete `key` if its deadline has arrived.

        A key is expired once the clock reaches its deadline: `now >= deadline`
        means gone, so at the exact boundary the key is already missing.
        """
        deadline = self._expires.get(key)
        if deadline is None:
            return
        if (self._clock() if now is None else now) >= deadline:
            self._remove(key)

    def _remove(self, key: bytes) -> None:
        del self._data[key]
        self._expires.pop(key, None)


def parse_int(value: bytes) -> int:
    """Read `value` as a canonical decimal signed 64-bit integer.

    Stricter than `int()`, which also accepts surrounding whitespace,
    underscore separators ("1_0"), a leading '+', and non-ASCII digits -- none
    of which Redis considers an integer. `fullmatch` rather than a '$' anchor,
    since '$' would also accept a trailing newline.
    """
    if _INTEGER_RE.fullmatch(value) is None:
        raise NotAnIntegerError
    parsed = int(value)
    if not INT64_MIN <= parsed <= INT64_MAX:
        raise NotAnIntegerError
    return parsed

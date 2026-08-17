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
from enum import StrEnum
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

#: What one key is modelled to cost beyond its bytes: two dictionary slots and
#: the headers of the key and value objects. A fixed, deliberately round
#: number -- `memory_used` is an accounting model of the keyspace, not a
#: measurement of the process.
ENTRY_OVERHEAD_BYTES: Final = 64

#: How many keys an eviction looks at before choosing the least recently used
#: of them. Sampling keeps eviction O(1)-ish; looking at everything would not.
MAXMEMORY_SAMPLES: Final = 5


class MaxmemoryPolicy(StrEnum):
    """What to do when a write would take the keyspace over `maxmemory`."""

    NOEVICTION = "noeviction"
    ALLKEYS_LRU = "allkeys-lru"


MAXMEMORY_POLICIES: Final = tuple(policy.value for policy in MaxmemoryPolicy)


def now_ms() -> int:
    """The production clock: Unix time in whole milliseconds."""
    return time.time_ns() // 1_000_000


class StoreError(Exception):
    """Base class for errors the store reports back to a client.

    `prefix` is the RESP error code the reply carries. Nearly everything is a
    plain `ERR`; running out of memory is the one thing Redis clients expect to
    recognise by its own code.
    """

    prefix = "ERR"


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


class OutOfMemoryError(StoreError):
    """The write cannot be admitted without exceeding `maxmemory`."""

    prefix = "OOM"

    def __init__(self) -> None:
        super().__init__("command not allowed when used memory > 'maxmemory'.")


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

    __slots__ = (
        "_access",
        "_clock",
        "_data",
        "_evict_cursor",
        "_evicted",
        "_expires",
        "_maxmemory",
        "_memory",
        "_policy",
        "_sweep_cursor",
        "_tick",
    )

    def __init__(
        self,
        clock: Callable[[], int] = now_ms,
        *,
        maxmemory: int = 0,
        policy: MaxmemoryPolicy = MaxmemoryPolicy.NOEVICTION,
    ) -> None:
        self._data: dict[bytes, bytes] = {}
        #: key -> absolute deadline in ms. Never holds a key absent from _data.
        self._expires: dict[bytes, int] = {}
        self._clock = clock
        #: Round-robin cursor over _expires, refilled once exhausted, so the
        #: active sweep covers every key without rescanning the keyspace each
        #: time it runs.
        self._sweep_cursor: list[bytes] = []

        #: 0 means unlimited.
        self._maxmemory = maxmemory
        self._policy = policy
        #: Maintained incrementally, so checking it never costs a scan.
        self._memory = 0
        #: key -> logical access counter. A counter rather than a timestamp:
        #: it orders accesses exactly, with no ties inside a clock tick and no
        #: dependence on the wall clock.
        self._access: dict[bytes, int] = {}
        self._tick = 0
        #: Round-robin cursor over the keyspace, for sampling eviction
        #: candidates without materialising every key each time.
        self._evict_cursor: list[bytes] = []
        #: Keys evicted by the most recent call, waiting to be reported.
        self._evicted: list[bytes] = []

    def set(self, key: bytes, value: bytes, *, ttl_ms: int | None = None) -> None:
        """Store `value` under `key`, replacing any existing value.

        A plain `set` clears any deadline the key had. Passing `ttl_ms`
        installs a new one, replacing whatever was there. A deadline costs
        nothing under the memory model, so a timed write is admitted on
        exactly the same terms as an untimed one.

        Raises:
            InvalidExpireError: `ttl_ms` is not positive, or the resulting
                deadline leaves the signed 64-bit range.
            OutOfMemoryError: the write cannot be admitted within `maxmemory`.
        """
        deadline = None if ttl_ms is None else self._deadline(ttl_ms)
        self._admit(key, value)
        self._write(key, value)
        if deadline is None:
            self._expires.pop(key, None)
        else:
            self._expires[key] = deadline
        self._touch(key)

    def get(self, key: bytes) -> bytes | None:
        """Return the value stored under `key`, or `None` if it is not set.

        `None` is distinct from `b""`: a key explicitly set to the empty value
        exists and returns `b""`. An expired key reads as missing.

        A hit counts as use, so it makes the key a later eviction candidate.
        """
        self._drop_if_expired(key)
        value = self._data.get(key)
        if value is not None:
            self._touch(key)
        return value

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
            OutOfMemoryError: the longer value cannot be admitted within
                `maxmemory`.

        The stored value and its deadline are left untouched when any of these
        is raised.
        """
        self._drop_if_expired(key)
        raw = self._data.get(key)
        current = 0 if raw is None else parse_int(raw)
        updated = current + 1
        if updated > INT64_MAX:
            raise IntegerOverflowError
        encoded = str(updated).encode("ascii")
        self._admit(key, encoded)
        self._write(key, encoded)
        self._touch(key)
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

    def deadline_ms(self, key: bytes) -> int | None:
        """The absolute deadline stored for `key`, if it has one.

        Deliberately does not evaluate expiry: callers use it immediately
        after a mutation to learn the deadline that was just installed.
        """
        return self._expires.get(key)

    def dbsize(self) -> int:
        """Return the number of live keys.

        Expired keys are collected first, so the count never includes a key
        that every other command would call missing. This deliberately differs
        from Redis, which reports keys it has not yet reclaimed. The cost is
        proportional to the number of keys carrying a deadline, not to the
        size of the keyspace.
        """
        self.drop_expired()
        return len(self._data)

    def drop_expired(self) -> int:
        """Remove every key whose deadline has passed; return how many."""
        now = self._clock()
        removed = 0
        for key in list(self._expires):
            if now >= self._expires[key]:
                self._remove(key)
                removed += 1
        return removed

    def flushdb(self) -> None:
        """Remove every key and every deadline. Safe on an empty store."""
        self._data.clear()
        self._expires.clear()
        self._sweep_cursor.clear()
        self._access.clear()
        self._evict_cursor.clear()
        self._memory = 0

    @property
    def memory_used(self) -> int:
        """Bytes the keyspace is modelled to occupy.

        A deterministic accounting model -- key bytes, value bytes, and a fixed
        per-key constant -- and deliberately not a measurement of the process.
        Deadlines are stored but not counted.
        """
        return self._memory

    def take_evicted(self) -> list[bytes]:
        """Return the keys evicted by the last call, and forget them.

        The caller records them; the store never touches the filesystem.
        """
        evicted = self._evicted
        self._evicted = []
        return evicted

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

    # -- Recovery primitives ------------------------------------------------
    #
    # Used only when replaying a persisted log. They are deliberately blind to
    # expiry: a replayed deadline may already be in the past, and evaluating it
    # mid-replay would drop a key that a later record still has something to
    # say about. One `drop_expired` pass after the replay settles that.

    def restore(self, key: bytes, value: bytes, *, deadline_ms: int | None = None) -> None:
        """Put `key` back exactly as recorded, deadline included.

        Recovery ignores `maxmemory`: a log that was written under one limit
        must load under another, and the first admitted write afterwards brings
        the keyspace back within bounds. It also does not count as use --
        replay must not invent a recency order that nobody actually created.
        """
        self._write(key, value)
        if deadline_ms is None:
            self._expires.pop(key, None)
        else:
            self._expires[key] = deadline_ms

    def set_deadline(self, key: bytes, deadline_ms: int) -> None:
        """Attach an absolute deadline to an existing key."""
        if key in self._data:
            self._expires[key] = deadline_ms

    def clear_deadline(self, key: bytes) -> None:
        """Drop `key`'s deadline, if it has one, without touching the value."""
        self._expires.pop(key, None)

    def discard(self, *keys: bytes) -> None:
        """Remove `keys` unconditionally, reporting nothing."""
        for key in keys:
            if key in self._data:
                self._remove(key)

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
        self._memory -= _cost(key, self._data.pop(key))
        self._expires.pop(key, None)
        self._access.pop(key, None)

    def _write(self, key: bytes, value: bytes) -> None:
        """Store a value, keeping the memory total exact."""
        previous = self._data.get(key)
        if previous is None:
            self._memory += _cost(key, value)
        else:
            self._memory += len(value) - len(previous)
        self._data[key] = value

    def _touch(self, key: bytes) -> None:
        """Mark `key` as just used, for eviction ordering."""
        self._tick += 1
        self._access[key] = self._tick

    # -- Memory limits ------------------------------------------------------

    def _admit(self, key: bytes, value: bytes) -> None:
        """Make room for `key` holding `value`, or refuse the write.

        Either this returns having freed enough, or it raises having changed
        nothing at all -- there is no half-evicted outcome. That holds because
        the one way eviction can fail is that the entry would not fit even in
        an empty keyspace, which is checked before a single key is removed.

        Raises:
            OutOfMemoryError: the write cannot be admitted.
        """
        if self._maxmemory == 0:
            return

        cost = _cost(key, value)
        if self._projected(key, cost) <= self._maxmemory:
            return
        if self._policy is MaxmemoryPolicy.NOEVICTION:
            raise OutOfMemoryError
        if cost > self._maxmemory:
            # Even an empty keyspace could not hold it; refuse before evicting.
            raise OutOfMemoryError

        # Reclaim what has merely expired before destroying anything live.
        self.drop_expired()
        while self._projected(key, cost) > self._maxmemory:
            victim = self._select_victim(key)
            if victim is None:  # pragma: no cover -- excluded by the check above
                raise OutOfMemoryError
            self._remove(victim)
            self._evicted.append(victim)

    def _projected(self, key: bytes, cost: int) -> int:
        """What `memory_used` would become if `key` came to cost `cost`."""
        current = _cost(key, self._data[key]) if key in self._data else 0
        return self._memory - current + cost

    def _select_victim(self, exclude: bytes) -> bytes | None:
        """The least recently used of a small sample, never `exclude`.

        Candidates come from a round-robin cursor rather than a random sample:
        drawing randomly would mean rebuilding the whole key list on every
        eviction, which is the keyspace scan sampling exists to avoid.
        """
        candidates: list[bytes] = []
        refilled = False
        while len(candidates) < MAXMEMORY_SAMPLES:
            if not self._evict_cursor:
                if refilled:
                    break
                self._evict_cursor = [key for key in self._data if key != exclude]
                refilled = True
                if not self._evict_cursor:
                    break
            candidate = self._evict_cursor.pop()
            if candidate != exclude and candidate in self._data:
                candidates.append(candidate)
        if not candidates:
            return None
        return min(candidates, key=lambda candidate: self._access.get(candidate, 0))


def _cost(key: bytes, value: bytes) -> int:
    """What one entry is modelled to occupy.

    Deadlines are stored outside this model on purpose: the intent is to
    approximate the cost of the keyspace payload, not to chase every Python
    allocation, so a timed write costs exactly what the same untimed write
    would.
    """
    return len(key) + len(value) + ENTRY_OVERHEAD_BYTES


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

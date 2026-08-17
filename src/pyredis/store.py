"""The in-memory keyspace: PyRedis' core key-value store.

Keys and values are `bytes` throughout, so any byte sequence -- including NUL
bytes and data that is not valid UTF-8 -- round-trips unchanged.

This module deliberately depends on nothing else in PyRedis and nothing in
`asyncio`. It knows nothing about RESP, sockets, configuration, or persistence;
it owns data semantics only. No method blocks or yields, so a single-threaded
caller gets atomic operations without locks.
"""

from __future__ import annotations

import re
from typing import Final

#: Values are constrained to the signed 64-bit range Redis inherits from C.
#: Python integers are arbitrary-precision, so the bound is enforced here
#: explicitly rather than being a property of the machine word.
INT64_MIN: Final = -(2**63)
INT64_MAX: Final = 2**63 - 1

#: A canonical decimal integer: optional '-', then either a lone '0' or a
#: digit sequence with no leading zero. Rejects '+1', '01', '-0', whitespace,
#: and anything non-ASCII.
_INTEGER_RE: Final = re.compile(rb"0|-?[1-9][0-9]*")


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


class KeyValueStore:
    """A single PyRedis database: a flat mapping of byte keys to byte values."""

    __slots__ = ("_data",)

    def __init__(self) -> None:
        self._data: dict[bytes, bytes] = {}

    def set(self, key: bytes, value: bytes) -> None:
        """Store `value` under `key`, replacing any existing value."""
        self._data[key] = value

    def get(self, key: bytes) -> bytes | None:
        """Return the value stored under `key`, or `None` if it is not set.

        `None` is distinct from `b""`: a key explicitly set to the empty value
        exists and returns `b""`.
        """
        return self._data.get(key)

    def delete(self, *keys: bytes) -> int:
        """Remove `keys`, returning how many of them were actually present.

        Missing keys are ignored rather than reported. A key repeated in
        `keys` counts once, because the later removals find nothing left.
        """
        removed = 0
        for key in keys:
            if key in self._data:
                del self._data[key]
                removed += 1
        return removed

    def exists(self, *keys: bytes) -> int:
        """Count how many of `keys` are set.

        Repeats are counted every time, matching Redis: asking twice about one
        existing key answers 2.
        """
        return sum(1 for key in keys if key in self._data)

    def incr(self, key: bytes) -> int:
        """Increment the integer at `key` by one and return the new value.

        A missing key is treated as 0, so it is created holding 1. The result
        is written back in canonical decimal form regardless of how the
        previous value was spelled.

        Raises:
            NotAnIntegerError: the stored value is not a canonical decimal
                integer within the signed 64-bit range.
            IntegerOverflowError: the result would leave that range.

        The stored value is left untouched when either error is raised.
        """
        raw = self._data.get(key)
        current = 0 if raw is None else _parse_int(raw)
        updated = current + 1
        if updated > INT64_MAX:
            raise IntegerOverflowError
        self._data[key] = str(updated).encode("ascii")
        return updated

    def dbsize(self) -> int:
        """Return the number of keys currently stored."""
        return len(self._data)

    def flushdb(self) -> None:
        """Remove every key. Safe to call on an empty store."""
        self._data.clear()


def _parse_int(value: bytes) -> int:
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

"""Command dispatch: a request of raw bytes in, an encoded RESP reply out.

The dispatcher owns command lookup, arity, and the translation of store errors
into RESP errors. It knows nothing about sockets, and the protocol layer knows
nothing about commands.

Arguments are never decoded: only the command name is uppercased, and only to
find the handler. `bytes.upper()` is ASCII-only by definition, so it cannot
corrupt a non-UTF-8 name.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Final

from pyredis import resp
from pyredis.store import KeyValueStore, StoreError

#: How much of an unknown command's arguments to quote back at the client.
_MAX_REPORTED_ARGS: Final = 20
_MAX_REPORTED_ARG_BYTES: Final = 128

_Handler = Callable[[KeyValueStore, list[bytes]], bytes]


@dataclass(frozen=True, slots=True)
class _Command:
    """One entry in the command table.

    `min_args`/`max_args` count arguments after the command name; `max_args` of
    None means unbounded. An explicit pair is used rather than Redis' signed
    `arity` int because that C-side trick cannot express an upper bound, which
    is why real Redis re-checks PING's argument count inside the handler.
    """

    name: str  # canonical lowercase, used only in error text
    handler: _Handler
    min_args: int
    max_args: int | None


def _ping(store: KeyValueStore, args: list[bytes]) -> bytes:
    if not args:
        return resp.PONG
    return resp.encode_bulk_string(args[0])


def _set(store: KeyValueStore, args: list[bytes]) -> bytes:
    store.set(args[0], args[1])
    return resp.OK


def _get(store: KeyValueStore, args: list[bytes]) -> bytes:
    value = store.get(args[0])
    if value is None:
        return resp.NULL_BULK_STRING
    return resp.encode_bulk_string(value)


def _delete(store: KeyValueStore, args: list[bytes]) -> bytes:
    return resp.encode_integer(store.delete(*args))


def _exists(store: KeyValueStore, args: list[bytes]) -> bytes:
    return resp.encode_integer(store.exists(*args))


def _incr(store: KeyValueStore, args: list[bytes]) -> bytes:
    return resp.encode_integer(store.incr(args[0]))


def _dbsize(store: KeyValueStore, args: list[bytes]) -> bytes:
    return resp.encode_integer(store.dbsize())


def _flushdb(store: KeyValueStore, args: list[bytes]) -> bytes:
    store.flushdb()
    return resp.OK


_COMMANDS: Final[dict[bytes, _Command]] = {
    b"PING": _Command("ping", _ping, 0, 1),
    b"SET": _Command("set", _set, 2, 2),
    b"GET": _Command("get", _get, 1, 1),
    b"DEL": _Command("del", _delete, 1, None),
    b"EXISTS": _Command("exists", _exists, 1, None),
    b"INCR": _Command("incr", _incr, 1, 1),
    b"DBSIZE": _Command("dbsize", _dbsize, 0, 0),
    b"FLUSHDB": _Command("flushdb", _flushdb, 0, 0),
}


def dispatch(store: KeyValueStore, request: list[bytes]) -> bytes:
    """Execute `request` against `store` and return the encoded RESP reply.

    `request` must be non-empty; the connection layer answers empty requests
    with silence and never calls here.

    Unknown commands and wrong arity are ordinary replies rather than
    exceptions -- they are outcomes on the reply path, not failures. Store
    errors become RESP errors and leave the connection usable.
    """
    name, *args = request
    command = _COMMANDS.get(name.upper())
    if command is None:
        return resp.encode_error(_unknown_command(name, args))
    if len(args) < command.min_args or (
        command.max_args is not None and len(args) > command.max_args
    ):
        return resp.encode_error(f"ERR wrong number of arguments for '{command.name}' command")

    try:
        return command.handler(store, args)
    except StoreError as exc:
        return resp.encode_error(f"ERR {exc}")


def _unknown_command(name: bytes, args: list[bytes]) -> str:
    quoted = "".join(f"'{_quote(arg)}', " for arg in args[:_MAX_REPORTED_ARGS])
    return f"ERR unknown command '{_quote(name)}', with args beginning with: {quoted}"


def _quote(raw: bytes) -> str:
    return resp.printable(raw[:_MAX_REPORTED_ARG_BYTES])

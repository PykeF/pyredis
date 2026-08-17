"""RESP2 framing: decoding client requests and encoding server replies.

This is the only module that knows the wire format. It reads directly from an
`asyncio.StreamReader`, which is already a correct incremental buffer: awaiting
it handles partial frames, coalesces fragments, and keeps whatever arrived
early for the next call. Pipelining therefore falls out of correct framing
rather than being a feature.

Requests are always an array of bulk strings; nothing else is accepted inbound.
Replies use simple strings, errors, integers, and bulk strings -- none of the
supported commands answers with an array, so no array encoder exists.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Final

CRLF: Final = b"\r\n"

#: Caps on what one request may claim, checked before anything is allocated.
#: These are deliberate PyRedis safety limits, not a claim of resource-limit
#: compatibility with real Redis, which permits far larger bulk strings.
#: Making them configurable is a P5 concern.
MAX_MULTIBULK_LENGTH: Final = 1024 * 1024
MAX_BULK_LENGTH: Final = 64 * 1024 * 1024

#: Fixed replies, encoded once.
OK: Final = b"+OK\r\n"
PONG: Final = b"+PONG\r\n"
NULL_BULK_STRING: Final = b"$-1\r\n"


class ProtocolError(Exception):
    """The byte stream does not hold a well-formed RESP2 request.

    A protocol error desynchronizes the connection: no later byte offset can be
    trusted, so the caller must reply and then close.
    """


async def read_command(reader: asyncio.StreamReader) -> list[bytes] | None:
    """Read one command from `reader`.

    Returns its arguments, or `None` when the peer disconnects cleanly at a
    frame boundary. Requests RESP treats as no-ops -- an empty or null
    multibulk -- yield an empty list, which the caller answers with silence.

    Raises:
        ProtocolError: the stream is not well-formed RESP2.
        ConnectionError: the peer vanished mid-frame.
    """
    header = await _read_line(reader, allow_eof=True)
    if header is None:
        return None
    if not header.startswith(b"*"):
        raise ProtocolError(f"expected '*', got '{printable(header[:1])}'")

    count = _parse_length(header[1:], "invalid multibulk length")
    if count > MAX_MULTIBULK_LENGTH:
        raise ProtocolError("invalid multibulk length")
    if count <= 0:
        # `*0` and `*-1` are discarded without a reply, as Redis does.
        return []

    return [await _read_bulk_string(reader) for _ in range(count)]


async def _read_bulk_string(reader: asyncio.StreamReader) -> bytes:
    header = await _read_line(reader)
    if header is None:  # pragma: no cover -- only _read_line(allow_eof=True) returns None
        raise _disconnected()
    if not header.startswith(b"$"):
        raise ProtocolError(f"expected '$', got '{printable(header[:1])}'")

    length = _parse_length(header[1:], "invalid bulk length")
    if length < 0 or length > MAX_BULK_LENGTH:
        raise ProtocolError("invalid bulk length")

    try:
        # Read the terminator too, so a payload containing CRLF survives intact
        # and a missing terminator is caught rather than silently absorbed.
        body = await reader.readexactly(length + len(CRLF))
    except asyncio.IncompleteReadError as exc:
        raise _disconnected() from exc
    if not body.endswith(CRLF):
        raise ProtocolError("invalid bulk length")
    return body[:length]


async def _read_line(reader: asyncio.StreamReader, *, allow_eof: bool = False) -> bytes | None:
    """Read one CRLF-terminated header line, without its terminator.

    Returns `None` only for a clean EOF before the first byte of a frame. The
    stream's own buffer limit bounds header lines; bulk payloads are read with
    `readexactly` instead and are bounded by MAX_BULK_LENGTH.
    """
    try:
        line = await reader.readuntil(CRLF)
    except asyncio.IncompleteReadError as exc:
        if allow_eof and not exc.partial:
            return None
        raise _disconnected() from exc
    except asyncio.LimitOverrunError as exc:
        raise ProtocolError("too big mbulk count string") from exc
    return line[: -len(CRLF)]


def _parse_length(raw: bytes, message: str) -> int:
    """Read a RESP length: an optional '-', then ASCII digits, nothing else."""
    digits = raw[1:] if raw.startswith(b"-") else raw
    if not digits or not digits.isdigit():
        raise ProtocolError(message)
    return int(raw)


def _disconnected() -> ConnectionError:
    return ConnectionResetError("peer disconnected mid-frame")


def encode_simple_string(value: bytes) -> bytes:
    return b"+" + value + CRLF


def encode_integer(value: int) -> bytes:
    return b":%d\r\n" % value


def encode_bulk_string(value: bytes) -> bytes:
    return b"$%d\r\n" % len(value) + value + CRLF


def encode_array(parts: Sequence[bytes]) -> bytes:
    """Encode an array of bulk strings.

    No reply uses this -- it exists so the append-only file can record
    commands in the same length-prefixed, binary-safe framing the wire uses.
    """
    encoded = [b"*%d\r\n" % len(parts)]
    encoded += [encode_bulk_string(part) for part in parts]
    return b"".join(encoded)


def encode_error(message: str) -> bytes:
    """Encode an error reply.

    CR and LF are replaced rather than trusted: an error embedding client bytes
    would otherwise let a client inject a counterfeit reply frame into the
    stream.
    """
    single_line = message.replace("\r", " ").replace("\n", " ")
    return b"-" + single_line.encode("utf-8", "replace") + CRLF


def printable(raw: bytes) -> str:
    """Render `raw` as ASCII-safe text for embedding in an error message."""
    return "".join(chr(b) if 0x20 <= b < 0x7F else f"\\x{b:02x}" for b in raw)

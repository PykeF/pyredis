from __future__ import annotations

import asyncio

import pytest

from pyredis.resp import (
    MAX_BULK_LENGTH,
    MAX_MULTIBULK_LENGTH,
    NULL_BULK_STRING,
    OK,
    PONG,
    ProtocolError,
    encode_bulk_string,
    encode_error,
    encode_integer,
    encode_simple_string,
    printable,
    read_command,
)

PING = b"*1\r\n$4\r\nPING\r\n"
SET_NAME = b"*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$4\r\nPyke\r\n"


def make_reader(*chunks: bytes, eof: bool = True) -> asyncio.StreamReader:
    """A StreamReader preloaded with `chunks`, as if they had already arrived."""
    reader = asyncio.StreamReader()
    for chunk in chunks:
        reader.feed_data(chunk)
    if eof:
        reader.feed_eof()
    return reader


# --------------------------------------------------------------------------
# Parsing: well-formed input
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reads_a_command_as_a_list_of_bytes() -> None:
    assert await read_command(make_reader(SET_NAME)) == [b"SET", b"name", b"Pyke"]


@pytest.mark.asyncio
async def test_returns_none_on_a_clean_eof() -> None:
    assert await read_command(make_reader()) is None


@pytest.mark.asyncio
async def test_reads_two_commands_from_one_buffer() -> None:
    reader = make_reader(PING + SET_NAME)

    assert await read_command(reader) == [b"PING"]
    assert await read_command(reader) == [b"SET", b"name", b"Pyke"]
    assert await read_command(reader) is None


@pytest.mark.asyncio
async def test_reassembles_a_command_delivered_one_byte_at_a_time() -> None:
    reader = asyncio.StreamReader()
    parsing = asyncio.ensure_future(read_command(reader))

    for index in range(len(SET_NAME)):
        assert not parsing.done(), "parser finished before the frame was complete"
        reader.feed_data(SET_NAME[index : index + 1])
        await asyncio.sleep(0)

    assert await asyncio.wait_for(parsing, timeout=1) == [b"SET", b"name", b"Pyke"]


@pytest.mark.asyncio
async def test_bulk_payload_may_contain_crlf_and_nul_bytes() -> None:
    payload = b"a\r\n\x00b"
    frame = b"*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n" % (len(payload), payload)

    assert await read_command(make_reader(frame)) == [b"GET", payload]


@pytest.mark.asyncio
async def test_empty_bulk_string_is_preserved() -> None:
    assert await read_command(make_reader(b"*2\r\n$3\r\nGET\r\n$0\r\n\r\n")) == [b"GET", b""]


@pytest.mark.parametrize("frame", [b"*0\r\n", b"*-1\r\n"])
@pytest.mark.asyncio
async def test_empty_and_null_multibulk_are_no_ops(frame: bytes) -> None:
    assert await read_command(make_reader(frame)) == []


# --------------------------------------------------------------------------
# Parsing: malformed input
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rejects_a_request_that_is_not_an_array() -> None:
    with pytest.raises(ProtocolError, match=r"expected '\*', got '\+'"):
        await read_command(make_reader(b"+PING\r\n"))


@pytest.mark.asyncio
async def test_rejects_a_nested_array() -> None:
    with pytest.raises(ProtocolError, match=r"expected '\$', got '\*'"):
        await read_command(make_reader(b"*1\r\n*1\r\n$4\r\nPING\r\n"))


@pytest.mark.asyncio
async def test_rejects_an_inline_command() -> None:
    # Telnet-style inline commands are deliberately unsupported.
    with pytest.raises(ProtocolError, match=r"expected '\*', got 'P'"):
        await read_command(make_reader(b"PING\r\n"))


@pytest.mark.parametrize(
    "frame",
    [
        pytest.param(b"*abc\r\n", id="letters"),
        pytest.param(b"*\r\n", id="empty"),
        pytest.param(b"*1.5\r\n", id="decimal"),
        pytest.param(b"*+1\r\n", id="explicit-plus"),
        pytest.param(b"* 1\r\n", id="leading-space"),
    ],
)
@pytest.mark.asyncio
async def test_rejects_a_malformed_multibulk_length(frame: bytes) -> None:
    with pytest.raises(ProtocolError, match="invalid multibulk length"):
        await read_command(make_reader(frame))


@pytest.mark.asyncio
async def test_rejects_an_oversized_multibulk_length() -> None:
    frame = b"*%d\r\n" % (MAX_MULTIBULK_LENGTH + 1)

    with pytest.raises(ProtocolError, match="invalid multibulk length"):
        await read_command(make_reader(frame))


@pytest.mark.parametrize(
    "frame",
    [
        pytest.param(b"*1\r\n$abc\r\n", id="letters"),
        pytest.param(b"*1\r\n$\r\n", id="empty"),
        pytest.param(b"*1\r\n$-1\r\n", id="null-bulk-in-request"),
        pytest.param(b"*1\r\n$-5\r\n", id="negative"),
    ],
)
@pytest.mark.asyncio
async def test_rejects_a_malformed_bulk_length(frame: bytes) -> None:
    with pytest.raises(ProtocolError, match="invalid bulk length"):
        await read_command(make_reader(frame))


@pytest.mark.asyncio
async def test_rejects_an_oversized_bulk_length() -> None:
    frame = b"*1\r\n$%d\r\n" % (MAX_BULK_LENGTH + 1)

    with pytest.raises(ProtocolError, match="invalid bulk length"):
        await read_command(make_reader(frame))


@pytest.mark.asyncio
async def test_rejects_a_bulk_string_not_terminated_by_crlf() -> None:
    with pytest.raises(ProtocolError, match="invalid bulk length"):
        await read_command(make_reader(b"*1\r\n$2\r\nabXX"))


@pytest.mark.asyncio
async def test_rejects_a_header_line_beyond_the_stream_limit() -> None:
    with pytest.raises(ProtocolError, match="too big mbulk count string"):
        await read_command(make_reader(b"*" + b"1" * 100_000))


@pytest.mark.parametrize(
    "frame",
    [
        pytest.param(b"*1\r\n", id="after-multibulk-header"),
        pytest.param(b"*1\r\n$4\r\n", id="after-bulk-header"),
        pytest.param(b"*1\r\n$4\r\nPI", id="mid-payload"),
        pytest.param(b"*2\r\n$4\r\nPING\r\n", id="between-elements"),
        pytest.param(b"*1", id="mid-header-line"),
    ],
)
@pytest.mark.asyncio
async def test_premature_eof_is_reported_as_a_lost_connection(frame: bytes) -> None:
    with pytest.raises(ConnectionError):
        await read_command(make_reader(frame))


# --------------------------------------------------------------------------
# Encoding
# --------------------------------------------------------------------------


def test_fixed_replies_are_correctly_encoded() -> None:
    assert OK == b"+OK\r\n"
    assert PONG == b"+PONG\r\n"
    assert NULL_BULK_STRING == b"$-1\r\n"


def test_encodes_a_simple_string() -> None:
    assert encode_simple_string(b"OK") == b"+OK\r\n"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, b":0\r\n"),
        (1, b":1\r\n"),
        (-1, b":-1\r\n"),
        (9223372036854775807, b":9223372036854775807\r\n"),
    ],
)
def test_encodes_an_integer(value: int, expected: bytes) -> None:
    assert encode_integer(value) == expected


def test_encodes_a_bulk_string() -> None:
    assert encode_bulk_string(b"hello") == b"$5\r\nhello\r\n"


def test_encodes_an_empty_bulk_string() -> None:
    assert encode_bulk_string(b"") == b"$0\r\n\r\n"


def test_bulk_string_length_counts_bytes_not_characters() -> None:
    # Two bytes, one character: a str-based store would send the wrong length.
    assert encode_bulk_string(b"\xc3\xa9") == b"$2\r\n\xc3\xa9\r\n"


def test_encodes_a_binary_bulk_string_verbatim() -> None:
    payload = b"\x00\xff\r\n\xfe"

    assert encode_bulk_string(payload) == b"$5\r\n" + payload + b"\r\n"


def test_encodes_an_error() -> None:
    assert encode_error("ERR unknown command 'FOO'") == b"-ERR unknown command 'FOO'\r\n"


def test_error_text_cannot_inject_a_second_frame() -> None:
    encoded = encode_error("ERR bad\r\n+INJECTED")

    assert encoded == b"-ERR bad  +INJECTED\r\n"
    assert encoded.count(b"\r\n") == 1


def test_printable_escapes_bytes_that_are_not_printable_ascii() -> None:
    assert printable(b"ok") == "ok"
    assert printable(b"\x00\xff\r\n") == "\\x00\\xff\\x0d\\x0a"

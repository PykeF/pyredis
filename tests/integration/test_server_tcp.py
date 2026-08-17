"""End-to-end tests over a real TCP socket against a real listener."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import suppress
from typing import Any

import pytest
import pytest_asyncio

from pyredis.config import Config
from pyredis.server import Server

TIMEOUT = 5


class RespClient:
    """A minimal RESP2 client that reports replies as raw wire frames."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer

    @classmethod
    async def connect(cls, port: int) -> RespClient:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        return cls(reader, writer)

    async def send(self, *args: bytes) -> None:
        await self.send_raw(encode_command(*args))

    async def send_raw(self, data: bytes) -> None:
        self._writer.write(data)
        await self._writer.drain()

    async def read_reply(self) -> bytes:
        line = await asyncio.wait_for(self._reader.readuntil(b"\r\n"), timeout=TIMEOUT)
        if not line.startswith(b"$"):
            return line
        length = int(line[1:-2])
        if length == -1:
            return line
        body = await asyncio.wait_for(
            self._reader.readexactly(length + 2), timeout=TIMEOUT
        )
        return line + body

    async def command(self, *args: bytes) -> bytes:
        await self.send(*args)
        return await self.read_reply()

    async def read_eof(self) -> bytes:
        return await asyncio.wait_for(self._reader.read(), timeout=TIMEOUT)

    async def close(self) -> None:
        self._writer.close()
        with suppress(ConnectionError):
            await self._writer.wait_closed()

    def abort(self) -> None:
        """Drop the connection without a graceful close."""
        self._writer.transport.abort()


def encode_command(*args: bytes) -> bytes:
    parts = [b"*%d\r\n" % len(args)]
    parts += [b"$%d\r\n%s\r\n" % (len(arg), arg) for arg in args]
    return b"".join(parts)


Connect = Callable[[], Coroutine[Any, Any, RespClient]]


@pytest_asyncio.fixture
async def server() -> AsyncIterator[Server]:
    instance = Server(Config(host="127.0.0.1", port=0))
    task = asyncio.ensure_future(instance.serve())
    await asyncio.wait_for(instance.ready.wait(), timeout=TIMEOUT)
    try:
        yield instance
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


@pytest_asyncio.fixture
async def connect(server: Server) -> AsyncIterator[Connect]:
    clients: list[RespClient] = []

    async def factory() -> RespClient:
        client = await RespClient.connect(server.port)
        clients.append(client)
        return client

    try:
        yield factory
    finally:
        for client in clients:
            await client.close()


@pytest_asyncio.fixture
async def client(connect: Connect) -> RespClient:
    return await connect()


# --------------------------------------------------------------------------
# Commands over the wire
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ping(client: RespClient) -> None:
    assert await client.command(b"PING") == b"+PONG\r\n"


@pytest.mark.asyncio
async def test_ping_with_a_message(client: RespClient) -> None:
    assert await client.command(b"PING", b"hello") == b"$5\r\nhello\r\n"


@pytest.mark.asyncio
async def test_set_then_get(client: RespClient) -> None:
    assert await client.command(b"SET", b"name", b"Pyke") == b"+OK\r\n"
    assert await client.command(b"GET", b"name") == b"$4\r\nPyke\r\n"


@pytest.mark.asyncio
async def test_get_missing_key_is_a_null_bulk_string(client: RespClient) -> None:
    assert await client.command(b"GET", b"missing") == b"$-1\r\n"


@pytest.mark.asyncio
async def test_binary_keys_and_values_survive_the_round_trip(client: RespClient) -> None:
    key = b"\x00\xff key\r\nwith\x00nul"
    value = b"\x89PNG\r\n\x1a\n\x00\xfe\xff binary"

    assert await client.command(b"SET", key, value) == b"+OK\r\n"
    assert await client.command(b"GET", key) == b"$%d\r\n%s\r\n" % (len(value), value)


@pytest.mark.asyncio
async def test_del_exists_dbsize_and_flushdb(client: RespClient) -> None:
    await client.command(b"SET", b"a", b"1")
    await client.command(b"SET", b"b", b"2")

    assert await client.command(b"DBSIZE") == b":2\r\n"
    assert await client.command(b"EXISTS", b"a", b"a", b"missing") == b":2\r\n"
    assert await client.command(b"DEL", b"a", b"missing") == b":1\r\n"
    assert await client.command(b"DBSIZE") == b":1\r\n"
    assert await client.command(b"FLUSHDB") == b"+OK\r\n"
    assert await client.command(b"DBSIZE") == b":0\r\n"


@pytest.mark.asyncio
async def test_incr(client: RespClient) -> None:
    assert await client.command(b"INCR", b"counter") == b":1\r\n"
    assert await client.command(b"INCR", b"counter") == b":2\r\n"
    assert await client.command(b"GET", b"counter") == b"$1\r\n2\r\n"


@pytest.mark.asyncio
async def test_many_sequential_commands_on_one_connection(client: RespClient) -> None:
    for expected in range(1, 21):
        assert await client.command(b"INCR", b"counter") == b":%d\r\n" % expected


# --------------------------------------------------------------------------
# Stream handling
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pipelined_commands_in_one_write_get_ordered_replies(
    client: RespClient,
) -> None:
    await client.send_raw(
        encode_command(b"SET", b"k", b"v")
        + encode_command(b"GET", b"k")
        + encode_command(b"DBSIZE")
    )

    assert await client.read_reply() == b"+OK\r\n"
    assert await client.read_reply() == b"$1\r\nv\r\n"
    assert await client.read_reply() == b":1\r\n"


@pytest.mark.asyncio
async def test_a_command_split_across_writes_is_reassembled(client: RespClient) -> None:
    frame = encode_command(b"SET", b"name", b"Pyke")
    await client.send_raw(frame[:7])
    await asyncio.sleep(0.05)
    await client.send_raw(frame[7:15])
    await asyncio.sleep(0.05)
    await client.send_raw(frame[15:])

    assert await client.read_reply() == b"+OK\r\n"


@pytest.mark.asyncio
async def test_an_empty_multibulk_gets_no_reply(client: RespClient) -> None:
    await client.send_raw(b"*0\r\n" + encode_command(b"PING"))

    assert await client.read_reply() == b"+PONG\r\n"


# --------------------------------------------------------------------------
# Errors
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unknown_command_errors_but_keeps_the_connection_usable(
    client: RespClient,
) -> None:
    # redis-cli sends COMMAND DOCS on connect; the session must survive it.
    reply = await client.command(b"COMMAND", b"DOCS")

    assert reply.startswith(b"-ERR unknown command 'COMMAND'")
    assert await client.command(b"PING") == b"+PONG\r\n"


@pytest.mark.asyncio
async def test_wrong_arity_errors_but_keeps_the_connection_usable(
    client: RespClient,
) -> None:
    reply = await client.command(b"SET", b"only-a-key")

    assert reply == b"-ERR wrong number of arguments for 'set' command\r\n"
    assert await client.command(b"PING") == b"+PONG\r\n"


@pytest.mark.asyncio
async def test_store_error_keeps_the_connection_usable(client: RespClient) -> None:
    await client.command(b"SET", b"key", b"abc")

    assert await client.command(b"INCR", b"key") == (
        b"-ERR value is not an integer or out of range\r\n"
    )
    assert await client.command(b"GET", b"key") == b"$3\r\nabc\r\n"


@pytest.mark.asyncio
async def test_protocol_error_replies_then_closes_the_connection(
    client: RespClient,
) -> None:
    await client.send_raw(b"+PING\r\n")

    assert await client.read_reply() == b"-ERR Protocol error: expected '*', got '+'\r\n"
    assert await client.read_eof() == b""


@pytest.mark.asyncio
async def test_oversized_bulk_length_is_refused_without_allocating(
    client: RespClient,
) -> None:
    await client.send_raw(b"*1\r\n$99999999999\r\n")

    assert await client.read_reply() == b"-ERR Protocol error: invalid bulk length\r\n"


# --------------------------------------------------------------------------
# Connection lifecycle
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_clients_share_one_keyspace(connect: Connect) -> None:
    writer_client = await connect()
    reader_client = await connect()

    await writer_client.command(b"SET", b"shared", b"value")

    assert await reader_client.command(b"GET", b"shared") == b"$5\r\nvalue\r\n"


@pytest.mark.asyncio
async def test_commands_from_many_clients_do_not_interleave(connect: Connect) -> None:
    clients = [await connect() for _ in range(10)]

    async def increment(target: RespClient) -> None:
        for _ in range(20):
            await target.command(b"INCR", b"counter")

    await asyncio.gather(*(increment(target) for target in clients))

    assert await clients[0].command(b"GET", b"counter") == b"$3\r\n200\r\n"


@pytest.mark.asyncio
async def test_an_abrupt_disconnect_leaves_the_server_healthy(connect: Connect) -> None:
    doomed = await connect()
    await doomed.send_raw(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n")  # frame left unfinished
    doomed.abort()
    await asyncio.sleep(0.05)

    survivor = await connect()
    assert await survivor.command(b"PING") == b"+PONG\r\n"


@pytest.mark.asyncio
async def test_a_client_disconnecting_cleanly_does_not_disturb_others(
    connect: Connect,
) -> None:
    leaving = await connect()
    staying = await connect()
    await leaving.command(b"SET", b"key", b"value")
    await leaving.close()

    assert await staying.command(b"GET", b"key") == b"$5\r\nvalue\r\n"


@pytest.mark.asyncio
async def test_shutdown_closes_live_connections() -> None:
    # Owns its own server so the shutdown itself is what is under test.
    instance = Server(Config(host="127.0.0.1", port=0))
    task = asyncio.ensure_future(instance.serve())
    await asyncio.wait_for(instance.ready.wait(), timeout=TIMEOUT)

    live = await RespClient.connect(instance.port)
    assert await live.command(b"PING") == b"+PONG\r\n"

    task.cancel()
    with suppress(asyncio.CancelledError):
        await task

    assert await live.read_eof() == b""
    await live.close()

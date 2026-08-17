"""Restart and recovery over real TCP, against a real append-only file."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any

import pytest

from pyredis import server as server_module
from pyredis.aof import FsyncPolicy, encode_record, encode_set
from pyredis.config import Config
from pyredis.server import Server
from pyredis.store import ENTRY_OVERHEAD_BYTES, MaxmemoryPolicy

TIMEOUT = 5


class Client:
    """A minimal RESP2 client returning replies as raw wire frames."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer

    async def command(self, *args: bytes) -> bytes:
        frame = [b"*%d\r\n" % len(args)]
        frame += [b"$%d\r\n%s\r\n" % (len(arg), arg) for arg in args]
        self._writer.write(b"".join(frame))
        await self._writer.drain()

        line = await asyncio.wait_for(self._reader.readuntil(b"\r\n"), timeout=TIMEOUT)
        if not line.startswith(b"$"):
            return line
        length = int(line[1:-2])
        if length == -1:
            return line
        body = await asyncio.wait_for(self._reader.readexactly(length + 2), timeout=TIMEOUT)
        return line + body

    async def close(self) -> None:
        self._writer.close()
        with suppress(ConnectionError):
            await self._writer.wait_closed()


Session = Callable[[], Coroutine[Any, Any, Client]]


@asynccontextmanager
async def running(config: Config) -> AsyncIterator[Session]:
    """Run a server for the duration of the block, then shut it down fully."""
    server = Server(config)
    task = asyncio.ensure_future(server.serve())
    await asyncio.wait_for(server.ready.wait(), timeout=TIMEOUT)
    clients: list[Client] = []

    async def connect() -> Client:
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        client = Client(reader, writer)
        clients.append(client)
        return client

    try:
        yield connect
    finally:
        for client in clients:
            await client.close()
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


def persistent(tmp_path: Path, **overrides: Any) -> Config:
    settings: dict[str, Any] = {
        "host": "127.0.0.1",
        "port": 0,
        "aof_enabled": True,
        "aof_path": str(tmp_path / "pyredis.aof"),
        "aof_fsync": FsyncPolicy.ALWAYS,
    }
    return Config(**{**settings, **overrides})


# --------------------------------------------------------------------------
# Recovery of values
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_write_survives_a_restart(tmp_path: Path) -> None:
    config = persistent(tmp_path)

    async with running(config) as connect:
        client = await connect()
        assert await client.command(b"SET", b"name", b"Pyke") == b"+OK\r\n"

    async with running(config) as connect:
        client = await connect()
        assert await client.command(b"GET", b"name") == b"$4\r\nPyke\r\n"


@pytest.mark.asyncio
async def test_only_the_final_value_survives(tmp_path: Path) -> None:
    config = persistent(tmp_path)

    async with running(config) as connect:
        client = await connect()
        await client.command(b"SET", b"k", b"first")
        await client.command(b"SET", b"k", b"second")

    async with running(config) as connect:
        client = await connect()
        assert await client.command(b"GET", b"k") == b"$6\r\nsecond\r\n"


@pytest.mark.asyncio
async def test_a_delete_survives_a_restart(tmp_path: Path) -> None:
    config = persistent(tmp_path)

    async with running(config) as connect:
        client = await connect()
        await client.command(b"SET", b"gone", b"value")
        await client.command(b"SET", b"kept", b"value")
        assert await client.command(b"DEL", b"gone") == b":1\r\n"

    async with running(config) as connect:
        client = await connect()
        assert await client.command(b"GET", b"gone") == b"$-1\r\n"
        assert await client.command(b"GET", b"kept") == b"$5\r\nvalue\r\n"
        assert await client.command(b"DBSIZE") == b":1\r\n"


@pytest.mark.asyncio
async def test_an_increment_survives_a_restart(tmp_path: Path) -> None:
    config = persistent(tmp_path)

    async with running(config) as connect:
        client = await connect()
        for _ in range(5):
            await client.command(b"INCR", b"counter")

    async with running(config) as connect:
        client = await connect()
        assert await client.command(b"GET", b"counter") == b"$1\r\n5\r\n"
        # Counting continues from the recovered value, not from zero.
        assert await client.command(b"INCR", b"counter") == b":6\r\n"


@pytest.mark.asyncio
async def test_binary_keys_and_values_survive_a_restart(tmp_path: Path) -> None:
    config = persistent(tmp_path)
    key = b"\x00\xff key\r\nwith\x00nul"
    value = b"\x89PNG\r\n\x1a\n\x00\xfe\xff binary"

    async with running(config) as connect:
        client = await connect()
        await client.command(b"SET", key, value)

    async with running(config) as connect:
        client = await connect()
        assert await client.command(b"GET", key) == b"$%d\r\n%s\r\n" % (len(value), value)


@pytest.mark.asyncio
async def test_flushdb_survives_a_restart(tmp_path: Path) -> None:
    config = persistent(tmp_path)

    async with running(config) as connect:
        client = await connect()
        await client.command(b"SET", b"a", b"1")
        await client.command(b"SET", b"b", b"2")
        await client.command(b"FLUSHDB")
        await client.command(b"SET", b"c", b"3")

    async with running(config) as connect:
        client = await connect()
        assert await client.command(b"DBSIZE") == b":1\r\n"
        assert await client.command(b"GET", b"c") == b"$1\r\n3\r\n"


# --------------------------------------------------------------------------
# Recovery of expiration
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_ttl_does_not_restart_at_full_length(tmp_path: Path) -> None:
    # The headline requirement: a key written with EX 60 must come back with
    # whatever is left of that minute, not with a fresh one.
    config = persistent(tmp_path)

    async with running(config) as connect:
        client = await connect()
        await client.command(b"SET", b"session", b"token", b"EX", b"60")

    # Rewrite the recorded deadline as though 45 seconds had passed, which is
    # exact and instant where sleeping would be neither.
    path = Path(config.aof_path)
    data = path.read_bytes()
    original = int(data.split(b"PXAT\r\n$13\r\n")[1][:13])
    path.write_bytes(data.replace(str(original).encode(), str(original - 45_000).encode()))

    async with running(config) as connect:
        client = await connect()
        assert await client.command(b"TTL", b"session") == b":15\r\n"
        assert await client.command(b"GET", b"session") == b"$5\r\ntoken\r\n"


@pytest.mark.asyncio
async def test_a_key_that_expired_while_down_is_gone(tmp_path: Path) -> None:
    config = persistent(tmp_path)

    async with running(config) as connect:
        client = await connect()
        await client.command(b"SET", b"brief", b"value", b"PX", b"50")
        await client.command(b"SET", b"lasting", b"value")

    await asyncio.sleep(0.1)  # the deadline passes while nothing is running

    async with running(config) as connect:
        client = await connect()
        assert await client.command(b"GET", b"brief") == b"$-1\r\n"
        assert await client.command(b"TTL", b"brief") == b":-2\r\n"
        assert await client.command(b"GET", b"lasting") == b"$5\r\nvalue\r\n"
        assert await client.command(b"DBSIZE") == b":1\r\n"


@pytest.mark.asyncio
async def test_expire_survives_a_restart(tmp_path: Path) -> None:
    config = persistent(tmp_path)

    async with running(config) as connect:
        client = await connect()
        await client.command(b"SET", b"k", b"v")
        await client.command(b"EXPIRE", b"k", b"600")

    async with running(config) as connect:
        client = await connect()
        assert await client.command(b"TTL", b"k") == b":600\r\n"


@pytest.mark.asyncio
async def test_persist_survives_a_restart(tmp_path: Path) -> None:
    config = persistent(tmp_path)

    async with running(config) as connect:
        client = await connect()
        await client.command(b"SET", b"k", b"v", b"PX", b"50")
        assert await client.command(b"PERSIST", b"k") == b":1\r\n"

    await asyncio.sleep(0.1)  # the original deadline comes and goes

    async with running(config) as connect:
        client = await connect()
        assert await client.command(b"GET", b"k") == b"$1\r\nv\r\n"
        assert await client.command(b"TTL", b"k") == b":-1\r\n"


@pytest.mark.asyncio
async def test_an_increment_keeps_its_ttl_across_a_restart(tmp_path: Path) -> None:
    config = persistent(tmp_path)

    async with running(config) as connect:
        client = await connect()
        await client.command(b"SET", b"counter", b"1", b"EX", b"600")
        await client.command(b"INCR", b"counter")

    async with running(config) as connect:
        client = await connect()
        assert await client.command(b"GET", b"counter") == b"$1\r\n2\r\n"
        assert await client.command(b"TTL", b"counter") == b":600\r\n"


# --------------------------------------------------------------------------
# Damaged files
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_torn_final_record_is_repaired_on_startup(tmp_path: Path) -> None:
    config = persistent(tmp_path)

    async with running(config) as connect:
        client = await connect()
        await client.command(b"SET", b"whole", b"value")

    path = Path(config.aof_path)
    path.write_bytes(path.read_bytes() + encode_set(b"torn", b"value", None)[:11])

    async with running(config) as connect:
        client = await connect()
        assert await client.command(b"GET", b"whole") == b"$5\r\nvalue\r\n"
        assert await client.command(b"GET", b"torn") == b"$-1\r\n"
        # The repaired file is usable again.
        await client.command(b"SET", b"after", b"value")

    async with running(config) as connect:
        client = await connect()
        assert await client.command(b"GET", b"after") == b"$5\r\nvalue\r\n"


@pytest.mark.asyncio
async def test_corruption_in_the_middle_stops_the_server_starting(tmp_path: Path) -> None:
    config = persistent(tmp_path)
    path = Path(config.aof_path)
    path.write_bytes(
        encode_set(b"a", b"1", None) + b"+GARBAGE\r\n" + encode_record(b"FLUSHDB")
    )

    server = Server(config)

    with pytest.raises(Exception, match="offset"):
        await server.serve()


# --------------------------------------------------------------------------
# Persistence switched off
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_nothing_is_written_when_the_aof_is_disabled(tmp_path: Path) -> None:
    path = tmp_path / "pyredis.aof"
    config = Config(host="127.0.0.1", port=0, aof_path=str(path))

    async with running(config) as connect:
        client = await connect()
        await client.command(b"SET", b"k", b"v")
        await client.command(b"INCR", b"c")

    assert not path.exists()

    async with running(config) as connect:
        client = await connect()
        assert await client.command(b"GET", b"k") == b"$-1\r\n"


# --------------------------------------------------------------------------
# Persistence failure at runtime
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_everysec_fsync_failure_stops_accepting_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server_module, "FSYNC_INTERVAL_SECONDS", 0.05)
    config = persistent(tmp_path, aof_fsync=FsyncPolicy.EVERYSEC)

    async with running(config) as connect:
        client = await connect()
        assert await client.command(b"SET", b"before", b"value") == b"+OK\r\n"

        def broken_fsync(fd: int) -> None:
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(os, "fsync", broken_fsync)
        await asyncio.sleep(0.3)  # several fsync cycles

        # The background failure is enough to close the door on writes.
        assert await client.command(b"SET", b"after", b"value") == (
            b"-ERR persistence failure\r\n"
        )
        assert await client.command(b"INCR", b"c") == b"-ERR persistence failure\r\n"
        assert await client.command(b"FLUSHDB") == b"-ERR persistence failure\r\n"

        # Reads carry on: what was already served is still true.
        assert await client.command(b"GET", b"before") == b"$5\r\nvalue\r\n"
        assert await client.command(b"EXISTS", b"before") == b":1\r\n"
        assert await client.command(b"TTL", b"before") == b":-1\r\n"
        assert await client.command(b"DBSIZE") == b":1\r\n"
        assert await client.command(b"PING") == b"+PONG\r\n"

        monkeypatch.setattr(os, "fsync", os.fsync)


# --------------------------------------------------------------------------
# Eviction
# --------------------------------------------------------------------------


ENTRY = 6 + 5 + ENTRY_OVERHEAD_BYTES  # a six-byte key holding a five-byte value


@pytest.mark.asyncio
async def test_noeviction_refuses_a_write_over_the_limit(tmp_path: Path) -> None:
    config = persistent(tmp_path, maxmemory=ENTRY)

    async with running(config) as connect:
        client = await connect()
        assert await client.command(b"SET", b"first_", b"value") == b"+OK\r\n"

        assert await client.command(b"SET", b"second", b"value") == (
            b"-OOM command not allowed when used memory > 'maxmemory'.\r\n"
        )
        # Nothing changed, and the connection is still perfectly usable.
        assert await client.command(b"DBSIZE") == b":1\r\n"
        assert await client.command(b"GET", b"first_") == b"$5\r\nvalue\r\n"
        assert await client.command(b"PING") == b"+PONG\r\n"


@pytest.mark.asyncio
async def test_allkeys_lru_evicts_instead_of_refusing(tmp_path: Path) -> None:
    config = persistent(
        tmp_path, maxmemory=2 * ENTRY, maxmemory_policy=MaxmemoryPolicy.ALLKEYS_LRU
    )

    async with running(config) as connect:
        client = await connect()
        await client.command(b"SET", b"first_", b"value")
        await client.command(b"SET", b"second", b"value")

        assert await client.command(b"SET", b"third_", b"value") == b"+OK\r\n"
        assert await client.command(b"DBSIZE") == b":2\r\n"
        assert await client.command(b"GET", b"first_") == b"$-1\r\n"
        assert await client.command(b"GET", b"third_") == b"$5\r\nvalue\r\n"


@pytest.mark.asyncio
async def test_reading_a_key_changes_the_eviction_victim(tmp_path: Path) -> None:
    config = persistent(
        tmp_path, maxmemory=2 * ENTRY, maxmemory_policy=MaxmemoryPolicy.ALLKEYS_LRU
    )

    async with running(config) as connect:
        client = await connect()
        await client.command(b"SET", b"first_", b"value")
        await client.command(b"SET", b"second", b"value")

        await client.command(b"GET", b"first_")  # promotes first_
        await client.command(b"SET", b"third_", b"value")

        assert await client.command(b"GET", b"first_") == b"$5\r\nvalue\r\n"
        assert await client.command(b"GET", b"second") == b"$-1\r\n"


@pytest.mark.asyncio
async def test_one_write_can_evict_several_keys_over_the_wire(tmp_path: Path) -> None:
    config = persistent(
        tmp_path, maxmemory=6 * ENTRY, maxmemory_policy=MaxmemoryPolicy.ALLKEYS_LRU
    )

    async with running(config) as connect:
        client = await connect()
        for index in range(6):
            await client.command(b"SET", b"key%03d" % index, b"value")

        assert await client.command(b"SET", b"big___", b"v" * (4 * ENTRY)) == b"+OK\r\n"

        remaining = int((await client.command(b"DBSIZE"))[1:-2])
        assert remaining < 6
        assert await client.command(b"GET", b"big___") != b"$-1\r\n"


@pytest.mark.asyncio
async def test_expired_keys_are_reclaimed_before_live_keys_are_evicted(
    tmp_path: Path,
) -> None:
    config = persistent(
        tmp_path, maxmemory=3 * ENTRY, maxmemory_policy=MaxmemoryPolicy.ALLKEYS_LRU
    )

    async with running(config) as connect:
        client = await connect()
        await client.command(b"SET", b"doomed", b"value", b"PX", b"50")
        await client.command(b"SET", b"live-1", b"value")
        await client.command(b"SET", b"live-2", b"value")

        await asyncio.sleep(0.1)  # doomed becomes garbage

        await client.command(b"SET", b"fresh_", b"value")

        # The expired key paid for the new one; both live keys survived.
        assert await client.command(b"GET", b"live-1") == b"$5\r\nvalue\r\n"
        assert await client.command(b"GET", b"live-2") == b"$5\r\nvalue\r\n"
        assert await client.command(b"GET", b"fresh_") == b"$5\r\nvalue\r\n"


@pytest.mark.asyncio
async def test_an_evicted_key_does_not_come_back_after_a_restart(
    tmp_path: Path,
) -> None:
    # Without a DEL record the victim's original SET would still be in the log,
    # and recovery would undo the eviction.
    config = persistent(
        tmp_path, maxmemory=2 * ENTRY, maxmemory_policy=MaxmemoryPolicy.ALLKEYS_LRU
    )

    async with running(config) as connect:
        client = await connect()
        await client.command(b"SET", b"first_", b"value")
        await client.command(b"SET", b"second", b"value")
        await client.command(b"SET", b"third_", b"value")
        assert await client.command(b"GET", b"first_") == b"$-1\r\n"

    async with running(config) as connect:
        client = await connect()
        assert await client.command(b"GET", b"first_") == b"$-1\r\n"
        assert await client.command(b"GET", b"second") == b"$5\r\nvalue\r\n"
        assert await client.command(b"GET", b"third_") == b"$5\r\nvalue\r\n"
        assert await client.command(b"DBSIZE") == b":2\r\n"


@pytest.mark.asyncio
async def test_a_persistence_failure_during_eviction_blocks_further_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = persistent(
        tmp_path, maxmemory=2 * ENTRY, maxmemory_policy=MaxmemoryPolicy.ALLKEYS_LRU
    )

    async with running(config) as connect:
        client = await connect()
        await client.command(b"SET", b"first_", b"value")
        await client.command(b"SET", b"second", b"value")

        def broken_fsync(fd: int) -> None:
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(os, "fsync", broken_fsync)

        # This write must evict, and the eviction's DEL is what fails.
        assert await client.command(b"SET", b"third_", b"value") == (
            b"-ERR persistence failure\r\n"
        )
        assert await client.command(b"SET", b"fourth", b"value") == (
            b"-ERR persistence failure\r\n"
        )
        assert await client.command(b"GET", b"second") == b"$5\r\nvalue\r\n"
        assert await client.command(b"PING") == b"+PONG\r\n"

        monkeypatch.setattr(os, "fsync", os.fsync)


@pytest.mark.asyncio
async def test_unlimited_memory_never_evicts(tmp_path: Path) -> None:
    config = persistent(tmp_path)  # maxmemory defaults to 0

    async with running(config) as connect:
        client = await connect()
        for index in range(200):
            assert await client.command(b"SET", b"key%03d" % index, b"value") == b"+OK\r\n"

        assert await client.command(b"DBSIZE") == b":200\r\n"

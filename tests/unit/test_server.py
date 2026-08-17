from __future__ import annotations

import asyncio
import io
import logging
import socket
from collections.abc import AsyncIterator, Iterator
from contextlib import closing, suppress

import pytest
import pytest_asyncio

from pyredis.config import Config
from pyredis.log import configure_logging
from pyredis.server import EXIT_CONFIG_ERROR, EXIT_LISTEN_ERROR, Server, main


@pytest.fixture(autouse=True)
def restore_root_logger() -> Iterator[None]:
    root = logging.getLogger()
    handlers = root.handlers[:]
    level = root.level
    try:
        yield
    finally:
        root.handlers[:] = handlers
        root.setLevel(level)


@pytest_asyncio.fixture
async def serving() -> AsyncIterator[Server]:
    """A server listening on an OS-assigned port, stopped on teardown."""
    server = Server(Config(host="127.0.0.1", port=0))
    task = asyncio.ensure_future(server.serve())
    await asyncio.wait_for(server.ready.wait(), timeout=5)
    try:
        yield server
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


def test_server_exposes_the_config_it_was_given() -> None:
    config = Config(port=7001)

    assert Server(config).config is config


def test_port_is_unavailable_before_the_listener_is_bound() -> None:
    with pytest.raises(RuntimeError, match="not listening"):
        _ = Server(Config()).port


@pytest.mark.asyncio
async def test_serve_binds_a_port_and_signals_readiness(serving: Server) -> None:
    assert serving.ready.is_set()
    assert serving.port > 0

    reader, writer = await asyncio.open_connection("127.0.0.1", serving.port)
    writer.write(b"*1\r\n$4\r\nPING\r\n")
    await writer.drain()

    assert await asyncio.wait_for(reader.readuntil(b"\r\n"), timeout=5) == b"+PONG\r\n"

    writer.close()
    await writer.wait_closed()


@pytest.mark.asyncio
async def test_serve_logs_the_bound_endpoint() -> None:
    stream = io.StringIO()
    configure_logging("INFO", stream=stream)
    server = Server(Config(host="127.0.0.1", port=0))
    task = asyncio.ensure_future(server.serve())
    await asyncio.wait_for(server.ready.wait(), timeout=5)
    port = server.port
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task

    assert f"ready to accept connections on 127.0.0.1:{port}" in stream.getvalue()


@pytest.mark.asyncio
async def test_cancelling_serve_releases_the_port() -> None:
    server = Server(Config(host="127.0.0.1", port=0))
    task = asyncio.ensure_future(server.serve())
    await asyncio.wait_for(server.ready.wait(), timeout=5)
    port = server.port

    task.cancel()
    with suppress(asyncio.CancelledError):
        await task

    assert task.cancelled()
    assert not server.ready.is_set()
    # The port is free again: rebinding it would fail if the listener leaked.
    with closing(socket.socket()) as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(("127.0.0.1", port))


@pytest.mark.asyncio
async def test_each_server_owns_a_separate_keyspace() -> None:
    first = Server(Config(host="127.0.0.1", port=0))
    second = Server(Config(host="127.0.0.1", port=0))
    tasks = [asyncio.ensure_future(server.serve()) for server in (first, second)]
    await asyncio.wait_for(asyncio.gather(first.ready.wait(), second.ready.wait()), timeout=5)

    try:
        reader_a, writer_a = await asyncio.open_connection("127.0.0.1", first.port)
        writer_a.write(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n")
        await writer_a.drain()
        await asyncio.wait_for(reader_a.readuntil(b"\r\n"), timeout=5)

        reader_b, writer_b = await asyncio.open_connection("127.0.0.1", second.port)
        writer_b.write(b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n")
        await writer_b.drain()

        assert await asyncio.wait_for(reader_b.readuntil(b"\r\n"), timeout=5) == b"$-1\r\n"

        for writer in (writer_a, writer_b):
            writer.close()
            await writer.wait_closed()
    finally:
        for task in tasks:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


@pytest.mark.asyncio
async def test_serve_fails_when_the_port_is_already_taken(serving: Server) -> None:
    clashing = Server(Config(host="127.0.0.1", port=serving.port))

    with pytest.raises(OSError):
        await clashing.serve()


def test_main_reports_a_listen_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    with closing(socket.socket()) as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen()
        monkeypatch.setenv("PYREDIS_PORT", str(taken.getsockname()[1]))

        assert main() == EXIT_LISTEN_ERROR


def test_main_reports_invalid_configuration_without_starting(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("PYREDIS_PORT", "99999")

    assert main() == EXIT_CONFIG_ERROR
    assert "invalid configuration" in capsys.readouterr().err


# --------------------------------------------------------------------------
# Active expiration
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_server_reclaims_expired_keys_without_anyone_reading_them() -> None:
    # Observed through the sweeper's own log line, since any command that
    # could see the key would also expire it lazily and prove nothing.
    stream = io.StringIO()
    configure_logging("DEBUG", stream=stream)
    server = Server(Config(host="127.0.0.1", port=0))
    task = asyncio.ensure_future(server.serve())
    await asyncio.wait_for(server.ready.wait(), timeout=5)
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        writer.write(b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nPX\r\n$2\r\n10\r\n")
        await writer.drain()
        assert await asyncio.wait_for(reader.readuntil(b"\r\n"), timeout=5) == b"+OK\r\n"
        writer.close()
        await writer.wait_closed()

        await asyncio.sleep(0.4)  # several 100 ms sweep cycles
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    assert "actively expired 1 key(s)" in stream.getvalue()


@pytest.mark.asyncio
async def test_the_expiration_task_does_not_outlive_the_server() -> None:
    server = Server(Config(host="127.0.0.1", port=0))
    task = asyncio.ensure_future(server.serve())
    await asyncio.wait_for(server.ready.wait(), timeout=5)

    task.cancel()
    with suppress(asyncio.CancelledError):
        await task

    sweepers = [
        pending
        for pending in asyncio.all_tasks()
        if getattr(pending.get_coro(), "__qualname__", "") == "Server._expire_cycle"
    ]
    assert sweepers == []

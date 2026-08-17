"""Checks that the installed entry point serves TCP and shuts down cleanly."""

from __future__ import annotations

import asyncio
import os
import re
import shutil
import signal
import subprocess
import sys
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager, suppress

import pytest

TIMEOUT = 15
READY = re.compile(r"ready to accept connections on 127\.0\.0\.1:(\d+)")

# The console script when it is installed, otherwise the module entry point.
_SCRIPT = shutil.which("pyredis")
ENTRY_POINT: Sequence[str] = [_SCRIPT] if _SCRIPT else [sys.executable, "-m", "pyredis"]


@asynccontextmanager
async def running(**env: str) -> AsyncIterator[asyncio.subprocess.Process]:
    process = await asyncio.create_subprocess_exec(
        *ENTRY_POINT,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={**os.environ, "PYREDIS_PORT": "0", **env},
    )
    try:
        yield process
    finally:
        if process.returncode is None:
            process.send_signal(signal.SIGINT)
            with suppress(TimeoutError):
                await asyncio.wait_for(process.wait(), timeout=TIMEOUT)
        if process.returncode is None:  # pragma: no cover -- only if SIGINT is ignored
            process.kill()
            await process.wait()


async def wait_for_port(process: asyncio.subprocess.Process) -> int:
    """Read stderr until the server announces the port the OS assigned it."""
    assert process.stderr is not None
    while True:
        line = await asyncio.wait_for(process.stderr.readline(), timeout=TIMEOUT)
        assert line, "server exited before it was ready"
        found = READY.search(line.decode())
        if found:
            return int(found.group(1))


@pytest.mark.asyncio
async def test_entry_point_serves_commands_and_exits_zero_on_sigint() -> None:
    async with running() as process:
        port = await wait_for_port(process)

        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$4\r\nPyke\r\n*2\r\n$3\r\nGET\r\n$4\r\nname\r\n")
        await writer.drain()

        assert await asyncio.wait_for(reader.readuntil(b"\r\n"), timeout=TIMEOUT) == b"+OK\r\n"
        expected = b"$4\r\nPyke\r\n"
        reply = await asyncio.wait_for(reader.readexactly(len(expected)), timeout=TIMEOUT)
        assert reply == expected

        writer.close()
        await writer.wait_closed()

        process.send_signal(signal.SIGINT)
        assert await asyncio.wait_for(process.wait(), timeout=TIMEOUT) == 0

        assert process.stdout is not None
        assert await process.stdout.read() == b"", "stdout must stay free of log output"


@pytest.mark.asyncio
async def test_entry_point_is_silent_at_error_log_level() -> None:
    async with running(PYREDIS_LOG_LEVEL="ERROR") as process:
        await asyncio.sleep(0.5)  # long enough to have logged a ready line
        process.send_signal(signal.SIGINT)
        assert await asyncio.wait_for(process.wait(), timeout=TIMEOUT) == 0

        assert process.stderr is not None
        assert await process.stderr.read() == b""


def test_entry_point_rejects_bad_configuration() -> None:
    result = subprocess.run(
        [*ENTRY_POINT],
        capture_output=True,
        text=True,
        timeout=TIMEOUT,
        env={**os.environ, "PYREDIS_PORT": "0abc"},
        check=False,
    )

    assert result.returncode == 2
    assert "invalid configuration" in result.stderr


@pytest.mark.asyncio
async def test_entry_point_reports_a_port_that_is_already_taken() -> None:
    async with running() as holder:
        port = await wait_for_port(holder)

        clash = await asyncio.create_subprocess_exec(
            *ENTRY_POINT,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, "PYREDIS_PORT": str(port)},
        )
        _, stderr = await asyncio.wait_for(clash.communicate(), timeout=TIMEOUT)

        assert clash.returncode == 1
        assert "cannot listen on" in stderr.decode()

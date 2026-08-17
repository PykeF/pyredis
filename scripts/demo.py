#!/usr/bin/env python3
"""Drive a live PyRedis server through everything it can do.

Starts a server on an ephemeral port with persistence and a small memory limit,
then talks to it over a plain TCP socket -- the point being that nothing here
imports PyRedis' own protocol code, so the wire format is exercised the way a
real client would exercise it. No third-party client library is needed.

    uv run python scripts/demo.py
"""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

ENTRY_OVERHEAD = 64  # what PyRedis models one key as costing beyond its bytes
HOST = "127.0.0.1"


class Client:
    """The smallest RESP2 client that can hold a conversation."""

    def __init__(self, port: int) -> None:
        self._socket = socket.create_connection((HOST, port), timeout=5)
        self._buffer = b""

    def __call__(self, *args: bytes | str) -> object:
        encoded = [arg.encode() if isinstance(arg, str) else arg for arg in args]
        frame = [b"*%d\r\n" % len(encoded)]
        frame += [b"$%d\r\n%s\r\n" % (len(arg), arg) for arg in encoded]
        self._socket.sendall(b"".join(frame))
        return self._read_reply()

    def _read_reply(self) -> object:
        line = self._read_line()
        kind, body = line[:1], line[1:]
        if kind == b"+":
            return body.decode()
        if kind == b"-":
            return f"(error) {body.decode()}"
        if kind == b":":
            return int(body)
        if kind == b"$":
            length = int(body)
            if length == -1:
                return None
            while len(self._buffer) < length + 2:
                self._fill()
            payload, self._buffer = self._buffer[:length], self._buffer[length + 2 :]
            return payload
        raise AssertionError(f"unexpected reply {line!r}")

    def _read_line(self) -> bytes:
        while b"\r\n" not in self._buffer:
            self._fill()
        line, _, self._buffer = self._buffer.partition(b"\r\n")
        return line

    def _fill(self) -> None:
        chunk = self._socket.recv(65536)
        if not chunk:
            raise ConnectionError("server closed the connection")
        self._buffer += chunk

    def close(self) -> None:
        self._socket.close()


def show(label: str, result: object) -> None:
    if isinstance(result, bytes):
        rendered = repr(result)
    elif result is None:
        rendered = "(nil)"
    else:
        rendered = str(result)
    print(f"  {label:<34} {rendered}")


@contextmanager
def server(**settings: str) -> Iterator[int]:
    """Run a PyRedis server on an OS-assigned port for the duration of a block."""
    executable = Path(sys.executable).with_name("pyredis")
    command = [str(executable)] if executable.exists() else [sys.executable, "-m", "pyredis"]
    process = subprocess.Popen(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        env={**os.environ, "PYREDIS_PORT": "0", **settings},
    )
    assert process.stderr is not None
    try:
        port = 0
        deadline = time.time() + 15
        while time.time() < deadline:
            line = process.stderr.readline().decode()
            if "ready to accept connections" in line:
                port = int(line.rsplit(":", 1)[1])
                break
        if not port:
            raise RuntimeError("server never became ready")
        yield port
    finally:
        process.send_signal(signal.SIGINT)
        process.wait(timeout=15)


def basics(port: int) -> None:
    print("\nStrings, counters, and binary safety")
    client = Client(port)
    show("PING", client("PING"))
    show("PING hello", client("PING", "hello"))
    show("SET name Pyke", client("SET", "name", "Pyke"))
    show("GET name", client("GET", "name"))
    show("GET missing", client("GET", "missing"))

    # Values are byte strings, not text: NUL bytes, CRLF, and data that is not
    # valid UTF-8 all survive untouched.
    payload = b"\x89PNG\r\n\x1a\n\x00\xff"
    show("SET logo <8 raw bytes>", client("SET", b"logo", payload))
    roundtripped = client("GET", "logo")
    show("GET logo", roundtripped)
    show("byte-identical?", roundtripped == payload)

    show("INCR visits", client("INCR", "visits"))
    show("INCR visits", client("INCR", "visits"))
    show("INCR name (not a number)", client("INCR", "name"))

    show("EXISTS name logo missing", client("EXISTS", "name", "logo", "missing"))
    show("DEL logo", client("DEL", "logo"))
    show("EXISTS logo", client("EXISTS", "logo"))
    show("DBSIZE", client("DBSIZE"))
    client.close()


def expiration(port: int) -> None:
    print("\nExpiration")
    client = Client(port)
    show("SET session token EX 60", client("SET", "session", "token", "EX", "60"))
    show("TTL session", client("TTL", "session"))
    show("PERSIST session", client("PERSIST", "session"))
    show("TTL session (persisted)", client("TTL", "session"))
    show("EXPIRE session 60", client("EXPIRE", "session", "60"))
    show("TTL session", client("TTL", "session"))
    show("TTL name (no deadline)", client("TTL", "name"))
    show("TTL nothing (no key)", client("TTL", "nothing"))

    show("SET flash v PX 100", client("SET", "flash", "v", "PX", "100"))
    show("GET flash", client("GET", "flash"))
    time.sleep(0.25)
    show("GET flash (after 250ms)", client("GET", "flash"))
    show("TTL flash", client("TTL", "flash"))
    client.close()


def persistence(aof: Path) -> None:
    print("\nPersistence: writing, then restarting from the log")
    settings = {"PYREDIS_AOF_ENABLED": "true", "PYREDIS_AOF_PATH": str(aof)}
    with server(**settings) as port:
        client = Client(port)
        client("SET", "name", "Pyke")
        client("INCR", "visits")
        client("INCR", "visits")
        client("SET", "session", "token", "EX", "600")
        client("SET", "doomed", "value")
        client("DEL", "doomed")
        show("wrote 3 keys, deleted 1; DBSIZE", client("DBSIZE"))
        show("TTL session", client("TTL", "session"))
        client.close()

    show("append-only file", f"{aof.stat().st_size} bytes")
    print("  -- server stopped; waiting 2s, then restarting on the same file --")
    time.sleep(2)

    with server(**settings) as port:
        client = Client(port)
        show("GET name", client("GET", "name"))
        show("GET visits (counter kept)", client("GET", "visits"))
        show("GET doomed (stayed deleted)", client("GET", "doomed"))
        # The deadline was recorded as an absolute time, so it kept counting
        # down while nothing was running. A relative TTL would show 600 again.
        show("TTL session (was 600, not reset)", client("TTL", "session"))
        show("DBSIZE", client("DBSIZE"))
        client.close()


def eviction() -> None:
    print("\nMemory limits: refusing writes, then evicting instead")
    entry = 6 + 5 + ENTRY_OVERHEAD  # six-byte key, five-byte value

    with server(PYREDIS_MAXMEMORY=str(entry)) as port:
        client = Client(port)
        show("maxmemory", f"{entry} bytes, policy noeviction")
        show("SET first_ value", client("SET", "first_", "value"))
        show("SET second value", client("SET", "second", "value"))
        show("DBSIZE (unchanged)", client("DBSIZE"))
        show("GET first_ (untouched)", client("GET", "first_"))
        show("PING (reads still work)", client("PING"))
        client.close()

    with server(
        PYREDIS_MAXMEMORY=str(2 * entry), PYREDIS_MAXMEMORY_POLICY="allkeys-lru"
    ) as port:
        client = Client(port)
        show("maxmemory", f"{2 * entry} bytes, policy allkeys-lru")
        client("SET", "first_", "value")
        client("SET", "second", "value")
        show("GET first_ (marks it recent)", client("GET", "first_"))
        show("SET third_ value", client("SET", "third_", "value"))
        show("DBSIZE (still two)", client("DBSIZE"))
        show("GET first_ (recently used)", client("GET", "first_"))
        show("GET second (evicted)", client("GET", "second"))
        show("GET third_", client("GET", "third_"))
        client.close()


def main() -> int:
    print("PyRedis demo -- every reply below came over TCP from a real server.")
    with server() as port:
        print(f"\nServer listening on {HOST}:{port}")
        basics(port)
        expiration(port)

    with tempfile.TemporaryDirectory() as scratch:
        persistence(Path(scratch) / "demo.aof")

    eviction()
    print("\nDone. Every server started here has been shut down.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

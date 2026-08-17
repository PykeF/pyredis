"""Server lifecycle and process entry point.

Owns the single `KeyValueStore` and the asyncio listener. One event loop, one
thread, one task per connection: because store methods never block or yield,
each command runs to completion without interleaving, so no locks exist
anywhere in PyRedis.
"""

from __future__ import annotations

import asyncio
import sys
from contextlib import suppress

from pyredis import __version__
from pyredis.config import Config, ConfigError
from pyredis.connection import handle_connection, peer_name
from pyredis.log import configure_logging, get_logger
from pyredis.store import KeyValueStore

EXIT_OK = 0
EXIT_LISTEN_ERROR = 1
EXIT_CONFIG_ERROR = 2

_logger = get_logger(__name__)


class Server:
    """Owns the runtime lifecycle of a PyRedis instance."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._store = KeyValueStore()
        self._ready = asyncio.Event()
        self._port: int | None = None
        self._clients: set[asyncio.StreamWriter] = set()

    @property
    def config(self) -> Config:
        return self._config

    @property
    def ready(self) -> asyncio.Event:
        """Set once the listener is bound and accepting connections."""
        return self._ready

    @property
    def port(self) -> int:
        """The port actually bound, which differs from the configured one when
        port 0 asks the OS to choose."""
        if self._port is None:
            raise RuntimeError("server is not listening")
        return self._port

    async def serve(self) -> None:
        """Accept and serve clients until cancelled."""
        _logger.info("pyredis %s starting", __version__)
        tcp = await asyncio.start_server(
            self._handle_client, self._config.host, self._config.port
        )
        self._port = int(tcp.sockets[0].getsockname()[1])
        self._ready.set()
        _logger.info("ready to accept connections on %s:%d", self._config.host, self._port)
        try:
            # Deliberately not `tcp.serve_forever()`: its own cancellation path
            # awaits `wait_closed()`, which since 3.12 waits for live client
            # handlers to finish. Those are blocked reading from clients that
            # have no reason to disconnect, so shutdown would deadlock before
            # any cleanup here could run. `start_server` is already accepting;
            # this just parks the task until it is cancelled.
            await asyncio.get_running_loop().create_future()
        finally:
            # Stop accepting, then hang up on live clients. Waiting for them
            # instead would deadlock: their handlers are blocked reading from
            # peers that have no reason to disconnect. Nothing is lost, since
            # a command never yields part-way through.
            tcp.close()
            self._disconnect_clients()
            self._ready.clear()
            self._port = None
            _logger.info("listener closed")

    def run(self) -> None:
        """Drive `serve` on a fresh event loop until it completes."""
        asyncio.run(self.serve())

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = peer_name(writer)
        _logger.debug("client connected: %s", peer)
        self._clients.add(writer)
        try:
            await handle_connection(reader, writer, self._store)
        finally:
            self._clients.discard(writer)
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()
            _logger.debug("client disconnected: %s", peer)

    def _disconnect_clients(self) -> None:
        for writer in list(self._clients):
            writer.close()


def main() -> int:
    """Console-script entry point. Returns the process exit code."""
    try:
        config = Config.from_env()
    except ConfigError as exc:
        # Logging is not configured yet -- report directly and bail out.
        print(f"pyredis: invalid configuration: {exc}", file=sys.stderr)
        return EXIT_CONFIG_ERROR

    configure_logging(config.log_level)
    try:
        Server(config).run()
    except KeyboardInterrupt:
        _logger.info("interrupted; shutting down")
    except OSError as exc:
        _logger.error("cannot listen on %s: %s", config.address, exc)
        return EXIT_LISTEN_ERROR
    return EXIT_OK

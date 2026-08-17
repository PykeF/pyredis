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
from pathlib import Path

from pyredis import __version__
from pyredis.aof import (
    NO_JOURNAL,
    AofError,
    AppendOnlyFile,
    FsyncPolicy,
    Journal,
    PersistenceError,
    load,
)
from pyredis.config import Config, ConfigError
from pyredis.connection import handle_connection, peer_name
from pyredis.log import configure_logging, get_logger
from pyredis.store import KeyValueStore

EXIT_OK = 0
EXIT_LISTEN_ERROR = 1
EXIT_CONFIG_ERROR = 2
EXIT_PERSISTENCE_ERROR = 3

#: Active expiration: how often to sweep, and how many keys carrying a deadline
#: to examine per sweep. Bounded on purpose -- reclaiming untouched expired
#: keys is eventual, while lazy expiration keeps every read correct meanwhile.
SWEEP_INTERVAL_SECONDS = 0.1
SWEEP_LIMIT = 100

#: How often the `everysec` policy asks the OS to commit the journal. The fsync
#: runs on the event loop and can stall it; once a second is the price of not
#: introducing threads or locks for it.
FSYNC_INTERVAL_SECONDS = 1.0

_logger = get_logger(__name__)


class Server:
    """Owns the runtime lifecycle of a PyRedis instance."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._store = KeyValueStore(
            maxmemory=config.maxmemory, policy=config.maxmemory_policy
        )
        self._ready = asyncio.Event()
        self._port: int | None = None
        self._clients: set[asyncio.StreamWriter] = set()
        self._journal: Journal = NO_JOURNAL

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
        # Recovery happens before the listener exists, so no client can ever
        # observe a half-loaded keyspace.
        self._recover()
        tcp = await asyncio.start_server(
            self._handle_client, self._config.host, self._config.port
        )
        self._port = int(tcp.sockets[0].getsockname()[1])
        sweeper = asyncio.create_task(self._expire_cycle())
        syncer = (
            asyncio.create_task(self._fsync_cycle())
            if self._config.aof_fsync is FsyncPolicy.EVERYSEC
            else None
        )
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
            # Stop expiring before anything else, so nothing mutates the
            # keyspace during teardown. Then stop accepting and hang up on live
            # clients: waiting for them instead would deadlock, since their
            # handlers are blocked reading from peers that have no reason to
            # disconnect. Nothing is lost, as a command never yields part-way
            # through. Awaiting the sweeper last leaves no pending task behind;
            # it is safe because its only suspension point is a sleep.
            sweeper.cancel()
            if syncer is not None:
                syncer.cancel()
            tcp.close()
            self._disconnect_clients()
            with suppress(asyncio.CancelledError):
                await sweeper
            if syncer is not None:
                with suppress(asyncio.CancelledError):
                    await syncer
            # Last chance to get everything on disk before the file goes.
            self._journal.close()
            self._journal = NO_JOURNAL
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
            await handle_connection(reader, writer, self._store, self._journal)
        finally:
            self._clients.discard(writer)
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()
            _logger.debug("client disconnected: %s", peer)

    def _disconnect_clients(self) -> None:
        for writer in list(self._clients):
            writer.close()

    def _recover(self) -> None:
        """Replay the append-only file, then open it for writing.

        Any failure here is fatal: starting with a keyspace that silently
        disagrees with the log on disk would be worse than not starting.
        """
        if not self._config.aof_enabled:
            return
        path = Path(self._config.aof_path)
        result = load(path, self._store)
        _logger.info(
            "loaded %d record(s) from %s: %d key(s), %d expired while down",
            result.records,
            path,
            result.keys,
            result.expired,
        )
        self._journal = AppendOnlyFile(path, self._config.aof_fsync)
        maxmemory = self._config.maxmemory
        if maxmemory and self._store.memory_used > maxmemory:
            # Recovery ignores the limit; the first admitted write brings the
            # keyspace back within it, evicting if the policy allows.
            _logger.warning(
                "recovered keyspace uses %d byte(s), over the %d byte limit",
                self._store.memory_used,
                maxmemory,
            )

    async def _fsync_cycle(self) -> None:
        """Commit the journal to disk once a second under the everysec policy."""
        while True:
            await asyncio.sleep(FSYNC_INTERVAL_SECONDS)
            with suppress(PersistenceError):
                # A failure has already been logged and has marked the journal
                # failed, which is what stops later writes.
                self._journal.sync()

    async def _expire_cycle(self) -> None:
        """Reclaim expired keys nobody is reading, on the same event loop.

        The sweep itself is synchronous, so it cannot interleave with a command
        -- it only ever runs while client tasks are suspended between commands.
        """
        while True:
            await asyncio.sleep(SWEEP_INTERVAL_SECONDS)
            try:
                removed = self._store.sweep_expired(SWEEP_LIMIT)
            except Exception:
                # A sweep failure must not take the server down with it.
                _logger.exception("active expiration failed")
                continue
            if removed:
                _logger.debug("actively expired %d key(s)", removed)


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
    except AofError as exc:
        _logger.error("cannot use the append-only file %s: %s", config.aof_path, exc)
        return EXIT_PERSISTENCE_ERROR
    except OSError as exc:
        _logger.error("cannot listen on %s: %s", config.address, exc)
        return EXIT_LISTEN_ERROR
    return EXIT_OK

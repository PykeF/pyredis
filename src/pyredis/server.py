"""Server lifecycle and process entry point.

Phase 0 wires the lifecycle only: load configuration, configure logging, enter
the asyncio run loop, and exit cleanly. `Server.serve` is already a coroutine
so that P2 can replace its body with `asyncio.start_server(...)` without
changing how the process starts, stops, or reports failure.
"""

from __future__ import annotations

import asyncio
import sys

from pyredis import __version__
from pyredis.config import Config, ConfigError
from pyredis.log import configure_logging, get_logger

EXIT_OK = 0
EXIT_CONFIG_ERROR = 2

_logger = get_logger(__name__)


class Server:
    """Owns the runtime lifecycle of a PyRedis instance."""

    def __init__(self, config: Config) -> None:
        self._config = config

    @property
    def config(self) -> Config:
        return self._config

    async def serve(self) -> None:
        """Run the server until shutdown.

        P0 has nothing to serve, so this returns immediately after announcing
        the resolved configuration. P2 replaces the body with a TCP listener
        that runs until cancelled.
        """
        _logger.info("pyredis %s starting", __version__)
        _logger.info("resolved endpoint %s (listener arrives in P2)", self._config.address)
        _logger.debug("configuration: %r", self._config)
        _logger.info("no listener in P0; shutting down")

    def run(self) -> None:
        """Drive `serve` on a fresh event loop until it completes."""
        asyncio.run(self.serve())


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
    return EXIT_OK

"""Centralized logging configuration.

Named `log` rather than `logging` so the module never shadows the standard
library module it wraps.

Thin wrapper over the standard library: one root handler writing to stderr in
a fixed format. Application modules call `get_logger(__name__)` and never
configure handlers themselves, so a future asyncio server logs consistently
from every task.
"""

from __future__ import annotations

import logging
import sys
from typing import TextIO

LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s %(message)s"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

#: Log levels PyRedis accepts, in decreasing verbosity. Names only -- numeric
#: levels are deliberately not supported, to keep configuration unambiguous.
LOG_LEVELS: tuple[str, ...] = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

_HANDLER_NAME = "pyredis"


def configure_logging(level: str, *, stream: TextIO | None = None) -> None:
    """Install PyRedis' root log handler at `level`.

    Safe to call more than once: the previous PyRedis handler is replaced
    rather than stacked, so records are never duplicated. `stream` defaults to
    stderr, keeping stdout free for future protocol or tooling output.
    """
    if level not in LOG_LEVELS:
        raise ValueError(f"unknown log level {level!r}; expected one of {', '.join(LOG_LEVELS)}")

    handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
    handler.set_name(_HANDLER_NAME)
    handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))

    root = logging.getLogger()
    for existing in [h for h in root.handlers if h.name == _HANDLER_NAME]:
        root.removeHandler(existing)
        existing.close()
    root.addHandler(handler)
    root.setLevel(level)


def get_logger(name: str) -> logging.Logger:
    """Return the logger a PyRedis module should log through."""
    return logging.getLogger(name)

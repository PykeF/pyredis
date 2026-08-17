"""Typed server configuration, loaded from the environment.

Every setting has a default, so PyRedis starts with no environment at all.
Overrides come from `PYREDIS_`-prefixed variables and are validated eagerly:
a bad value fails at startup with a precise message rather than surfacing as a
confusing error deep inside the server.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from pyredis.log import LOG_LEVELS

ENV_PREFIX: Final = "PYREDIS_"

DEFAULT_HOST: Final = "127.0.0.1"
#: 6380 rather than Redis' 6379, so PyRedis never collides with a real Redis
#: running locally -- both can be up at once while developing.
DEFAULT_PORT: Final = 6380
DEFAULT_LOG_LEVEL: Final = "INFO"

#: Port 0 is permitted and means "let the OS assign a free port", which is how
#: throwaway and test instances bind without racing for a fixed number. Note
#: this is not Redis' meaning for port 0, which is "do not listen on TCP".
_MIN_PORT: Final = 0
_MAX_PORT: Final = 65535


class ConfigError(ValueError):
    """Raised when the environment holds a value PyRedis cannot use."""


@dataclass(frozen=True, slots=True)
class Config:
    """Server-level settings.

    Immutable: configuration is resolved once at startup and then read freely
    from any task without synchronization.
    """

    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    log_level: str = DEFAULT_LOG_LEVEL

    def __post_init__(self) -> None:
        if not self.host:
            raise ConfigError(f"{ENV_PREFIX}HOST must not be empty")
        if not _MIN_PORT <= self.port <= _MAX_PORT:
            raise ConfigError(
                f"{ENV_PREFIX}PORT must be between {_MIN_PORT} and {_MAX_PORT}, got {self.port}"
            )
        if self.log_level not in LOG_LEVELS:
            raise ConfigError(
                f"{ENV_PREFIX}LOG_LEVEL must be one of {', '.join(LOG_LEVELS)}, "
                f"got {self.log_level!r}"
            )

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Config:
        """Build a `Config` from `env`, defaulting to the process environment.

        Raises `ConfigError` if any recognized variable holds an unusable value.
        """
        source = os.environ if env is None else env
        return cls(
            host=_read_str(source, "HOST", DEFAULT_HOST),
            port=_read_port(source, "PORT", DEFAULT_PORT),
            log_level=_read_str(source, "LOG_LEVEL", DEFAULT_LOG_LEVEL).upper(),
        )

    @property
    def address(self) -> str:
        """The `host:port` endpoint, for logs and future listener setup."""
        return f"{self.host}:{self.port}"


def _read_str(env: Mapping[str, str], name: str, default: str) -> str:
    return env.get(ENV_PREFIX + name, default).strip()


def _read_port(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(ENV_PREFIX + name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError:
        raise ConfigError(f"{ENV_PREFIX}{name} must be an integer, got {raw!r}") from None

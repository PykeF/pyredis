"""Typed server configuration, loaded from the environment.

Every setting has a default, so PyRedis starts with no environment at all.
Overrides come from `PYREDIS_`-prefixed variables and are validated eagerly:
a bad value fails at startup with a precise message rather than surfacing as a
confusing error deep inside the server.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from pyredis.aof import FSYNC_POLICIES, FsyncPolicy
from pyredis.log import LOG_LEVELS
from pyredis.store import MAXMEMORY_POLICIES, MaxmemoryPolicy

ENV_PREFIX: Final = "PYREDIS_"

DEFAULT_HOST: Final = "127.0.0.1"
#: 6380 rather than Redis' 6379, so PyRedis never collides with a real Redis
#: running locally -- both can be up at once while developing.
DEFAULT_PORT: Final = 6380
DEFAULT_LOG_LEVEL: Final = "INFO"

#: Persistence is off unless asked for, so starting PyRedis never writes files
#: into whatever directory it happened to be launched from. Redis' own
#: `appendonly` default is likewise "no".
DEFAULT_AOF_ENABLED: Final = False
DEFAULT_AOF_PATH: Final = "pyredis.aof"
DEFAULT_AOF_FSYNC: Final = FsyncPolicy.EVERYSEC

_TRUE: Final = frozenset({"1", "true", "yes", "on"})
_FALSE: Final = frozenset({"0", "false", "no", "off"})

#: 0 means unlimited, as in Redis.
DEFAULT_MAXMEMORY: Final = 0
DEFAULT_MAXMEMORY_POLICY: Final = MaxmemoryPolicy.NOEVICTION

#: Sizes are binary and only binary. Redis reads `1k` as 1000 and `1kb` as
#: 1024; rather than quietly disagree with it, the ambiguous single-letter
#: forms are rejected outright.
_MEMORY_UNITS: Final = {"": 1, "kb": 1024, "mb": 1024**2, "gb": 1024**3}
_MEMORY_RE: Final = re.compile(r"([0-9]+)(kb|mb|gb)?")

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
    aof_enabled: bool = DEFAULT_AOF_ENABLED
    aof_path: str = DEFAULT_AOF_PATH
    aof_fsync: FsyncPolicy = DEFAULT_AOF_FSYNC
    maxmemory: int = DEFAULT_MAXMEMORY
    maxmemory_policy: MaxmemoryPolicy = DEFAULT_MAXMEMORY_POLICY

    def __post_init__(self) -> None:
        if not self.host:
            raise ConfigError(f"{ENV_PREFIX}HOST must not be empty")
        if self.aof_enabled and not self.aof_path:
            raise ConfigError(f"{ENV_PREFIX}AOF_PATH must not be empty when the AOF is enabled")
        if self.maxmemory < 0:
            raise ConfigError(f"{ENV_PREFIX}MAXMEMORY must not be negative")
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
            aof_enabled=_read_bool(source, "AOF_ENABLED", DEFAULT_AOF_ENABLED),
            aof_path=_read_str(source, "AOF_PATH", DEFAULT_AOF_PATH),
            aof_fsync=_read_fsync(source, "AOF_FSYNC", DEFAULT_AOF_FSYNC),
            maxmemory=_read_memory(source, "MAXMEMORY", DEFAULT_MAXMEMORY),
            maxmemory_policy=_read_policy(
                source, "MAXMEMORY_POLICY", DEFAULT_MAXMEMORY_POLICY
            ),
        )

    @property
    def address(self) -> str:
        """The `host:port` endpoint, for logs and future listener setup."""
        return f"{self.host}:{self.port}"


def _read_str(env: Mapping[str, str], name: str, default: str) -> str:
    return env.get(ENV_PREFIX + name, default).strip()


def _read_bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    """Read a flag, accepting only spellings that are unambiguous.

    Python truthiness is deliberately not used: "false" is a non-empty string
    and would otherwise switch the feature on.
    """
    raw = env.get(ENV_PREFIX + name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in _TRUE:
        return True
    if value in _FALSE:
        return False
    raise ConfigError(
        f"{ENV_PREFIX}{name} must be one of "
        f"{', '.join(sorted(_TRUE | _FALSE))}, got {raw!r}"
    )


def _read_fsync(env: Mapping[str, str], name: str, default: FsyncPolicy) -> FsyncPolicy:
    raw = env.get(ENV_PREFIX + name)
    if raw is None:
        return default
    try:
        return FsyncPolicy(raw.strip().lower())
    except ValueError:
        raise ConfigError(
            f"{ENV_PREFIX}{name} must be one of {', '.join(FSYNC_POLICIES)}, got {raw!r}"
        ) from None


def _read_memory(env: Mapping[str, str], name: str, default: int) -> int:
    """Read a size in bytes, with optional binary units.

    Accepts a bare byte count or a `kb`/`mb`/`gb` suffix meaning 1024, 1024**2
    and 1024**3. Fractions, negatives, and the ambiguous `k`/`m`/`g` forms are
    refused rather than guessed at.
    """
    raw = env.get(ENV_PREFIX + name)
    if raw is None:
        return default
    matched = _MEMORY_RE.fullmatch(raw.strip().lower())
    if matched is None:
        raise ConfigError(
            f"{ENV_PREFIX}{name} must be a byte count, optionally suffixed with "
            f"kb, mb or gb, got {raw!r}"
        )
    return int(matched.group(1)) * _MEMORY_UNITS[matched.group(2) or ""]


def _read_policy(
    env: Mapping[str, str], name: str, default: MaxmemoryPolicy
) -> MaxmemoryPolicy:
    raw = env.get(ENV_PREFIX + name)
    if raw is None:
        return default
    try:
        return MaxmemoryPolicy(raw.strip().lower())
    except ValueError:
        raise ConfigError(
            f"{ENV_PREFIX}{name} must be one of {', '.join(MAXMEMORY_POLICIES)}, got {raw!r}"
        ) from None


def _read_port(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(ENV_PREFIX + name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError:
        raise ConfigError(f"{ENV_PREFIX}{name} must be an integer, got {raw!r}") from None

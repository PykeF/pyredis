from __future__ import annotations

import pytest

from pyredis.config import (
    DEFAULT_HOST,
    DEFAULT_LOG_LEVEL,
    DEFAULT_PORT,
    Config,
    ConfigError,
)
from pyredis.store import MaxmemoryPolicy


def test_defaults_apply_when_environment_is_empty() -> None:
    config = Config.from_env({})

    assert config.host == DEFAULT_HOST
    assert config.port == DEFAULT_PORT
    assert config.log_level == DEFAULT_LOG_LEVEL


def test_default_port_avoids_the_real_redis_port() -> None:
    assert DEFAULT_PORT == 6380


def test_environment_overrides_every_setting() -> None:
    config = Config.from_env(
        {
            "PYREDIS_HOST": "0.0.0.0",
            "PYREDIS_PORT": "7000",
            "PYREDIS_LOG_LEVEL": "DEBUG",
        }
    )

    assert config.host == "0.0.0.0"
    assert config.port == 7000
    assert config.log_level == "DEBUG"


def test_unprefixed_variables_are_ignored() -> None:
    config = Config.from_env({"HOST": "0.0.0.0", "PORT": "7000"})

    assert config.host == DEFAULT_HOST
    assert config.port == DEFAULT_PORT


def test_values_are_stripped_and_log_level_is_normalized() -> None:
    config = Config.from_env(
        {"PYREDIS_HOST": " localhost ", "PYREDIS_PORT": " 7000 ", "PYREDIS_LOG_LEVEL": "debug"}
    )

    assert config.host == "localhost"
    assert config.port == 7000
    assert config.log_level == "DEBUG"


def test_from_env_reads_process_environment_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PYREDIS_PORT", "6399")

    assert Config.from_env().port == 6399


def test_non_integer_port_is_rejected() -> None:
    with pytest.raises(ConfigError, match="PYREDIS_PORT must be an integer"):
        Config.from_env({"PYREDIS_PORT": "not-a-port"})


@pytest.mark.parametrize("port", ["-1", "65536", "99999"])
def test_out_of_range_port_is_rejected(port: str) -> None:
    with pytest.raises(ConfigError, match="PYREDIS_PORT must be between"):
        Config.from_env({"PYREDIS_PORT": port})


def test_port_zero_is_allowed_and_means_an_os_assigned_port() -> None:
    assert Config.from_env({"PYREDIS_PORT": "0"}).port == 0


def test_empty_host_is_rejected() -> None:
    with pytest.raises(ConfigError, match="PYREDIS_HOST must not be empty"):
        Config.from_env({"PYREDIS_HOST": "   "})


def test_unknown_log_level_is_rejected() -> None:
    with pytest.raises(ConfigError, match="PYREDIS_LOG_LEVEL must be one of"):
        Config.from_env({"PYREDIS_LOG_LEVEL": "TRACE"})


def test_direct_construction_is_validated_too() -> None:
    with pytest.raises(ConfigError):
        Config(port=70000)


def test_address_joins_host_and_port() -> None:
    assert Config(host="127.0.0.1", port=6380).address == "127.0.0.1:6380"


def test_config_is_immutable() -> None:
    config = Config()

    with pytest.raises(AttributeError):
        config.port = 1234  # type: ignore[misc]


# --------------------------------------------------------------------------
# Memory limits
# --------------------------------------------------------------------------


def test_memory_defaults_to_unlimited_with_no_eviction() -> None:
    config = Config.from_env({})

    assert config.maxmemory == 0
    assert config.maxmemory_policy == MaxmemoryPolicy.NOEVICTION


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param("0", 0, id="unlimited"),
        pytest.param("1024", 1024, id="bare-bytes"),
        pytest.param("64kb", 64 * 1024, id="kilobytes"),
        pytest.param("10mb", 10 * 1024**2, id="megabytes"),
        pytest.param("1gb", 1024**3, id="gigabytes"),
        pytest.param("10MB", 10 * 1024**2, id="uppercase"),
        pytest.param(" 10mb ", 10 * 1024**2, id="surrounding-space"),
    ],
)
def test_memory_sizes_use_binary_units(raw: str, expected: int) -> None:
    assert Config.from_env({"PYREDIS_MAXMEMORY": raw}).maxmemory == expected


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param("1k", id="single-letter-kilo"),
        pytest.param("1m", id="single-letter-mega"),
        pytest.param("1g", id="single-letter-giga"),
        pytest.param("1.5mb", id="fractional"),
        pytest.param("-1", id="negative"),
        pytest.param("-1mb", id="negative-with-unit"),
        pytest.param("", id="empty"),
        pytest.param("mb", id="unit-without-number"),
        pytest.param("10 mb", id="internal-space"),
        pytest.param("10tb", id="unsupported-unit"),
        pytest.param("lots", id="letters"),
    ],
)
def test_malformed_memory_sizes_are_rejected(raw: str) -> None:
    with pytest.raises(ConfigError, match="PYREDIS_MAXMEMORY must be a byte count"):
        Config.from_env({"PYREDIS_MAXMEMORY": raw})


@pytest.mark.parametrize("raw", ["noeviction", "allkeys-lru", "ALLKEYS-LRU"])
def test_the_eviction_policy_is_read_case_insensitively(raw: str) -> None:
    assert Config.from_env({"PYREDIS_MAXMEMORY_POLICY": raw}).maxmemory_policy == (
        MaxmemoryPolicy(raw.lower())
    )


@pytest.mark.parametrize("raw", ["allkeys-lfu", "volatile-lru", "random", ""])
def test_an_unsupported_eviction_policy_is_rejected(raw: str) -> None:
    with pytest.raises(ConfigError, match="PYREDIS_MAXMEMORY_POLICY must be one of"):
        Config.from_env({"PYREDIS_MAXMEMORY_POLICY": raw})


def test_a_negative_maxmemory_cannot_be_constructed_directly() -> None:
    with pytest.raises(ConfigError, match="must not be negative"):
        Config(maxmemory=-1)

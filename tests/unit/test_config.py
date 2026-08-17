from __future__ import annotations

import pytest

from pyredis.config import (
    DEFAULT_HOST,
    DEFAULT_LOG_LEVEL,
    DEFAULT_PORT,
    Config,
    ConfigError,
)


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

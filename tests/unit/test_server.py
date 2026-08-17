from __future__ import annotations

import io
import logging
from collections.abc import Iterator

import pytest

from pyredis.config import Config
from pyredis.log import configure_logging
from pyredis.server import EXIT_CONFIG_ERROR, EXIT_OK, Server, main


@pytest.fixture(autouse=True)
def restore_root_logger() -> Iterator[None]:
    root = logging.getLogger()
    handlers = root.handlers[:]
    level = root.level
    try:
        yield
    finally:
        root.handlers[:] = handlers
        root.setLevel(level)


def test_server_exposes_the_config_it_was_given() -> None:
    config = Config(port=7001)

    assert Server(config).config is config


@pytest.mark.asyncio
async def test_serve_returns_immediately_and_reports_the_endpoint() -> None:
    stream = io.StringIO()
    configure_logging("INFO", stream=stream)

    await Server(Config(host="localhost", port=7002)).serve()

    assert "localhost:7002" in stream.getvalue()


def test_run_drives_serve_to_completion() -> None:
    stream = io.StringIO()
    configure_logging("INFO", stream=stream)

    Server(Config(port=7003)).run()

    assert "shutting down" in stream.getvalue()


def test_main_exits_zero_with_a_valid_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PYREDIS_PORT", "7004")

    assert main() == EXIT_OK


def test_main_reports_invalid_configuration_without_starting(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("PYREDIS_PORT", "99999")

    assert main() == EXIT_CONFIG_ERROR
    assert "invalid configuration" in capsys.readouterr().err

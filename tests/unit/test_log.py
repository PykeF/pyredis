from __future__ import annotations

import io
import logging
from collections.abc import Iterator

import pytest

from pyredis.log import configure_logging, get_logger


@pytest.fixture(autouse=True)
def restore_root_logger() -> Iterator[None]:
    """Keep handler changes from leaking between tests."""
    root = logging.getLogger()
    handlers = root.handlers[:]
    level = root.level
    try:
        yield
    finally:
        root.handlers[:] = handlers
        root.setLevel(level)


def test_configure_logging_installs_a_single_handler_at_the_requested_level() -> None:
    configure_logging("WARNING", stream=io.StringIO())

    root = logging.getLogger()
    assert root.level == logging.WARNING
    # pytest attaches capture handlers of its own; only ours is asserted on.
    assert [h.name for h in root.handlers].count("pyredis") == 1


def test_repeated_configuration_does_not_duplicate_records() -> None:
    stream = io.StringIO()
    configure_logging("INFO", stream=io.StringIO())
    configure_logging("INFO", stream=stream)

    get_logger("pyredis.test").info("hello")

    assert stream.getvalue().count("hello") == 1


def test_records_below_the_configured_level_are_dropped() -> None:
    stream = io.StringIO()
    configure_logging("INFO", stream=stream)

    get_logger("pyredis.test").debug("invisible")

    assert stream.getvalue() == ""


def test_formatted_output_carries_level_logger_name_and_message() -> None:
    stream = io.StringIO()
    configure_logging("INFO", stream=stream)

    get_logger("pyredis.test").warning("disk %s", "full")

    line = stream.getvalue().strip()
    assert "WARNING" in line
    assert "pyredis.test" in line
    assert line.endswith("disk full")


def test_unknown_level_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown log level"):
        configure_logging("TRACE")

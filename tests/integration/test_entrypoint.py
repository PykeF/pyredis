"""End-to-end checks that the installed entry point starts and exits cleanly."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys

import pytest

TIMEOUT_SECONDS = 30


def _run(args: list[str], **env: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        capture_output=True,
        text=True,
        timeout=TIMEOUT_SECONDS,
        env={**os.environ, **env},
        check=False,
    )


def test_module_entry_point_exits_cleanly() -> None:
    result = _run([sys.executable, "-m", "pyredis"], PYREDIS_PORT="7101")

    assert result.returncode == 0
    assert "7101" in result.stderr
    assert result.stdout == ""


def test_module_entry_point_honours_log_level() -> None:
    quiet = _run([sys.executable, "-m", "pyredis"], PYREDIS_LOG_LEVEL="ERROR")
    verbose = _run([sys.executable, "-m", "pyredis"], PYREDIS_LOG_LEVEL="DEBUG")

    assert quiet.returncode == 0
    assert quiet.stderr == ""
    assert verbose.returncode == 0
    assert "Config(" in verbose.stderr


def test_module_entry_point_rejects_bad_configuration() -> None:
    result = _run([sys.executable, "-m", "pyredis"], PYREDIS_PORT="0")

    assert result.returncode == 2
    assert "invalid configuration" in result.stderr


@pytest.mark.skipif(shutil.which("pyredis") is None, reason="console script not installed")
def test_console_script_exits_cleanly() -> None:
    script = shutil.which("pyredis")
    assert script is not None

    result = _run([script], PYREDIS_PORT="7102")

    assert result.returncode == 0
    assert "7102" in result.stderr

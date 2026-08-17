"""Allow `python -m pyredis` alongside the `pyredis` console script."""

from __future__ import annotations

import sys

from pyredis.server import main

if __name__ == "__main__":
    sys.exit(main())

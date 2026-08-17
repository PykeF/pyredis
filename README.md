# PyRedis

An in-memory key-value store written from scratch in Python, built to speak the
Redis wire protocol.

> **Status: Phase 0 (foundation).** PyRedis does not store keys, speak RESP, or
> accept TCP connections yet. It is **not** Redis-compatible today — that claim
> belongs to P2, once RESP2 and real commands exist.

## Why this exists

This is a learning and portfolio project, not a Redis replacement. The point is
to build the interesting parts of a database server by hand — protocol parsing,
an async network layer, expiration, an append-only log, and bounded memory with
eviction — rather than to reimplement all of Redis. Scope is deliberately
narrow: no clustering, replication, Sentinel, Pub/Sub, Streams, Lua, or ACLs.

## Planned architecture

```text
        TCP socket (asyncio)          P2
                |
        RESP2 codec                   P2
                |
        command dispatch              P2
                |
        keyspace  ──  expiration      P1 / P3
                |
        AOF append + replay           P4
                |
        memory accounting + eviction  P5
```

Design constraints carried through every phase:

- Python 3.12+, `asyncio` for networking. No web framework, no HTTP layer.
- Keys and values are `bytes`; nothing assumes UTF-8.
- Standard library first — third-party runtime dependencies must earn their place.

## Phases

| Phase | Scope                                   | Status         |
| ----- | --------------------------------------- | -------------- |
| P0    | Foundation: config, logging, entry point | **Implemented** |
| P1    | Core in-memory key-value store           | Planned        |
| P2    | RESP2 protocol + async TCP server        | Planned        |
| P3    | TTL / expiration                         | Planned        |
| P4    | AOF persistence and recovery             | Planned        |
| P5    | Memory limits and eviction               | Planned        |

## What P0 actually implements

- `pyredis.config` — an immutable, validated `Config` (host, port, log level)
  loaded from `PYREDIS_`-prefixed environment variables, with defaults.
- `pyredis.log` — one stderr log handler in a fixed format, configured once
  at startup.
- `pyredis.server` — the process lifecycle: load config, configure logging,
  enter the asyncio run loop, exit cleanly. `Server.serve()` is already a
  coroutine so P2 can drop a listener into it without reshaping startup.

## Configuration

| Variable            | Default     | Meaning                                        |
| ------------------- | ----------- | ---------------------------------------------- |
| `PYREDIS_HOST`      | `127.0.0.1` | Interface to bind (used from P2 onwards)       |
| `PYREDIS_PORT`      | `6380`      | TCP port — avoids a local Redis on 6379        |
| `PYREDIS_LOG_LEVEL` | `INFO`      | `DEBUG`/`INFO`/`WARNING`/`ERROR`/`CRITICAL`    |

Invalid values fail at startup with exit code `2` and a message on stderr.

See [.env.example](.env.example). PyRedis reads the process environment and does
not parse `.env` files itself; load one with `uv run --env-file .env pyredis`.

## Getting started

Requires [uv](https://docs.astral.sh/uv/) and Python 3.12+.

```bash
uv sync
```

Run the application:

```bash
uv run pyredis
```

In P0 this prints its resolved configuration and exits `0` — it does **not**
open a socket.

## Development

Run the tests:

```bash
uv run pytest
```

Lint and type-check:

```bash
uv run ruff check .
```

```bash
uv run mypy src
```

Layout:

```text
src/pyredis/     application package (src layout — tests run against the installed package)
tests/unit/      fast, in-process tests
tests/integration/  subprocess tests of the real entry point
```

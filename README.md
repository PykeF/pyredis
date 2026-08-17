# PyRedis

An in-memory key-value store written from scratch in Python, built to speak the
Redis wire protocol.

> **Status: Phase 1 (core in-memory store).** The keyspace works as a Python
> library, but PyRedis does not speak RESP or accept TCP connections yet, so no
> Redis client can talk to it. It is **not** Redis-compatible today — that claim
> belongs to P2, once RESP2 and a command layer exist.

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
| P1    | Core in-memory key-value store           | **Implemented** |
| P2    | RESP2 protocol + async TCP server        | Planned        |
| P3    | TTL / expiration                         | Planned        |
| P4    | AOF persistence and recovery             | Planned        |
| P5    | Memory limits and eviction               | Planned        |

## What is actually implemented

**P0 — foundation**

- `pyredis.config` — an immutable, validated `Config` (host, port, log level)
  loaded from `PYREDIS_`-prefixed environment variables, with defaults.
- `pyredis.log` — one stderr log handler in a fixed format, configured once
  at startup.
- `pyredis.server` — the process lifecycle: load config, configure logging,
  enter the asyncio run loop, exit cleanly. `Server.serve()` is already a
  coroutine so P2 can drop a listener into it without reshaping startup.

**P1 — core in-memory store**

`pyredis.store.KeyValueStore` is a synchronous, in-process keyspace holding
scalar values. It depends on nothing else in PyRedis and nothing in `asyncio`.

| Operation | Method | Returns |
| --------- | ------ | ------- |
| SET     | `set(key, value)`  | `None` — unconditional overwrite |
| GET     | `get(key)`         | the value, or `None` if the key is not set |
| DEL     | `delete(*keys)`    | how many of the given keys existed |
| EXISTS  | `exists(*keys)`    | how many are set, counting repeated keys each time |
| INCR    | `incr(key)`        | the new value; a missing key starts from 0 |
| DBSIZE  | `dbsize()`         | number of keys |
| FLUSHDB | `flushdb()`        | `None` — removes every key |

Keys and values are **binary-safe `bytes`** internally: any byte sequence —
including NUL bytes and data that is not valid UTF-8 — is stored and returned
unchanged, and keys collide only on exact byte equality. Nothing decodes to
`str`, so no encoding is ever assumed.

`INCR` follows Redis' integer rules: values must be canonical decimal within
the signed 64-bit range, so `+1`, `01`, `-0`, and surrounding whitespace are
rejected with `NotAnIntegerError`, and exceeding the range raises
`IntegerOverflowError`. A failed `INCR` leaves the stored value untouched.

The store is deliberately **not** thread-safe and takes no locks; from P2 it is
owned by a single event-loop thread, which is what makes each operation atomic.

**Not implemented yet:** RESP2 parsing/serialization, TCP networking, a command
dispatcher, TTL/expiration, AOF persistence, and eviction. There is no way to
reach the store over a socket — it is importable Python only. Multiple
databases, `SELECT`, `SET` options such as `NX`/`EX`, and every non-scalar
Redis type are also out of scope so far.

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

This logs its resolved configuration and exits `0`. It does **not** open a
socket, and it does not yet start a keyspace — until P2 wires the two together,
the store is reachable only from Python:

```python
from pyredis.store import KeyValueStore

store = KeyValueStore()
store.set(b"greeting", b"hello")
store.get(b"greeting")   # b"hello"
store.incr(b"visits")    # 1
```

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

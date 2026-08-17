# PyRedis

An in-memory key-value store written from scratch in Python, built to speak the
Redis wire protocol.

> **Status: Phase 2 (RESP2 + TCP server).** PyRedis now speaks RESP2 over raw
> TCP, so real Redis clients such as `redis-cli -p 6380` can drive it — but only
> for the eight commands listed below. This is **not** general Redis
> compatibility: there is no RESP3, no TTL, no persistence, and no eviction, and
> the vast majority of Redis commands are simply unknown.

## Why this exists

This is a learning and portfolio project, not a Redis replacement. The point is
to build the interesting parts of a database server by hand — protocol parsing,
an async network layer, expiration, an append-only log, and bounded memory with
eviction — rather than to reimplement all of Redis. Scope is deliberately
narrow: no clustering, replication, Sentinel, Pub/Sub, Streams, Lua, or ACLs.

## Architecture

```text
        TCP socket (asyncio)          server.py      done
                |
        RESP2 codec                   resp.py        done
                |
        command dispatch              commands.py    done
                |
        keyspace  ──  expiration      store.py       done / P3
                |
        AOF append + replay                          P4
                |
        memory accounting + eviction                 P5
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
| P2    | RESP2 protocol + async TCP server        | **Implemented** |
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
  run the event loop, exit cleanly with a meaningful exit code.

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

The store is deliberately **not** thread-safe and takes no locks; it is owned by
a single event-loop thread, which is what makes each operation atomic.

**P2 — RESP2 protocol and TCP server**

- `pyredis.resp` — RESP2 framing. Requests are read straight from an
  `asyncio.StreamReader`, so partial frames, fragmented delivery, and several
  commands arriving in one packet are all handled by construction; pipelining
  falls out of correct framing rather than being a special case.
- `pyredis.commands` — the command table: lookup, arity, and the translation of
  store errors into RESP errors.
- `pyredis.connection` — one client session: read, dispatch, reply, repeat.
- `pyredis.server` — `asyncio.start_server`, one task per client, and a
  shutdown that stops accepting and hangs up on live connections.

Supported commands, and exactly what each answers:

| Command | Reply |
| ------- | ----- |
| `PING` | `+PONG` |
| `PING message` | the message, as a bulk string |
| `SET key value` | `+OK`. **No options** — `EX`/`PX`/`NX`/`XX` are not accepted |
| `GET key` | the value as a bulk string, or a null bulk string if unset |
| `DEL key [key ...]` | integer: how many keys were removed |
| `EXISTS key [key ...]` | integer: how many exist, counting repeats |
| `INCR key` | integer: the new value |
| `DBSIZE` | integer: number of keys |
| `FLUSHDB` | `+OK` |

Everything is binary-safe end to end: bulk strings carry arbitrary bytes, so
keys and values containing NUL, CRLF, or non-UTF-8 data survive the round trip
unchanged. Only the command name is uppercased, and only to find the handler.

Errors are Redis-shaped and never expose Python internals:

```
-ERR unknown command 'COMMAND', with args beginning with: 'DOCS',
-ERR wrong number of arguments for 'set' command
-ERR value is not an integer or out of range
-ERR Protocol error: expected '*', got '+'
```

Unknown commands, wrong arity, and store errors leave the connection usable. A
protocol error desynchronizes the byte stream, so it is answered and then the
connection is closed.

### Compatibility boundaries

PyRedis targets real RESP2 clients **for the eight commands above, and nothing
more**. Concretely:

- **RESP2 only.** RESP3 is not implemented and `HELLO` is not a command, so
  `redis-cli -3` will not negotiate. Plain `redis-cli` never sends `HELLO` and
  works normally.
- **Unknown commands are answered with an error, not a disconnect.** This is
  what keeps an interactive `redis-cli` session working: it sends
  `COMMAND DOCS` on connect to build completion hints, sees the error, and
  carries on.
- **No inline (telnet-style) commands.** Requests must be RESP arrays, so
  `redis-cli` works but ad-hoc `nc` typing does not.
- **No `AUTH`, `SELECT`, `INFO`, `COMMAND`, `CONFIG`, or transactions**, one
  implicit database, and scalar byte values only.
- **Request limits** are PyRedis' own safety bounds, not Redis' values: at most
  1,048,576 elements per request and 64 MiB per bulk string. Configurable
  resource limits belong to P5.

**Not implemented yet:** TTL/expiration and `EXPIRE`, AOF persistence and
recovery, memory limits and eviction, RESP3, replication, clustering, Pub/Sub,
transactions, authentication, Lua, and every Redis type other than scalar byte
values.

## Configuration

| Variable            | Default     | Meaning                                        |
| ------------------- | ----------- | ---------------------------------------------- |
| `PYREDIS_HOST`      | `127.0.0.1` | Interface to bind                              |
| `PYREDIS_PORT`      | `6380`      | TCP port — avoids a local Redis on 6379        |
| `PYREDIS_LOG_LEVEL` | `INFO`      | `DEBUG`/`INFO`/`WARNING`/`ERROR`/`CRITICAL`    |

Port `0` is accepted and asks the OS for a free port, which the startup log then
reports. (That is not Redis' meaning for port 0, which is "do not listen".)

Exit codes: `0` on a clean shutdown, `1` if the port cannot be bound, `2` for
invalid configuration.

See [.env.example](.env.example). PyRedis reads the process environment and does
not parse `.env` files itself; load one with `uv run --env-file .env pyredis`.

## Getting started

Requires [uv](https://docs.astral.sh/uv/) and Python 3.12+.

```bash
uv sync
```

Start the server — it listens on `127.0.0.1:6380` until you press Ctrl-C:

```bash
uv run pyredis
```

Then talk to it with a real Redis client:

```bash
redis-cli -p 6380
```

```text
127.0.0.1:6380> PING
PONG
127.0.0.1:6380> SET name Pyke
OK
127.0.0.1:6380> GET name
"Pyke"
127.0.0.1:6380> INCR counter
(integer) 1
127.0.0.1:6380> DBSIZE
(integer) 2
127.0.0.1:6380> FLUSHDB
OK
```

The keyspace is also usable directly as a library, with no server involved:

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

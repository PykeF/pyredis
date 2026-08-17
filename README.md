# PyRedis

An in-memory key-value store written from scratch in Python, built to speak the
Redis wire protocol.

> **Status: Phase 4 (AOF persistence).** PyRedis speaks RESP2 over raw TCP with
> key expiration and optional durability, so real Redis clients such as
> `redis-cli -p 6380` can drive it — but only for the eleven commands listed
> below. This is **not** general Redis compatibility: there is no RESP3, no
> eviction, no AOF rewrite, and the vast majority of Redis commands are simply
> unknown.

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
        keyspace  ──  expiration      store.py       done
                |
        AOF append + replay           aof.py         done
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
| P3    | TTL / expiration                         | **Implemented** |
| P4    | AOF persistence and recovery             | **Implemented** |
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
| `SET key value [EX s \| PX ms]` | `+OK`. Only those two options; no `NX`/`XX`/`KEEPTTL` |
| `GET key` | the value as a bulk string, or a null bulk string if unset |
| `DEL key [key ...]` | integer: how many keys were removed |
| `EXISTS key [key ...]` | integer: how many exist, counting repeats |
| `INCR key` | integer: the new value |
| `EXPIRE key seconds` | integer: `1` if a deadline was set, `0` if the key is gone |
| `TTL key` | integer: seconds left, `-1` if no deadline, `-2` if no key |
| `PERSIST key` | integer: `1` if a deadline was removed, else `0` |
| `DBSIZE` | integer: number of **live** keys |
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

**P3 — expiration**

Deadlines are **absolute Unix timestamps in integer milliseconds**, held in a
dictionary separate from the values. Absolute rather than relative so a deadline
keeps its meaning across a restart — which is what P4 will have to persist — and
integers so boundary comparisons and TTL arithmetic are exact. The trade-off of
a wall clock is that a system time jump moves every deadline with it.

A key expires **when the clock reaches its deadline**: at `now == deadline` it is
already gone, so `GET` returns nil, `EXISTS` returns `0`, and `TTL` returns `-2`.
`TTL` rounds to the nearest second (`(remaining_ms + 500) // 1000`), so a key set
with `EX 2` answers `2` immediately afterwards rather than `1`.

Expiration runs two ways, as Redis does:

- **Lazily** — `GET`, `DEL`, `EXISTS`, `INCR`, `EXPIRE`, `TTL`, `PERSIST`, and
  `DBSIZE` drop an expired key before answering, so no command ever reports a
  key that should be gone.
- **Actively** — an asyncio task on the same event loop sweeps every **100 ms**,
  examining at most **100 keys carrying a deadline** per cycle via a round-robin
  cursor. Bounded on purpose: it never scans the whole keyspace in one tick, and
  it reclaims keys nobody reads, which lazy expiration alone would leak forever.
  Reclamation is therefore eventual, while correctness is immediate. There is no
  thread — the sweep is synchronous and can only run between commands.

How expiration interacts with the other commands:

| Rule |
| --- |
| A plain `SET` on a key with a TTL **clears** the TTL |
| `SET … EX/PX` replaces any existing TTL |
| `INCR` **preserves** the TTL; a failed `INCR` changes neither value nor TTL |
| `DEL` and `FLUSHDB` remove the expiration metadata with the key |
| `EXPIRE key 0` or a negative time **deletes the key** and answers `1` |
| `SET k v EX 0` is an **error** — `invalid expire time in 'set' command` |

**`DBSIZE` counts only logically live keys**, cleaning up expired ones as it
counts. This deliberately differs from Redis, which reports keys it has not yet
reclaimed and can therefore claim a key exists that every other command says is
gone. The cost is proportional to the number of keys with deadlines, not to the
size of the keyspace.

**Not implemented:** `PTTL`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `GETEX`,
`SET … KEEPTTL/NX/XX/GET`, and keyspace notifications. They return unknown-command
or syntax errors rather than pretending to work.

**P4 — append-only persistence**

Off by default. Enable it with `PYREDIS_AOF_ENABLED=true`, and every mutation is
appended to a log that is replayed at startup.

Records are RESP2 arrays — the same framing as the wire protocol, so binary keys
and values need no escaping — but the recorded vocabulary is deliberately a
**superset** of the client vocabulary. Commands whose meaning depends on *when*
they ran are rewritten into an absolute form; everything else is recorded as
issued:

| Command | Recorded as |
| ------- | ----------- |
| `SET k v` | `SET k v` |
| `SET k v EX 60` / `PX 5000` | `SET k v PXAT <absolute ms>` |
| `EXPIRE k 60` | `PEXPIREAT k <absolute ms>` |
| `EXPIRE k 0` (deletes) | `DEL k` |
| `INCR k` | `SET k <resulting value>` [`PXAT …`] |
| `DEL`, `PERSIST`, `FLUSHDB` | as issued |
| reads, errors, and no-ops | nothing at all |

`PXAT` and `PEXPIREAT` exist only inside the file; no client can send them.
Because deadlines are recorded as absolute timestamps, **a key written with
`EX 60` comes back with whatever is left of that minute, never a fresh one**,
and a key whose deadline passed during downtime is simply gone on startup.

`INCR` is recorded as the value it produced rather than as an increment, so
every record is a statement of fact rather than a computation and replay cannot
fail on the data.

**Recovery** happens before the listener binds, so no client ever sees a
half-loaded keyspace. Replay uses lower-level store primitives rather than the
command dispatcher, and **expiration is switched off while it runs** — otherwise
a stale deadline applied mid-replay would delete a key that a later `PERSIST`
was about to save. One sweep afterwards drops whatever really did expire.

**Damaged files** are treated differently depending on where the damage is:

| File state | Startup |
| ---------- | ------- |
| Intact | Loads |
| Final record torn by a crash mid-append | **Repaired** — truncated to the last good record, with a warning |
| Corruption anywhere earlier | **Refuses to start**, naming the byte offset |

The asymmetry is deliberate: a torn tail is the normal result of losing power
mid-write, while corruption in the middle would mean silently discarding every
write that followed it.

**Durability** is controlled by `PYREDIS_AOF_FSYNC`. Every append flushes to the
operating system regardless of policy, so killing the process loses nothing —
the policy only decides how much a *power* failure can take:

| Policy | Loses at most | Cost |
| ------ | ------------- | ---- |
| `always` | one unacknowledged write | an fsync per write; **stalls the event loop** on every write |
| `everysec` *(default)* | ~1 second of writes | one blocking fsync per second |
| `no` | whatever the OS hasn't flushed | none |

fsync runs on the event loop, with no thread and no lock — that is what keeps
every store operation atomic without synchronisation, and it is why `always` is
slow rather than merely expensive.

**When persistence fails** (a full disk, an I/O error), the journal enters a
failed state and stays there:

- the command that hit the failure is answered `-ERR persistence failure`; its
  mutation has already happened in memory and is **not** rolled back
- every later mutation is refused *before* it touches the store, so nothing new
  happens in memory that could never reach disk
- reads (`GET`, `EXISTS`, `TTL`, `DBSIZE`, `PING`) keep working
- the state never clears itself — **restarting the server is the recovery
  mechanism**

**Not implemented:** AOF rewrite/compaction, so the log grows without bound and
an `INCR`-heavy workload makes for a slow reload; RDB snapshots; a format
version header; per-record checksums; and streaming replay (the file is read
into memory at startup).

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

**Not implemented yet:** memory limits and eviction (P5), RESP3, replication,
clustering, Pub/Sub, transactions, authentication, Lua, and every Redis type
other than scalar byte values.

## Configuration

| Variable            | Default     | Meaning                                        |
| ------------------- | ----------- | ---------------------------------------------- |
| `PYREDIS_HOST`      | `127.0.0.1` | Interface to bind                              |
| `PYREDIS_PORT`      | `6380`      | TCP port — avoids a local Redis on 6379        |
| `PYREDIS_LOG_LEVEL` | `INFO`      | `DEBUG`/`INFO`/`WARNING`/`ERROR`/`CRITICAL`    |
| `PYREDIS_AOF_ENABLED` | `false`   | Append every mutation to a log and replay it at startup |
| `PYREDIS_AOF_PATH`  | `pyredis.aof` | Where that log lives                       |
| `PYREDIS_AOF_FSYNC` | `everysec`  | `always` / `everysec` / `no`                   |

Port `0` is accepted and asks the OS for a free port, which the startup log then
reports. (That is not Redis' meaning for port 0, which is "do not listen".)

Exit codes: `0` on a clean shutdown, `1` if the port cannot be bound, `2` for
invalid configuration, `3` if the append-only file cannot be read, repaired, or
opened.

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
127.0.0.1:6380> SET session token EX 60
OK
127.0.0.1:6380> TTL session
(integer) 60
127.0.0.1:6380> PERSIST session
(integer) 1
127.0.0.1:6380> DBSIZE
(integer) 3
127.0.0.1:6380> FLUSHDB
OK
```

To keep the data across restarts, turn on the append-only file:

```bash
PYREDIS_AOF_ENABLED=true uv run pyredis
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

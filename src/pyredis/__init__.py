"""PyRedis: an in-memory key-value server written from scratch in Python.

Speaks RESP2 over raw TCP for a small, deliberate subset of Redis: scalar byte
values, TTL expiration, append-only persistence, and bounded memory with LRU
eviction.

The modules layer strictly downwards. `store` owns data semantics and imports
nothing else; `resp` owns the wire format; `commands` maps one onto the other;
`connection` and `server` own sockets and process lifecycle.
"""

__version__ = "1.0.0"

__all__ = ["__version__"]

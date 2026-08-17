"""One client session: read a command, dispatch it, write the reply, repeat.

The only module that touches `StreamReader`/`StreamWriter`. It decides how each
class of failure ends: a protocol error desynchronizes the stream and closes
the connection, while command and store errors are ordinary replies that leave
it usable.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress

from pyredis import resp
from pyredis.aof import NO_JOURNAL, Journal
from pyredis.commands import dispatch
from pyredis.log import get_logger
from pyredis.store import KeyValueStore

_logger = get_logger(__name__)


async def handle_connection(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    store: KeyValueStore,
    journal: Journal = NO_JOURNAL,
) -> None:
    """Serve one client until it disconnects or desynchronizes the stream."""
    peer = peer_name(writer)
    while True:
        try:
            request = await resp.read_command(reader)
        except resp.ProtocolError as exc:
            _logger.debug("protocol error from %s: %s", peer, exc)
            with suppress(ConnectionError):
                writer.write(resp.encode_error(f"ERR Protocol error: {exc}"))
                await writer.drain()
            return
        except ConnectionError:
            _logger.debug("%s disconnected mid-frame", peer)
            return

        if request is None:
            return
        if not request:
            continue

        try:
            reply = dispatch(store, request, journal)
        except Exception:
            # Boundary catch: a bug must not reach the client as a traceback,
            # and it has not desynchronized the stream, so the session goes on.
            _logger.exception("error executing command from %s", peer)
            reply = resp.encode_error("ERR internal error")

        try:
            writer.write(reply)
            await writer.drain()
        except ConnectionError:
            _logger.debug("%s disconnected while writing", peer)
            return


def peer_name(writer: asyncio.StreamWriter) -> str:
    return str(writer.get_extra_info("peername"))

"""Autoclose sweeper: closes due and long-idle sessions through the discard path."""

from __future__ import annotations

import asyncio
import logging

from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.db import sessions

log = logging.getLogger(__name__)

AUTOCLOSE_POLL_SECONDS = 10


async def sweep(pool: AsyncConnectionPool, close_grace_seconds: int) -> None:
    """Run close_expired and discard_idle each poll until cancelled; a failed pass is retried.
    discard_idle logs each session it flips (idle discard or self-close)."""
    while True:
        try:
            async with pool.connection() as conn:
                for session_id in await sessions.close_expired(conn):
                    log.info("autoclose: session %s is due, discarding", session_id)
                await sessions.discard_idle(conn, close_grace_seconds)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("autoclose pass failed; retrying next poll")
        await asyncio.sleep(AUTOCLOSE_POLL_SECONDS)

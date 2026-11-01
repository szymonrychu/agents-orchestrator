"""Autoclose sweeper: closes due and long-idle sessions through the discard path."""

from __future__ import annotations

import asyncio
import logging

from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.db import sessions

log = logging.getLogger(__name__)

AUTOCLOSE_POLL_SECONDS = 10


async def sweep(pool: AsyncConnectionPool) -> None:
    """Run close_expired and discard_idle each poll until cancelled; a failed pass is retried."""
    while True:
        try:
            async with pool.connection() as conn:
                for session_id in await sessions.close_expired(conn):
                    log.info("autoclose: session %s is due, discarding", session_id)
                for session_id in await sessions.discard_idle(conn):
                    log.info("idle discard: session %s has been idle long enough", session_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("autoclose pass failed; retrying next poll")
        await asyncio.sleep(AUTOCLOSE_POLL_SECONDS)

"""api.project_post_sweep: the retention sweeper for project room posts (T-117).

Hourly it deletes posts older than `project_chat_retention_days`, in batches that each commit on
their own. The api sweeper is the only deleter of `project_posts`."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from agents_orchestrator.db import project_posts

if TYPE_CHECKING:
    from fastapi import FastAPI
    from psycopg_pool import AsyncConnectionPool

    from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)

SWEEP_SECONDS = 3600
BATCH = 1000


async def sweep_once(pool: AsyncConnectionPool, settings: Settings) -> int:
    """Delete expired posts batch by batch; returns the total deleted."""
    total = 0
    while True:
        async with pool.connection() as conn, conn.transaction():
            gone = await project_posts.sweep_older(
                conn, days=settings.project_chat_retention_days, batch=BATCH
            )
        total += gone
        if gone < BATCH:
            return total


async def sweep(app: FastAPI) -> None:
    """Until cancelled: a pass every `SWEEP_SECONDS`; an error is logged and the loop goes on."""
    while True:
        try:
            await sweep_once(app.state.pool, app.state.settings)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("project post sweep failed; retrying next pass")
        await asyncio.sleep(SWEEP_SECONDS)

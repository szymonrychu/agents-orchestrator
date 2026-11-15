"""Credential expiry notices: one taskboard task per threshold, posted by the api.

Each pass picks the current phase when it is warn, refuse or expired (only the current one: a jump
past warn does not post warn) plus `auth_failed` when a live agent reports a failed login. A
threshold is posted once per credential end date (its `client_key` also makes a retry idempotent
on the taskboard); a failed post is retried on the next pass. The set of posted keys is in memory,
so a restart re-posts at most once more, which the taskboard answers as a duplicate.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

import httpx
from fastapi import FastAPI
from psycopg_pool import AsyncConnectionPool

from agents_orchestrator import credential, taskboard_client
from agents_orchestrator.db import status
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)


async def sweep_once(
    pool: AsyncConnectionPool,
    settings: Settings,
    now: datetime,
    posted: set[str],
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> list[str]:
    """One pass; the keys posted in it."""
    if not settings.taskboard_available or not settings.credential_notice_project:
        return []
    expires_at = settings.claude_credential_expires_at
    current = credential.phase(
        now, expires_at, settings.credential_warn_days, settings.credential_refuse_hours
    )
    thresholds: list[credential.Threshold] = []
    if current in ("warn", "refuse", "expired"):
        thresholds.append(current)
    async with pool.connection() as conn:
        if await status.any_auth_failed(conn):
            thresholds.append("auth_failed")
    done: list[str] = []
    for threshold in thresholds:
        key = credential.notice_key(threshold, expires_at)
        if key in posted:
            continue
        title, body = credential.notice_text(
            threshold, expires_at, settings.credential_warn_days, settings.credential_refuse_hours
        )
        if await taskboard_client.post_notice(settings, key, title, body, transport=transport):
            posted.add(key)
            done.append(key)
    return done


async def sweep(app: FastAPI) -> None:
    """Until cancelled: a pass every `credential_sweep_seconds`; a failed pass is logged."""
    settings: Settings = app.state.settings
    posted: set[str] = set()
    while True:
        try:
            await sweep_once(app.state.pool, settings, datetime.now(UTC), posted)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("credential sweep failed: %s", type(exc).__name__)
        await asyncio.sleep(settings.credential_sweep_seconds)

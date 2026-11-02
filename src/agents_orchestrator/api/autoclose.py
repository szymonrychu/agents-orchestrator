"""Autoclose sweeper: closes due, inactive and closing sessions through the discard path, and
reminds quiet ones (hive-inactivity-limits)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime

from psycopg_pool import AsyncConnectionPool

from agents_orchestrator import inactivity
from agents_orchestrator.api import sessions as sessions_api
from agents_orchestrator.db import sessions
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)

AUTOCLOSE_POLL_SECONDS = 10


async def sweep(pool: AsyncConnectionPool, settings: Settings) -> None:
    """Each poll until cancelled, in order: close due sessions, release inactivity closes an
    inbound message answered, discard inactivity closes past their deadline, request inactivity
    closes, send reminders, discard idle closing sessions. A failed pass is retried. The limits
    are passed on every poll, so they apply to running sessions."""
    while True:
        try:
            async with pool.connection() as conn:
                for session_id in await sessions.close_expired(conn):
                    log.info("autoclose: session %s is due, discarding", session_id)
                await sessions.release_inactive_close(conn)
                await sessions.close_overdue(conn)
                await sessions.close_inactive(
                    conn,
                    settings.inactivity_close_seconds,
                    settings.inactivity_handoff_seconds,
                    _close_notice(settings),
                )
                await sessions.remind_inactive(
                    conn, settings.inactivity_reminder_seconds, _reminder_notice(settings)
                )
                await sessions.discard_idle(conn, settings.close_grace_seconds)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("autoclose pass failed; retrying next poll")
        await asyncio.sleep(AUTOCLOSE_POLL_SECONDS)


def _reminder_notice(settings: Settings) -> Callable[[sessions.Session, datetime, int], str]:
    def compose(session: sessions.Session, last_update: datetime, pods: int) -> str:
        return inactivity.reminder_text(
            last_update=last_update,
            now=datetime.now(UTC),
            limits=inactivity.effective(session.spec, settings),
            pods=pods,
            chat=settings.chat_available,
            taskboard=settings.taskboard_available,
            handoff_seconds=settings.inactivity_handoff_seconds,
        )

    return compose


def _close_notice(settings: Settings) -> Callable[[sessions.Session, datetime, datetime], str]:
    def compose(session: sessions.Session, last_update: datetime, deadline: datetime) -> str:
        assert session.close_requested_at is not None
        return inactivity.close_text(
            quiet_seconds=int((session.close_requested_at - last_update).total_seconds()),
            deadline=deadline,
            grace_seconds=settings.close_grace_seconds,
            chat=settings.chat_available,
            taskboard=settings.taskboard_available,
            workdir_saved=sessions_api._backup(session, settings).workdir,
        )

    return compose

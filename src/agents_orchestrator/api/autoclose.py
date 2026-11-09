"""Autoclose sweeper: delivers handover notices (voluntary-handover), closes due, inactive and
closing sessions through the discard path, and reminds quiet ones (hive-inactivity-limits)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from agents_orchestrator import inactivity, taskboard_client
from agents_orchestrator.api import sessions as sessions_api
from agents_orchestrator.db import handover, sessions
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)

AUTOCLOSE_POLL_SECONDS = 10


async def sweep(pool: AsyncConnectionPool, settings: Settings) -> None:
    """Each poll until cancelled, in order: deliver handover notices to new leaders, close due
    sessions, release inactivity closes an inbound message answered, discard inactivity closes
    past their deadline, request inactivity closes, send reminders, discard idle closing
    sessions. Each step runs on its own connection
    and a step that fails is logged and skipped: the other steps still run, and the failed one
    is retried next poll. The limits are passed on every poll, so they apply to running
    sessions."""

    async def close_expired(conn: AsyncConnection) -> None:
        for session_id in await sessions.close_expired(conn):
            log.info("autoclose: session %s is due, discarding", session_id)

    steps: list[tuple[str, Callable[[AsyncConnection], Awaitable[object]]]] = [
        (
            "deliver_handover",
            lambda conn: handover.deliver_handover(
                conn,
                budget_bytes=settings.handover_transcript_bytes,
                output_chars=settings.handover_output_chars,
                decisions=lambda session_id, slug, projects: taskboard_client.open_decisions(
                    settings, session_id, slug, projects
                ),
            ),
        ),
        ("close_expired", close_expired),
        ("release_inactive_close", sessions.release_inactive_close),
        ("close_overdue", sessions.close_overdue),
        (
            "close_inactive",
            lambda conn: sessions.close_inactive(
                conn,
                settings.inactivity_close_seconds,
                settings.inactivity_handoff_seconds,
                _close_notice(settings),
            ),
        ),
        (
            "remind_inactive",
            lambda conn: sessions.remind_inactive(
                conn, settings.inactivity_reminder_seconds, _reminder_notice(settings)
            ),
        ),
        ("discard_idle", lambda conn: sessions.discard_idle(conn, settings.close_grace_seconds)),
    ]
    while True:
        for name, step in steps:
            try:
                async with pool.connection() as conn:
                    await step(conn)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("autoclose step %s failed; retrying next poll", name)
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
        if session.close_requested_at is None:
            raise ValueError(f"session {session.id} has no close request to compose a notice for")
        return inactivity.close_text(
            quiet_seconds=int((session.close_requested_at - last_update).total_seconds()),
            deadline=deadline,
            grace_seconds=settings.close_grace_seconds,
            chat=settings.chat_available,
            taskboard=settings.taskboard_available,
            workdir_saved=sessions_api.session_backup(session, settings).workdir,
        )

    return compose

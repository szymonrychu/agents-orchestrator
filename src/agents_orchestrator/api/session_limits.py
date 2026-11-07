"""Limits and the reply right shared by `session_send` and the chat relay."""

from __future__ import annotations

import uuid
from typing import Any

from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.db import queue as q
from agents_orchestrator.db import session_messaging
from agents_orchestrator.db.sessions import Session
from agents_orchestrator.settings import Settings

RATE_WINDOW_SECONDS = 60
PAIR_WINDOW_SECONDS = 3600


async def may_message(conn: Any, own: Session, target: Session, settings: Settings) -> bool:
    """True when either end is a superleader, or the target messaged `own` within the reply
    window (an answer needs a question, so only target -> own opens the right)."""
    if own.spec.session_messaging or target.spec.session_messaging:
        return True
    return await session_messaging.sent_to_within(
        conn,
        sender=target.id,
        receiver=own.id,
        seconds=settings.session_reply_window_seconds,
    )


async def check_limits(
    conn: Any, agent_id: uuid.UUID, own: Session, target: Session, settings: Settings
) -> int:
    """The hop, rate, pair and target-queue checks; raises ApiError, returns the new hop."""
    previous = await q.turn_hop(conn, agent_id)
    hop = previous + 1 if previous is not None else 1
    if hop > settings.session_message_max_hops:
        raise ApiError(
            409,
            "hop_limit",
            f"hop {hop} exceeds the maximum of {settings.session_message_max_hops}",
        )
    rate = settings.session_message_rate_per_minute
    if await q.count_sent_by(conn, own.id, RATE_WINDOW_SECONDS) >= rate:
        raise ApiError(429, "rate_limited", f"at most {rate} messages per minute")
    pair = settings.session_message_pair_max_per_hour
    if await q.count_between(conn, own.id, target.id, PAIR_WINDOW_SECONDS) >= pair:
        raise ApiError(429, "pair_budget", f"at most {pair} messages per hour between two sessions")
    cap = settings.session_message_queued_max
    if await q.count_queued_session_messages(conn, target.id) >= cap:
        raise ApiError(
            409, "target_queue_full", f"session {target.slug} holds {cap} queued session messages"
        )
    return hop

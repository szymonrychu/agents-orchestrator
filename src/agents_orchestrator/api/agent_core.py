"""The request-free core of the in-hive `<agent-send>` (S-71), called by the envelope relay
worker (`envelope_outbox/worker.py`).

It resolves `to` among the live agents of the sender's own session and queues one
`channel = 'agent'` message in one (savepoint-compatible) transaction, so a refusal writes
nothing. The row has no `sender_session_id`: no session-keyed counter, activity query, unhandled
list or delivery-time scope check sees it. `to = "leader"` stays untargeted, so the claim's
leader routing picks the leader current at dispatch (a handover in between is followed). A
refusal is an `ApiError`.
"""

from __future__ import annotations

from typing import Any

from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import leadership
from agents_orchestrator.db import queue as q
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.settings import Settings

LEADER = "leader"


async def agent_send_core(
    conn: Any,
    own: sessions_repo.Session,
    sender: agents_repo.Agent,
    to: str,
    text: str,
    *,
    settings: Settings,
    lane: str,
    notify: str | None,
    hop: int,
    envelope_ref: str | None = None,
) -> int:
    """Queue `text` from `sender` to the hive member `to` (a pod name or `leader`); returns the
    message id. `hop` is the hop of the sender's turn, carried unchanged inside the hive."""
    async with conn.transaction():
        if own.desired == "deleting":
            raise ApiError(409, "session_deleting", f"session {own.slug} is being discarded")
        if own.spec.alone:
            raise ApiError(404, "unknown_agent", "this session runs no other agents")
        target_id = None
        if to == LEADER:
            leader = await leadership.current_leader(conn, own.id)
            if leader is not None and leader.agent_id == sender.id:
                raise ApiError(422, "self_send", "the leader cannot send a message to itself")
        elif to == sender.pod_name:
            raise ApiError(422, "self_send", "an agent cannot send a message to itself")
        else:
            members = await agents_repo.list_members(conn, own.id)
            target_id = next((m["id"] for m in members if m["pod_name"] == to), None)
            if target_id is None:
                raise ApiError(404, "unknown_agent", f"agent {to} is not a member of this hive")
        if await q.count_agent_sent_by(conn, sender.id, 60) >= (
            settings.agent_message_rate_per_minute
        ):
            raise ApiError(429, "rate_limited", "too many hive member messages in the last minute")
        if await q.count_queued_agent_messages(conn, own.id, target_id) >= (
            settings.agent_message_queued_max
        ):
            raise ApiError(409, "target_queue_full", f"too many messages already queued for {to}")
        try:
            return await q.enqueue_message(
                conn,
                own.id,
                text,
                target_id,
                sender_slug=sender.pod_name,
                hop=hop,
                lane=lane,
                channel="agent",
                sender_agent_id=sender.id,
                notify=notify,
                envelope_ref=envelope_ref,
            )
        except q.SessionBeingDeleted as exc:
            raise ApiError(409, "session_deleting", "the session is being discarded") from exc

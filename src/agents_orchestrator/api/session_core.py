"""The request-free core of `session_send`, shared by the agent route (`api/agent.py`) and the
envelope relay worker (`envelope_outbox/worker.py`).

The route resolves the caller and computes the hop; the core runs the checks in the order of D7
and queues the message in one (savepoint-compatible) transaction, so a refusal writes nothing and
a call inside an outer transaction commits with it. A refusal is an `ApiError`.
"""

from __future__ import annotations

from typing import Any, NamedTuple

from agents_orchestrator.api import session_limits
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import leadership, session_messaging
from agents_orchestrator.db import queue as q
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.settings import Settings


class Sent(NamedTuple):
    message_id: int
    handled: list[int]


async def session_send_core(
    conn: Any,
    own: sessions_repo.Session,
    agent: agents_repo.Agent,
    target_slug: str,
    text: str,
    *,
    settings: Settings,
    lane: str,
    handled: list[int],
    hop: int,
    envelope_ref: str | None = None,
    allow_interrupt: bool = True,
) -> Sent:
    """Queue `text` to the leader of `target_slug` as `own`; returns the message id and the
    handled ids. `hop` is the hop of the caller's current turn (0 when it delivered no session
    message); the queued message carries hop + 1."""
    if lane == "interrupt" and not allow_interrupt:
        raise ApiError(422, "lane_forbidden", "an envelope cannot use the interrupt lane")
    async with conn.transaction():
        if not await leadership.leads(conn, agent.session_id, agent.id, alone=own.spec.alone):
            raise ApiError(403, "not_leader", "only the leader of a session can send to another")
        target = await sessions_repo.get_by_slug(conn, target_slug)
        if target is None:
            raise ApiError(404, "not_found", f"session {target_slug} not found")
        if target.id == own.id:
            raise ApiError(422, "self_send", "a session cannot send a message to itself")
        if not await session_limits.may_message(conn, own, target, settings):
            # may_message allows every same-project pair, so a refusal is always cross-project
            window = settings.session_reply_window_seconds
            message = (
                f"session {target_slug} is in another project and neither session is a superleader"
            )
            if window > 0:
                message = (
                    f"session {target_slug} is in another project, neither session is a"
                    f" superleader, and it has not messaged the caller's session in the last"
                    f" {window // 3600} hours"
                )
            raise ApiError(403, "project_forbidden", message)
        if target.desired == "deleting":
            raise ApiError(409, "session_deleting", f"session {target_slug} is being discarded")
        new_hop = await session_limits.check_limits(conn, own, target, settings, hop)
        try:
            message_id = await q.enqueue_message(
                conn,
                target.id,
                text,
                sender_session_id=own.id,
                sender_slug=own.slug,
                hop=new_hop,
                lane=lane,
                envelope_ref=envelope_ref,
            )
        except q.SessionBeingDeleted as exc:
            raise ApiError(409, "session_deleting", "target session is being discarded") from exc
        return Sent(message_id, await session_messaging.mark_handled(conn, own.id, handled))

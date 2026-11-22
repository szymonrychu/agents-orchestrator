"""The request-free core of the project room post, shared by the agent route
(`api/project_chat.py`) and the envelope relay worker (`envelope_outbox/worker.py`).

`project_post_core` does everything the route does inside its transaction (project, hop, rate,
budget and recipient checks, the post row, the fan-out, the handled marks) in one
savepoint-compatible transaction: a refusal is an `ApiError` and writes nothing, and a call
inside an outer transaction commits with it. `post_project_effects` runs after that commit.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel

from agents_orchestrator.api import admin
from agents_orchestrator.api import forward as forward_core
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.session_limits import RATE_WINDOW_SECONDS
from agents_orchestrator.api.sessions import Pool
from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import leadership, project_posts, session_messaging
from agents_orchestrator.db import projects as projects_repo
from agents_orchestrator.db import queue as q
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.settings import Settings

PATH = "/agent/v1/project/messages"
BUDGET_WINDOW_SECONDS = 3600


class SkippedRecipient(BaseModel):
    slug: str
    reason: Literal["queue_full", "session_deleting"]


class ProjectPostQueued(BaseModel):
    post_id: int
    delivered: list[str]
    skipped: list[SkippedRecipient]
    handled: list[int]


class ProjectPosted(ProjectPostQueued):
    """The queued post plus the room it went to (for the post-commit audit)."""

    project: str


def room_of(own: sessions_repo.Session, project: str | None) -> str:
    """The project a caller posts to or reads: a superleader must name one; any other session
    names one of its projects (home or extra, T-120) or gets its home project (read live from
    its row, so a moved session follows its new project and a revoked grant ends at once)."""
    if own.spec.session_messaging:
        if project is None:
            raise ApiError(422, "project_required", "a superleader session must give project")
        return project
    if project is None:
        return own.project
    if project not in own.projects:
        raise ApiError(
            403, "project_forbidden", "a session can only use the rooms of its own projects"
        )
    return project


async def _check_limits(
    conn: Any, own: sessions_repo.Session, room: str, settings: Settings, previous_hop: int
) -> int:
    """Hop, rate (direct messages plus posts, a post counting once) and the project's hourly
    budget; raises ApiError, returns the hop of the fan-out rows."""
    hop = previous_hop + 1
    if hop > settings.session_message_max_hops:
        raise ApiError(
            409,
            "hop_limit",
            f"hop {hop} exceeds the maximum of {settings.session_message_max_hops}",
        )
    rate = settings.session_message_rate_per_minute
    sent = await q.count_sent_by(conn, own.id, RATE_WINDOW_SECONDS)
    if sent + await project_posts.count_posts_by(conn, own.id, RATE_WINDOW_SECONDS) >= rate:
        raise ApiError(429, "rate_limited", f"at most {rate} messages per minute")
    budget = settings.project_chat_posts_per_hour
    if await project_posts.count_project_posts(conn, room, BUDGET_WINDOW_SECONDS) >= budget:
        raise ApiError(
            429, "project_budget", f"project {room} has had {budget} room posts in the last hour"
        )
    return hop


async def _recipients(
    conn: Any, room: str, own: sessions_repo.Session, to: list[str], settings: Settings
) -> list[project_posts.Recipient]:
    candidates = await project_posts.recipients(conn, room, own.id)
    if to:
        known = {r.slug for r in candidates}
        outside = sorted(set(to) - known)
        if outside:
            raise ApiError(
                422,
                "not_in_project",
                f"not a leader session of project {room} other than the caller:"
                f" {', '.join(outside)}",
            )
        candidates = [r for r in candidates if r.slug in set(to)]
    cap = settings.project_chat_max_recipients
    if len(candidates) > cap:
        raise ApiError(
            409,
            "too_many_recipients",
            f"{len(candidates)} recipients exceed the maximum of {cap}; narrow the push with to",
        )
    return candidates


async def project_post_core(
    conn: Any,
    own: sessions_repo.Session,
    agent: agents_repo.Agent,
    *,
    settings: Settings,
    text: str,
    to: list[str],
    reply_to: int | None,
    handled: list[int],
    project: str | None,
    hop: int,
    envelope_ref: str | None = None,
) -> ProjectPosted:
    """Post `text` to the room of `project` (None: the caller's home project) as `own`. `hop` is
    the hop of the caller's current turn (0 when it delivered no session message); the fan-out
    rows carry hop + 1 and `envelope_ref`."""
    delivered: list[str] = []
    skipped: list[SkippedRecipient] = []
    async with conn.transaction():
        if not await leadership.leads(conn, agent.session_id, agent.id, alone=own.spec.alone):
            raise ApiError(
                403, "not_leader", "only the leader of a session can use the project room"
            )
        room = room_of(own, project)
        try:
            await projects_repo.require_active(conn, room, lock="share")
        except projects_repo.UnknownProject:
            raise ApiError(404, "unknown_project", f"project {room} not found") from None
        except projects_repo.ProjectArchived:
            raise ApiError(409, "project_archived", f"project {room} is archived") from None
        new_hop = await _check_limits(conn, own, room, settings, hop)
        recipients = await _recipients(conn, room, own, to, settings)
        try:
            post_id = await project_posts.insert_post(
                conn,
                project=room,
                sender_kind="superleader" if own.spec.session_messaging else "leader",
                sender_session_id=own.id,
                sender_slug=own.slug,
                text=text,
                reply_to=reply_to,
                addressed=to,
            )
        except ValueError:
            raise ApiError(
                422, "invalid_reply_to", f"reply_to is not a post of project {room}"
            ) from None
        cap = settings.session_message_queued_max
        for r in recipients:
            if await q.count_queued_session_messages(conn, r.id) >= cap:
                skipped.append(SkippedRecipient(slug=r.slug, reason="queue_full"))
                continue
            try:
                await project_posts.enqueue_fanout(
                    conn,
                    r.id,
                    text,
                    sender_session_id=own.id,
                    sender_slug=own.slug,
                    hop=new_hop,
                    post_id=post_id,
                    envelope_ref=envelope_ref,
                )
            except q.SessionBeingDeleted:
                skipped.append(SkippedRecipient(slug=r.slug, reason="session_deleting"))
                continue
            delivered.append(r.slug)
        await project_posts.set_recipients(conn, post_id, delivered)
        marked = await session_messaging.mark_handled(conn, own.id, handled)
    return ProjectPosted(
        post_id=post_id, delivered=delivered, skipped=skipped, handled=marked, project=room
    )


async def post_project_effects(
    pool: Pool,
    own: sessions_repo.Session,
    agent: agents_repo.Agent,
    posted: ProjectPosted,
) -> None:
    """After the post committed: stamp the session's last update and audit a superleader post."""
    await forward_core.touch_on_success(pool, own.id, 202)
    if own.spec.session_messaging:
        data = {
            "method": "POST",
            "path": PATH,
            "status": 202,
            "session": own.slug,
            "project": posted.project,
            "post_id": posted.post_id,
        }
        await admin.write_audit(pool, own, agent, data)

"""Project room routes of the agent port (T-117): `POST` and `GET /agent/v1/project/messages`.

A leader posts to its own project's room; the post is one `project_posts` row plus an
untargeted session message (`messages.project_post_id`) to the leader of every other active,
non-superleader session of the project, all in one transaction. A refusal writes nothing.
Leaders read their own room; a superleader (`spec.session_messaging`) names the project, may
read any one, and every post it makes is audited like an admin call.

`PageQuery`, `read_page` and `post_view` are shared with the main-port read route.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict, Field

from agents_orchestrator.api import admin
from agents_orchestrator.api import forward as forward_core
from agents_orchestrator.api.agent import Caller, _leads, _session
from agents_orchestrator.api.errors import ApiError, not_found
from agents_orchestrator.api.session_limits import RATE_WINDOW_SECONDS
from agents_orchestrator.api.sessions import Cfg, Pool
from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import project_posts, session_messaging
from agents_orchestrator.db import projects as projects_repo
from agents_orchestrator.db import queue as q
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.settings import Settings

router = APIRouter(prefix="/agent/v1", tags=["agent"])
main_router = APIRouter(prefix="/api/v1", tags=["projects"])

PATH = "/agent/v1/project/messages"
PROJECT_POST_MAX_CHARS = 4000
PAGE_DEFAULT_LIMIT = 20
PAGE_MAX_LIMIT = 100
BUDGET_WINDOW_SECONDS = 3600


class ProjectPostIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str = Field(min_length=1, max_length=PROJECT_POST_MAX_CHARS)
    #: Session slugs to push to; empty: every other leader of the project.
    to: list[str] = Field(default_factory=list, max_length=20)
    #: The post of the same project this one answers.
    reply_to: int | None = Field(default=None, ge=1)
    #: Ids of incoming session messages this post answers or that need no answer.
    handled: list[int] = Field(default_factory=list, max_length=50)
    #: Required for a superleader session; a leader may only name its own project.
    project: str | None = None


class SkippedRecipient(BaseModel):
    slug: str
    reason: Literal["queue_full", "session_deleting"]


class ProjectPostQueued(BaseModel):
    post_id: int
    delivered: list[str]
    skipped: list[SkippedRecipient]
    handled: list[int]


class ProjectPostView(BaseModel):
    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    id: int
    from_: str = Field(alias="from")
    sender_kind: str
    sender_session_id: uuid.UUID
    at: datetime
    text: str
    reply_to: int | None
    to: list[str]


class ProjectPostsPage(BaseModel):
    # first: a reader's output budget cuts the tail, never the cursor
    next_before_id: int | None
    posts: list[ProjectPostView]


def post_view(p: project_posts.Post) -> ProjectPostView:
    return ProjectPostView(
        id=p.id,
        from_=p.sender_slug,
        sender_kind=p.sender_kind,
        sender_session_id=p.sender_session_id,
        at=p.created_at,
        text=p.text,
        reply_to=p.reply_to,
        to=p.addressed,
    )


@dataclass(frozen=True)
class PageQuery:
    before_id: int | None
    after_id: int | None
    limit: int


def page_query(
    before_id: Annotated[int | None, Query(ge=1)] = None,
    after_id: Annotated[int | None, Query(ge=1)] = None,
    limit: Annotated[int, Query(ge=1, le=PAGE_MAX_LIMIT)] = PAGE_DEFAULT_LIMIT,
) -> PageQuery:
    """The cursor and limit of a room read; `before_id` and `after_id` exclude each other."""
    if before_id is not None and after_id is not None:
        raise ApiError(422, "invalid_cursor", "give before_id or after_id, not both")
    return PageQuery(before_id=before_id, after_id=after_id, limit=limit)


Page = Annotated[PageQuery, Depends(page_query)]


async def read_page(conn: Any, project: str, query: PageQuery) -> ProjectPostsPage:
    """Newest first (oldest first after `after_id`). `next_before_id` is the id of the last post
    of a full page read backwards, else null: an `after_id` read pages forward from its own
    newest id."""
    posts = await project_posts.page(
        conn, project, before_id=query.before_id, after_id=query.after_id, limit=query.limit
    )
    full = len(posts) == query.limit and query.after_id is None
    return ProjectPostsPage(
        next_before_id=posts[-1].id if full else None, posts=[post_view(p) for p in posts]
    )


async def _leader_session(conn: Any, agent: agents_repo.Agent) -> sessions_repo.Session:
    own = await _session(conn, agent)
    if not await _leads(conn, agent, own):
        raise ApiError(403, "not_leader", "only the leader of a session can use the project room")
    return own


def _room_of(own: sessions_repo.Session, project: str | None) -> str:
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
    conn: Any, agent: agents_repo.Agent, own: sessions_repo.Session, room: str, settings: Settings
) -> int:
    """Hop, rate (direct messages plus posts, a post counting once) and the project's hourly
    budget; raises ApiError, returns the hop of the fan-out rows."""
    previous = await q.turn_hop(conn, agent.id)
    hop = previous + 1 if previous is not None else 1
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


@router.post(
    "/project/messages",
    status_code=202,
    response_model=ProjectPostQueued,
    summary="Post to the project room, pushed to the other leaders of the project (leader only)",
)
async def project_post(
    body: ProjectPostIn, agent: Caller, pool: Pool, settings: Cfg
) -> ProjectPostQueued:
    """Checks in order (leader, project, hop, rate, budget, recipients) in one transaction; a
    refusal writes nothing. A recipient with a full queue or being deleted is skipped."""
    delivered: list[str] = []
    skipped: list[SkippedRecipient] = []
    async with pool.connection() as conn, conn.transaction():
        own = await _leader_session(conn, agent)
        room = _room_of(own, body.project)
        try:
            await projects_repo.require_active(conn, room, lock="share")
        except projects_repo.UnknownProject:
            raise ApiError(404, "unknown_project", f"project {room} not found") from None
        except projects_repo.ProjectArchived:
            raise ApiError(409, "project_archived", f"project {room} is archived") from None
        hop = await _check_limits(conn, agent, own, room, settings)
        recipients = await _recipients(conn, room, own, body.to, settings)
        try:
            post_id = await project_posts.insert_post(
                conn,
                project=room,
                sender_kind="superleader" if own.spec.session_messaging else "leader",
                sender_session_id=own.id,
                sender_slug=own.slug,
                text=body.text,
                reply_to=body.reply_to,
                addressed=body.to,
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
                    body.text,
                    sender_session_id=own.id,
                    sender_slug=own.slug,
                    hop=hop,
                    post_id=post_id,
                )
            except q.SessionBeingDeleted:
                skipped.append(SkippedRecipient(slug=r.slug, reason="session_deleting"))
                continue
            delivered.append(r.slug)
        await project_posts.set_recipients(conn, post_id, delivered)
        handled = await session_messaging.mark_handled(conn, own.id, body.handled)
    await forward_core.touch_on_success(pool, own.id, 202)
    if own.spec.session_messaging:
        data = {
            "method": "POST",
            "path": PATH,
            "status": 202,
            "session": own.slug,
            "project": room,
            "post_id": post_id,
        }
        await admin.write_audit(pool, own, agent, data)
    return ProjectPostQueued(post_id=post_id, delivered=delivered, skipped=skipped, handled=handled)


@router.get(
    "/project/messages",
    response_model=ProjectPostsPage,
    summary="Read the project room, newest first (leader only)",
)
async def project_read(
    agent: Caller, pool: Pool, page: Page, project: str | None = None
) -> ProjectPostsPage:
    async with pool.connection() as conn:
        own = await _leader_session(conn, agent)
        room = _room_of(own, project)
        if await projects_repo.get_project(conn, room) is None:
            raise ApiError(404, "unknown_project", f"project {room} not found")
        return await read_page(conn, room, page)


@main_router.get(
    "/projects/{slug}/messages",
    response_model=ProjectPostsPage,
    summary="Read a project room, newest first (main port)",
)
async def project_messages(slug: str, pool: Pool, page: Page) -> ProjectPostsPage:
    async with pool.connection() as conn:
        if await projects_repo.get_project(conn, slug) is None:
            raise not_found(f"project {slug}")
        return await read_page(conn, slug, page)

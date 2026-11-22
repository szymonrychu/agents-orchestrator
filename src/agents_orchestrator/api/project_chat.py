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
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict, Field

from agents_orchestrator.api.agent import Caller, _leads, _session
from agents_orchestrator.api.errors import ApiError, not_found
from agents_orchestrator.api.project_post_core import (
    ProjectPostQueued,
    post_project_effects,
    project_post_core,
    room_of,
)
from agents_orchestrator.api.session_limits import current_hop
from agents_orchestrator.api.sessions import Cfg, Pool
from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import project_posts
from agents_orchestrator.db import projects as projects_repo
from agents_orchestrator.db import sessions as sessions_repo

router = APIRouter(prefix="/agent/v1", tags=["agent"])
main_router = APIRouter(prefix="/api/v1", tags=["projects"])

PROJECT_POST_MAX_CHARS = 4000
PAGE_DEFAULT_LIMIT = 20
PAGE_MAX_LIMIT = 100


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
    async with pool.connection() as conn:
        own = await _session(conn, agent)
        queued = await project_post_core(
            conn,
            own,
            agent,
            settings=settings,
            text=body.text,
            to=body.to,
            reply_to=body.reply_to,
            handled=body.handled,
            project=body.project,
            hop=await current_hop(conn, agent.id),
        )
    await post_project_effects(pool, own, agent, queued)
    return ProjectPostQueued(
        post_id=queued.post_id,
        delivered=queued.delivered,
        skipped=queued.skipped,
        handled=queued.handled,
    )


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
        room = room_of(own, project)
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

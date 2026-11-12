"""Event paging and the two SSE streams (D11, D14)."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.sse import EventSourceResponse, ServerSentEvent
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel

from agents_orchestrator.api.errors import ApiError, not_found
from agents_orchestrator.api.listener import Listener, Subscription, SubscriptionDropped
from agents_orchestrator.api.messages import DEFAULT_ATTEMPTS, message_view
from agents_orchestrator.api.sessions import Cfg, Pool, agent_view
from agents_orchestrator.api.sessions import _view as session_view
from agents_orchestrator.db import events as ev
from agents_orchestrator.db import sessions as repo
from agents_orchestrator.db import views

router = APIRouter(prefix="/api/v1")

DEFAULT_POLL_SECONDS = 5.0
_STREAM_PAGE = ev.MAX_LIMIT
_RECENT_MESSAGES = 100


class EventView(BaseModel):
    id: int
    session_id: uuid.UUID
    agent_id: uuid.UUID
    wrapper_session_id: str
    seq: int
    ts: datetime
    type: str
    turn_id: str | None
    data: Any


class EventsPage(BaseModel):
    events: list[EventView]
    has_more_before: bool
    next_after_id: int | None


def get_listener(request: Request) -> Listener:
    return request.app.state.listener


def get_poll_seconds(request: Request) -> float:
    return request.app.state.stream_poll_seconds


Notifier = Annotated[Listener, Depends(get_listener)]
PollSeconds = Annotated[float, Depends(get_poll_seconds)]


def _bad_request(message: str) -> ApiError:
    return ApiError(400, "bad_request", message)


def _split_types(types: list[str] | None) -> list[str] | None:
    out = [t for raw in types or [] for t in raw.split(",") if t]
    return out or None


async def events_page(
    conn,
    session_id: uuid.UUID,
    *,
    before_id: int | None,
    after_id: int | None,
    limit: int,
    agent_id: uuid.UUID | None,
    types: list[str] | None,
) -> EventsPage:
    """One page of a session's events; 400 when both cursors are given."""
    if before_id is not None and after_id is not None:
        raise _bad_request("before_id and after_id are mutually exclusive")
    page = await ev.page(
        conn,
        session_id,
        before_id=before_id,
        after_id=after_id,
        limit=limit,
        agent_id=agent_id,
        types=_split_types(types),
    )
    return EventsPage(
        events=[EventView(**e) for e in page.events],
        has_more_before=page.has_more_before,
        next_after_id=page.next_after_id,
    )


@router.get("/sessions/{session_id}/events", response_model=EventsPage)
async def list_events(
    session_id: uuid.UUID,
    pool: Pool,
    before_id: Annotated[int | None, Query(ge=0)] = None,
    after_id: Annotated[int | None, Query(ge=0)] = None,
    limit: Annotated[int, Query(ge=1, le=ev.MAX_LIMIT)] = 200,
    agent_id: uuid.UUID | None = None,
    types: Annotated[list[str] | None, Query()] = None,
) -> EventsPage:
    if before_id is not None and after_id is not None:
        raise _bad_request("before_id and after_id are mutually exclusive")
    async with pool.connection() as conn:
        if await repo.get(conn, session_id) is None:
            raise not_found("session")
        return await events_page(
            conn,
            session_id,
            before_id=before_id,
            after_id=after_id,
            limit=limit,
            agent_id=agent_id,
            types=types,
        )


async def resume_cursor(
    session_id: uuid.UUID,
    pool: Pool,
    after_id: Annotated[int | None, Query(ge=0)] = None,
    last_event_id: Annotated[str | None, Header()] = None,
) -> tuple[uuid.UUID, int]:
    """Validate the session and resolve where the stream starts (before any byte is sent)."""
    cursor = after_id
    if cursor is None and last_event_id not in (None, ""):
        try:
            cursor = int(last_event_id)  # type: ignore[arg-type]
        except ValueError:
            raise _bad_request("Last-Event-ID must be an integer event id") from None
    async with pool.connection() as conn:
        if await repo.get(conn, session_id) is None:
            raise not_found("session")
        if cursor is None:
            cur = await conn.execute(
                "SELECT COALESCE(MAX(id), 0) FROM events WHERE session_id = %s", (session_id,)
            )
            cursor = (await cur.fetchone())[0]
    return session_id, cursor


def _matches(payload: dict, session_id: uuid.UUID) -> bool:
    return payload.get("session_id") == str(session_id)


async def _wait_for_session(sub: Subscription, session_id: uuid.UUID, poll: float) -> bool:
    """Wait for a NOTIFY about this session or for the poll interval; other sessions are ignored."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + poll
    while (remaining := deadline - loop.time()) > 0:
        if await sub.wait(remaining) and any(_matches(p, session_id) for _, p in sub.drain()):
            return True
    return False


@router.get("/sessions/{session_id}/events/stream", response_class=EventSourceResponse)
async def stream_events(
    start: Annotated[tuple[uuid.UUID, int], Depends(resume_cursor)],
    pool: Pool,
    listener: Notifier,
    poll: PollSeconds,
) -> AsyncIterator[ServerSentEvent]:
    session_id, cursor = start
    sub = listener.subscribe(["events"])
    try:
        while True:
            while True:  # replay stored rows after the cursor, page by page
                async with pool.connection() as conn:
                    page = await ev.page(conn, session_id, after_id=cursor, limit=_STREAM_PAGE)
                for row in page.events:
                    yield ServerSentEvent(
                        data=EventView(**row), event=row["type"], id=str(row["id"])
                    )
                if page.events:
                    cursor = page.events[-1]["id"]
                if len(page.events) < _STREAM_PAGE:
                    break
            try:
                if not await _wait_for_session(sub, session_id, poll):
                    continue
            except SubscriptionDropped:
                return
    finally:
        sub.close()


# ------------------------------------------------------------------ status stream


async def _session_state(conn, session: repo.Session, settings) -> dict[str, Any]:
    aggs = await views.session_aggregates(conn, session.id)
    agg = aggs.get(session.id) or {}
    rows = await views.agent_rows(conn, session.id)
    msgs = await views.list_messages(conn, session.id, None, _RECENT_MESSAGES, DEFAULT_ATTEMPTS)
    view = session_view(session, agg, settings).model_dump(mode="json")
    return {
        "session": view,
        "agents": {str(r["id"]): agent_view(r).model_dump(mode="json") for r in rows},
        "messages": {m["id"]: message_view(m).model_dump(mode="json") for m in msgs},
        "queued": agg.get("queued", 0),
        "failed": agg.get("failed", 0),
        "oldest_queued_at": view["summary"]["oldest_queued_at"],
    }


def _snapshot_entry(state: dict[str, Any]) -> dict[str, Any]:
    return {"session": state["session"], "agents": list(state["agents"].values())}


def _deltas(cid: str, old: dict[str, Any] | None, new: dict[str, Any]) -> list[ServerSentEvent]:
    out: list[ServerSentEvent] = []
    if old is None or old["session"] != new["session"]:
        out.append(ServerSentEvent(data={"session": new["session"]}, event="session_status"))
    old_agents = old["agents"] if old else {}
    for aid, agent in new["agents"].items():
        if old_agents.get(aid) != agent:
            out.append(
                ServerSentEvent(data={"session_id": cid, "agent": agent}, event="agent_status")
            )
    old_msgs = old["messages"] if old else {}
    changed = [m for mid, m in new["messages"].items() if old_msgs.get(mid) != m]
    counts = ("queued", "failed", "oldest_queued_at")
    counts_changed = old is None or any(old[k] != new[k] for k in counts)
    if changed or counts_changed:
        out.append(
            ServerSentEvent(
                data={
                    "session_id": cid,
                    "queued": new["queued"],
                    "failed": new["failed"],
                    "oldest_queued_at": new["oldest_queued_at"],
                    "messages": changed,
                },
                event="message_status",
            )
        )
    return out


async def _refresh(
    pool: AsyncConnectionPool,
    settings,
    known: dict[str, dict[str, Any]],
    only: set[str] | None,
) -> list[ServerSentEvent]:
    """Re-read the touched sessions (all when `only` is None) and emit what changed."""
    out: list[ServerSentEvent] = []
    async with pool.connection() as conn:
        sessions = {str(c.id): c for c in await repo.list_sessions(conn)}
        for cid in list(known):
            if cid not in sessions and (only is None or cid in only):
                del known[cid]
                out.append(ServerSentEvent(data={"session_id": cid}, event="session_removed"))
        for cid, session in sessions.items():
            if only is not None and cid not in only:
                continue
            new = await _session_state(conn, session, settings)
            out.extend(_deltas(cid, known.get(cid), new))
            known[cid] = new
    return out


@router.get("/stream", response_class=EventSourceResponse)
async def stream_status(
    pool: Pool, settings: Cfg, listener: Notifier, poll: PollSeconds
) -> AsyncIterator[ServerSentEvent]:
    sub: Subscription = listener.subscribe(["status", "session_changed", "leader_changed"])
    try:
        known: dict[str, dict[str, Any]] = {}
        async with pool.connection() as conn:
            for session in await repo.list_sessions(conn):
                known[str(session.id)] = await _session_state(conn, session, settings)
        yield ServerSentEvent(
            data={"sessions": [_snapshot_entry(s) for s in known.values()]}, event="snapshot"
        )
        while True:
            try:
                notified = await sub.wait(poll)
                drained = sub.drain()
            except SubscriptionDropped:
                return
            touched = {p["session_id"] for _, p in drained if "session_id" in p}
            for channel, payload in drained:
                if channel == "leader_changed":
                    yield ServerSentEvent(
                        data={
                            k: payload.get(k) for k in ("session_id", "agent_id", "epoch", "reason")
                        },
                        event="leader_changed",
                    )
            # a NOTIFY refreshes its session; a timeout is the poll and refreshes everything
            for event in await _refresh(
                pool, settings, known, (touched or None) if notified else None
            ):
                yield event
    finally:
        sub.close()

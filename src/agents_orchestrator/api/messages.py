"""Message, command and check routes (D4, D11, D24). The api only inserts and cancels rows."""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Query, Request, Response

from agents_orchestrator.api import attachments as attachments_mod
from agents_orchestrator.api import schemas, service_auth
from agents_orchestrator.api.errors import ApiError, not_found
from agents_orchestrator.api.sessions import Cfg, Pool
from agents_orchestrator.db import queue, views
from agents_orchestrator.db import sessions as repo
from agents_orchestrator.spec import SessionSpec

router = APIRouter(prefix="/api/v1")

DEFAULT_ATTEMPTS: int = SessionSpec.model_fields["max_attempts"].default


def message_view(r: dict) -> schemas.MessageView:
    turn = (
        schemas.TurnView(
            status=r["turn_status"],
            started_at=r["turn_started_at"],
            completed_at=r["turn_completed_at"],
            final_text=r["turn_final_text"],
            usage=r["turn_usage"],
        )
        if r["has_turn"]
        else None
    )
    return schemas.MessageView(
        id=r["id"],
        session_id=r["session_id"],
        text=r["text"],
        status=r["status"],
        lane=r["lane"],
        target_agent_id=r["target_agent_id"],
        agent_id=r["agent_id"],
        turn_id=r["turn_id"],
        attempts=r["attempts"],
        max_attempts=r["max_attempts"],
        next_attempt_at=r["not_before"],
        error=r["error"],
        created_at=r["created_at"],
        dispatched_at=r["dispatched_at"],
        turn=turn,
        notice=r["notice"],
        attachments=[
            schemas.MessageAttachmentView(**a, path=f".attachments/orch-{r['id']}/{a['name']}")
            for a in r["attachments"]
        ],
    )


async def _require_open_session(conn, session_id: uuid.UUID) -> repo.Session:
    session = await repo.get(conn, session_id)
    if session is None:
        raise not_found("session")
    if session.desired == "deleting":
        raise ApiError(409, "session_deleting", "session is being discarded")
    return session


@router.post(
    "/sessions/{session_id}/messages", status_code=202, response_model=schemas.MessageAccepted
)
async def post_message(
    session_id: uuid.UUID, body: schemas.PostMessage, request: Request, pool: Pool, settings: Cfg
) -> schemas.MessageAccepted:
    from_user = (
        body.from_user
        if body.from_user is not None
        else service_auth.calling_service(request) is None
    )
    stored = attachments_mod.decode(body.attachments, settings)
    async with pool.connection() as conn, conn.transaction():
        await _require_open_session(conn, session_id)
        if body.target_agent_id is not None:
            agent = await views.get_agent_ref(conn, body.target_agent_id)
            if agent is None or agent["session_id"] != session_id:
                raise ApiError(422, "invalid_request", "target agent is not in this session")
            if agent["gone_at"] is not None:
                raise ApiError(422, "target_gone", "target agent is gone")
        if from_user:
            # before the enqueue: it share-locks the session row, and upgrading that lock to the
            # update here would deadlock concurrent posts to one session
            await repo.mark_user_message(conn, session_id)
        try:
            mid = await queue.enqueue_message(
                conn,
                session_id,
                body.text,
                body.target_agent_id,
                lane=body.lane,
                attachments=stored,
            )
        except repo.SessionBeingDeleted as exc:
            raise ApiError(409, "session_deleting", "session is being discarded") from exc
    return schemas.MessageAccepted(message_id=mid)


def parse_statuses(status: str | None) -> list[str] | None:
    """The comma-separated `status` filter as a list; 422 for an unknown or empty one."""
    if not status:
        return None
    statuses = [s.strip() for s in status.split(",") if s.strip()]
    bad = [s for s in statuses if s not in views.MESSAGE_STATUSES]
    if bad or not statuses:
        raise ApiError(422, "invalid_request", f"unknown status {', '.join(bad) or status!r}")
    return statuses


@router.get("/sessions/{session_id}/messages", response_model=list[schemas.MessageView])
async def list_messages(
    session_id: uuid.UUID,
    pool: Pool,
    status: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> list[schemas.MessageView]:
    statuses = parse_statuses(status)
    async with pool.connection() as conn:
        if await repo.get(conn, session_id) is None:
            raise not_found("session")
        rows = await views.list_messages(conn, session_id, statuses, limit, DEFAULT_ATTEMPTS)
    return [message_view(r) for r in rows]


@router.get("/messages/{message_id}", response_model=schemas.MessageView)
async def get_message(message_id: int, pool: Pool) -> schemas.MessageView:
    async with pool.connection() as conn:
        row = await views.get_message(conn, message_id, DEFAULT_ATTEMPTS)
    if row is None:
        raise not_found("message")
    return message_view(row)


async def cancel(conn, message_id: int, session_id: uuid.UUID | None) -> None:
    """Cancel a queued message, scoped to `session_id` when given: an unknown id and another
    session's message answer alike (404); a message that is not queued is 409."""
    if await queue.cancel_message(conn, message_id, session_id):
        return
    row = await views.get_message(conn, message_id, DEFAULT_ATTEMPTS)
    if row is None or (session_id is not None and row["session_id"] != session_id):
        raise not_found("message")
    raise ApiError(409, "not_queued", "only a queued message can be cancelled")


@router.delete("/messages/{message_id}", status_code=204)
async def cancel_message(message_id: int, pool: Pool) -> Response:
    async with pool.connection() as conn:
        await cancel(conn, message_id, None)
    return Response(status_code=204)


@router.post(
    "/messages/{message_id}/retry", status_code=202, response_model=schemas.MessageAccepted
)
async def retry_message(message_id: int, pool: Pool) -> schemas.MessageAccepted:
    async with pool.connection() as conn:
        if await views.get_message(conn, message_id, DEFAULT_ATTEMPTS) is None:
            raise not_found("message")
        if not await queue.retry_message(conn, message_id):
            raise ApiError(409, "not_failed", "only a failed message can be retried")
    return schemas.MessageAccepted(message_id=message_id)


async def enqueue_agent_command(
    conn,
    agent_id: uuid.UUID,
    kind: str,
    issued_by_session: uuid.UUID | None = None,
    note_message_id: int | None = None,
) -> int:
    agent = await views.get_agent_ref(conn, agent_id)
    if agent is None:
        raise not_found("agent")
    if agent["gone_at"] is not None:
        raise ApiError(409, "agent_gone", "agent is gone")
    return await queue.enqueue_command(
        conn, agent_id, kind, issued_by_session=issued_by_session, note_message_id=note_message_id
    )


async def _command(agent_id: uuid.UUID, kind: str, pool) -> schemas.CommandAccepted:
    async with pool.connection() as conn:
        return schemas.CommandAccepted(command_id=await enqueue_agent_command(conn, agent_id, kind))


@router.post(
    "/agents/{agent_id}/interrupt", status_code=202, response_model=schemas.CommandAccepted
)
async def interrupt_agent(agent_id: uuid.UUID, pool: Pool) -> schemas.CommandAccepted:
    return await _command(agent_id, "interrupt", pool)


@router.post("/agents/{agent_id}/compact", status_code=202, response_model=schemas.CommandAccepted)
async def compact_agent(agent_id: uuid.UUID, pool: Pool) -> schemas.CommandAccepted:
    return await _command(agent_id, "compact", pool)


@router.post("/agents/{agent_id}/restart", status_code=202, response_model=schemas.CommandAccepted)
async def restart_agent(agent_id: uuid.UUID, pool: Pool) -> schemas.CommandAccepted:
    return await _command(agent_id, "restart", pool)


async def restart_all(
    conn,
    session_id: uuid.UUID,
    issued_by_session: uuid.UUID | None = None,
    note_message_id: int | None = None,
) -> list[int]:
    """One restart command per live agent of the session; the caller owns the transaction."""
    await _require_open_session(conn, session_id)
    agent_ids = await views.live_agent_ids(conn, session_id)
    return [
        await queue.enqueue_command(
            conn,
            agent_id,
            "restart",
            issued_by_session=issued_by_session,
            note_message_id=note_message_id,
        )
        for agent_id in agent_ids
    ]


@router.post(
    "/sessions/{session_id}/restart", status_code=202, response_model=schemas.SessionRestartAccepted
)
async def restart_session(session_id: uuid.UUID, pool: Pool) -> schemas.SessionRestartAccepted:
    """One restart command per live agent of the session, all in one transaction."""
    async with pool.connection() as conn, conn.transaction():
        return schemas.SessionRestartAccepted(command_ids=await restart_all(conn, session_id))


async def command_view(conn, command_id: int) -> schemas.CommandView:
    """A command, else 404."""
    row = await views.get_command(conn, command_id)
    if row is None:
        raise not_found("command")
    return schemas.CommandView(**row)


@router.get("/commands/{command_id}", response_model=schemas.CommandView)
async def get_command(command_id: int, pool: Pool) -> schemas.CommandView:
    async with pool.connection() as conn:
        return await command_view(conn, command_id)


@router.post("/sessions/{session_id}/checks", status_code=202, response_model=schemas.CheckAccepted)
async def post_check(
    session_id: uuid.UUID, body: schemas.PostCheck, pool: Pool
) -> schemas.CheckAccepted:
    async with pool.connection() as conn:
        await _require_open_session(conn, session_id)
        cid = await queue.enqueue_check(conn, session_id, body.command)
    return schemas.CheckAccepted(check_id=cid)


@router.get("/checks/{check_id}", response_model=schemas.CheckView)
async def get_check(check_id: int, pool: Pool) -> schemas.CheckView:
    async with pool.connection() as conn:
        row = await views.get_check(conn, check_id)
    if row is None:
        raise not_found("check")
    return schemas.CheckView(**row)

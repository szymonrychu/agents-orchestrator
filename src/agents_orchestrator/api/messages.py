"""Message, command and check routes (D4, D11, D24). The api only inserts and cancels rows."""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Query, Response

from agents_orchestrator import slash
from agents_orchestrator.api import schemas
from agents_orchestrator.api.errors import ApiError, not_found
from agents_orchestrator.api.sessions import Pool
from agents_orchestrator.db import queue, views
from agents_orchestrator.db import sessions as repo
from agents_orchestrator.spec import SessionSpec

router = APIRouter(prefix="/api/v1")

_DEFAULT_ATTEMPTS: int = SessionSpec.model_fields["max_attempts"].default


def _message_view(r: dict) -> schemas.MessageView:
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
    command = (
        schemas.CommandOutcomeView(
            state=r["command_state"],
            result=r["command_result"],
            error=r["command_error"],
            turn_id=r["command_turn_id"],
            at=r["command_at"],
        )
        if r["command_state"]
        else None
    )
    return schemas.MessageView(
        id=r["id"],
        session_id=r["session_id"],
        text=r["text"],
        kind=r["kind"],
        status=r["status"],
        target_agent_id=r["target_agent_id"],
        agent_id=r["agent_id"],
        turn_id=r["turn_id"],
        wrapper_command_id=r["wrapper_command_id"],
        attempts=r["attempts"],
        max_attempts=r["max_attempts"],
        next_attempt_at=r["not_before"],
        error=r["error"],
        created_at=r["created_at"],
        dispatched_at=r["dispatched_at"],
        turn=turn,
        command=command,
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
    session_id: uuid.UUID, body: schemas.PostMessage, pool: Pool
) -> schemas.MessageAccepted:
    async with pool.connection() as conn:
        await _require_open_session(conn, session_id)
        if body.target_agent_id is not None:
            agent = await views.get_agent_ref(conn, body.target_agent_id)
            if agent is None or agent["session_id"] != session_id:
                raise ApiError(422, "invalid_request", "target agent is not in this session")
            if agent["gone_at"] is not None:
                raise ApiError(422, "target_gone", "target agent is gone")
        kind = "command" if slash.parse(body.text) else "turn"
        mid = await queue.enqueue_message(
            conn, session_id, body.text, body.target_agent_id, kind=kind
        )
    return schemas.MessageAccepted(message_id=mid)


@router.get("/sessions/{session_id}/messages", response_model=list[schemas.MessageView])
async def list_messages(
    session_id: uuid.UUID,
    pool: Pool,
    status: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> list[schemas.MessageView]:
    statuses = None
    if status:
        statuses = [s.strip() for s in status.split(",") if s.strip()]
        bad = [s for s in statuses if s not in views.MESSAGE_STATUSES]
        if bad or not statuses:
            raise ApiError(422, "invalid_request", f"unknown status {', '.join(bad) or status!r}")
    async with pool.connection() as conn:
        if await repo.get(conn, session_id) is None:
            raise not_found("session")
        rows = await views.list_messages(conn, session_id, statuses, limit, _DEFAULT_ATTEMPTS)
    return [_message_view(r) for r in rows]


@router.get("/messages/{message_id}", response_model=schemas.MessageView)
async def get_message(message_id: int, pool: Pool) -> schemas.MessageView:
    async with pool.connection() as conn:
        row = await views.get_message(conn, message_id, _DEFAULT_ATTEMPTS)
    if row is None:
        raise not_found("message")
    return _message_view(row)


@router.delete("/messages/{message_id}", status_code=204)
async def cancel_message(message_id: int, pool: Pool) -> Response:
    async with pool.connection() as conn:
        if await views.get_message(conn, message_id, _DEFAULT_ATTEMPTS) is None:
            raise not_found("message")
        if not await queue.cancel_message(conn, message_id):
            raise ApiError(409, "not_queued", "only a queued message can be cancelled")
    return Response(status_code=204)


@router.post(
    "/messages/{message_id}/retry", status_code=202, response_model=schemas.MessageAccepted
)
async def retry_message(message_id: int, pool: Pool) -> schemas.MessageAccepted:
    async with pool.connection() as conn:
        if await views.get_message(conn, message_id, _DEFAULT_ATTEMPTS) is None:
            raise not_found("message")
        if not await queue.retry_message(conn, message_id):
            raise ApiError(409, "not_failed", "only a failed message can be retried")
    return schemas.MessageAccepted(message_id=message_id)


async def _command(agent_id: uuid.UUID, kind: str, pool) -> schemas.CommandAccepted:
    async with pool.connection() as conn:
        agent = await views.get_agent_ref(conn, agent_id)
        if agent is None:
            raise not_found("agent")
        if agent["gone_at"] is not None:
            raise ApiError(409, "agent_gone", "agent is gone")
        return schemas.CommandAccepted(command_id=await queue.enqueue_command(conn, agent_id, kind))


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


@router.get("/commands/{command_id}", response_model=schemas.CommandView)
async def get_command(command_id: int, pool: Pool) -> schemas.CommandView:
    async with pool.connection() as conn:
        row = await views.get_command(conn, command_id)
    if row is None:
        raise not_found("command")
    return schemas.CommandView(**row)


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

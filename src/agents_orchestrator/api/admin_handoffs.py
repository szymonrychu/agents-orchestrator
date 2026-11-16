"""Superleader reads of the handoff history (S-68): the list of `agent_handoffs` rows and one row
in full, with its text. Read only. `kind` is the row's trigger (strong, shutdown, voluntary).
The `session` filter takes a session id or slug."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated

from fastapi import Query
from pydantic import BaseModel

from agents_orchestrator.api.admin import FORBIDDEN, router
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.sessions import Pool
from agents_orchestrator.db import handoffs
from agents_orchestrator.db import sessions as sessions_repo

_NOT_FOUND = {"description": "not_found: no such session or handoff"}


class HandoffItem(BaseModel):
    id: int
    session_id: uuid.UUID
    session_slug: str
    agent_id: uuid.UUID
    kind: str
    created_at: datetime
    size: int


class HandoffDetail(HandoffItem):
    handoff_text: str | None


class HandoffPage(BaseModel):
    handoffs: list[HandoffItem]
    has_more: bool


@router.get(
    "/handoffs",
    response_model=HandoffPage,
    responses={403: FORBIDDEN, 404: _NOT_FOUND},
)
async def list_handoffs(
    pool: Pool,
    session: Annotated[str | None, Query(max_length=128)] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    before_id: Annotated[int | None, Query(ge=1)] = None,
) -> HandoffPage:
    async with pool.connection() as conn:
        session_id = None
        if session:
            try:
                session_id = uuid.UUID(session)
            except ValueError:
                row = await sessions_repo.get_by_slug(conn, session)
                if row is None:
                    raise ApiError(404, "not_found", "no session has that id or slug") from None
                session_id = row.id
        rows, more = await handoffs.list_handoffs(
            conn, session_id=session_id, limit=limit, before_id=before_id
        )
    return HandoffPage(handoffs=[HandoffItem(**r) for r in rows], has_more=more)


@router.get(
    "/handoffs/{id}",
    response_model=HandoffDetail,
    responses={403: FORBIDDEN, 404: _NOT_FOUND},
)
async def get_handoff(pool: Pool, id: int) -> HandoffDetail:
    async with pool.connection() as conn:
        row = await handoffs.get_handoff(conn, id)
    if row is None:
        raise ApiError(404, "handoff_not_found", "no handoff has that id")
    return HandoffDetail(**row)

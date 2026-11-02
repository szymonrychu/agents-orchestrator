"""`PUT /agent/v1/session/labels`, served only on the agent port.

An agent replaces the labels of its own session. `require_agent` proves which session calls; the
route has no id in the path and the body forbids extra fields, so another session cannot be
targeted. Labels are normalised and limited exactly as on `PATCH /api/v1/sessions/{id}/labels`
(`SessionLabelsIn`, `labels.normalize_labels`) and written by the same repo function. Like the
taskboard relay there is no trigger-session refusal and no leader check (leader decision
2026-11-02). Replacing is idempotent; labels are not part of the spec, so no generation bump.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter
from pydantic import BaseModel

from agents_orchestrator.api.agent import Caller
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.schemas import SessionLabelsIn
from agents_orchestrator.api.sessions import Pool
from agents_orchestrator.db import sessions as sessions_repo

router = APIRouter(prefix="/agent/v1/session", tags=["agent"])


class OwnLabelsView(BaseModel):
    session_id: uuid.UUID
    labels: list[str]


@router.put("/labels", response_model=OwnLabelsView)
async def set_own_labels(body: SessionLabelsIn, caller: Caller, pool: Pool) -> OwnLabelsView:
    """Replace the calling session's labels."""
    async with pool.connection() as conn:
        try:
            session = await sessions_repo.set_labels(conn, caller.session_id, body.labels)
        except sessions_repo.SessionNotFound:
            raise ApiError(404, "not_found", "session not found") from None
    return OwnLabelsView(session_id=session.id, labels=session.labels)

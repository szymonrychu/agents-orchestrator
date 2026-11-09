"""Change proposal routes (preset-change-proposals plan 3.3, 4).

`proposal_router` (agent port, `/agent/v1/admin/proposals`, audited by `AdminRoute`) lets the
proposer file, list, read, revise and withdraw its OWN proposals; another session's proposal, or
one whose target the caller may no longer see, answers 404 exactly like an unknown id.

`main_router` (main port, `/api/v1/proposals`) is the human's: list, read and review every
proposal. `PortGate` keeps it off the agent port and `ServiceGate` off the service port; no
agent route and no MCP tool reaches the review.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request, Response

from agents_orchestrator.api import proposals, schemas
from agents_orchestrator.api.admin import ADMIN_PREFIX, AdminRoute, _admit, caller_of
from agents_orchestrator.api.agent import Caller, _leads
from agents_orchestrator.api.errors import ApiError, not_found
from agents_orchestrator.api.sessions import Cfg, Pool
from agents_orchestrator.db import proposals as repo
from agents_orchestrator.db import sessions as sessions_repo

LIST_LIMIT_MAX = 200

Status = Annotated[str, Query(description="active, open, closed or all")]
Limit = Annotated[int, Query(ge=1, le=LIST_LIMIT_MAX)]


async def require_proposer(request: Request, agent: Caller, pool: Pool) -> sessions_repo.Session:
    """The caller's session, admitted for the audit like `require_superleader`: a superleader
    session (any of its agents), or any other session's leader (else 403 not_leader). The
    create and revise handlers keep a regular leader on its own session (plan 3.3)."""
    async with pool.connection() as conn:
        session = await sessions_repo.get(conn, agent.session_id)
        assert session is not None
        if not session.spec.session_messaging and not await _leads(conn, agent, session):
            raise ApiError(403, "not_leader", "only the session's leader may propose changes")
    return _admit(request, agent, session)


proposal_router = APIRouter(
    prefix=ADMIN_PREFIX,
    tags=["admin"],
    route_class=AdminRoute,
    dependencies=[Depends(require_proposer)],
)
main_router = APIRouter(prefix="/api/v1", tags=["proposals"])


async def _own(conn, proposal_id: int, caller: sessions_repo.Session) -> None:
    """404 unless the caller filed the proposal and may still see its target. Its own committed
    read: the service's write that follows then opens a real transaction, not a savepoint."""
    async with conn.transaction():
        found = await repo.get(conn, proposal_id, caller.id, visible_only=True)
    if found is None:
        raise not_found("proposal")


# ---------------------------------------------------------------- agent port


@proposal_router.post("/proposals", status_code=201, response_model=schemas.ProposalView)
async def create_proposal(
    body: schemas.ProposalIn, request: Request, response: Response, pool: Pool, settings: Cfg
) -> schemas.ProposalView:
    """201 with the new proposal; 200 with the first one on a `client_key` replay."""
    _, caller = caller_of(request)
    async with pool.connection() as conn:
        view, created = await proposals.create(
            request,
            conn,
            settings,
            body,
            caller=caller,
            superleader=bool(caller.spec.session_messaging),
        )
    if not created:
        response.status_code = 200
    return view


@proposal_router.get("/proposals", response_model=list[schemas.ProposalView])
async def list_own_proposals(
    request: Request, pool: Pool, settings: Cfg, status: Status = "active", limit: Limit = 50
) -> list[schemas.ProposalView]:
    """The caller's own proposals, newest first, current revision only."""
    _, caller = caller_of(request)
    async with pool.connection() as conn:
        return await proposals.list_views(
            conn, settings, proposer=caller.id, status=status, limit=limit, visible_only=True
        )


@proposal_router.get("/proposals/{proposal_id}", response_model=schemas.ProposalView)
async def get_own_proposal(
    proposal_id: int, request: Request, pool: Pool, settings: Cfg
) -> schemas.ProposalView:
    _, caller = caller_of(request)
    async with pool.connection() as conn:
        await _own(conn, proposal_id, caller)
        return await proposals.get_view(conn, settings, proposal_id, proposer=caller.id)


@proposal_router.post("/proposals/{proposal_id}/revise", response_model=schemas.ProposalView)
async def revise_proposal(
    proposal_id: int,
    body: schemas.ProposalRevise,
    request: Request,
    pool: Pool,
    settings: Cfg,
) -> schemas.ProposalView:
    _, caller = caller_of(request)
    async with pool.connection() as conn:
        await _own(conn, proposal_id, caller)
        return await proposals.revise(request, conn, settings, proposal_id, body, caller=caller)


@proposal_router.post("/proposals/{proposal_id}/withdraw", response_model=schemas.ProposalView)
async def withdraw_proposal(
    proposal_id: int, request: Request, pool: Pool, settings: Cfg
) -> schemas.ProposalView:
    _, caller = caller_of(request)
    async with pool.connection() as conn:
        await _own(conn, proposal_id, caller)
        return await proposals.withdraw(request, conn, settings, proposal_id, caller=caller)


# ---------------------------------------------------------------- main port (human only)


@main_router.get("/proposals", response_model=list[schemas.ProposalView])
async def list_proposals(
    pool: Pool, settings: Cfg, status: Status = "active", limit: Limit = 50
) -> list[schemas.ProposalView]:
    """Every session's proposals, newest first, current revision only."""
    async with pool.connection() as conn:
        return await proposals.list_views(conn, settings, proposer=None, status=status, limit=limit)


@main_router.get("/proposals/{proposal_id}", response_model=schemas.ProposalView)
async def get_proposal(proposal_id: int, pool: Pool, settings: Cfg) -> schemas.ProposalView:
    async with pool.connection() as conn:
        return await proposals.get_view(conn, settings, proposal_id, proposer=None)


@main_router.post("/proposals/{proposal_id}/review", response_model=schemas.ProposalView)
async def review_proposal(
    proposal_id: int, body: schemas.ReviewIn, request: Request, pool: Pool, settings: Cfg
) -> schemas.ProposalView:
    """The human decides every item of the current revision (plan 3.4 step 4, 3.5)."""
    async with pool.connection() as conn:
        return await proposals.review(request, conn, settings, proposal_id, body)

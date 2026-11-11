"""Project grant routes (T-120 slice 1).

The admin routes sit on `admin.router` (agent port, superleader gate, audited): a grant of an
extra project to a session or preset is filed as a change proposal the human reviews (202 with
the proposal, user answer Q4 A); a revoke is direct (200 with the target's view). Targets follow
the admin visibility rule (`admin._target`, `admin.manageable_preset`: else 404).

`main_router` (main port, the human): `PUT .../extra-projects` sets the whole list directly.
`PortGate` keeps it off the agent port.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Request, Response

from agents_orchestrator.api import presets as presets_api
from agents_orchestrator.api import project_grants, proposals, schemas
from agents_orchestrator.api import sessions as sessions_api
from agents_orchestrator.api.admin import _target as admin_target
from agents_orchestrator.api.admin import caller_of, manageable_preset
from agents_orchestrator.api.admin import router as admin_router
from agents_orchestrator.api.errors import ApiError, not_found
from agents_orchestrator.api.sessions import Cfg, Pool
from agents_orchestrator.api.spec_edit import note_fields
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.db.queue import SessionBeingDeleted

main_router = APIRouter(prefix="/api/v1", tags=["projects"])


def _deleting() -> ApiError:
    return ApiError(409, "session_deleting", "the session is being discarded")


async def _file(
    request: Request,
    response: Response,
    pool,
    settings,
    kind: str,
    ref: str,
    body: schemas.ProjectGrantIn,
) -> schemas.ProposalView:
    _, caller = caller_of(request)
    async with pool.connection() as conn:
        view, created = await proposals.create_projects(
            request, conn, settings, kind=kind, ref=ref, body=body, caller=caller
        )
    if not created:
        response.status_code = 200
    return view


@admin_router.post(
    "/sessions/{slug}/projects", status_code=202, response_model=schemas.ProposalView
)
async def grant_session_project(
    slug: str,
    body: schemas.ProjectGrantIn,
    request: Request,
    response: Response,
    pool: Pool,
    settings: Cfg,
) -> schemas.ProposalView:
    """File a grant of an extra project to a session as a `session_projects` proposal; the
    session gets full membership in that project once the user approves it (no restart). 200
    with the first proposal on a `client_key` replay."""
    return await _file(request, response, pool, settings, "session_projects", slug, body)


@admin_router.delete("/sessions/{slug}/projects/{project}", response_model=schemas.SessionView)
async def revoke_session_project(
    slug: str, project: str, request: Request, pool: Pool, settings: Cfg
) -> schemas.SessionView:
    """Remove an extra project from a session at once (toward safe, the superleader's alone);
    the session gets a notice. 404 when the project is not one of its extra projects."""
    _, caller = caller_of(request)
    note_fields(request, ["projects"])
    async with pool.connection() as conn:
        async with conn.transaction():
            target = await admin_target(conn, slug, caller)
            try:
                locked = await sessions_repo.lock_active(conn, target.id)
            except SessionBeingDeleted:
                raise _deleting() from None
            if project not in locked.extra_projects:
                raise not_found("project grant")
            kept = [p for p in locked.extra_projects if p != project]
            session = await sessions_repo.set_extra_projects(conn, target.id, kept)
            await project_grants.notify(conn, target.id, project_grants.revoked_text(project))
        return await sessions_api.one_view(conn, session, settings)


@admin_router.post("/presets/{ref}/projects", status_code=202, response_model=schemas.ProposalView)
async def grant_preset_project(
    ref: str,
    body: schemas.ProjectGrantIn,
    request: Request,
    response: Response,
    pool: Pool,
    settings: Cfg,
) -> schemas.ProposalView:
    """File a grant of an extra project to a preset as a `preset_projects` proposal; sessions
    created from the preset after the approval get it."""
    return await _file(request, response, pool, settings, "preset_projects", ref, body)


@admin_router.delete("/presets/{ref}/projects/{project}", response_model=schemas.PresetView)
async def revoke_preset_project(
    ref: str, project: str, request: Request, pool: Pool
) -> schemas.PresetView:
    """Remove an extra project from a preset at once; running sessions keep theirs (revoke those
    on each session). 404 when the project is not one of its extra projects."""
    note_fields(request, ["projects"])
    async with pool.connection() as conn:
        async with conn.transaction():
            found = await manageable_preset(conn, ref)
            locked = await presets_repo.get_for_update(conn, found.id)
            if locked is None:
                raise not_found("preset")
            if project not in locked.extra_projects:
                raise not_found("project grant")
            kept = [p for p in locked.extra_projects if p != project]
            await presets_repo.set_extra_projects(conn, found.id, kept)
        return await presets_api.get_one(conn, found.id)


@main_router.put("/sessions/{session_id}/extra-projects", response_model=schemas.SessionView)
async def put_session_extra_projects(
    session_id: uuid.UUID, body: schemas.ExtraProjectsIn, pool: Pool, settings: Cfg
) -> schemas.SessionView:
    """The human sets a live session's extra projects directly; the change applies on the next
    request (no restart) and the session gets a notice per added or removed project."""
    async with pool.connection() as conn:
        async with conn.transaction():
            peek = await sessions_repo.get(conn, session_id)
            if peek is None:
                raise not_found("session")
            wanted = await project_grants.check_set(
                conn, peek.project, peek.extra_projects, body.extra_projects
            )
            try:
                locked = await sessions_repo.lock_active(conn, session_id)
            except sessions_repo.SessionNotFound:
                raise not_found("session") from None
            except SessionBeingDeleted:
                raise _deleting() from None
            if (locked.project, locked.extra_projects) != (peek.project, peek.extra_projects):
                raise ApiError(409, "conflict", "the session's projects changed meanwhile; retry")
            session = await sessions_repo.set_extra_projects(conn, session_id, wanted)
            for slug in sorted(set(wanted) - set(locked.extra_projects)):
                text = project_grants.granted_text(slug, "set by the user")
                await project_grants.notify(conn, session_id, text)
            for slug in sorted(set(locked.extra_projects) - set(wanted)):
                await project_grants.notify(conn, session_id, project_grants.revoked_text(slug))
        return await sessions_api.one_view(conn, session, settings)


@main_router.put("/presets/{preset_id}/extra-projects", response_model=schemas.PresetView)
async def put_preset_extra_projects(
    preset_id: uuid.UUID, body: schemas.ExtraProjectsIn, pool: Pool
) -> schemas.PresetView:
    """The human sets a preset's extra projects directly; sessions created from it afterwards
    get them, running sessions keep theirs."""
    async with pool.connection() as conn:
        async with conn.transaction():
            peek = await presets_repo.get(conn, preset_id)
            if peek is None:
                raise not_found("preset")
            wanted = await project_grants.check_set(
                conn, peek.project, peek.extra_projects, body.extra_projects
            )
            locked = await presets_repo.get_for_update(conn, preset_id)
            if locked is None:
                raise not_found("preset")
            if (locked.project, locked.extra_projects) != (peek.project, peek.extra_projects):
                raise ApiError(409, "conflict", "the preset's projects changed meanwhile; retry")
            await presets_repo.set_extra_projects(conn, preset_id, wanted)
        return await presets_api.get_one(conn, preset_id)

"""Admin routes of the agent port (`/agent/v1/admin`), for a superleader session (D3, D10).

One gate covers the whole router: `require_superleader` (a live agent of a session with
`session_messaging` set that no trigger service created). Every call is audited without values
or bodies (D15): one log line and one `events` row the UI feed shows.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, Request, Response
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.routing import APIRoute
from starlette.responses import Response as StarletteResponse

from agents_orchestrator.api import messages as messages_api
from agents_orchestrator.api import presets as presets_api
from agents_orchestrator.api import schemas
from agents_orchestrator.api import sessions as sessions_api
from agents_orchestrator.api.agent import Caller
from agents_orchestrator.api.errors import ApiError, not_found
from agents_orchestrator.api.sessions import Cfg, Pool
from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import events as events_repo
from agents_orchestrator.db import queue as q
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)

ADMIN_PREFIX = "/agent/v1/admin"
_WRAPPER_SESSION = "admin"  # the events key of audit rows, which no wrapper session uses


async def require_superleader(request: Request, agent: Caller, pool: Pool) -> sessions_repo.Session:
    """The caller's session. 403 `trigger_session` first, then 403 `not_superleader`."""
    async with pool.connection() as conn:
        session = await sessions_repo.get(conn, agent.session_id)
    assert session is not None
    if session.trigger_created:
        raise ApiError(403, "trigger_session", "trigger-created sessions have no admin access")
    if not session.spec.session_messaging:
        raise ApiError(403, "not_superleader", "admin routes need a superleader session")
    request.state.admin_caller = (agent, session)
    request.state.admin_secrets = []
    return session


Superleader = Depends(require_superleader)


def caller_of(request: Request):
    """The (agent, session) the gate admitted."""
    return request.state.admin_caller


async def _audit(request: Request, status: int) -> None:
    admitted = getattr(request.state, "admin_caller", None)
    if admitted is None:
        return  # refused at the gate: not an admin call
    agent, session = admitted
    secrets = sorted(set(request.state.admin_secrets))
    path = request.url.path
    log.info(
        "admin %s %s -> %s session=%s secrets=%s",
        request.method,
        path,
        status,
        session.slug,
        secrets,
    )
    data = {
        "method": request.method,
        "path": path,
        "status": status,
        "session": session.slug,
        "secrets": secrets,
    }
    try:
        async with request.app.state.pool.connection() as conn:
            await events_repo.insert_events(
                conn,
                session.id,
                agent.id,
                _WRAPPER_SESSION,
                [{"seq": time.time_ns() // 1000, "type": "admin_call", "data": data}],
            )
    except Exception:
        log.exception("admin audit row not written")


class AdminRoute(APIRoute):
    """Audits each call once it has an outcome, whatever the outcome."""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, StarletteResponse]]:
        original = super().get_route_handler()

        async def handler(request: Request) -> StarletteResponse:
            status = 500
            try:
                response = await original(request)
                status = response.status_code
                return response
            except ApiError as exc:
                status = exc.status
                raise
            except HTTPException as exc:
                status = exc.status_code
                raise
            except RequestValidationError:
                status = 422
                raise
            finally:
                await _audit(request, status)

        return handler


router = APIRouter(
    prefix=ADMIN_PREFIX, tags=["admin"], route_class=AdminRoute, dependencies=[Superleader]
)


def touched(request: Request, *names: str) -> None:
    request.state.admin_secrets.extend(names)


def _sent_secrets(body: schemas.PresetIn) -> list[str]:
    return [n for n, e in body.spec.env.items() if e.protected and e.value is not None]


# ---------------------------------------------------------------- presets (O6)


@router.get("/presets", response_model=list[schemas.PresetView])
async def list_presets(pool: Pool) -> list[schemas.PresetView]:
    async with pool.connection() as conn:
        return await presets_api.list_all(conn)


@router.post("/presets", status_code=201, response_model=schemas.PresetView)
async def create_preset(
    body: schemas.PresetIn, request: Request, pool: Pool, settings: Cfg
) -> schemas.PresetView:
    touched(request, *_sent_secrets(body))
    async with pool.connection() as conn:
        return await presets_api.create(request, conn, settings, body)


@router.get("/presets/{ref}", response_model=schemas.PresetView)
async def get_preset(ref: str, pool: Pool) -> schemas.PresetView:
    async with pool.connection() as conn:
        found = await presets_api.by_ref(conn, ref)
        return await presets_api.get_one(conn, found.id)


@router.put("/presets/{ref}", response_model=schemas.PresetView)
async def replace_preset(
    ref: str, body: schemas.PresetIn, request: Request, pool: Pool, settings: Cfg
) -> schemas.PresetView:
    touched(request, *_sent_secrets(body))
    async with pool.connection() as conn:
        found = await presets_api.by_ref(conn, ref)
        return await presets_api.replace(request, conn, settings, found.id, body)


@router.delete("/presets/{ref}", status_code=204, response_model=None)
async def delete_preset(ref: str, request: Request, pool: Pool, settings: Cfg) -> Response:
    async with pool.connection() as conn:
        found = await presets_api.by_ref(conn, ref)
        await presets_api.remove(request, conn, settings, found.id)
    return Response(status_code=204)


@router.post("/presets/{ref}/copy", status_code=201, response_model=schemas.PresetView)
async def copy_preset(
    ref: str, pool: Pool, body: schemas.CopyPreset | None = None
) -> schemas.PresetView:
    async with pool.connection() as conn:
        found = await presets_api.by_ref(conn, ref)
        return await presets_api.copy(conn, found.id, body.name if body else None)


@router.put("/presets/{ref}/secrets/{name}", response_model=schemas.SecretView)
async def set_preset_secret(
    ref: str, name: str, body: schemas.SecretIn, request: Request, pool: Pool, settings: Cfg
) -> schemas.SecretView:
    touched(request, name)
    async with pool.connection() as conn:
        return await presets_api.put_secret(request, conn, settings, ref, name, body)


# ---------------------------------------------------------------- sessions (O7)


async def _target(conn, slug: str) -> sessions_repo.Session:
    session = await sessions_repo.get_by_slug(conn, slug)
    if session is None:
        raise not_found("session")
    return session


@router.get("/sessions/{slug}", response_model=schemas.SessionView)
async def get_session(slug: str, pool: Pool, settings: Cfg) -> schemas.SessionView:
    async with pool.connection() as conn:
        return await sessions_api.one_view(conn, await _target(conn, slug), settings)


@router.post("/sessions", status_code=201, response_model=schemas.SessionView)
async def create_session(
    body: schemas.CreateSession, request: Request, pool: Pool, settings: Cfg
) -> schemas.SessionView:
    """Same body and rules as the main port; the caller's session is recorded as the creator
    and `created_by_service` stays empty, so the new session is never a trigger session."""
    _, caller = caller_of(request)
    return await sessions_api.create(pool, settings, body, created_by_session=caller.id)


@router.delete("/sessions/{slug}", status_code=202, response_model=schemas.SessionView)
async def discard_session(
    slug: str, request: Request, pool: Pool, settings: Cfg
) -> schemas.SessionView:
    _, caller = caller_of(request)
    async with pool.connection() as conn:
        target = await _target(conn, slug)
        if target.id == caller.id:
            raise ApiError(422, "own_session", "a session cannot discard itself")
        return await sessions_api.discard(conn, target.id, settings)


# ---------------------------------------------------------------- restart (O8)


async def _self_restart(conn, settings: Settings, caller: sessions_repo.Session) -> None:
    """Claim the caller's cooldown slot or refuse with 429. Runs inside the restart's
    transaction, so a failure later rolls the claim back and spends nothing."""
    claim = await sessions_repo.claim_self_restart(
        conn, caller.id, settings.self_restart_cooldown_seconds
    )
    if not claim.claimed:
        raise ApiError(
            429,
            "restart_cooldown",
            f"this session restarted itself recently; try again in {claim.retry_after} seconds",
            retry_after=claim.retry_after,
        )


def _self_note(what: str, settings: Settings) -> str:
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%SZ")
    return (
        f"This restart was self-initiated: you asked for {what} to restart at {now}. "
        f"Another self-restart is refused for {settings.self_restart_cooldown_seconds} seconds. "
        "Do not restart again unless it is required."
    )


@router.post(
    "/sessions/{slug}/restart", status_code=202, response_model=schemas.SessionRestartAccepted
)
async def restart_session(
    slug: str, request: Request, pool: Pool, settings: Cfg
) -> schemas.SessionRestartAccepted:
    """Restart every live agent of a session. The caller's own session claims the self-restart
    cooldown and gets one note, all in the same transaction as the commands."""
    _, caller = caller_of(request)
    async with pool.connection() as conn, conn.transaction():
        target = await _target(conn, slug)
        own = target.id == caller.id
        if own:
            await _self_restart(conn, settings, caller)
        ids = await messages_api.restart_all(conn, target.id)
        if own:
            await q.enqueue_message(
                conn, caller.id, _self_note("your session", settings), lane="priority"
            )
    return schemas.SessionRestartAccepted(command_ids=ids)


@router.post(
    "/sessions/{slug}/agents/{pod}/restart", status_code=202, response_model=schemas.CommandAccepted
)
async def restart_agent(
    slug: str, pod: str, request: Request, pool: Pool, settings: Cfg
) -> schemas.CommandAccepted:
    """Restart one agent by pod name; an agent of the caller's own session is a self-restart."""
    _, caller = caller_of(request)
    async with pool.connection() as conn, conn.transaction():
        target = await _target(conn, slug)
        if target.desired == "deleting":
            raise ApiError(409, "session_deleting", "session is being discarded")
        agent_id = await _member_id(conn, target.id, pod)
        own = target.id == caller.id
        if own:
            await _self_restart(conn, settings, caller)
        command_id = await messages_api.enqueue_agent_command(conn, agent_id, "restart")
        if own:
            await q.enqueue_message(conn, caller.id, _self_note(pod, settings), lane="priority")
    return schemas.CommandAccepted(command_id=command_id)


async def _member_id(conn, session_id: uuid.UUID, pod_name: str) -> uuid.UUID:
    for member in await agents_repo.list_members(conn, session_id):
        if member["pod_name"] == pod_name:
            return member["id"]
    raise not_found(f"agent {pod_name}")


@router.get("/commands/{command_id}", response_model=schemas.CommandView)
async def get_command(command_id: int, pool: Pool) -> schemas.CommandView:
    async with pool.connection() as conn:
        return await messages_api.command_view(conn, command_id)

"""Admin routes of the agent port (`/agent/v1/admin`), for a superleader session (D3, D10).

One gate covers `router`: `require_superleader` (a live agent of a session with
`session_messaging` set, whoever created the session). `session_router` (GET and PATCH
`/sessions/{slug}`, POST `/sessions/{slug}/leadership/withdraw`) carries
`require_session_editor` on each route instead, which also admits any other session's leader on
its own slug. Every admitted call is audited without values or bodies (D15): one log line and
one `events` row the UI feed shows.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query, Request, Response
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.routing import APIRoute
from pydantic import BaseModel
from starlette.responses import Response as StarletteResponse

from agents_orchestrator.api import messages as messages_api
from agents_orchestrator.api import presets as presets_api
from agents_orchestrator.api import projects as projects_api
from agents_orchestrator.api import schemas, session_spec, streams
from agents_orchestrator.api import sessions as sessions_api
from agents_orchestrator.api.agent import (
    ACTIVITY_DEFAULT_LIMIT,
    ACTIVITY_MAX_LIMIT,
    Activity,
    Caller,
    LockReassignIn,
    LocksOut,
    _leads,
    activity_lines,
    locks_of,
    member_id,
    normalise_all,
)
from agents_orchestrator.api.errors import ApiError, not_found, not_manageable
from agents_orchestrator.api.session_labels import OwnLabelsView
from agents_orchestrator.api.sessions import Cfg, Pool, Usage
from agents_orchestrator.api.spec_edit import note_fields
from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import events as events_repo
from agents_orchestrator.db import file_locks, leadership, views
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import queue as q
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)

ADMIN_PREFIX = "/agent/v1/admin"
_WRAPPER_SESSION = "admin"  # the events key of audit rows, which no wrapper session uses


def _admit(
    request: Request, agent: agents_repo.Agent, session: sessions_repo.Session
) -> sessions_repo.Session:
    """Hand the admitted caller to the route and the audit."""
    request.state.admin_caller = (agent, session)
    request.state.admin_secrets = []
    request.state.admin_vault = []
    return session


async def require_superleader(request: Request, agent: Caller, pool: Pool) -> sessions_repo.Session:
    """The caller's session; 403 `not_superleader` unless its spec enables `session_messaging`.
    Who created the session does not matter."""
    async with pool.connection() as conn:
        session = await sessions_repo.get(conn, agent.session_id)
    assert session is not None
    if not session.spec.session_messaging:
        raise ApiError(403, "not_superleader", "admin routes need a superleader session")
    return _admit(request, agent, session)


async def require_session_editor(
    request: Request, agent: Caller, pool: Pool, slug: str
) -> sessions_repo.Session:
    """The caller's session. A superleader session passes for any slug (the route resolves the
    target with _writable_target). Any other session passes only for its own slug (else 403
    not_superleader, before any lookup, so another slug and an unknown one answer alike) and
    only for its leader (else 403 not_leader). Sets admin_caller and admin_secrets like
    require_superleader."""
    async with pool.connection() as conn:
        session = await sessions_repo.get(conn, agent.session_id)
        assert session is not None
        if not session.spec.session_messaging:
            if slug != session.slug:
                raise session_spec.not_superleader()
            if not await _leads(conn, agent, session):
                raise session_spec.not_leader()
    return _admit(request, agent, session)


Superleader = Depends(require_superleader)
SessionEditor = Depends(require_session_editor)


def caller_of(request: Request):
    """The (agent, session) the gate admitted."""
    return request.state.admin_caller


def note_detail(request: Request, **detail: Any) -> None:
    """Record non-value detail (ids, names, counts, paths; JSON scalars only) in the audit row.
    A non-scalar is a programming error and raises here, at the call site."""
    for key, value in detail.items():
        if value is not None and not isinstance(value, str | int | float | bool):
            raise TypeError(
                f"admin audit detail {key!r} must be a JSON scalar, not {type(value).__name__}"
            )
    request.state.admin_detail = detail


async def _audit(request: Request, status: int) -> None:
    admitted = getattr(request.state, "admin_caller", None)
    if admitted is None:
        return  # refused at the gate: not an admin call
    agent, session = admitted
    secrets = sorted(set(request.state.admin_secrets))
    vault = sorted({(v["env"], v["entry"], v["entry_env"]) for v in request.state.admin_vault})
    vault = [{"env": e, "entry": i, "entry_env": x} for e, i, x in vault]
    fields = getattr(request.state, "preset_fields", None)
    detail = getattr(request.state, "admin_detail", None)
    detail = detail if isinstance(detail, dict) else None
    path = request.url.path
    log.info(
        "admin %s %s -> %s session=%s secrets=%s%s%s%s",
        request.method,
        path,
        status,
        session.slug,
        secrets,
        f" vault={vault}" if vault else "",
        "" if fields is None else f" fields={fields}",
        "" if detail is None else f" detail={json.dumps(detail, sort_keys=True)}",
    )
    data = {
        "method": request.method,
        "path": path,
        "status": status,
        "session": session.slug,
        "secrets": secrets,
    }
    if vault:
        data["vault"] = vault
    if fields is not None:
        data["fields"] = fields
    if detail is not None:
        data["detail"] = detail
    await write_audit(request.app.state.pool, session, agent, data)


async def write_audit(
    pool: Any, session: sessions_repo.Session, agent: agents_repo.Agent, data: dict[str, Any]
) -> None:
    """Write one `admin_call` events row for an admitted superleader call. Best effort: the call
    already happened, so a failed write is logged, never raised."""
    try:
        async with pool.connection() as conn:
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


FORBIDDEN: dict[int | str, Any] = {
    "description": "not_superleader: the caller is not a superleader"
}
WRITE_FORBIDDEN: dict[int | str, Any] = {
    "description": "not_superleader: the caller is not a superleader; not_manageable: the target "
    "is not superleader_manageable (needs the user's approval)"
}
OWN_SESSION: dict[int | str, Any] = {
    "description": "own_session: the target is the caller's own session "
    "(use the leader-only agent route)"
}
SESSION_NOT_FOUND: dict[int | str, Any] = {"description": "not_found: no session has that slug"}
router = APIRouter(
    prefix=ADMIN_PREFIX, tags=["admin"], route_class=AdminRoute, dependencies=[Superleader]
)
# The routes with {slug} carry the editor gate themselves: it reads the path parameter `slug`, so
# a router-level dependency would make it a required query parameter on any route without one.
session_router = APIRouter(prefix=ADMIN_PREFIX, tags=["admin"], route_class=AdminRoute)


def own_session(what: str) -> ApiError:
    return ApiError(
        422, "own_session", f"{what} of your own session goes through the leader-only agent route"
    )


def touched(request: Request, *names: str) -> None:
    request.state.admin_secrets.extend(names)


# What each non-relay admin route withholds from the superleader: "preset" and "session" routes
# are read open (200 for any target) and write-guarded (403 `not_manageable` outside its remit),
# "list" and "create" are filtered or forced,
# "proposal" GETs show a superleader every proposal; its revise and withdraw see only the
# caller's own on targets it may manage (404 otherwise).
ADMIN_VISIBILITY: dict[tuple[str, str], str] = {
    ("GET", f"{ADMIN_PREFIX}/presets"): "list",
    ("GET", f"{ADMIN_PREFIX}/vault"): "list",
    ("GET", f"{ADMIN_PREFIX}/sessions"): "list",
    ("GET", f"{ADMIN_PREFIX}/meta"): "list",
    ("GET", f"{ADMIN_PREFIX}/archives"): "list",
    ("GET", f"{ADMIN_PREFIX}/archives/settings"): "list",
    ("PUT", f"{ADMIN_PREFIX}/archives/settings"): "list",
    ("GET", f"{ADMIN_PREFIX}/archives/cleanup/preview"): "list",
    ("POST", f"{ADMIN_PREFIX}/presets"): "create",
    ("GET", f"{ADMIN_PREFIX}/presets/{{ref}}"): "preset",
    ("PUT", f"{ADMIN_PREFIX}/presets/{{ref}}"): "preset",
    ("PATCH", f"{ADMIN_PREFIX}/presets/{{ref}}"): "preset",
    ("DELETE", f"{ADMIN_PREFIX}/presets/{{ref}}"): "preset",
    ("POST", f"{ADMIN_PREFIX}/presets/{{ref}}/copy"): "preset",
    ("PUT", f"{ADMIN_PREFIX}/presets/{{ref}}/vault/{{entry_id}}"): "preset",
    ("DELETE", f"{ADMIN_PREFIX}/presets/{{ref}}/vault/{{entry_id}}"): "preset",
    ("GET", f"{ADMIN_PREFIX}/sessions/{{slug}}"): "session",
    ("GET", f"{ADMIN_PREFIX}/sessions/{{slug}}/activity"): "session",
    ("GET", f"{ADMIN_PREFIX}/sessions/{{slug}}/events"): "session",
    ("GET", f"{ADMIN_PREFIX}/sessions/{{slug}}/messages"): "session",
    ("GET", f"{ADMIN_PREFIX}/sessions/{{slug}}/locks"): "session",
    ("GET", f"{ADMIN_PREFIX}/sessions/{{slug}}/commands"): "session",
    ("PATCH", f"{ADMIN_PREFIX}/sessions/{{slug}}"): "session",
    ("POST", f"{ADMIN_PREFIX}/sessions/{{slug}}/leadership/withdraw"): "session",
    ("POST", f"{ADMIN_PREFIX}/sessions"): "create",
    ("DELETE", f"{ADMIN_PREFIX}/sessions/{{slug}}"): "session",
    ("POST", f"{ADMIN_PREFIX}/sessions/{{slug}}/restart"): "session",
    ("POST", f"{ADMIN_PREFIX}/sessions/{{slug}}/agents/{{pod}}/restart"): "session",
    ("GET", f"{ADMIN_PREFIX}/commands/{{command_id}}"): "none",
    ("GET", f"{ADMIN_PREFIX}/usage"): "none",
    ("DELETE", f"{ADMIN_PREFIX}/specs/{{key}}"): "none",
    ("POST", f"{ADMIN_PREFIX}/specs/{{key}}/comments/{{comment_id}}/dismiss"): "none",
    ("POST", f"{ADMIN_PREFIX}/projects"): "none",
    ("PATCH", f"{ADMIN_PREFIX}/projects/{{slug}}"): "none",
    ("PUT", f"{ADMIN_PREFIX}/sessions/{{slug}}/labels"): "session",
    ("POST", f"{ADMIN_PREFIX}/sessions/{{slug}}/scale"): "session",
    ("POST", f"{ADMIN_PREFIX}/sessions/{{slug}}/locks/reassign"): "session",
    ("DELETE", f"{ADMIN_PREFIX}/sessions/{{slug}}/messages/{{message_id}}"): "session",
    ("POST", f"{ADMIN_PREFIX}/sessions/{{slug}}/projects"): "session",
    ("DELETE", f"{ADMIN_PREFIX}/sessions/{{slug}}/projects/{{project}}"): "session",
    ("POST", f"{ADMIN_PREFIX}/presets/{{ref}}/projects"): "preset",
    ("DELETE", f"{ADMIN_PREFIX}/presets/{{ref}}/projects/{{project}}"): "preset",
    ("POST", f"{ADMIN_PREFIX}/proposals"): "proposal",
    ("GET", f"{ADMIN_PREFIX}/proposals"): "proposal",
    ("GET", f"{ADMIN_PREFIX}/proposals/{{proposal_id}}"): "proposal",
    ("POST", f"{ADMIN_PREFIX}/proposals/{{proposal_id}}/revise"): "proposal",
    ("POST", f"{ADMIN_PREFIX}/proposals/{{proposal_id}}/withdraw"): "proposal",
}


# ---------------------------------------------------------------- projects (T-113)


@router.post("/projects", status_code=201, response_model=schemas.ProjectView)
async def create_project(
    body: schemas.ProjectIn, request: Request, pool: Pool
) -> schemas.ProjectView:
    """Create a project, same body, answer and errors as the main-port route. The audit row
    carries the field names only."""
    note_fields(request, ["slug", "name"])
    return await projects_api.create(pool, body)


@router.patch(
    "/projects/{slug}",
    response_model=schemas.ProjectView,
    responses={
        403: FORBIDDEN,
        404: {"description": "not_found: no project has that slug"},
        409: {"description": "default_project: the default project cannot be archived"},
    },
)
async def update_project(
    slug: str, body: schemas.ProjectPatch, request: Request, pool: Pool
) -> schemas.ProjectView:
    """Rename, describe, archive or unarchive a project, same body, answer and errors as the
    main-port route. An explicit null keeps the stored value and is still listed in the audit
    row, which carries the set field names only."""
    note_fields(request, sorted(body.model_fields_set))
    return await projects_api.patch(pool, slug, body)


# ---------------------------------------------------------------- presets (O6)


async def readable_preset(conn, ref: str) -> presets_repo.Preset:
    """A preset by id or name; 404 only when unknown."""
    return await presets_api.by_ref(conn, ref)


async def writable_preset(conn, ref: str) -> presets_repo.Preset:
    """A preset by id or name the superleader may write: 403 `not_manageable` for any other."""
    found = await readable_preset(conn, ref)
    if not found.superleader_manageable:
        raise not_manageable("preset")
    return found


@router.get("/meta", response_model=schemas.Meta, responses={403: FORBIDDEN})
async def get_meta(settings: Cfg) -> schemas.Meta:
    """The deploy settings the main port answers (defaults, images, models, caps)."""
    return await sessions_api.meta(settings)


@router.get("/presets", response_model=list[schemas.PresetView])
async def list_presets(pool: Pool) -> list[schemas.PresetView]:
    async with pool.connection() as conn:
        return await presets_api.list_all(conn)


@router.get("/sessions", response_model=list[schemas.SessionView], responses={403: FORBIDDEN})
async def list_sessions(
    pool: Pool,
    settings: Cfg,
    label: Annotated[list[str] | None, Query()] = None,
    project: str | None = None,
    superleaders: Literal["include"] | None = None,
    session_slug: str | None = None,
) -> list[schemas.SessionView]:
    """Every session, raw-spec and unmanageable ones included, as the redacted `SessionView`;
    the main port's filters and errors. The slug filter is `session_slug`: the editor gate
    invariant keeps a parameter named `slug` path-only."""
    async with pool.connection() as conn:
        return await sessions_api.list_views(
            conn,
            settings,
            label=label,
            project=project,
            superleaders=superleaders == "include",
            slug=session_slug,
        )


@router.post("/presets", status_code=201, response_model=schemas.PresetView)
async def create_preset(
    body: schemas.PresetIn, request: Request, pool: Pool, settings: Cfg
) -> schemas.PresetView:
    async with pool.connection() as conn:
        return await presets_api.create(request, conn, settings, body, superleader=True)


@router.get("/presets/{ref}", response_model=schemas.PresetView)
async def get_preset(ref: str, pool: Pool) -> schemas.PresetView:
    async with pool.connection() as conn:
        found = await readable_preset(conn, ref)
        return await presets_api.get_one(conn, found.id)


@router.put("/presets/{ref}", response_model=schemas.PresetView)
async def replace_preset(
    ref: str, body: schemas.PresetIn, request: Request, pool: Pool, settings: Cfg
) -> schemas.PresetView:
    async with pool.connection() as conn:
        found = await writable_preset(conn, ref)
        return await presets_api.replace(request, conn, settings, found.id, body, superleader=True)


@router.patch(
    "/presets/{ref}",
    response_model=schemas.PresetView,
    responses={
        403: WRITE_FORBIDDEN,
        404: {"description": "not_found: no preset has that name or id"},
        409: {
            "description": "conflict: changed meanwhile; open_proposals: the preset has active "
            "change proposals (extra.proposals lists their ids)"
        },
    },
)
async def patch_preset(
    ref: str, body: schemas.PresetPatch, request: Request, pool: Pool, settings: Cfg
) -> schemas.PresetView:
    async with pool.connection() as conn:
        found = await writable_preset(conn, ref)
        return await presets_api.patch(request, conn, settings, found.id, body)


@router.delete("/presets/{ref}", status_code=204, response_model=None)
async def delete_preset(ref: str, request: Request, pool: Pool) -> Response:
    async with pool.connection() as conn:
        found = await writable_preset(conn, ref)
        await presets_api.remove(request, conn, found.id, superleader=True)
    return Response(status_code=204)


@router.post("/presets/{ref}/copy", status_code=201, response_model=schemas.PresetView)
async def copy_preset(
    ref: str, request: Request, pool: Pool, body: schemas.CopyPreset | None = None
) -> schemas.PresetView:
    async with pool.connection() as conn:
        found = await writable_preset(conn, ref)
        body = body or schemas.CopyPreset()
        return await presets_api.copy(
            conn, found.id, body.name, superleader=True, request=request, project=body.project
        )


@router.put("/presets/{ref}/vault/{entry_id}", response_model=schemas.VaultAttachView)
async def attach_vault(
    ref: str,
    entry_id: uuid.UUID,
    body: schemas.VaultAttachIn,
    request: Request,
    pool: Pool,
    settings: Cfg,
) -> schemas.VaultAttachView:
    """Attach a vault entry as refs, in one save, on a manageable preset (privileged ones too).
    `envs` maps entry env to preset env; one entry env may back several preset envs through
    repeated calls, which keep the refs already there. Without `envs` every env of the entry is
    attached under its own name. A preset env that is taken is 409 `env_exists` unless
    `replace`. The answer marks `attachment_privileged` (marked, never blocked)."""
    async with pool.connection() as conn:
        found = await writable_preset(conn, ref)
        view = await presets_api.attach_vault(request, conn, settings, found.id, entry_id, body)
    return schemas.VaultAttachView(**view.model_dump(), attachment_privileged=bool(view.privileged))


@router.delete("/presets/{ref}/vault/{entry_id}", response_model=schemas.PresetView)
async def detach_vault(
    ref: str,
    entry_id: uuid.UUID,
    request: Request,
    pool: Pool,
    settings: Cfg,
    env: str | None = None,
) -> schemas.PresetView:
    """Remove every ref to the entry from a manageable preset, or only the one under preset env
    `?env=`; 404 `not_attached` when nothing matches."""
    async with pool.connection() as conn:
        found = await writable_preset(conn, ref)
        return await presets_api.detach_vault(request, conn, settings, found.id, entry_id, env)


# ---------------------------------------------------------------- sessions (O7)


async def _readable_target(conn, slug: str, caller: sessions_repo.Session) -> sessions_repo.Session:
    """Any session by slug (raw-spec ones too); 404 only when unknown. The route gate already
    limits a non-superleader caller to its own slug."""
    session = await sessions_repo.get_by_slug(conn, slug)
    if session is None:
        raise not_found("session")
    return session


async def _writable_target(conn, slug: str, caller: sessions_repo.Session) -> sessions_repo.Session:
    """The caller's own session, or one whose preset the superleader may manage; any other
    (unmanageable preset, no preset) is 403 `not_manageable`."""
    session = await _readable_target(conn, slug, caller)
    if session.id != caller.id and (
        session.preset_id is None
        or session.preset_id not in await presets_repo.manageable_ids(conn)
    ):
        raise not_manageable("session")
    return session


@session_router.get(
    "/sessions/{slug}", response_model=schemas.SessionView, dependencies=[SessionEditor]
)
async def get_session(
    slug: str, request: Request, pool: Pool, settings: Cfg
) -> schemas.SessionView:
    """A superleader reads any session; any other leader its own."""
    _, caller = caller_of(request)
    async with pool.connection() as conn:
        return await sessions_api.one_view(
            conn, await _readable_target(conn, slug, caller), settings
        )


@session_router.patch(
    "/sessions/{slug}", response_model=schemas.SessionView, dependencies=[SessionEditor]
)
async def patch_session(
    slug: str, body: schemas.SessionSpecPatch, request: Request, pool: Pool, settings: Cfg
) -> schemas.SessionView:
    """RFC 7396 merge patch of a live session's spec: a superleader on its own or a manageable
    session, any other leader on its own. The source preset is never written."""
    agent, caller = caller_of(request)
    superleader = bool(caller.spec.session_messaging)
    async with pool.connection() as conn:
        target = await _writable_target(conn, slug, caller) if superleader else caller
        return await session_spec.patch(
            request,
            conn,
            settings,
            target,
            body,
            caller=caller,
            caller_agent=agent,
            superleader=superleader,
        )


@session_router.post(
    "/sessions/{slug}/leadership/withdraw",
    response_model=schemas.Withdrawn,
    dependencies=[SessionEditor],
)
async def withdraw_handover(
    slug: str, body: schemas.WithdrawIn, request: Request, pool: Pool
) -> schemas.Withdrawn:
    """Withdraw a pending handover request (`request_id`), or every one of the session when it
    is omitted: the session's own leader, or a superleader on its own or a manageable session.
    A withdrawn request is rejected with reason `withdrawn` and never applied. The reconciler's
    `rollout` request is never withdrawn (409 `rollout_request` by id, left queued without)."""
    _, caller = caller_of(request)
    async with pool.connection() as conn:
        target = (
            await _writable_target(conn, slug, caller) if caller.spec.session_messaging else caller
        )
        withdrawn = await leadership.withdraw_handovers(conn, target.id, body.request_id)
        if body.request_id is not None and not withdrawn:
            reason = await leadership.handover_reason(conn, target.id, body.request_id)
            if reason is None:
                raise not_found("handover request")
            if reason == "rollout":
                raise ApiError(
                    409, "rollout_request", "a rollout handover request cannot be withdrawn"
                )
            raise ApiError(409, "not_pending", "the handover request is already decided")
    return schemas.Withdrawn(withdrawn=withdrawn)


@router.post("/sessions", status_code=201, response_model=schemas.SessionView)
async def create_session(
    body: schemas.CreateSession, request: Request, pool: Pool, settings: Cfg, usage: Usage
) -> schemas.SessionView:
    """Same body and rules as the main port, usage gate included; the caller's session is
    recorded as the creator and `created_by_service` stays empty."""
    _, caller = caller_of(request)
    return await sessions_api.create(
        pool,
        settings,
        body,
        created_by_session=caller.id,
        usage=usage,
        vault=request.app.state.vault,
    )


@router.delete("/sessions/{slug}", status_code=202, response_model=schemas.SessionView)
async def discard_session(
    slug: str, request: Request, pool: Pool, settings: Cfg, force: sessions_api.Force = False
) -> schemas.SessionView:
    """`?force=true` abandons the target's owed pre-discard hooks, also when it is already
    deleting (a discard stuck on a hook receiver that is down)."""
    _, caller = caller_of(request)
    force = sessions_api.force_flag(request)
    async with pool.connection() as conn:
        target = await _writable_target(conn, slug, caller)
        if target.id == caller.id:
            raise ApiError(422, "own_session", "a session cannot discard itself")
        return await sessions_api.discard(conn, target.id, settings, force=force)


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


async def _hold_note(conn, caller: sessions_repo.Session, text: str) -> int:
    """The self-restart note, held until the restart commands finished (the pod is gone by
    then), so the old pod cannot take it and the hive cannot send it twice."""
    return await q.enqueue_message(conn, caller.id, text, lane="priority", held=True)


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
        target = await _writable_target(conn, slug, caller)
        own = target.id == caller.id
        note_id = None
        if own:
            await _self_restart(conn, settings, caller)
            note_id = await _hold_note(conn, caller, _self_note("your session", settings))
        ids = await messages_api.restart_all(conn, target.id, caller.id, note_id)
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
        target = await _writable_target(conn, slug, caller)
        if target.desired == "deleting":
            raise ApiError(409, "session_deleting", "session is being discarded")
        agent_id = await member_id(conn, target.id, pod)
        own = target.id == caller.id
        note_id = None
        if own:
            await _self_restart(conn, settings, caller)
            note_id = await _hold_note(conn, caller, _self_note(pod, settings))
        command_id = await messages_api.enqueue_agent_command(
            conn, agent_id, "restart", caller.id, note_id
        )
    return schemas.CommandAccepted(command_id=command_id)


@router.get("/commands/{command_id}", response_model=schemas.CommandView)
async def get_command(command_id: int, pool: Pool) -> schemas.CommandView:
    """Any command, whoever issued it; 404 only when unknown."""
    async with pool.connection() as conn:
        return await messages_api.command_view(conn, command_id)


# ------------------------------------------------- live work of any session (T-120)


@router.get(
    "/sessions/{slug}/activity",
    response_model=Activity,
    responses={403: FORBIDDEN, 404: SESSION_NOT_FOUND},
)
async def session_activity(
    slug: str,
    request: Request,
    pool: Pool,
    peer: Annotated[str | None, Query(alias="agent")] = None,
    limit: Annotated[int, Query(ge=1, le=ACTIVITY_MAX_LIMIT)] = ACTIVITY_DEFAULT_LIMIT,
) -> Activity:
    """The agent-port activity digest of any session."""
    _, caller = caller_of(request)
    async with pool.connection() as conn:
        target = await _readable_target(conn, slug, caller)
        return await activity_lines(conn, target, peer, limit)


@router.get(
    "/sessions/{slug}/events",
    response_model=streams.EventsPage,
    responses={
        400: {"description": "bad_request: before_id and after_id are mutually exclusive"},
        403: FORBIDDEN,
        404: SESSION_NOT_FOUND,
    },
)
async def session_events(
    slug: str,
    request: Request,
    pool: Pool,
    before_id: Annotated[int | None, Query(ge=0)] = None,
    after_id: Annotated[int | None, Query(ge=0)] = None,
    limit: Annotated[int, Query(ge=1, le=events_repo.MAX_LIMIT)] = 200,
    agent_id: uuid.UUID | None = None,
    types: Annotated[list[str] | None, Query()] = None,
) -> streams.EventsPage:
    """One page of any session's stored events, as the main port's events route (no SSE)."""
    _, caller = caller_of(request)
    async with pool.connection() as conn:
        target = await _readable_target(conn, slug, caller)
        return await streams.events_page(
            conn,
            target.id,
            before_id=before_id,
            after_id=after_id,
            limit=limit,
            agent_id=agent_id,
            types=types,
        )


@router.get(
    "/sessions/{slug}/messages",
    response_model=list[schemas.MessageView],
    responses={403: FORBIDDEN, 404: SESSION_NOT_FOUND},
)
async def session_messages(
    slug: str,
    request: Request,
    pool: Pool,
    status: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> list[schemas.MessageView]:
    """The message queue of any session, with the main port's `status` filter."""
    _, caller = caller_of(request)
    statuses = messages_api.parse_statuses(status)
    async with pool.connection() as conn:
        target = await _readable_target(conn, slug, caller)
        rows = await views.list_messages(
            conn, target.id, statuses, limit, messages_api.DEFAULT_ATTEMPTS
        )
    return [messages_api.message_view(r) for r in rows]


@router.get(
    "/sessions/{slug}/locks",
    response_model=LocksOut,
    responses={403: FORBIDDEN, 404: SESSION_NOT_FOUND},
)
async def session_locks(slug: str, request: Request, pool: Pool) -> LocksOut:
    """The file locks held by the live agents of any session."""
    _, caller = caller_of(request)
    async with pool.connection() as conn:
        return await locks_of(conn, (await _readable_target(conn, slug, caller)).id)


@router.get(
    "/sessions/{slug}/commands",
    response_model=list[schemas.CommandView],
    responses={403: FORBIDDEN, 404: SESSION_NOT_FOUND},
)
async def session_commands(
    slug: str,
    request: Request,
    pool: Pool,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> list[schemas.CommandView]:
    """The newest commands issued to the agents of any session."""
    _, caller = caller_of(request)
    async with pool.connection() as conn:
        target = await _readable_target(conn, slug, caller)
        rows = await views.list_commands(conn, target.id, limit)
    return [schemas.CommandView(**r) for r in rows]


# ---- superleader writes on any manageable session (T-120 slice 3)


@router.put(
    "/sessions/{slug}/labels",
    response_model=OwnLabelsView,
    responses={403: WRITE_FORBIDDEN, 404: SESSION_NOT_FOUND},
)
async def set_session_labels(
    slug: str, body: schemas.SessionLabelsIn, request: Request, pool: Pool
) -> OwnLabelsView:
    """Replace the labels of the caller's own session or of a manageable one; labels are
    normalised and limited as on the main port."""
    _, caller = caller_of(request)
    async with pool.connection() as conn, conn.transaction():
        target = await _writable_target(conn, slug, caller)
        session = await sessions_repo.set_labels(conn, target.id, body.labels)
    note_fields(request, ["labels"])
    return OwnLabelsView(session_id=session.id, labels=session.labels)


@router.post(
    "/sessions/{slug}/scale",
    status_code=202,
    response_model=schemas.ScaleResult,
    responses={
        403: WRITE_FORBIDDEN,
        404: SESSION_NOT_FOUND,
        409: {"description": "usage_quota, static_session, leader_would_leave or session_deleting"},
        422: {
            "description": "out_of_range: replicas outside 1..max_replicas; "
            "own_session: the caller's own session"
        },
    },
)
async def scale_session(
    slug: str,
    body: schemas.ScaleRequest,
    request: Request,
    pool: Pool,
    settings: Cfg,
    usage: Usage,
) -> schemas.ScaleResult:
    """Set spec.replicas of a manageable session in place; the caller's own session is refused
    (422 `own_session`). A scale-up over the usage quota is 409 `usage_quota`: that check reads
    on its own connection and fails open, like the main port's, so it runs between a short
    read of the target and the transaction (never while a connection is held). No kube call."""
    _, caller = caller_of(request)
    async with pool.connection() as conn:
        target = await _writable_target(conn, slug, caller)
    if target.id == caller.id:
        raise own_session("scale")
    await sessions_api.gate_scale_up(pool, usage, settings, target.id, body.replicas)
    async with pool.connection() as conn, conn.transaction():
        target = await _writable_target(conn, slug, caller)
        out = await sessions_api.scale(conn, target.id, body.replicas)
    note_fields(request, ["replicas"])
    note_detail(request, replicas_before=out.previous_replicas, replicas_after=out.replicas)
    return out


class AdminLockReassigned(BaseModel):
    path: str
    agent: str | None
    previous: str | None


@router.post(
    "/sessions/{slug}/locks/reassign",
    response_model=AdminLockReassigned,
    responses={
        400: {"description": "invalid_path: empty or outside the target session workdir"},
        403: WRITE_FORBIDDEN,
        404: {
            "description": "not_found: no session has that slug, or `to` is not a live agent of it"
        },
        422: OWN_SESSION,
    },
)
async def reassign_session_lock(
    slug: str, body: LockReassignIn, request: Request, pool: Pool
) -> AdminLockReassigned:
    """Give a file lock of a manageable session to one of its agents, or discard it with
    `to: null`. The path is read against the target's workdir; the caller's own session is
    refused (422 `own_session`)."""
    _, caller = caller_of(request)
    async with pool.connection() as conn, conn.transaction():
        target = await _writable_target(conn, slug, caller)
        if target.id == caller.id:
            raise own_session("lock reassign")
        (path,) = normalise_all([body.path], target.spec.workdir)
        to_id = await member_id(conn, target.id, body.to) if body.to is not None else None
        previous = await file_locks.holder_for_update(conn, target.id, path)
        await file_locks.assign(conn, target.id, path, to_id)
    note_fields(request, ["path", "to"])
    note_detail(request, path=path, previous=previous, to=body.to)
    return AdminLockReassigned(path=path, agent=body.to, previous=previous)


@router.delete(
    "/sessions/{slug}/messages/{message_id}",
    status_code=204,
    response_model=None,
    responses={
        403: WRITE_FORBIDDEN,
        404: {
            "description": "not_found: no session has that slug, or no message with that id in it"
        },
        409: {"description": "not_queued: only a queued message can be cancelled"},
    },
)
async def cancel_session_message(
    slug: str, message_id: int, request: Request, pool: Pool
) -> Response:
    """Cancel a queued message of a manageable session (the caller's own included). The id is
    matched inside the session: another session's message is 404 and stays queued."""
    _, caller = caller_of(request)
    async with pool.connection() as conn, conn.transaction():
        target = await _writable_target(conn, slug, caller)
        await messages_api.cancel(conn, message_id, target.id)
    return Response(status_code=204)


# ---------------------------------------------------------------- usage


@router.get("/usage", response_model=schemas.UsageView)
async def get_usage(usage: Usage) -> schemas.UsageView:
    """The account's five_hour and seven_day windows, read-only, from the same cached
    `UsageSource` the usage gate reads. An unconfigured or unreachable usage service answers
    200 with `available` false and null windows: the gate allows then, and so does this."""
    windows = await usage.current() if usage is not None else None
    windows = windows or {}

    def view(metric: str) -> schemas.UsageWindowView | None:
        w = windows.get(metric)
        if w is None:
            return None
        return schemas.UsageWindowView(
            utilization_pct=w.percent,
            resets_at=w.resets_at,
            stale=w.stale,
            observed_at=w.observed_at,
            source=w.source,
        )

    five, seven = view("five_hour"), view("seven_day")
    seen = [w.observed_at for w in (five, seven) if w is not None and w.observed_at]
    return schemas.UsageView(
        configured=usage is not None,
        available=bool(windows),
        source="agents-usage",
        updated_at=max(seen, default=None),
        five_hour=five,
        seven_day=seven,
    )

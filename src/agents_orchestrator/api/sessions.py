"""Session, agent and meta routes (D11, D13, D15)."""

from __future__ import annotations

import uuid
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Annotated

from agents_common.templating import TemplateError
from fastapi import APIRouter, Depends, Request
from fastapi.concurrency import run_in_threadpool
from psycopg_pool import AsyncConnectionPool
from pydantic import ValidationError

from agents_orchestrator import rendering
from agents_orchestrator.api import schemas, secret_store, usage_gate
from agents_orchestrator.api.errors import ApiError, not_found
from agents_orchestrator.api.service_auth import calling_service
from agents_orchestrator.api.usage_gate import UsageSource
from agents_orchestrator.cache.keys import ArchiveNameError
from agents_orchestrator.cache.save_rule import save_skip
from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import leadership, presets, views
from agents_orchestrator.db import sessions as repo
from agents_orchestrator.secret_view import redact_spec, session_metas
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import (
    HOOK_EVENT_SPECS,
    INIT_DIR,
    PERMISSION_MODES,
    WORKDIR_ROOT,
    SessionSpec,
    SpecInvalid,
    check_caller_cache_key,
    check_rendered_cache_name,
    default_cache_key,
    fill_resource_defaults,
    generated_slug,
    init_scripts_hash,
    validate_slug,
    validate_spec,
)


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_pool(request: Request) -> AsyncConnectionPool:
    return request.app.state.pool


def get_usage(request: Request) -> UsageSource | None:
    return request.app.state.usage


Pool = Annotated[AsyncConnectionPool, Depends(get_pool)]
Cfg = Annotated[Settings, Depends(get_settings)]
Usage = Annotated[UsageSource | None, Depends(get_usage)]

router = APIRouter(prefix="/api/v1")

_CREATE_LOCK = 0x4F524348  # serialises session creation so max_sessions cannot be overshot


def _init_edited(session: repo.Session) -> bool:
    """True when a run exists (not skipped) and the stored scripts differ from the ones it ran."""
    init = session.status.get("init")
    if not isinstance(init, dict) or init.get("state") in (None, "skipped"):
        return False
    try:
        rendered = rendering.render_init_scripts(session)
    except TemplateError:
        return True
    return init.get("scripts_hash") != init_scripts_hash(session.spec.init_scripts, rendered)


def _closes_at(session: repo.Session) -> datetime | None:
    after = session.spec.autoclose_after_seconds
    return None if after is None else session.created_at + timedelta(seconds=after)


def _last_backup(run: object) -> schemas.BackupRun | None:
    if not isinstance(run, dict) or run.get("state") != "saved":
        return None
    return schemas.BackupRun(finished_at=run["finished_at"], bytes=run["bytes"])


def _backup(session: repo.Session, settings: Settings) -> schemas.SessionBackup:
    """What the discard would save, by the reconciler's own save rule (`save_skip`)."""
    cache = session.status.get("cache")
    cache = cache if isinstance(cache, dict) else {}
    if session.spec.cache is None:
        skips = ["no_session_cache"] * 2
    elif not settings.cache_available:
        skips = ["cache_not_configured"] * 2
    else:
        restore_only = not session.spec.cache.save_key
        skips = [
            save_skip(True, session.status, target, restore_only=restore_only)
            for target in ("workdir", "sessions")
        ]
    return schemas.SessionBackup(
        workdir=skips[0] is None,
        transcripts=skips[1] is None,
        reason=next((s for s in skips if s is not None), None),
        last_transcripts_backup=_last_backup(cache.get("sessions_save")),
    )


def _view(session: repo.Session, agg: dict | None, settings: Settings) -> schemas.SessionView:
    agg = agg or {}
    return schemas.SessionView(
        id=session.id,
        slug=session.slug,
        spec=redact_spec(session.spec, session_metas(session.secret_versions, session.created_at)),
        desired=session.desired,
        status=session.status,
        generation=session.generation,
        observed_generation=session.observed_generation,
        created_at=session.created_at,
        preset_id=session.preset_id,
        preset_name=session.preset_name,
        summary=views.summarize(session, agg, settings),
        hive=session.spec.hive,
        max_replicas=session.spec.max_replicas or session.spec.replicas,
        static=session.spec.static,
        leader_agent_id=agg.get("leader_agent_id"),
        leader_epoch=agg.get("leader_epoch"),
        leader_since=agg.get("leader_since"),
        leader_reason=agg.get("leader_reason"),
        closes_at=_closes_at(session),
        init_edited_since_run=_init_edited(session),
        backup=_backup(session, settings),
        created_by_service=session.created_by_service,
        created_by_session=session.created_by_session,
    )


def agent_view(r: dict) -> schemas.AgentView:
    known = r["context_pct"] is not None or r["context_used_tokens"] is not None
    return schemas.AgentView(
        id=r["id"],
        ordinal=r["ordinal"],
        pod_name=r["pod_name"],
        ready=r["ready"],
        state=r["state"] or "unknown",
        free_since=r["free_since"],
        current_turn_id=r["current_turn_id"],
        context=schemas.AgentContext(
            used_tokens=r["context_used_tokens"],
            window_tokens=r["context_window_tokens"],
            used_percentage=r["context_pct"],
        )
        if known
        else None,
        context_pct=r["context_pct"],
        leader=bool(r.get("leader")),
        peers=len(r["peers"] or []),
        gone=r["gone_at"] is not None,
        attention=schemas.AgentAttention(reason=r["waiting_reason"], since=r["waiting_since"])
        if r["waiting_reason"]
        else None,
        bootstrap=_bootstrap_view(r["bootstrap_step"]),
        bootstrap_failure=_bootstrap_view(r["bootstrap_failure"]),
        permission_mode_state=r["permission_mode_state"],
        permission_mode_reason=r["permission_mode_reason"],
    )


def _bootstrap_view(step: dict | None) -> schemas.AgentBootstrapStep | None:
    return schemas.AgentBootstrapStep(**step) if step else None


async def one_view(conn, session: repo.Session, settings: Settings) -> schemas.SessionView:
    aggs = await views.session_aggregates(conn, session.id)
    return _view(session, aggs.get(session.id), settings)


def check_spec(
    spec: SessionSpec,
    settings: Settings,
    stored: SessionSpec | None = None,
    derived_cache_key: str | None = None,
) -> SessionSpec:
    """Validate the spec and return it with empty resource keys filled from the defaults.

    A model, effort or image equal to the one in `stored` is accepted even when it has left the
    settings lists; scheduling and preset creation pass no `stored` and validate fully.
    `derived_cache_key` is the one `preset-` cache key the spec may carry (its own preset's).
    """
    try:
        check_caller_cache_key(spec, derived_cache_key)
    except ValueError as exc:
        raise ApiError(422, "invalid_request", str(exc)) from None
    try:
        validate_spec(spec, settings, stored)
    except SpecInvalid as exc:
        raise ApiError(422, "invalid_request", "; ".join(exc.errors)) from None
    return fill_resource_defaults(spec, settings)


@router.get("/meta", response_model=schemas.Meta)
async def meta(settings: Cfg) -> schemas.Meta:
    hook = settings.discard_hook()
    return schemas.Meta(
        defaults=schemas.MetaDefaults(
            model=settings.default_model,
            effort=settings.default_effort,
            workdir=SessionSpec.model_fields["workdir"].default,
            resources=schemas.MetaResources(**settings.default_resources()),
            any_mcp_config=settings.default_any_mcp_config(),
            discard_hook=hook.kind if hook else "none",
            replicas=settings.default_replicas,
            auto_compact_pct=settings.default_auto_compact_pct,
            pvc_size=settings.pvc_size,
            storage_class=settings.pvc_storage_class,
            max_attempts=SessionSpec.model_fields["max_attempts"].default,
        ),
        agent_image=settings.agent_image,
        agent_images=sorted(settings.image_allowlist()),
        models=list(settings.models),
        efforts=list(settings.efforts),
        caps=schemas.MetaCaps(
            max_replicas=settings.max_replicas,
            max_pvc_size=settings.max_pvc_size,
            max_sessions=settings.max_sessions,
        ),
        discard_hook=hook.kind if hook else "none",
        cache=schemas.MetaCache(
            available=settings.cache_available, max_bytes=settings.cache_max_bytes
        ),
        kube_access=schemas.MetaKubeAccess(available=settings.kube_access_available),
        docker=schemas.MetaDocker(
            available=settings.docker_available,
            storage=settings.docker_storage,
            max_storage=settings.max_docker_storage,
            max_cpu=settings.max_docker_cpu,
            max_memory=settings.max_docker_memory,
        ),
        hook_events=[schemas.MetaHookEvent(**asdict(e)) for e in HOOK_EVENT_SPECS],
        permission_modes=[schemas.MetaPermissionMode(**m) for m in PERMISSION_MODES],
        usage=schemas.MetaUsage(
            configured=settings.usage_available, quota_default_pct=settings.usage_quota_default
        ),
    )


@router.get("/sessions", response_model=list[schemas.SessionView])
async def list_sessions(pool: Pool, settings: Cfg) -> list[schemas.SessionView]:
    async with pool.connection() as conn:
        found = await repo.list_sessions(conn)
        aggs = await views.session_aggregates(conn)
    return [_view(c, aggs.get(c.id), settings) for c in found]


async def create(
    pool,
    settings: Settings,
    body: schemas.CreateSession,
    creator: str | None = None,
    created_by_session: uuid.UUID | None = None,
    *,
    usage: UsageSource | None,
) -> schemas.SessionView:
    """Create a session from a spec or a preset; shared by the main port and the admin routes.
    `creator` is the trigger service on the service port; `created_by_session` the superleader
    that created it through the admin routes. `usage` is the usage gate's source (None: gate
    off), keyword-only with no default so no caller can skip the gate: the usage is read before
    the create lock (no network I/O under it) and judged against the resolved spec's quota
    inside the transaction, so a refusal (409 `usage_quota`) rolls the insert back."""
    if (body.spec is None) == (body.preset_id is None):
        raise ApiError(422, "invalid_request", "exactly one of spec or preset_id")
    if created_by_session is not None and body.spec is not None:
        raise ApiError(422, "preset_required", "the superleader creates sessions from a preset")
    if body.spec is not None:
        if body.slug is None:
            raise ApiError(422, "invalid_request", "slug is required with spec")
        body_spec, _ignored = body.spec.split()  # a value has no preset to live in: refused below
        cache = body_spec.cache
        if cache is not None and cache.restore_key is None and cache.save_key is None:
            raise ApiError(
                422,
                "invalid_request",
                "cache.restore_key or cache.save_key is required without a preset",
            )
        spec = await run_in_threadpool(check_spec, body_spec, settings)
    if body.slug is not None:
        try:
            validate_slug(body.slug)
        except ValueError as exc:
            raise ApiError(422, "invalid_request", str(exc)) from None
    windows = await usage.current() if usage is not None else None
    async with pool.connection() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(%s)", (_CREATE_LOCK,))
            if await repo.count_active(conn) >= settings.max_sessions:
                raise ApiError(
                    409, "session_limit", f"at most {settings.max_sessions} sessions are allowed"
                )
            if body.preset_id is None:
                if spec.protected_env:
                    raise _secret_unset(spec.protected_env)
                session = await _insert(
                    conn,
                    body.slug,
                    spec,
                    created_by_service=creator,
                    created_by_session=created_by_session,
                )
            else:
                session = await _create_from_preset(
                    conn, body.preset_id, body.slug, settings, creator, created_by_session
                )
            usage_gate.check(
                windows, session.spec.usage_quota, settings.usage_quota_default, datetime.now(UTC)
            )
        return await one_view(conn, session, settings)


@router.post("/sessions", status_code=201, response_model=schemas.SessionView)
async def create_session(
    body: schemas.CreateSession,
    request: Request,
    pool: Pool,
    settings: Cfg,
    usage: Usage,
) -> schemas.SessionView:
    return await create(pool, settings, body, calling_service(request), usage=usage)


_SLUG_ATTEMPTS = 5


def _secret_unset(names: list[str]) -> ApiError:
    return ApiError(
        422, "secret_unset", "protected variables with no value: " + ", ".join(sorted(names))
    )


def check_archive_names(
    slug: str,
    spec: SessionSpec,
    preset_name: str | None,
    created_by_service: str | None,
    derived_key: str | None = None,
) -> None:
    """Render both cache key templates with this session's real context; 422 on a bad name.

    `validate_spec` already rendered them against the preview sample, which cannot see a slug,
    preset name or user that squeezes to nothing (`{{ preset_name }}` for a name of only symbols).
    The save name is rendered again at save time with the clock then.
    """
    cache = spec.cache
    if cache is None:
        return
    context = rendering.archive_context(slug, preset_name, created_by_service, datetime.now(UTC))
    for field, template in (("restore_key", cache.restore_key), ("save_key", cache.save_key)):
        if not template:
            continue
        try:
            name = rendering.render_name(template, context, allow_prefix=field == "restore_key")
            check_rendered_cache_name(name, field, derived_key)
        except ValueError as exc:  # ArchiveNameError is a ValueError
            if not isinstance(exc, ArchiveNameError):
                raise ApiError(422, "invalid_request", str(exc)) from None
            where = f" (line {exc.line})" if exc.line else ""
            raise ApiError(422, "invalid_request", f"cache.{field}{where}: {exc}") from None


async def _insert(conn, slug: str, spec: SessionSpec, **provenance) -> repo.Session:
    await run_in_threadpool(
        check_archive_names,
        slug,
        spec,
        provenance.get("preset_name"),
        provenance.get("created_by_service"),
        default_cache_key(provenance["preset_id"]) if provenance.get("preset_id") else None,
    )
    try:
        return await repo.create(conn, slug, spec, **provenance)
    except repo.SessionExists:
        raise ApiError(409, "conflict", f"session {slug!r} already exists") from None


async def _create_from_preset(
    conn,
    preset_id: uuid.UUID,
    slug: str | None,
    settings: Settings,
    created_by_service: str | None = None,
    created_by_session: uuid.UUID | None = None,
) -> repo.Session:
    """Snapshot the preset's spec, filled and validated against the current settings."""
    preset = await presets.get_for_share(conn, preset_id)
    if preset is None or (created_by_session is not None and not preset.superleader_manageable):
        raise not_found("preset")
    raw = dict(preset.spec)
    raw.setdefault("replicas", settings.default_replicas)
    try:
        spec = await run_in_threadpool(
            check_spec,
            SessionSpec.model_validate(raw),
            settings,
            derived_cache_key=default_cache_key(preset.id),
        )
    except ValidationError as exc:
        raise ApiError(422, "invalid_request", f"preset '{preset.name}': {exc}") from None
    except ApiError as exc:
        raise ApiError(422, "invalid_request", f"preset '{preset.name}': {exc.message}") from None
    if spec.cache is not None and spec.cache.restore_key is None and spec.cache.save_key is None:
        derived = default_cache_key(preset.id)
        cache = spec.cache.model_copy(update={"restore_key": derived, "save_key": derived})
        spec = spec.model_copy(update={"cache": cache})
    try:
        versions = await secret_store.snapshot_versions(conn, preset.id, spec.protected_env)
    except secret_store.SecretUnset as exc:
        raise _secret_unset(exc.names) from None
    provenance = {
        "preset_id": preset.id,
        "preset_name": preset.name,
        "created_by_service": created_by_service,
        "secret_versions": versions,
        "created_by_session": created_by_session,
    }
    if slug is not None:
        return await _insert(conn, slug, spec, **provenance)
    for _ in range(_SLUG_ATTEMPTS):
        generated = generated_slug(preset.name)
        await run_in_threadpool(
            check_archive_names,
            generated,
            spec,
            preset.name,
            created_by_service,
            default_cache_key(preset.id),
        )
        try:
            return await repo.create(conn, generated, spec, **provenance)
        except repo.SessionExists:
            continue
    raise ApiError(409, "conflict", "could not generate a free session slug; retry")


@router.get("/sessions/{session_id}", response_model=schemas.SessionView)
async def get_session(
    session_id: uuid.UUID,
    pool: Pool,
    settings: Cfg,
) -> schemas.SessionView:
    async with pool.connection() as conn:
        session = await repo.get(conn, session_id)
        if session is None:
            raise not_found("session")
        return await one_view(conn, session, settings)


async def discard(conn, session_id: uuid.UUID, settings: Settings) -> schemas.SessionView:
    try:
        session = await repo.request_delete(conn, session_id)
    except repo.SessionNotFound:
        raise not_found("session") from None
    return await one_view(conn, session, settings)


@router.delete("/sessions/{session_id}", status_code=202, response_model=schemas.SessionView)
async def discard_session(
    session_id: uuid.UUID,
    request: Request,
    pool: Pool,
    settings: Cfg,
) -> schemas.SessionView:
    if request.query_params:
        raise ApiError(
            422, "invalid_request", "discard takes no parameters; it always deletes everything"
        )
    async with pool.connection() as conn:
        return await discard(conn, session_id, settings)


def init_scripts_view(
    conn, session: repo.Session, settings: Settings, preset_updated: bool = False
) -> schemas.InitScriptsView:
    """The init scripts of a session with the last run; shared with the agent routes."""
    init = session.status.get("init")
    base = f"{WORKDIR_ROOT}/{session.spec.workdir}/{INIT_DIR}"
    return schemas.InitScriptsView(
        session_id=session.id,
        scripts=session.spec.init_scripts,
        preset_id=session.preset_id,
        preset_name=session.preset_name,
        status=schemas.InitStatus.model_validate(init) if isinstance(init, dict) else None,
        edited_since_run=_init_edited(session),
        script_path=f"{base}/init.sh",
        log_path=f"{base}/init.log",
        preset_updated=preset_updated,
    )


async def edit_init_scripts(
    conn, session_id: uuid.UUID, scripts: list[str], settings: Settings
) -> schemas.InitScriptsView:
    """Validate and write the scripts to the session and its source preset."""
    try:
        SessionSpec.model_validate({"init_scripts": scripts})
    except ValidationError as exc:
        msg = "; ".join(e["msg"].removeprefix("Value error, ") for e in exc.errors())
        raise ApiError(422, "invalid_request", msg) from None
    current = await repo.get(conn, session_id)
    if current is None:
        raise not_found("session")
    for i, script in enumerate(scripts):
        error, _ = await run_in_threadpool(
            rendering.problem, script, "init_script", current.spec.plain_env
        )
        if error is not None:
            hint = rendering.ESCAPE_HINT
            raise ApiError(422, "invalid_request", f"init_scripts[{i}]: {error} ({hint})")
    try:
        session, preset_updated = await repo.set_init_scripts(conn, session_id, scripts)
    except repo.SessionNotFound:
        raise not_found("session") from None
    except repo.SessionBeingDeleted:
        raise ApiError(409, "conflict", "session is being deleted") from None
    return init_scripts_view(conn, session, settings, preset_updated)


@router.get("/sessions/{session_id}/init-scripts", response_model=schemas.InitScriptsView)
async def get_init_scripts(
    session_id: uuid.UUID, pool: Pool, settings: Cfg
) -> schemas.InitScriptsView:
    async with pool.connection() as conn:
        session = await repo.get(conn, session_id)
        if session is None:
            raise not_found("session")
        return init_scripts_view(conn, session, settings)


@router.put("/sessions/{session_id}/init-scripts", response_model=schemas.InitScriptsView)
async def put_init_scripts(
    session_id: uuid.UUID, body: schemas.InitScriptsIn, pool: Pool, settings: Cfg
) -> schemas.InitScriptsView:
    async with pool.connection() as conn:
        return await edit_init_scripts(conn, session_id, body.scripts, settings)


@router.put("/sessions/{session_id}/permission-mode", response_model=schemas.PermissionModeView)
async def put_permission_mode(
    session_id: uuid.UUID, body: schemas.PermissionModeIn, pool: Pool
) -> schemas.PermissionModeView:
    """Set `spec.wrapper.permission_mode` on the session and its source preset.

    Applied by the normal config-hash rollout: idle pods restart, busy ones at the turn end.
    """
    async with pool.connection() as conn:
        try:
            session, preset_updated = await repo.set_permission_mode(
                conn, session_id, body.permission_mode
            )
        except repo.SessionNotFound:
            raise not_found("session") from None
        except repo.SessionBeingDeleted:
            raise ApiError(409, "conflict", "session is being deleted") from None
    return schemas.PermissionModeView(
        session_id=session.id,
        permission_mode=body.permission_mode,
        preset_id=session.preset_id,
        preset_name=session.preset_name,
        preset_updated=preset_updated,
    )


@router.get("/sessions/{session_id}/agents", response_model=list[schemas.AgentView])
async def list_agents(session_id: uuid.UUID, pool: Pool) -> list[schemas.AgentView]:
    async with pool.connection() as conn:
        if await repo.get(conn, session_id) is None:
            raise not_found("session")
        rows = await views.agent_rows(conn, session_id)
    return [agent_view(r) for r in rows]


SCALE_STATUS = {
    "static_session": 409,
    "out_of_range": 422,
    "leader_would_leave": 409,
    "session_deleting": 409,
}


async def gate_scale_up(
    pool, usage: UsageSource | None, settings: Settings, session_id: uuid.UUID, replicas: int
) -> None:
    """Refuse a scale-up over the usage quota (409 `usage_quota`), from the session's own
    snapshotted quota. Scale-down, an unchanged count and a gate that is off are never gated;
    nor is a count `set_replicas` refuses on its own (missing, deleting, static, out of range),
    so its error stays the answer. Check-then-act: a concurrent scale between this read and
    `set_replicas` is not re-judged (fail-open, like missing data)."""
    if usage is None:
        return
    async with pool.connection() as conn:
        session = await repo.get(conn, session_id)
    if session is None or session.desired != "active" or session.spec.static:
        return
    spec = session.spec
    if not spec.replicas < replicas <= (spec.max_replicas or spec.replicas):
        return
    await usage_gate.enforce(usage, spec.usage_quota, settings.usage_quota_default)


@router.post("/sessions/{session_id}/scale", status_code=202, response_model=schemas.ScaleResult)
async def scale_session(
    session_id: uuid.UUID, body: schemas.ScaleRequest, pool: Pool, settings: Cfg, usage: Usage
) -> schemas.ScaleResult:
    """Set spec.replicas in place (no caller agent: the public route has no leader check).
    A scale-up over the usage quota is refused 409 `usage_quota`."""
    await gate_scale_up(pool, usage, settings, session_id, body.replicas)
    async with pool.connection() as conn:
        try:
            out = await repo.set_replicas(conn, session_id, body.replicas)
        except repo.SessionNotFound:
            raise not_found("session") from None
        except repo.ScaleRefused as exc:
            status = SCALE_STATUS.get(exc.code, 409)
            raise ApiError(status, exc.code, str(exc)) from None
    return schemas.ScaleResult(
        session_id=out.session_id,
        replicas=out.replicas,
        previous_replicas=out.previous_replicas,
        max_replicas=out.max_replicas,
    )


@router.post(
    "/sessions/{session_id}/leader", status_code=202, response_model=schemas.LeadershipRequested
)
async def make_leader(
    session_id: uuid.UUID, body: schemas.MakeLeaderIn, pool: Pool
) -> schemas.LeadershipRequested:
    """Queue a user leadership override; the dispatcher decides at apply time."""
    async with pool.connection() as conn:
        session = await repo.get(conn, session_id)
        if session is None:
            raise not_found("session")
        target = await agents_repo.get_agent(conn, body.agent_id)
        if target is None or target.session_id != session_id:
            raise not_found("agent")
        if session.spec.alone:
            raise ApiError(409, "alone", "a single-agent session has no leader to change")
        request_id = await leadership.insert_request(
            conn, session_id, "user", None, target.id, "user", None
        )
    return schemas.LeadershipRequested(request_id=request_id)

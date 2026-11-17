"""Backups api: named, versioned directory backups. List, versions, manifest, download, copy,
delete, and the cleanup settings, preview and run.

A backup is a name with immutable versions (decision 766): the later save or copy becomes the
current version, an earlier one stays and can be copied back; there is no merge. Rows live in
`backups` / `backup_versions` (`db.backups`); the objects are `b/<name>/<now>-<actor>-<rand>`.

Routes that touch S3 use the api pod's own key pair (`ORCH_CACHE_S3_API_*`); without it they
answer 503 `unavailable`. The api never signs a PUT: a download is a presigned GET (five minutes,
forced download name) behind a 302, so the archive bytes never pass through the api. The one
write the api makes to the bucket is the server-side CopyObject of a copy (the api key pair
therefore needs write access under the version prefix). Deletes remove the database row first,
then the object, after re-checking references, the current version and the grace period; an
object whose delete failed is an orphan the sweep removes. Every delete and copy writes an audit
log line.

The cleanup policy (retention, keep-newest, grace, enabled, dry-run, interval) is read from
`cache_sweep_settings`, else the `ORCH_CACHE_*` environment (`db.cache_sweep_settings.effective`).
`GET`/`PUT /backups/cleanup/settings` and `POST /backups/cleanup/run` need only the database: the
api writes `cache_sweep_settings` and `cache_sweep_requests`, the reconciler's sweeper acts on
them. The routes sit on the main port only: the service port serves a fixed route list that does
not include them (`api/service_auth.py`).

`{name}` is path-encoded (a name holds `/`) and validated with `keys.validate_name`.
"""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from datetime import datetime, timedelta
from typing import Annotated

import psycopg
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, Query, Request, Response
from fastapi.responses import RedirectResponse
from psycopg_pool import AsyncConnectionPool

from agents_orchestrator import rendering
from agents_orchestrator.api import schemas
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.service_auth import calling_service
from agents_orchestrator.api.sessions import Cfg, Pool
from agents_orchestrator.cache import presign as s3
from agents_orchestrator.cache.keys import SUFFIX, ArchiveNameError, object_key_for, validate_name
from agents_orchestrator.cache.policy import SweepPolicy
from agents_orchestrator.cache.sweep import References, build_plan, references
from agents_orchestrator.db import backups as repo
from agents_orchestrator.db import cache_sweep_settings, cache_sweeps
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1")

LIST_LIMIT_MAX = repo.MAX_LIMIT
_NAME_CURSOR_RE = re.compile(r"^[a-z0-9._/-]{1,300}$")
_NoStore = {"Cache-Control": "no-store"}
_Limit = Annotated[int, Query(ge=1, le=LIST_LIMIT_MAX)]
_Cursor = Annotated[str | None, Query(max_length=1024)]


def require_s3(settings: Settings) -> s3.S3Credentials:
    if not settings.api_s3_configured:
        raise ApiError(503, "unavailable", "backups are not configured for the api")
    return s3.S3Credentials(*settings.api_s3_credentials())


def valid_name(name: str) -> str:
    try:
        return validate_name(name, allow_prefix=False)
    except ArchiveNameError as exc:
        raise ApiError(400, "bad_request", str(exc)) from None


def _download_name(name: str, version_id: uuid.UUID) -> str:
    return f"{name.replace('/', '-')}-{str(version_id)[:8]}{SUFFIX}"


async def _policy(conn: psycopg.AsyncConnection, settings: Settings) -> SweepPolicy:
    return (await cache_sweep_settings.effective(conn, settings)).policy


async def _settings_view(
    conn: psycopg.AsyncConnection, settings: Settings
) -> schemas.CleanupSettingsView:
    effective = await cache_sweep_settings.effective(conn, settings)
    return schemas.CleanupSettingsView(
        policy=schemas.SweepPolicyView(**effective.policy.to_dict()),
        defaults=schemas.SweepPolicyView(**SweepPolicy.from_settings(settings).to_dict()),
        source=effective.source,
        updated_at=effective.updated_at,
        run_requested_at=await cache_sweep_settings.requested_at(conn),
        run_pending=await cache_sweep_settings.run_pending(conn),
        last_sweep=await cache_sweeps.last(conn),
    )


def _caller(request: Request) -> str:
    return calling_service(request) or (request.client.host if request.client else "unknown")


def _created_by(request: Request) -> str:
    """Who a user copy is recorded as: the main port has no user identity (the source allowlist
    is the auth), so a fixed label, never the peer address."""
    return calling_service(request) or "user"


async def _db_now(pool: AsyncConnectionPool) -> datetime:
    async with pool.connection() as conn:
        return (await (await conn.execute("SELECT now()")).fetchone())[0]


def _version_view(v: repo.Version, refs: References) -> schemas.BackupVersionView:
    by = refs.slugs_of("", [v.id])
    return schemas.BackupVersionView(
        id=v.id,
        name=v.name,
        size=v.size,
        digest=v.digest,
        saved_at=v.created_at,
        origin=v.origin,
        state=v.state,
        current=v.state == "current",
        session_slug=v.session_slug,
        created_by=v.created_by,
        referenced_by=by,
        protected=bool(by),
    )


def _current_view(v: repo.Version) -> schemas.BackupCurrentView:
    return schemas.BackupCurrentView(
        id=v.id, size=v.size, saved_at=v.created_at, session_slug=v.session_slug, origin=v.origin
    )


# ------------------------------------------------------------------ reads (shared with the admin)


async def list_backups(
    pool: AsyncConnectionPool,
    settings: Settings,
    q: str | None,
    cursor: str | None,
    limit: int,
) -> schemas.BackupNamePage:
    require_s3(settings)
    if cursor is not None and not _NAME_CURSOR_RE.fullmatch(cursor):
        raise ApiError(400, "bad_request", "invalid cursor")
    async with pool.connection() as conn:
        page = await repo.list_names(conn, q=q or None, limit=limit, cursor=cursor)
        refs = await references(conn, settings)
        by_name: dict[str, set[str]] = {}
        for n in page.items:
            versions = await repo.name_versions(conn, n.name)
            by_name[n.name] = set(refs.slugs_of(n.name, [v.id for v in versions]))
    items = [
        schemas.BackupNameView(
            name=n.name,
            current=_current_view(n.current) if n.current else None,
            version_count=n.version_count,
            referenced_by=sorted(by_name[n.name]),
            protected=bool(by_name[n.name]),
        )
        for n in page.items
    ]
    return schemas.BackupNamePage(items=items, next_cursor=page.next_cursor)


def _versions_cursor(cursor: str | None) -> str | None:
    if cursor is None:
        return None
    ts, sep, vid = cursor.partition("|")
    try:
        datetime.fromisoformat(ts)
        uuid.UUID(vid)
    except ValueError:
        sep = ""
    if not sep:
        raise ApiError(400, "bad_request", "invalid cursor")
    return cursor


async def list_versions(
    pool: AsyncConnectionPool, settings: Settings, name: str, cursor: str | None, limit: int
) -> schemas.BackupVersionPage:
    require_s3(settings)
    valid_name(name)
    cursor = _versions_cursor(cursor)
    async with pool.connection() as conn:
        page = await repo.versions(conn, name, limit=limit, cursor=cursor)
        if not page.items and cursor is None:
            raise ApiError(404, "backup_not_found", "no backup has that name")
        refs = await references(conn, settings)
    return schemas.BackupVersionPage(
        items=[_version_view(v, refs) for v in page.items], next_cursor=page.next_cursor
    )


async def _version_or_404(
    conn: psycopg.AsyncConnection, name: str, version_id: uuid.UUID
) -> repo.Version:
    found = await repo.get_version(conn, name, version_id)
    if found is None:
        raise ApiError(404, "version_not_found", "no such version of this backup")
    return found


async def get_manifest(
    pool: AsyncConnectionPool, settings: Settings, name: str, version_id: uuid.UUID
) -> schemas.BackupManifestView:
    require_s3(settings)
    valid_name(name)
    async with pool.connection() as conn:
        found = await _version_or_404(conn, name, version_id)
    files = found.manifest if isinstance(found.manifest, list) else None
    return schemas.BackupManifestView(name=name, version=found.id, files=files)


# ------------------------------------------------------------------ copy


def _storage_error(exc: Exception) -> ApiError:
    code = exc.response.get("Error", {}).get("Code", "") if isinstance(exc, ClientError) else ""
    if code in {"NoSuchKey", "404", "NotFound"}:
        return ApiError(404, "version_not_found", "the version's object is missing from storage")
    return ApiError(502, "storage_error", "the object store refused the copy")


async def copy_backup(
    pool: AsyncConnectionPool,
    settings: Settings,
    body: schemas.BackupCopyIn,
    created_by: str,
) -> tuple[schemas.BackupVersionView, bool]:
    """Copy a version into a new object of `target` and record it there (origin `copy`).

    Replace only (decision 766): the copy becomes the target's current version, the earlier
    versions stay (copying an older version into its own name is "copy back"). Returns the new
    version and True; when the target's current version already has the copied content nothing is
    recorded and that current version is returned with False.
    """
    creds = require_s3(settings)
    source, target = valid_name(body.source), valid_name(body.target)
    async with pool.connection() as conn:
        src = (
            await _version_or_404(conn, source, body.source_version)
            if body.source_version
            else await repo.current(conn, source)
        )
    if src is None:
        raise ApiError(404, "backup_not_found", "no backup has that name")
    stamp = rendering.format_now(await _db_now(pool))
    new_key = object_key_for(settings.cache_s3_prefix, target, stamp, "copy")
    try:
        await asyncio.to_thread(
            s3.copy_object, settings, src.object_key, new_key, credentials=creds
        )
    except (ClientError, BotoCoreError) as exc:
        raise _storage_error(exc) from None
    async with pool.connection() as conn, conn.transaction():
        outcome, created = await repo.record_save(
            conn,
            name=target,
            object_key=new_key,
            size=src.size,
            digest=src.digest,
            manifest=src.manifest,
            base_version=None,
            origin="copy",
            session_slug=None,
            preset_id=src.preset_id,
            project=src.project,
            created_by=created_by,
            orphan=False,
        )
        present = created or await repo.current(conn, target)
        refs = await references(conn, settings)
    if created is None:  # the same content is already current: drop the fresh object
        await asyncio.to_thread(s3.delete_keys, settings, [new_key], credentials=creds)
    assert present is not None
    log.info(
        "audit: backup copy by=%s source=%s version=%s target=%s result=%s",
        created_by,
        source,
        src.id,
        target,
        outcome,
    )
    return _version_view(present, refs), created is not None


# ------------------------------------------------------------------ deletes


async def _delete_objects(settings: Settings, creds: s3.S3Credentials, keys: list[str]) -> None:
    failures = await asyncio.to_thread(s3.delete_keys, settings, keys, credentials=creds)
    for f in failures:  # the row is gone: the object is an orphan the sweep removes
        log.warning("backup delete could not remove %s: %s %s", f.key, f.code, f.message)


async def delete_version(
    pool: AsyncConnectionPool, settings: Settings, name: str, version_id: uuid.UUID, who: str
) -> None:
    creds = require_s3(settings)
    valid_name(name)
    async with pool.connection() as conn, conn.transaction():
        grace = (await _policy(conn, settings)).grace_seconds
        found = await _version_or_404(conn, name, version_id)
        siblings = await repo.name_versions(conn, name)
        if found.state == "current" and len(siblings) > 1:
            raise ApiError(
                409, "current_version", "the current version cannot be deleted; copy another first"
            )
        refs = await references(conn, settings)
        if found.id in refs.versions:
            who_refs = ", ".join(refs.versions[found.id])
            raise ApiError(409, "referenced", f"referenced by session {who_refs}")
        if found.created_at > await _db_now(pool) - timedelta(seconds=grace):
            raise ApiError(409, "too_young", "younger than the grace period")
        if not await repo.delete_version(conn, name, version_id):
            raise ApiError(409, "current_version", "the version could not be deleted")
    await _delete_objects(settings, creds, [found.object_key])
    log.info("audit: backup version delete by=%s name=%s version=%s", who, name, version_id)


async def delete_backup(pool: AsyncConnectionPool, settings: Settings, name: str, who: str) -> None:
    creds = require_s3(settings)
    valid_name(name)
    async with pool.connection() as conn, conn.transaction():
        grace = (await _policy(conn, settings)).grace_seconds
        rows = await repo.name_versions(conn, name)
        if not rows:
            raise ApiError(404, "backup_not_found", "no backup has that name")
        refs = await references(conn, settings)
        used = refs.slugs_of(name, [v.id for v in rows])
        if used:
            raise ApiError(409, "referenced", f"referenced by session {', '.join(used)}")
        young = await _db_now(pool) - timedelta(seconds=grace)
        if any(v.created_at > young for v in rows):
            raise ApiError(409, "too_young", "a version is younger than the grace period")
        keys = await repo.delete_name(conn, name)
    await _delete_objects(settings, creds, keys)
    log.info("audit: backup delete by=%s name=%s versions=%d", who, name, len(keys))


# ------------------------------------------------------------------ cleanup


async def cleanup_preview(
    pool: AsyncConnectionPool, settings: Settings, response: Response
) -> schemas.CleanupPreview:
    creds = require_s3(settings)
    objects = await asyncio.to_thread(
        s3.list_archives, settings, settings.cache_s3_prefix, credentials=creds
    )
    async with pool.connection() as conn:
        policy = await _policy(conn, settings)
        last = await cache_sweeps.last(conn)
    plan, _refs, _now = await build_plan(settings, pool, objects, policy)
    response.headers["Cache-Control"] = "no-store"
    return schemas.CleanupPreview(
        would_delete=[
            schemas.CleanupItem(
                key=d.key, size=d.size, reason=d.reason, name=d.name, version=d.version_id
            )
            for d in plan.deletes
        ],
        count=len(plan.deletes),
        total_bytes=plan.bytes,
        retention_days=policy.retention_days,
        keep_newest=policy.keep_newest,
        grace_seconds=policy.grace_seconds,
        sweep_enabled=policy.enabled,
        sweep_dry_run=policy.dry_run,
        last_sweep=last,
    )


async def write_cleanup_settings(
    pool: AsyncConnectionPool, settings: Settings, body: schemas.SweepPolicyIn, by: str
) -> schemas.CleanupSettingsView:
    """Replace the saved sweep policy (read-for-update, put and NOTIFY in one transaction) and
    log the change with `by`; shared by the main port and the superleader route."""
    new = SweepPolicy(**body.model_dump())
    async with pool.connection() as conn, conn.transaction():
        stored = await cache_sweep_settings.get(conn, for_update=True)
        await cache_sweep_settings.put(conn, new)
        view = await _settings_view(conn, settings)
    log.info(
        "audit: cache cleanup settings by=%s old=%s new=%s",
        by,
        stored.policy.to_dict() if stored else "env",
        new.to_dict(),
    )
    return view


# ------------------------------------------------------------------ routes
# Declared in this order: the fixed paths (`copy`, `cleanup/...`) before `{name:path}`.


@router.get("/backups", response_model=schemas.BackupNamePage)
async def list_names_route(
    pool: Pool,
    settings: Cfg,
    q: Annotated[str | None, Query(max_length=200)] = None,
    cursor: _Cursor = None,
    limit: _Limit = 200,
) -> schemas.BackupNamePage:
    return await list_backups(pool, settings, q, cursor, limit)


@router.post("/backups/copy", response_model=schemas.BackupVersionView, status_code=201)
async def copy_route(
    body: schemas.BackupCopyIn, request: Request, pool: Pool, settings: Cfg
) -> schemas.BackupVersionView:
    view, _created = await copy_backup(pool, settings, body, _created_by(request))
    return view


@router.get("/backups/cleanup/settings", response_model=schemas.CleanupSettingsView)
async def get_cleanup_settings(
    pool: Pool, settings: Cfg, response: Response
) -> schemas.CleanupSettingsView:
    response.headers["Cache-Control"] = "no-store"
    async with pool.connection() as conn:
        return await _settings_view(conn, settings)


@router.put("/backups/cleanup/settings", response_model=schemas.CleanupSettingsView)
async def put_cleanup_settings(
    body: schemas.SweepPolicyIn, request: Request, pool: Pool, settings: Cfg, response: Response
) -> schemas.CleanupSettingsView:
    view = await write_cleanup_settings(pool, settings, body, _caller(request))
    response.headers["Cache-Control"] = "no-store"
    return view


@router.get("/backups/cleanup/preview", response_model=schemas.CleanupPreview)
async def cleanup_preview_route(
    pool: Pool, settings: Cfg, response: Response
) -> schemas.CleanupPreview:
    return await cleanup_preview(pool, settings, response)


@router.post("/backups/cleanup/preview", response_model=schemas.CleanupPreview)
async def cleanup_preview_post_route(
    pool: Pool, settings: Cfg, response: Response
) -> schemas.CleanupPreview:
    """Same read-only answer as the GET (the plugin reads GET, the web may POST)."""
    return await cleanup_preview(pool, settings, response)


@router.post("/backups/cleanup/run", status_code=202, response_model=schemas.CleanupSettingsView)
async def request_cleanup_run(
    request: Request, pool: Pool, settings: Cfg, response: Response
) -> schemas.CleanupSettingsView:
    async with pool.connection() as conn:
        await cache_sweep_settings.request_run(conn)
        view = await _settings_view(conn, settings)
    log.info("audit: cache cleanup run requested by=%s", _caller(request))
    response.headers["Cache-Control"] = "no-store"
    return view


@router.get(
    "/backups/{name:path}/versions/{version_id}/manifest", response_model=schemas.BackupManifestView
)
async def manifest_route(
    name: str, version_id: uuid.UUID, pool: Pool, settings: Cfg
) -> schemas.BackupManifestView:
    return await get_manifest(pool, settings, name, version_id)


@router.get(
    "/backups/{name:path}/versions/{version_id}/download",
    status_code=302,
    response_class=RedirectResponse,
)
async def download_route(
    name: str, version_id: uuid.UUID, pool: Pool, settings: Cfg
) -> RedirectResponse:
    creds = require_s3(settings)
    valid_name(name)
    async with pool.connection() as conn:
        found = await _version_or_404(conn, name, version_id)
    url = s3.presign_get(
        settings,
        found.object_key,
        disposition=_download_name(name, found.id),
        credentials=creds,
    )
    return RedirectResponse(url, status_code=302, headers=_NoStore)


@router.delete("/backups/{name:path}/versions/{version_id}", status_code=204)
async def delete_version_route(
    name: str, version_id: uuid.UUID, request: Request, pool: Pool, settings: Cfg
) -> Response:
    await delete_version(pool, settings, name, version_id, _caller(request))
    return Response(status_code=204)


@router.get("/backups/{name:path}/versions", response_model=schemas.BackupVersionPage)
async def versions_route(
    name: str, pool: Pool, settings: Cfg, cursor: _Cursor = None, limit: _Limit = 200
) -> schemas.BackupVersionPage:
    return await list_versions(pool, settings, name, cursor, limit)


@router.delete("/backups/{name:path}", status_code=204)
async def delete_backup_route(name: str, request: Request, pool: Pool, settings: Cfg) -> Response:
    await delete_backup(pool, settings, name, _caller(request))
    return Response(status_code=204)

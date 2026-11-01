"""Cache archives: list, download, cleanup preview and manual delete.

These routes use the api pod's own S3 key pair (`ORCH_CACHE_S3_API_*`, list/get/delete only; the
reconciler keeps the read/write pair that presigns uploads). Without that pair every route answers
503 `unavailable`. The api never signs a PUT: it presigns a GET (five minutes, forced download
name) and redirects to it, so the archive bytes never pass through the api.

A key is accepted only inside the cache prefix, ending `.tar.zst`, and either a valid archive
name (`cache.keys.validate_name`) or a transcripts object `sessions/<segment>`. The delete
re-checks everything server side against the database and the bucket right before it deletes: a
key a session references, one younger than the sweep grace period, a missing one, or an invalid
one is refused with its own reason and never sent to S3. Every delete writes an audit log line.
The routes sit on the main port only: the service port serves a fixed route list that does not
include them (`api/service_auth.py`).
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Query, Request, Response
from fastapi.responses import RedirectResponse
from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.api import schemas
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.service_auth import calling_service
from agents_orchestrator.api.sessions import Cfg, Pool
from agents_orchestrator.cache import presign as s3
from agents_orchestrator.cache.keys import CACHE_KEY_RE, SUFFIX, ArchiveNameError, validate_name
from agents_orchestrator.cache.sweep import SESSIONS_GROUP, plan_sweep, referenced_keys
from agents_orchestrator.db import cache_sweeps
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1")

LIST_LIMIT_MAX = 1000
_SUBPREFIX_RE = re.compile(r"^[a-z0-9._/-]{0,200}$")
_CURSOR_RE = re.compile(r"^[A-Za-z0-9+/=_.~-]{1,1024}$")
_NoStore = {"Cache-Control": "no-store"}


def _require_s3(settings: Settings) -> s3.S3Credentials:
    if not settings.api_s3_configured:
        raise ApiError(503, "unavailable", "the cache archives are not configured for the api")
    return s3.S3Credentials(*settings.api_s3_credentials())


def _name_of(settings: Settings, key: str) -> str:
    """The archive name inside `key`, raising ValueError for any key the api may not touch."""
    prefix = settings.cache_s3_prefix
    if not key.startswith(prefix) or not key.endswith(SUFFIX) or len(key) > 1024:
        raise ValueError("key is not an archive inside the cache prefix")
    name = key[len(prefix) : -len(SUFFIX)]
    head, sep, slug = name.partition("/")
    if head == SESSIONS_GROUP.rstrip("/") and sep:
        if CACHE_KEY_RE.fullmatch(slug):
            return name
        raise ValueError("invalid transcripts key")
    try:
        return validate_name(name, allow_prefix=False)
    except ArchiveNameError as exc:
        raise ValueError(str(exc)) from None


def _download_name(name: str) -> str:
    return name.replace("/", "_") + SUFFIX


async def _protected(pool: AsyncConnectionPool, settings: Settings) -> dict[str, list[str]]:
    async with pool.connection() as conn:
        return await referenced_keys(conn, settings)


async def _db_now(pool: AsyncConnectionPool) -> datetime:
    async with pool.connection() as conn:
        return (await (await conn.execute("SELECT now()")).fetchone())[0]


@router.get("/cache/archives", response_model=schemas.ArchivePage)
async def list_archives(
    pool: Pool,
    settings: Cfg,
    prefix: str = "",
    cursor: Annotated[str | None, Query(max_length=1024)] = None,
    limit: Annotated[int, Query(ge=1, le=LIST_LIMIT_MAX)] = 200,
) -> schemas.ArchivePage:
    creds = _require_s3(settings)
    if not _SUBPREFIX_RE.fullmatch(prefix) or ".." in prefix.split("/") or prefix.startswith("/"):
        raise ApiError(400, "bad_request", "invalid prefix")
    if cursor is not None and not _CURSOR_RE.fullmatch(cursor):
        raise ApiError(400, "bad_request", "invalid cursor")
    found, token = await asyncio.to_thread(
        s3.list_page,
        settings,
        f"{settings.cache_s3_prefix}{prefix}",
        token=cursor,
        limit=limit,
        credentials=creds,
    )
    refs = await _protected(pool, settings)
    items = []
    for obj in found:
        if not obj.key.endswith(SUFFIX):
            continue
        by = refs.get(obj.key, [])
        items.append(
            schemas.ArchiveView(
                key=obj.key,
                name=obj.key[len(settings.cache_s3_prefix) : -len(SUFFIX)],
                size=obj.size,
                last_modified=obj.last_modified,
                referenced_by=by,
                protected=bool(by),
            )
        )
    return schemas.ArchivePage(items=items, next_cursor=token)


@router.get("/cache/archives/download", status_code=302, response_class=RedirectResponse)
async def download_archive(settings: Cfg, key: str) -> RedirectResponse:
    creds = _require_s3(settings)
    try:
        name = _name_of(settings, key)
        url = s3.presign_get(settings, key, disposition=_download_name(name), credentials=creds)
    except ValueError as exc:
        raise ApiError(400, "bad_request", str(exc)) from None
    return RedirectResponse(url, status_code=302, headers=_NoStore)


@router.get("/cache/cleanup/preview", response_model=schemas.CleanupPreview)
async def cleanup_preview(pool: Pool, settings: Cfg, response: Response) -> schemas.CleanupPreview:
    creds = _require_s3(settings)
    objects = await asyncio.to_thread(
        s3.list_archives, settings, settings.cache_s3_prefix, credentials=creds
    )
    refs = await _protected(pool, settings)
    now = await _db_now(pool)
    grace = settings.cache_sweep_grace_seconds or 0
    plan = plan_sweep(
        objects,
        refs,
        now,
        retention_days=settings.cache_retention_days,
        keep_newest=settings.cache_keep_newest,
        grace_seconds=grace,
        prefix=settings.cache_s3_prefix,
    )
    async with pool.connection() as conn:
        last = await cache_sweeps.last(conn)
    response.headers["Cache-Control"] = "no-store"
    return schemas.CleanupPreview(
        would_delete=[
            schemas.CleanupItem(key=d.key, size=d.size, reason=d.reason) for d in plan.deletes
        ],
        count=len(plan.deletes),
        total_bytes=plan.bytes,
        retention_days=settings.cache_retention_days,
        keep_newest=settings.cache_keep_newest,
        grace_seconds=grace,
        sweep_enabled=settings.cache_sweep_enabled,
        sweep_dry_run=settings.cache_sweep_dry_run,
        last_sweep=last,
    )


@router.delete("/cache/archives", response_model=schemas.ArchiveDeleteResult)
async def delete_archives(
    body: schemas.DeleteArchives, request: Request, pool: Pool, settings: Cfg
) -> schemas.ArchiveDeleteResult:
    creds = _require_s3(settings)
    keys = list(dict.fromkeys(body.keys))
    refused: list[schemas.ArchiveRefusal] = []
    valid: list[str] = []
    for key in keys:
        try:
            _name_of(settings, key)
            valid.append(key)
        except ValueError as exc:
            refused.append(schemas.ArchiveRefusal(key=key, reason=str(exc)))
    deletable: list[str] = []
    if valid:
        # one fresh read of the bucket and of the database, taken right before the delete
        listed = await asyncio.to_thread(
            s3.list_archives, settings, settings.cache_s3_prefix, credentials=creds
        )
        by_key = {o.key: o for o in listed}
        young = await _db_now(pool) - timedelta(seconds=settings.cache_sweep_grace_seconds or 0)
        refs = await _protected(pool, settings)  # the last read before the delete
        for key in valid:
            obj = by_key.get(key)
            if obj is None:
                refused.append(schemas.ArchiveRefusal(key=key, reason="not found"))
            elif refs.get(key):
                who = ", ".join(refs[key])
                refused.append(
                    schemas.ArchiveRefusal(key=key, reason=f"referenced by session {who}")
                )
            elif obj.last_modified > young:
                refused.append(
                    schemas.ArchiveRefusal(key=key, reason="younger than the grace period")
                )
            else:
                deletable.append(key)
    failures = (
        await asyncio.to_thread(s3.delete_keys, settings, deletable, credentials=creds)
        if deletable
        else []
    )
    failed = {f.key for f in failures}
    deleted = [k for k in deletable if k not in failed]
    who = calling_service(request) or (request.client.host if request.client else "unknown")
    log.info(
        "audit: cache archives delete by=%s requested=%d deleted=%s refused=%d errors=%d",
        who,
        len(keys),
        deleted,
        len(refused),
        len(failures),
    )
    return schemas.ArchiveDeleteResult(
        deleted=deleted,
        refused=refused,
        errors=[
            schemas.ArchiveDeleteError(key=f.key, code=f.code, message=f.message) for f in failures
        ],
    )

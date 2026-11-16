"""Admin mirrors of the backups api (superleader): the reads, the copy and the cleanup settings
and preview, each calling the main-port handler of `api/backups.py` so answers and errors (400
`bad_request`, 404, 503 `unavailable` without the api S3 pair) are identical. The presigned
download and the deletes are NOT exposed (a download URL carries a credential; a delete is for
the user). The router guard is the superleader gate (the reader guard of step 13 replaces it for
the read routes). Writes: the copy (S3 CopyObject plus a version row) and the cleanup settings
PUT (Postgres only)."""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import Query, Request, Response

from agents_orchestrator.api import backups, schemas
from agents_orchestrator.api.admin import FORBIDDEN, caller_of, note_detail, router
from agents_orchestrator.api.sessions import Cfg, Pool
from agents_orchestrator.api.spec_edit import note_fields

_UNAVAILABLE = {"description": "unavailable: backups are not configured for the api"}
_BAD_REQUEST = {"description": "bad_request: invalid name or cursor"}
_NOT_FOUND = {"description": "backup_not_found or version_not_found"}
_Limit = Annotated[int, Query(ge=1, le=backups.LIST_LIMIT_MAX)]
_Cursor = Annotated[str | None, Query(max_length=1024)]


@router.get(
    "/backups",
    response_model=schemas.BackupNamePage,
    responses={400: _BAD_REQUEST, 403: FORBIDDEN, 503: _UNAVAILABLE},
)
async def list_backups(
    pool: Pool,
    settings: Cfg,
    q: Annotated[str | None, Query(max_length=200)] = None,
    cursor: _Cursor = None,
    limit: _Limit = 200,
) -> schemas.BackupNamePage:
    return await backups.list_backups(pool, settings, q, cursor, limit)


@router.post(
    "/backups/copy",
    response_model=schemas.BackupVersionView,
    status_code=201,
    responses={
        400: _BAD_REQUEST,
        403: FORBIDDEN,
        404: _NOT_FOUND,
        502: {"description": "storage_error: the object store refused the copy"},
        503: _UNAVAILABLE,
    },
)
async def copy_backup(
    body: schemas.BackupCopyIn, request: Request, pool: Pool, settings: Cfg
) -> schemas.BackupVersionView:
    """Copy a version to a target name; the copy becomes the target's current version (replace
    only, no merge). The audit row names source and target, never content."""
    _, caller = caller_of(request)
    view, _created = await backups.copy_backup(pool, settings, body, f"superleader {caller.slug}")
    note_detail(request, source=body.source, target=body.target)
    return view


@router.get(
    "/backups/cleanup/settings",
    response_model=schemas.CleanupSettingsView,
    responses={403: FORBIDDEN},
)
async def backups_cleanup_settings(
    pool: Pool, settings: Cfg, response: Response
) -> schemas.CleanupSettingsView:
    return await backups.get_cleanup_settings(pool, settings, response)


@router.put(
    "/backups/cleanup/settings",
    response_model=schemas.CleanupSettingsView,
    responses={403: FORBIDDEN},
)
async def put_backups_cleanup_settings(
    body: schemas.SweepPolicyIn, request: Request, pool: Pool, settings: Cfg, response: Response
) -> schemas.CleanupSettingsView:
    """Replace the whole cleanup policy (every field required, same bounds as the main port).
    Postgres only: it needs no api S3 key pair. The audit row lists the field names, no values."""
    _, caller = caller_of(request)
    view = await backups.write_cleanup_settings(pool, settings, body, f"superleader {caller.slug}")
    note_fields(request, sorted(schemas.SweepPolicyIn.model_fields))
    response.headers["Cache-Control"] = "no-store"
    return view


@router.get(
    "/backups/cleanup/preview",
    response_model=schemas.CleanupPreview,
    responses={403: FORBIDDEN, 503: _UNAVAILABLE},
)
async def backups_cleanup_preview(
    pool: Pool, settings: Cfg, response: Response
) -> schemas.CleanupPreview:
    return await backups.cleanup_preview(pool, settings, response)


@router.get(
    "/backups/{name:path}/versions/{version_id}/manifest",
    response_model=schemas.BackupManifestView,
    responses={400: _BAD_REQUEST, 403: FORBIDDEN, 404: _NOT_FOUND, 503: _UNAVAILABLE},
)
async def backup_manifest(
    name: str, version_id: uuid.UUID, pool: Pool, settings: Cfg
) -> schemas.BackupManifestView:
    return await backups.get_manifest(pool, settings, name, version_id)


@router.get(
    "/backups/{name:path}/versions",
    response_model=schemas.BackupVersionPage,
    responses={400: _BAD_REQUEST, 403: FORBIDDEN, 404: _NOT_FOUND, 503: _UNAVAILABLE},
)
async def backup_versions(
    name: str, pool: Pool, settings: Cfg, cursor: _Cursor = None, limit: _Limit = 200
) -> schemas.BackupVersionPage:
    return await backups.list_versions(pool, settings, name, cursor, limit)

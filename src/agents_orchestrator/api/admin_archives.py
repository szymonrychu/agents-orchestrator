"""Admin reads of the cache archives (T-120): the list, the cleanup settings and the cleanup
preview, for a superleader. Each route calls the main-port handler of `api/archives.py`, so the
answers and errors (400 `bad_request`, 503 `unavailable` without the api S3 pair) are identical.
The presigned download is NOT exposed (its URL carries a credential). The one write is the
cleanup settings PUT, which stores a policy in Postgres only (no S3 call)."""

from __future__ import annotations

from typing import Annotated

from fastapi import Query, Request, Response

from agents_orchestrator.api import archives, schemas
from agents_orchestrator.api.admin import FORBIDDEN, caller_of, router
from agents_orchestrator.api.sessions import Cfg, Pool
from agents_orchestrator.api.spec_edit import note_fields

_UNAVAILABLE = {"description": "unavailable: the cache archives are not configured for the api"}
_BAD_REQUEST = {"description": "bad_request: invalid prefix or cursor"}


@router.get(
    "/archives",
    response_model=schemas.ArchivePage,
    responses={400: _BAD_REQUEST, 403: FORBIDDEN, 503: _UNAVAILABLE},
)
async def list_archives(
    pool: Pool,
    settings: Cfg,
    prefix: str = "",
    cursor: Annotated[str | None, Query(max_length=1024)] = None,
    limit: Annotated[int, Query(ge=1, le=archives.LIST_LIMIT_MAX)] = 200,
) -> schemas.ArchivePage:
    return await archives.list_archives(pool, settings, prefix, cursor, limit)


@router.get(
    "/archives/settings",
    response_model=schemas.CleanupSettingsView,
    responses={403: FORBIDDEN, 503: _UNAVAILABLE},
)
async def archives_settings(
    pool: Pool, settings: Cfg, response: Response
) -> schemas.CleanupSettingsView:
    return await archives.get_cleanup_settings(pool, settings, response)


@router.get(
    "/archives/cleanup/preview",
    response_model=schemas.CleanupPreview,
    responses={403: FORBIDDEN, 503: _UNAVAILABLE},
)
async def archives_cleanup_preview(
    pool: Pool, settings: Cfg, response: Response
) -> schemas.CleanupPreview:
    return await archives.cleanup_preview(pool, settings, response)


@router.put(
    "/archives/settings",
    response_model=schemas.CleanupSettingsView,
    responses={403: FORBIDDEN},
)
async def put_archives_settings(
    body: schemas.SweepPolicyIn, request: Request, pool: Pool, settings: Cfg, response: Response
) -> schemas.CleanupSettingsView:
    """Replace the whole cleanup policy (every field required, same bounds as the main port).
    Postgres only: it needs no api S3 key pair. The audit row lists the field names, no values."""
    _, caller = caller_of(request)
    view = await archives.write_cleanup_settings(pool, settings, body, f"superleader {caller.slug}")
    note_fields(request, sorted(schemas.SweepPolicyIn.model_fields))
    response.headers["Cache-Control"] = "no-store"
    return view

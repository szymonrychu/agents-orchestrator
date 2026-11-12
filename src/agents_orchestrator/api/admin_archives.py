"""Admin reads of the cache archives (T-120): the list, the cleanup settings and the cleanup
preview, for a superleader. Each route calls the main-port handler of `api/archives.py`, so the
answers and errors (400 `bad_request`, 503 `unavailable` without the api S3 pair) are identical.
The presigned download is NOT exposed (its URL carries a credential), nor any write."""

from __future__ import annotations

from typing import Annotated

from fastapi import Query, Response

from agents_orchestrator.api import archives, schemas
from agents_orchestrator.api.admin import router
from agents_orchestrator.api.sessions import Cfg, Pool

_FORBIDDEN = {"description": "not_superleader: the caller is not a superleader"}
_UNAVAILABLE = {"description": "unavailable: the cache archives are not configured for the api"}
_BAD_REQUEST = {"description": "bad_request: invalid prefix or cursor"}


@router.get(
    "/archives",
    response_model=schemas.ArchivePage,
    responses={400: _BAD_REQUEST, 403: _FORBIDDEN, 503: _UNAVAILABLE},
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
    responses={403: _FORBIDDEN, 503: _UNAVAILABLE},
)
async def archives_settings(
    pool: Pool, settings: Cfg, response: Response
) -> schemas.CleanupSettingsView:
    return await archives.get_cleanup_settings(pool, settings, response)


@router.get(
    "/archives/cleanup/preview",
    response_model=schemas.CleanupPreview,
    responses={403: _FORBIDDEN, 503: _UNAVAILABLE},
)
async def archives_cleanup_preview(
    pool: Pool, settings: Cfg, response: Response
) -> schemas.CleanupPreview:
    return await archives.cleanup_preview(pool, settings, response)

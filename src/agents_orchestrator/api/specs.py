"""Spec relay (`/agent/v1/specs`), served only on the agent port.

Same contract as the task relay (`api/tasks.py`, which holds the shared `relay` core): the caller
is verified, the upstream is `{ORCH_TASKBOARD_URL}/taskboard/api/v1/specs...`, the forwarded
request carries the taskboard bearer, `X-Orch-Session`, `X-Orch-Slug` and `X-Orch-Projects`, the
body is capped at 256 KiB (413 `payload_too_large`) and the upstream status and JSON body come back
unchanged. The allowlist is the spec, its documents and versions, and its comments with resolve.
Accept and dismiss are user-only and are not relayed, DELETE does not exist here, and a path with
`..` is refused: all are 404 `not_found` without an upstream call. `labels=mine` is not expanded.
"""

from __future__ import annotations

import re

from fastapi import APIRouter, Request, Response

from agents_orchestrator.api.agent import Caller
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.sessions import Cfg, Pool
from agents_orchestrator.api.tasks import _RESPONSES, relay

router = APIRouter(prefix="/agent/v1/specs", tags=["agent"])

_KEY = r"S-[0-9]+"
_P = r"[a-z0-9][a-z0-9._/-]{0,119}\.md"
_ALLOWED = re.compile(
    rf"/specs(?:|/{_KEY}(?:|/docs|/docs/{_P}|/docs/{_P}/versions"
    rf"|/comments|/comments/[0-9]+|/comments/[0-9]+/resolve))"
)


async def _relay(request: Request, agent: Caller, pool: Pool, settings: Cfg, path: str) -> Response:
    if ".." in path:
        raise ApiError(404, "not_found", "no such taskboard path")
    return await relay(request, agent, pool, settings, "/specs" + path, _ALLOWED, mine=False)


@router.get(
    "",
    summary="Taskboard specs collection (list, create) as the caller's session",
    responses=_RESPONSES,
)
@router.post(
    "",
    summary="Taskboard specs collection (list, create) as the caller's session",
    responses=_RESPONSES,
)
@router.put(
    "",
    summary="Taskboard specs collection (list, create) as the caller's session",
    responses=_RESPONSES,
)
@router.patch(
    "",
    summary="Taskboard specs collection (list, create) as the caller's session",
    responses=_RESPONSES,
)
async def collection(request: Request, agent: Caller, pool: Pool, settings: Cfg) -> Response:
    return await _relay(request, agent, pool, settings, "")


@router.get(
    "/{path:path}",
    summary="Taskboard spec, documents, versions and comments as the caller's session",
    responses=_RESPONSES,
)
@router.post(
    "/{path:path}",
    summary="Taskboard spec, documents, versions and comments as the caller's session",
    responses=_RESPONSES,
)
@router.put(
    "/{path:path}",
    summary="Taskboard spec, documents, versions and comments as the caller's session",
    responses=_RESPONSES,
)
@router.patch(
    "/{path:path}",
    summary="Taskboard spec, documents, versions and comments as the caller's session",
    responses=_RESPONSES,
)
async def item(path: str, request: Request, agent: Caller, pool: Pool, settings: Cfg) -> Response:
    return await _relay(request, agent, pool, settings, "/" + path)

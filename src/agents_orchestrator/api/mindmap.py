"""Mindmap relay (`/agent/v1/mindmap`), served only on the agent port.

Same contract as the task relay (`api/tasks.py`, which holds the shared `relay` core): the caller
is verified, the upstream is `{ORCH_MINDMAP_URL}/mindmap/api/v1/...`, the forwarded request
carries the mindmap bearer (`ORCH_MINDMAP_TOKEN`), `X-Orch-Session`, `X-Orch-Slug` and
`X-Orch-Projects`, the body is capped at 256 KiB (413 `payload_too_large`) and the upstream status
and JSON body (a 204 stays a 204) come back unchanged. The service, not the relay, applies the
read rule from the project header: agents read their active project only. An unconfigured upstream
is 503 `mindmap_unconfigured`, a failed call 503 `mindmap_unavailable`. `labels=mine` is not
expanded. The allowlist is the service listener's route surface per method (agents-mindmap
`openapi-service.json`); any other method or path, and a path with `..`, `.` or empty segments, is
404 `not_found` without an upstream call.
"""

from __future__ import annotations

import re

from fastapi import APIRouter, Request, Response

from agents_orchestrator.api import forward as forward_core
from agents_orchestrator.api.agent import Caller
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.sessions import Cfg, Pool
from agents_orchestrator.api.tasks import relay
from agents_orchestrator.settings import Settings

router = APIRouter(prefix="/agent/v1/mindmap", tags=["agent"])

MAX_BODY_BYTES = forward_core.MAX_BODY_BYTES
TIMEOUT_SECONDS = forward_core.TIMEOUT_SECONDS
STATE_ATTR = "mindmap_http"
# the mindmap path prefix; the service listener serves its API under it
UPSTREAM_PREFIX = "/mindmap/api/v1"

_M, _N, _G, _E, _D = (rf"{p}-[0-9]+" for p in "MNGED")
_ALLOWED = {
    "GET": re.compile(
        rf"/(?:maps|links|directions|projects|search)|/maps/{_M}(?:|/graph)"
        rf"|/nodes/{_N}(?:|/traverse|/versions|/versions/[0-9]+)"
    ),
    "POST": re.compile(
        rf"/(?:maps|edges|knowledge/notes)|/maps/{_M}/(?:nodes|directions|links)"
        rf"|/nodes/{_N}/(?:growth-areas|links)"
    ),
    "PATCH": re.compile(rf"/maps/{_M}|/nodes/{_N}|/growth-areas/{_G}|/directions/{_D}"),
    "DELETE": re.compile(rf"/edges/{_E}|/links/[0-9]+"),
}
_PASS_THROUGH = {"description": "The mindmap upstream's status and JSON body, unchanged"}
_RESPONSES: dict[int | str, dict[str, str]] = {
    200: _PASS_THROUGH,
    204: {"description": "No content (edge and link delete), passed through"},
    401: {"description": "unauthorized: missing or invalid agent credentials"},
    403: {"description": "forbidden (not a live agent pod)"},
    404: {"description": "not_found: the method and path are not one the relay forwards"},
    413: {"description": "payload_too_large: the body exceeds 256 KiB"},
    503: {"description": "mindmap_unconfigured or mindmap_unavailable"},
}


def mindmap_target(settings: Settings) -> forward_core.Target:
    """The mindmap upstream; callers check `settings.mindmap_available` first."""
    return forward_core.Target(
        "mindmap relay",
        settings.mindmap_url.rstrip("/") + UPSTREAM_PREFIX,
        settings.mindmap_token,
        STATE_ATTR,
        "mindmap_unavailable",
        TIMEOUT_SECONDS,
    )


_SUMMARY = "Mindmap maps, nodes, edges, notes, links and directions as the caller's session"


@router.get("/{path:path}", summary=_SUMMARY, responses=_RESPONSES)
@router.post("/{path:path}", summary=_SUMMARY, responses=_RESPONSES)
@router.patch("/{path:path}", summary=_SUMMARY, responses=_RESPONSES)
@router.delete("/{path:path}", summary=_SUMMARY, responses=_RESPONSES)
async def item(path: str, request: Request, agent: Caller, pool: Pool, settings: Cfg) -> Response:
    if ".." in path or any(seg in ("", ".") for seg in path.split("/")):
        raise ApiError(404, "not_found", "no such relay path")
    return await relay(
        request,
        agent,
        pool,
        settings,
        "/" + path,
        _ALLOWED[request.method],
        target=mindmap_target(settings),
        available=settings.mindmap_available,
        unconfigured="mindmap_unconfigured",
        mine=False,
    )

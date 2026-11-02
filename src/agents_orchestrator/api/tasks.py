"""Taskboard relay (`/agent/v1/tasks`), served only on the agent port.

An agent session reaches agents-taskboard through the orchestrator, which vouches for its
identity: every route runs `require_agent`, then requires the taskboard upstream configured (503
`taskboard_unconfigured`), then caps the body at 256 KiB (413 `payload_too_large`) and forwards the
call to `{ORCH_TASKBOARD_URL}/taskboard/api/v1/tasks/...`. There is no trigger-session refusal and
no leader check (leader decision 2026-11-02): trigger-created sessions may use the relay, they
still cannot answer decisions because answering exists only on the web listener, so the relay
does not forward `.../answer` (nor `/stream`): any path outside the allowed set is 404
`not_found` here. The forwarded request carries only the taskboard bearer
(`ORCH_TASKBOARD_TOKEN`), `X-Orch-Session` and `X-Orch-Slug` of the caller (slug read from the
sessions repo), and for a body `Content-Type: application/json`; no inbound header and never the
agent token. On `GET tasks` a `labels=mine` parameter becomes one `label` parameter per label of
the calling session; an unlabelled session gets an empty page without calling the upstream. The
upstream status and JSON body come back unchanged; a connection error, a timeout or a non-JSON
answer is 503 `taskboard_unavailable`. The relay stores nothing and logs method, path and status
only.
"""

from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import parse_qsl, urlencode

import httpx
from fastapi import APIRouter, FastAPI, Request, Response

from agents_orchestrator.api import forward as forward_core
from agents_orchestrator.api.agent import Caller, _session
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.sessions import Cfg, Pool

router = APIRouter(prefix="/agent/v1/tasks", tags=["agent"])

MAX_BODY_BYTES = forward_core.MAX_BODY_BYTES
TIMEOUT_SECONDS = forward_core.TIMEOUT_SECONDS
UPSTREAM_PREFIX = "/taskboard/api/v1"
STATE_ATTR = "taskboard_http"

_ID = r"[A-Za-z0-9_-]+"
_KEY = r"T-[0-9]+"
_ALLOWED = re.compile(
    rf"(?:|/{_KEY}(?:|/comments|/todo|/todo/{_ID}|/decisions|/decisions/{_ID}/withdraw))"
)


def client(app: FastAPI) -> httpx.AsyncClient:
    """The app's upstream client, created on first use; tests set `app.state.taskboard_http`."""
    return forward_core.client(app, STATE_ATTR)


async def close_client(app: FastAPI) -> None:
    await forward_core.close_client(app, STATE_ATTR)


def _expand_mine(query: str, labels: list[str]) -> str:
    """The query with `labels=mine` replaced by one `label` parameter per session label."""
    pairs: list[tuple[str, str]] = []
    for key, value in parse_qsl(query, keep_blank_values=True):
        if (key, value) == ("labels", "mine"):
            pairs.extend(("label", label) for label in labels)
        else:
            pairs.append((key, value))
    return urlencode(pairs)


def _wants_mine(request: Request, path: str) -> bool:
    query = request.scope.get("query_string", b"").decode("latin-1")
    return (
        request.method == "GET"
        and path == ""
        and ("labels", "mine") in parse_qsl(query, keep_blank_values=True)
    )


_PASS_THROUGH = {"description": "The taskboard upstream's status and JSON body, unchanged"}
_RESPONSES: dict[int | str, dict[str, Any]] = {
    200: _PASS_THROUGH,
    401: {"description": "unauthorized: missing or invalid agent credentials"},
    403: {"description": "forbidden (not a live agent pod)"},
    404: {"description": "not_found: the path is not one the relay forwards"},
    413: {"description": "payload_too_large: the body exceeds 256 KiB"},
    503: {"description": "taskboard_unconfigured or taskboard_unavailable"},
}


async def _relay(request: Request, agent: Caller, pool: Pool, settings: Cfg, path: str) -> Response:
    if not _ALLOWED.fullmatch(path):
        raise ApiError(404, "not_found", "no such taskboard path")
    async with pool.connection() as conn:
        own = await _session(conn, agent)
    if not settings.taskboard_available:
        raise ApiError(503, "taskboard_unconfigured", "no taskboard upstream is configured")
    query = None
    if _wants_mine(request, path):
        if not own.labels:
            empty = json.dumps({"items": [], "next_cursor": None}).encode()
            return Response(empty, 200, media_type="application/json")
        query = _expand_mine(request.scope.get("query_string", b"").decode("latin-1"), own.labels)
    body = None if request.method == "GET" else await forward_core.capped_body(request)
    target = forward_core.Target(
        "taskboard relay",
        settings.taskboard_url.rstrip("/") + UPSTREAM_PREFIX,
        settings.taskboard_token,
        STATE_ATTR,
        "taskboard_unavailable",
        TIMEOUT_SECONDS,
    )
    status, content = await forward_core.forward(
        request, target, request.method, "/tasks" + path, body, str(own.id), own.slug, query=query
    )
    return Response(content, status, media_type="application/json")


@router.get(
    "",
    summary="Taskboard tasks collection (list, create) as the caller's session",
    responses=_RESPONSES,
)
@router.post(
    "",
    summary="Taskboard tasks collection (list, create) as the caller's session",
    responses=_RESPONSES,
)
@router.put(
    "",
    summary="Taskboard tasks collection (list, create) as the caller's session",
    responses=_RESPONSES,
)
@router.patch(
    "",
    summary="Taskboard tasks collection (list, create) as the caller's session",
    responses=_RESPONSES,
)
async def collection(request: Request, agent: Caller, pool: Pool, settings: Cfg) -> Response:
    return await _relay(request, agent, pool, settings, "")


@router.get(
    "/{path:path}",
    summary="Taskboard task, comments, todo and decisions as the caller's session",
    responses=_RESPONSES,
)
@router.post(
    "/{path:path}",
    summary="Taskboard task, comments, todo and decisions as the caller's session",
    responses=_RESPONSES,
)
@router.put(
    "/{path:path}",
    summary="Taskboard task, comments, todo and decisions as the caller's session",
    responses=_RESPONSES,
)
@router.patch(
    "/{path:path}",
    summary="Taskboard task, comments, todo and decisions as the caller's session",
    responses=_RESPONSES,
)
async def item(path: str, request: Request, agent: Caller, pool: Pool, settings: Cfg) -> Response:
    return await _relay(request, agent, pool, settings, "/" + path)

"""Taskboard relay (`/agent/v1/tasks`), served only on the agent port.

An agent session reaches agents-taskboard through the orchestrator, which vouches for its identity:
every route runs `require_agent`, then requires the taskboard upstream configured (503
`taskboard_unconfigured`), then caps the body at 256 KiB (413 `payload_too_large`) and forwards the
call to `{ORCH_TASKBOARD_URL}/taskboard/api/v1/tasks/...`. There is no trigger-session refusal and
no leader check (leader decision 2026-11-02): trigger-created sessions may use the relay, they still
cannot answer decisions because answering exists only on the web listener, so the relay does not
forward `.../answer` (nor `/stream`): any path outside the allowed set is 404 `not_found` here. The
forwarded request carries only the taskboard bearer (`ORCH_TASKBOARD_TOKEN`), `X-Orch-Session` and
`X-Orch-Slug` of the caller (slug read from the sessions repo), `X-Orch-Projects` (the caller's
project scope, `scope.scope_of`: `*` for a superleader, else its project, read live), and for a body
`Content-Type: application/json`; no inbound header and never the agent token. On `GET tasks` a
`labels=mine` parameter becomes one `label` parameter per label of the calling session; an
unlabelled session gets an empty page without calling the upstream. The upstream status and JSON
body come back unchanged; a connection error, a timeout or a non-JSON answer is 503
`taskboard_unavailable`. The relay stores nothing and logs method, path and status only.
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
from agents_orchestrator.api.scope import projects_header
from agents_orchestrator.api.sessions import Cfg, Pool
from agents_orchestrator.settings import Settings
from agents_orchestrator.taskboard_client import UPSTREAM_PREFIX

router = APIRouter(prefix="/agent/v1/tasks", tags=["agent"])
summary_router = APIRouter(prefix="/agent/v1/projects", tags=["agent"])

MAX_BODY_BYTES = forward_core.MAX_BODY_BYTES
TIMEOUT_SECONDS = forward_core.TIMEOUT_SECONDS
STATE_ATTR = "taskboard_http"

_ID = r"[A-Za-z0-9_-]+"
_KEY = r"T-[0-9]+"
_ALLOWED = re.compile(
    rf"/tasks(?:|/{_KEY}(?:|/comments|/todo|/todo/{_ID}|/deps|/decisions|/decisions/{_ID}/withdraw))"
)
_SUMMARY = re.compile(r"/projects/summary")


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


def _wants_mine(request: Request, upstream_path: str) -> bool:
    query = request.scope.get("query_string", b"").decode("latin-1")
    return (
        request.method == "GET"
        and upstream_path == "/tasks"
        and ("labels", "mine") in parse_qsl(query, keep_blank_values=True)
    )


# Relayed writes that count as a user-visible update of the session (spec 3.2).
_WRITES = frozenset({"POST", "PUT", "PATCH"})
_PASS_THROUGH = {"description": "The taskboard upstream's status and JSON body, unchanged"}
_RESPONSES: dict[int | str, dict[str, Any]] = {
    200: _PASS_THROUGH,
    401: {"description": "unauthorized: missing or invalid agent credentials"},
    403: {"description": "forbidden (not a live agent pod)"},
    404: {"description": "not_found: the path is not one the relay forwards"},
    413: {"description": "payload_too_large: the body exceeds 256 KiB"},
    503: {"description": "taskboard_unconfigured or taskboard_unavailable"},
}


def taskboard_target(settings: Settings) -> forward_core.Target:
    """The taskboard upstream; callers check `settings.taskboard_available` first."""
    return forward_core.Target(
        "taskboard relay",
        settings.taskboard_url.rstrip("/") + UPSTREAM_PREFIX,
        settings.taskboard_token,
        STATE_ATTR,
        "taskboard_unavailable",
        TIMEOUT_SECONDS,
    )


async def relay(
    request: Request,
    agent: Caller,
    pool: Pool,
    settings: Settings,
    upstream_path: str,
    allowed: re.Pattern[str],
    *,
    mine: bool,
) -> Response:
    """Forward one agent call to the taskboard as the caller's session.

    `upstream_path` is the whole path under `/taskboard/api/v1` (`/tasks/T-1`, `/specs`) and
    `allowed` must match it in full, else 404 `not_found` without an upstream call. `mine` turns
    on the `labels=mine` expansion of `GET /tasks`; the specs relay passes False.
    """
    if not allowed.fullmatch(upstream_path):
        raise ApiError(404, "not_found", "no such taskboard path")
    async with pool.connection() as conn:
        own = await _session(conn, agent)
    if not settings.taskboard_available:
        raise ApiError(503, "taskboard_unconfigured", "no taskboard upstream is configured")
    query = None
    if mine and _wants_mine(request, upstream_path):
        if not own.labels:
            empty = json.dumps({"items": [], "next_cursor": None}).encode()
            return Response(empty, 200, media_type="application/json")
        query = _expand_mine(request.scope.get("query_string", b"").decode("latin-1"), own.labels)
    body = None if request.method == "GET" else await forward_core.capped_body(request)
    status, content = await forward_core.forward(
        request,
        taskboard_target(settings),
        request.method,
        upstream_path,
        body,
        str(own.id),
        own.slug,
        query=query,
        extra_headers=projects_header(own),
    )
    if request.method in _WRITES:
        await forward_core.touch_on_success(pool, own.id, status)
    return Response(content, status, media_type="application/json")


async def _relay(request: Request, agent: Caller, pool: Pool, settings: Cfg, path: str) -> Response:
    return await relay(request, agent, pool, settings, "/tasks" + path, _ALLOWED, mine=True)


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


@summary_router.get(
    "/summary",
    summary="Per-project task and spec counts within the caller's project scope (taskboard relay)",
    responses=_RESPONSES,
)
async def projects_summary(request: Request, agent: Caller, pool: Pool, settings: Cfg) -> Response:
    return await relay(request, agent, pool, settings, "/projects/summary", _SUMMARY, mine=False)

"""Server-side confirmation of chat room requests (matrix-integration, Q5).

An agent asks the connector to open a chat or invite someone (`POST /chats/room-requests`, a relay
route). Nothing is queued without the user's yes: the orchestrator asks a taskboard decision AS THE
CALLING SESSION (so the answer notice reaches it) and attaches the decision id to the request
through the connector's internal routes (`/chat/v1/internal`, never relayed, so no agent can reach
them). `POST /chats/room-requests/{request_id}/confirm` then reads that decision and confirms or
rejects the request. The connector refuses a confirm whose decision id differs from the attached
one, so a decision the agent asked itself on the same task never confirms anything.

Failures: a taskboard that is not configured is 503 `taskboard_unconfigured`, a taskboard that
fails 503 `taskboard_unavailable`, the connector failing 503 `upstream_unavailable`. A request
for a task the taskboard does not know is 422 `unknown_task` and nothing is forwarded.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import re
from typing import Any
from urllib.parse import quote

from fastapi import Request, Response
from fastapi.responses import JSONResponse

from agents_orchestrator.api import forward as forward_core
from agents_orchestrator.api.admin import caller_of, router
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.scope import ALL_PROJECTS, PROJECTS_HEADER
from agents_orchestrator.api.sessions import Cfg
from agents_orchestrator.api.tasks import taskboard_target
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)

ADMIN_UPSTREAM = "/chat/v1/admin"
INTERNAL_UPSTREAM = "/chat/v1/internal"
STATE_ATTR = "admin_http"  # the connector shares the admin relay's client (see admin_relay)
TASK_KEY = re.compile(r"T-[0-9]+")
YES, NO = "Yes, open it", "No"


def connector_target(settings: Settings) -> forward_core.Target:
    upstream = settings.admin_upstream("connector")
    if upstream is None:
        raise ApiError(503, "upstream_unconfigured", "no connector upstream is configured")
    return forward_core.Target(
        "chat admin relay", upstream[0], upstream[1], STATE_ATTR, "upstream_unavailable"
    )


def _taskboard(settings: Settings) -> forward_core.Target:
    if not settings.taskboard_available:
        raise ApiError(503, "taskboard_unconfigured", "no taskboard upstream is configured")
    return taskboard_target(settings)


def _task_path(task_key: str, suffix: str = "") -> str:
    return "/tasks/" + quote(task_key, safe="") + suffix


async def _call(
    request: Request,
    target: forward_core.Target,
    method: str,
    path: str,
    body: Any = None,
    *,
    extra_headers: dict[str, str] | None = None,
) -> tuple[int, Any]:
    """One call as the calling session; the JSON answer with its status."""
    _, session = caller_of(request)
    raw = None if body is None else json.dumps(body).encode()
    status, content = await forward_core.forward(
        request,
        target,
        method,
        path,
        raw,
        str(session.id),
        session.slug,
        empty_ok=True,
        query="",
        extra_headers=extra_headers,
    )
    return status, (json.loads(content) if content else None)


async def _board_call(
    settings: Settings, request: Request, method: str, path: str, body: Any = None
) -> tuple[int, Any]:
    """A taskboard call for a room request: a room is not project-bound, so the scope is `*`."""
    return await _call(
        request,
        _taskboard(settings),
        method,
        path,
        body,
        extra_headers={PROJECTS_HEADER: ALL_PROJECTS},
    )


async def check_task(request: Request, settings: Settings, body: bytes | None) -> None:
    """Before a room request is forwarded: the taskboard must be configured and know the task
    named by the body's `task_key` (422 `unknown_task`). A body without a valid key is left to
    the connector, which answers 422 itself."""
    _taskboard(settings)
    try:
        key = json.loads(body).get("task_key") if body else None
    except (ValueError, AttributeError):
        key = None
    if not isinstance(key, str) or not TASK_KEY.fullmatch(key):
        return
    status, _ = await _board_call(settings, request, "GET", _task_path(key))
    if status == 404:
        raise ApiError(422, "unknown_task", f"the taskboard has no task {key}")
    if not 200 <= status < 300:
        raise ApiError(503, "taskboard_unavailable", f"the taskboard answered {status}")


def decision_key(request_id: Any, secret: str) -> str:
    """The decision's `client_key`: keyed with the connector token, so no agent can guess it and
    pre-create a decision with spoofed wording that `ensure_decision` would then reuse."""
    mac = hmac.new(secret.encode(), f"room-request-{request_id}".encode(), hashlib.sha256)
    return f"room-request-{request_id}-{mac.hexdigest()[:32]}"


def _question(row: dict[str, Any]) -> str:
    target = row.get("target")
    if row.get("action") == "invite":
        return f"Invite {target} into room {row.get('room_id')}?"
    if row.get("target_kind") == "facebook":
        who = row.get("display_name") or target
        return f"Open a Messenger chat with {who} (facebook id {target}) as Hermes?"
    return f"Open a Matrix DM with {target}?"


async def ensure_decision(
    request: Request, settings: Settings, row: dict[str, Any]
) -> dict[str, Any]:
    """Ask the user's yes on the request's task, as the calling session, and attach it.

    Returns the row (as the connector now holds it) plus `decision: {id, url}`. A row that is not
    `pending_confirmation`, or already has a decision, is returned unchanged. The ask is
    idempotent through `client_key`, the attach is a guarded update on the connector.
    """
    if row.get("status") != "pending_confirmation" or row.get("decision_id") is not None:
        return row
    _, session = caller_of(request)
    key = row["task_key"]
    ask = {
        "question": _question(row),
        "context": f"Requested by session {session.slug} with chat_open; room request {row['id']}.",
        "options": [{"label": YES, "recommended": True}, {"label": NO}],
        "client_key": decision_key(row["id"], settings.connector_token),
    }
    status, decision = await _board_call(
        settings, request, "POST", _task_path(key, "/decisions"), ask
    )
    if status not in (200, 201) or not isinstance(decision, dict) or "id" not in decision:
        raise ApiError(503, "taskboard_unavailable", f"the taskboard answered {status}")
    attach = {"task_key": key, "decision_id": decision["id"]}
    path = f"{INTERNAL_UPSTREAM}/room-requests/{row['id']}/decision"
    status, view = await _call(request, connector_target(settings), "PUT", path, attach)
    if status == 409:  # attached meanwhile, or no longer pending: the connector's row is the truth
        status, view = await _call(
            request,
            connector_target(settings),
            "GET",
            f"{ADMIN_UPSTREAM}/room-requests/{row['id']}",
        )
    if status != 200 or not isinstance(view, dict):
        raise ApiError(503, "upstream_unavailable", f"the connector answered {status}")
    return {**view, "decision": {"id": decision["id"], "url": decision.get("url")}}


async def on_created(request: Request, settings: Settings, row: dict[str, Any]) -> dict[str, Any]:
    """`ensure_decision` for a row the connector just created. A failure here must not hide the
    row (it exists and counts against the hourly cap): the answer carries `decision: null` and
    the agent calls the confirm route, which asks again (idempotent through `client_key`)."""
    try:
        return await ensure_decision(request, settings, row)
    except ApiError as exc:
        log.warning("room request %s: decision not asked (%s)", row.get("id"), exc.code)
        return {**row, "decision": None}


async def confirm(request: Request, settings: Cfg, request_id: int) -> Response:
    """Confirm or reject a room request from the answer to its attached decision."""
    connector = connector_target(settings)
    status, row = await _call(
        request, connector, "GET", f"{ADMIN_UPSTREAM}/room-requests/{request_id}"
    )
    if status != 200 or not isinstance(row, dict):
        if 400 <= status < 500:
            return JSONResponse(row, status)
        raise ApiError(503, "upstream_unavailable", f"the connector answered {status}")
    if row["status"] != "pending_confirmation":
        raise ApiError(
            409,
            "not_pending",
            f"the room request is {row['status']}, not pending_confirmation",
            extra={"status": row["status"]},
        )
    if row["decision_id"] is None:
        return JSONResponse(await ensure_decision(request, settings, row), 202)
    status, task = await _board_call(settings, request, "GET", _task_path(row["task_key"]))
    if status != 200 or not isinstance(task, dict):
        raise ApiError(503, "taskboard_unavailable", f"the taskboard answered {status}")
    decision = next(
        (d for d in task.get("decisions") or [] if d.get("id") == row["decision_id"]), None
    )
    if decision is None:
        raise ApiError(502, "decision_missing", "the attached decision is not on the task")
    if decision.get("status") == "open":
        raise ApiError(
            409,
            "decision_open",
            "the user has not answered yet",
            extra={"url": decision.get("url")},
        )
    answer = decision.get("answer")
    approved = (
        decision.get("status") == "answered"
        and isinstance(answer, dict)
        and answer.get("choice") == 0
    )
    path = f"{INTERNAL_UPSTREAM}/room-requests/{request_id}/confirm"
    body = {"decision_id": row["decision_id"], "approved": approved}
    status, view = await _call(request, connector, "POST", path, body)
    if status >= 500 or view is None:
        raise ApiError(503, "upstream_unavailable", f"the connector answered {status}")
    return JSONResponse(view, status)


router.add_api_route(
    "/chats/room-requests/{request_id}/confirm",
    confirm,
    methods=["POST"],
    status_code=200,
    response_class=Response,
    responses={
        202: {"description": "No decision existed; it was asked and attached"},
        409: {"description": "not_pending, or decision_open (the user has not answered yet)"},
    },
    operation_id="admin_connector_post_room_requests_request_id_confirm",
    summary="POST connector /room-requests/{request_id}/confirm",
    tags=["admin"],
)

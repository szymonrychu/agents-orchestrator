"""Chat relay (`/agent/v1/chat`), served only on the agent port (design D2, D5, D12).

A session leader reaches the user's chat through the orchestrator: every route runs
`require_agent`, then the trigger-session check (403 `trigger_session`: a session created by
agents-webhook or agents-schedule works on attacker-controlled text and never reaches the user's
chat), then the leader check of `session_send` (403 `not_leader`), then requires the
chat upstream configured (503 `chat_unconfigured`), then caps the body at 256 KiB (413
`payload_too_large`) and forwards the call to `{ORCH_CHAT_URL}/chat/v1/...`. The forwarded request
carries only the chat bearer (`ORCH_CHAT_TOKEN`), `X-Orch-Session` and `X-Orch-Slug` of the caller
(the slug read from the sessions repo), and for a POST the body and `Content-Type:
application/json`; no inbound header and never the agent token. The upstream status and JSON body
come back unchanged; a connection error, a timeout or a non-JSON answer is 503 `chat_unavailable`.
The orchestrator does not validate the bodies (the upstream does) and stores nothing. Log lines
carry the method, path and status only, never a token or a body.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

import httpx
from fastapi import APIRouter, FastAPI, Request, Response
from pydantic import BaseModel, ConfigDict

from agents_orchestrator.api import forward as forward_core
from agents_orchestrator.api.agent import Caller, _leads, _session
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.sessions import Cfg, Pool

router = APIRouter(prefix="/agent/v1/chat", tags=["agent"])

MAX_BODY_BYTES = forward_core.MAX_BODY_BYTES
TIMEOUT_SECONDS = forward_core.TIMEOUT_SECONDS
UPSTREAM_PREFIX = "/chat/v1"
STATE_ATTR = "chat_http"


def client(app: FastAPI) -> httpx.AsyncClient:
    """The app's upstream client, created on first use; tests set `app.state.chat_http`."""
    return forward_core.client(app, STATE_ATTR)


async def close_client(app: FastAPI) -> None:
    await forward_core.close_client(app, STATE_ATTR)


class ChatMessageIn(BaseModel):
    """A chat message as the upstream reads it (OpenAPI only; the upstream validates it)."""

    model_config = ConfigDict(extra="forbid")

    text: str
    room: str | None = None
    reply_to: str | None = None


class ChatReactionIn(BaseModel):
    """A reaction as the upstream reads it (OpenAPI only; the upstream validates it)."""

    model_config = ConfigDict(extra="forbid")

    to: str
    key: str


_PASS_THROUGH = {"description": "The chat upstream's status and JSON body, unchanged"}
_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: {"description": "unauthorized: missing or invalid agent credentials"},
    403: {"description": "forbidden (not a live agent pod), trigger_session or not_leader"},
    503: {"description": "chat_unconfigured or chat_unavailable"},
}
_POST_RESPONSES: dict[int | str, dict[str, Any]] = {
    **_RESPONSES,
    413: {"description": "payload_too_large: the body exceeds 256 KiB"},
}


def _json_body(model: type[BaseModel]) -> dict[str, Any]:
    schema = {"application/json": {"schema": model.model_json_schema()}}
    return {"requestBody": {"required": True, "content": schema}}


async def forward(
    request: Request, method: str, path: str, body: bytes | None, session: str, slug: str
) -> Response:
    """Send one call to the chat upstream and hand its status and JSON body back unchanged."""
    settings = request.app.state.settings
    target = forward_core.Target(
        "chat relay",
        settings.chat_url.rstrip("/") + UPSTREAM_PREFIX,
        settings.chat_token,
        STATE_ATTR,
        "chat_unavailable",
        TIMEOUT_SECONDS,
    )
    status, content = await forward_core.forward(request, target, method, path, body, session, slug)
    return Response(content, status, media_type="application/json")


async def _relay(
    request: Request, agent: Caller, pool: Pool, settings: Cfg, method: str, path: str
) -> Response:
    async with pool.connection() as conn:
        own = await _session(conn, agent)
        if own.trigger_created:
            raise ApiError(
                403, "trigger_session", "sessions started by a trigger service cannot use the chat"
            )
        if not await _leads(conn, agent, own):
            raise ApiError(403, "not_leader", "only the leader of a session can use the chat")
    if not settings.chat_available:
        raise ApiError(503, "chat_unconfigured", "no chat upstream is configured")
    body = await forward_core.capped_body(request) if method == "POST" else None
    return await forward(request, method, path, body, str(own.id), own.slug)


@router.get(
    "/rooms",
    summary="Rooms the caller's session may post to (leader only)",
    responses={200: _PASS_THROUGH, **_RESPONSES},
)
async def rooms(request: Request, agent: Caller, pool: Pool, settings: Cfg) -> Response:
    return await _relay(request, agent, pool, settings, "GET", "/rooms")


@router.post(
    "/messages",
    status_code=202,
    summary="Send a message to the chat (leader only)",
    responses={202: _PASS_THROUGH, **_POST_RESPONSES},
    openapi_extra=_json_body(ChatMessageIn),
)
async def send_message(request: Request, agent: Caller, pool: Pool, settings: Cfg) -> Response:
    return await _relay(request, agent, pool, settings, "POST", "/messages")


@router.post(
    "/reactions",
    status_code=202,
    summary="React to a chat message (leader only)",
    responses={202: _PASS_THROUGH, **_POST_RESPONSES},
    openapi_extra=_json_body(ChatReactionIn),
)
async def send_reaction(request: Request, agent: Caller, pool: Pool, settings: Cfg) -> Response:
    return await _relay(request, agent, pool, settings, "POST", "/reactions")


@router.get(
    "/messages/{send_id}",
    summary="Delivery status of a send of the caller's session (leader only)",
    responses={200: _PASS_THROUGH, **_RESPONSES},
)
async def message_status(
    send_id: str, request: Request, agent: Caller, pool: Pool, settings: Cfg
) -> Response:
    # Dots are encoded too, so `.` and `..` stay one segment instead of walking up the path.
    segment = quote(send_id, safe="").replace(".", "%2E")
    return await _relay(request, agent, pool, settings, "GET", f"/messages/{segment}")

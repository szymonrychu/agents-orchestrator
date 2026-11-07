"""Chat relay (`/agent/v1/chat`), served only on the agent port (design D2, D5, D12).

A session leader reaches the user's chat through the orchestrator: every route runs
`require_agent`, then the leader check of `session_send` (403 `not_leader`), then requires the
chat upstream configured (503 `chat_unconfigured`), then caps the body at 256 KiB (413
`payload_too_large`) and forwards the call to `{ORCH_CHAT_URL}/chat/v1/...`. The forwarded request
carries only the chat bearer (`ORCH_CHAT_TOKEN`), `X-Orch-Session` and `X-Orch-Slug` of the caller
(the slug read from the sessions repo), and for a POST the body and `Content-Type:
application/json`; no inbound header and never the agent token. The upstream status and JSON body
come back unchanged, except that the `occupier` field of a 409 `room_bound` never reaches the
agent, and `POST /messages` answered 409 `room_bound` with an occupier is relayed through the
occupier's session (`chat_relay`, 202 `relayed`); a connection error, a timeout or a non-JSON
answer is 503 `chat_unavailable`.
The orchestrator does not validate the bodies (the upstream does) and stores nothing. Log lines
carry the method, path and status only, never a token or a body.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

import httpx
from fastapi import APIRouter, FastAPI, Request, Response
from pydantic import BaseModel, ConfigDict

from agents_orchestrator.api import chat_relay
from agents_orchestrator.api import forward as forward_core
from agents_orchestrator.api.agent import Caller, _leads, _session
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.sessions import Cfg, Pool
from agents_orchestrator.settings import Settings

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
    403: {"description": "forbidden (not a live agent pod) or not_leader"},
    503: {"description": "chat_unconfigured or chat_unavailable"},
}
_POST_RESPONSES: dict[int | str, dict[str, Any]] = {
    **_RESPONSES,
    413: {"description": "payload_too_large: the body exceeds 256 KiB"},
}


def _json_body(model: type[BaseModel]) -> dict[str, Any]:
    schema = {"application/json": {"schema": model.model_json_schema()}}
    return {"requestBody": {"required": True, "content": schema}}


def _target(settings: Settings) -> forward_core.Target:
    return forward_core.Target(
        "chat relay",
        settings.chat_url.rstrip("/") + UPSTREAM_PREFIX,
        settings.chat_token,
        STATE_ATTR,
        "chat_unavailable",
        TIMEOUT_SECONDS,
    )


async def _relay(
    request: Request,
    agent: Caller,
    pool: Pool,
    settings: Cfg,
    method: str,
    path: str,
    *,
    counts_as_update: bool = False,
    relay_bound: bool = False,
) -> Response:
    async with pool.connection() as conn:
        own = await _session(conn, agent)
        if not await _leads(conn, agent, own):
            raise ApiError(403, "not_leader", "only the leader of a session can use the chat")
    if not settings.chat_available:
        raise ApiError(503, "chat_unconfigured", "no chat upstream is configured")
    body = await forward_core.capped_body(request) if method == "POST" else None
    target = _target(settings)
    status, content = await forward_core.forward(
        request, target, method, path, body, str(own.id), own.slug
    )
    occupier = chat_relay.occupier_of(status, content) if relay_bound else None
    if occupier is not None:
        assert body is not None
        response = await chat_relay.relay(
            request, pool, settings, target, agent.id, own, body, occupier, content
        )
    else:
        response = Response(
            chat_relay.without_occupier(content), status, media_type="application/json"
        )
    if counts_as_update:
        await forward_core.touch_on_success(pool, own.id, response.status_code)
    return response


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
    responses={
        202: {
            "description": "The chat upstream's status and JSON body, unchanged, or ChatRelayed"
            " when the room is held by another session and the post was relayed through it",
            "content": {"application/json": {"schema": chat_relay.ChatRelayed.model_json_schema()}},
        },
        **_POST_RESPONSES,
    },
    openapi_extra=_json_body(ChatMessageIn),
)
async def send_message(request: Request, agent: Caller, pool: Pool, settings: Cfg) -> Response:
    return await _relay(
        request, agent, pool, settings, "POST", "/messages", counts_as_update=True, relay_bound=True
    )


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

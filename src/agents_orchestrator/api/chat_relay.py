"""The chat relay: a post refused because another session holds the room goes out in its name.

The connector answers 409 `room_bound` with the `occupier` (the session bound to a room agents
may start a chat in) to a session that is not the occupier. `chat._relay` hands that case to
`relay`: the text is posted as the occupier with the sender's slug as prefix, a relay notice is
queued to the occupier under the session-message limits, and the sender gets 202 `relayed`.
Every other answer goes back without any `occupier` field: agents never see the occupier id.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Literal

from fastapi import Request, Response
from pydantic import BaseModel

from agents_orchestrator.api import forward as forward_core
from agents_orchestrator.api import session_limits
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.sessions import Pool
from agents_orchestrator.db import session_messaging
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.db.sessions import Session
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)

MAX_TEXT_CHARS = 16000  # the connector's cap, relay prefix included
CLOSING_RETRY_SECONDS = 60


class ChatRelayed(BaseModel):
    """The 202 answer of a relayed post. `message_id` is None only when the notice was lost
    after the text was posted."""

    status: Literal["relayed"]
    via: str
    send_id: str
    message_id: int | None


def _error_object(content: bytes) -> dict[str, Any] | None:
    try:
        body = json.loads(content)
    except ValueError:
        return None
    error = body.get("error") if isinstance(body, dict) else None
    return error if isinstance(error, dict) else None


def occupier_of(status: int, content: bytes) -> uuid.UUID | None:
    """The occupier of a 409 `room_bound` answer, None for any other answer."""
    if status != 409:
        return None
    error = _error_object(content)
    if error is None or error.get("code") != "room_bound":
        return None
    value = error.get("occupier")
    if not isinstance(value, str):
        return None
    try:
        return uuid.UUID(value)
    except ValueError:
        return None


def without_occupier(content: bytes) -> bytes:
    """The same bytes when the error object has no `occupier`, else the JSON without it."""
    error = _error_object(content)
    if error is None or "occupier" not in error:
        return content
    body = json.loads(content)
    del body["error"]["occupier"]
    return json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode()


def _plain(status: int, content: bytes) -> Response:
    return Response(without_occupier(content), status, media_type="application/json")


def _parse(body: bytes) -> tuple[str, str] | None:
    """(text, room) of a relayable body, None when it cannot be relayed (`reply_to` present,
    `room` missing or any field of the wrong type)."""
    try:
        data = json.loads(body)
    except ValueError:
        return None
    if not isinstance(data, dict) or data.get("reply_to") is not None:
        return None
    text, room = data.get("text"), data.get("room")
    if not isinstance(text, str) or not isinstance(room, str) or not room:
        return None
    return text, room


async def relay(
    request: Request,
    pool: Pool,
    settings: Settings,
    target: forward_core.Target,
    agent_id: uuid.UUID,
    own: Session,
    body: bytes,
    occupier_id: uuid.UUID,
    refusal: bytes,
) -> Response:
    """Post `body` as the occupier and queue it a relay notice; see the module docstring.

    All checks run before any write, so on a refusal nothing is posted or queued. `refusal` is
    the connector's 409 answer, passed back (without the occupier) when the post is not relayable.
    """
    parsed = _parse(body)
    if parsed is None:
        return _plain(409, refusal)
    text, room = parsed
    prefixed = f"[{own.slug}] {text}"
    if len(prefixed) > MAX_TEXT_CHARS:
        raise ApiError(
            422,
            "invalid_request",
            f"text: at most {MAX_TEXT_CHARS} characters including the relay prefix",
        )
    async with pool.connection() as conn:
        occupier = await sessions_repo.get(conn, occupier_id)
        if occupier is None:
            return _plain(409, refusal)
        if occupier.desired == "deleting":
            raise ApiError(
                409,
                "occupier_closing",
                "the session holding the room is closing; retry after "
                f"{CLOSING_RETRY_SECONDS} seconds",
                retry_after=CLOSING_RETRY_SECONDS,
            )
        hop = await session_limits.check_limits(conn, agent_id, own, occupier, settings)
    payload = json.dumps({"text": prefixed, "room": room}).encode()
    status, content = await forward_core.forward(
        request, target, "POST", "/messages", payload, str(occupier.id), occupier.slug
    )
    if not 200 <= status < 300:
        return _plain(status, content)
    send_id = json.loads(content)["id"]
    message_id: int | None = None
    try:
        async with pool.connection() as conn:
            message_id = await session_messaging.enqueue_relay_notice(
                conn,
                occupier.id,
                text,
                sender_session_id=own.id,
                sender_slug=own.slug,
                hop=hop,
                send_id=send_id,
                room_id=room,
            )
    except Exception:  # SessionBeingDeleted or a DB error: the text is already in the chat
        log.warning(
            "chat relay notice lost from=%s to=%s send=%s", own.slug, occupier.slug, send_id
        )
    log.info(
        "chat relay from=%s to=%s room=%s send=%s status=202",
        own.slug,
        occupier.slug,
        room,
        send_id,
    )
    answer = ChatRelayed(
        status="relayed", via=occupier.slug, send_id=send_id, message_id=message_id
    )
    return Response(answer.model_dump_json(), 202, media_type="application/json")

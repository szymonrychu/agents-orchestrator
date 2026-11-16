"""The chat upstream's room notes for the orchestrator itself (the handoff package).

The same upstream, bearer and identity headers as the agent relay in `api/chat.py`, without a
request: a short read with the caller's `X-Orch-Session` and `X-Orch-Slug`. Failures raise
`ChatUnavailable` with a status or exception type only, never a body or a token.
"""

from __future__ import annotations

import uuid

import httpx

from agents_orchestrator.settings import Settings

UPSTREAM_PREFIX = "/chat/v1"
TIMEOUT_SECONDS = 3.0


class ChatUnavailable(Exception):
    """The chat upstream could not be read (no body, no token in the message)."""


async def room_notes(
    settings: Settings,
    session_id: uuid.UUID,
    slug: str,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> str:
    """The notes body of the room the session is bound to (`GET /notes`, empty when none)."""
    if not settings.chat_available:
        raise ChatUnavailable("not configured")
    url = settings.chat_url.rstrip("/") + UPSTREAM_PREFIX + "/notes"
    headers = {
        "Authorization": f"Bearer {settings.chat_token}",
        "X-Orch-Session": str(session_id),
        "X-Orch-Slug": slug,
    }
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS, transport=transport) as http:
            response = await http.get(url, headers=headers)
        if not response.is_success:
            raise ChatUnavailable(f"status {response.status_code}")
        return str(response.json()["body"])
    except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
        raise ChatUnavailable(type(exc).__name__) from None

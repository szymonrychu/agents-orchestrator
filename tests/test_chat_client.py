"""The chat upstream's room notes read by the orchestrator itself (the handoff package)."""

from __future__ import annotations

import uuid

import httpx
import pytest
from fakes.fake_chat import FakeChat

from agents_orchestrator.chat_client import ChatUnavailable, room_notes
from agents_orchestrator.settings import Settings

SESSION = uuid.uuid4()
TOKEN = "CANARY-chat-token"


def settings(**kw) -> Settings:
    values = {
        "database_url": "postgresql://unused/db",
        "master_key": "k" * 32,
        "chat_url": "http://chat",
        "chat_token": TOKEN,
    }
    return Settings(**{**values, **kw})


async def test_notes_body_with_the_session_identity():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"body": "room memory", "version": 3})

    body = await room_notes(settings(), SESSION, "slug", transport=httpx.MockTransport(handler))
    assert body == "room memory"
    (request,) = seen
    assert request.url.path == "/chat/v1/notes" and request.method == "GET"
    assert request.headers["authorization"] == f"Bearer {TOKEN}"
    assert request.headers["x-orch-session"] == str(SESSION)
    assert request.headers["x-orch-slug"] == "slug"


async def test_notes_body_is_the_rendered_sections_of_the_chat():
    """The handoff package reads the whole `body` of a sectioned GET /notes (no `section`)."""
    chat = FakeChat(TOKEN)
    transport = httpx.ASGITransport(app=chat.app)
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "X-Orch-Session": str(SESSION),
        "X-Orch-Slug": "slug",
    }
    async with httpx.AsyncClient(transport=transport, base_url="http://chat") as http:
        for key, text in (("preferences", "short answers"), ("requests", "ship friday")):
            put = {"body": text, "expected_version": 0}
            r = await http.put(f"/chat/v1/notes/sections/{key}", json=put, headers=headers)
            assert r.status_code == 200, r.text
    body = await room_notes(settings(), SESSION, "slug", transport=transport)
    assert body == "## Preferences\nshort answers\n\n## Requests\nship friday"


@pytest.mark.parametrize("status", [401, 409, 503])
async def test_failure_raises_without_body_or_token(status):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json={"error": {"message": TOKEN}})

    with pytest.raises(ChatUnavailable) as err:
        await room_notes(settings(), SESSION, "slug", transport=httpx.MockTransport(handler))
    assert TOKEN not in str(err.value)


async def test_unconfigured_and_connect_error_raise():
    with pytest.raises(ChatUnavailable):
        await room_notes(settings(chat_url="", chat_token=""), SESSION, "slug")

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(ChatUnavailable):
        await room_notes(settings(), SESSION, "slug", transport=httpx.MockTransport(handler))

"""Contract between the orchestrator's chat relay and the connector's agent listener (design D16).

`tests/fakes/chat_openapi.json` is the connector's committed `openapi-agent.json`, vendored at the
commit named in `tests/fakes/chat_openapi.source`. `FakeChat` serves the `/chat/v1` upstream and
checks every request and every answer against that document, so the relay's idea of the upstream
cannot drift from what the connector really serves. No case here needs a database or Docker.
"""

from __future__ import annotations

import re
import uuid
from typing import Any

import httpx
import pytest
from fakes import fake_chat
from fakes.fake_chat import FakeChat, request_errors, response_errors
from test_agent_chat import CHAT_TOKEN, make_rig, settings

from agents_orchestrator.api import chat
from agents_orchestrator.api.app import create_app

SESSION = str(uuid.uuid4())
GOOD = {"Authorization": f"Bearer {CHAT_TOKEN}", "X-Orch-Session": SESSION, "X-Orch-Slug": "s"}
RELAY_PREFIX = "/agent/v1/chat"


def upstream_client(fake: FakeChat) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=fake.app), base_url="http://chat")


def concrete(template: str) -> str:
    return template.replace("{id}", "s1").replace("{name}", "demo")


def normalise(path: str) -> str:
    """A path with every `{param}` replaced by `{}` so parameter names do not matter."""
    return re.sub(r"\{[^}]+\}", "{}", path)


# ------------------------------------------------------------------------------ the document


def test_source_line_names_a_full_sha():
    text = fake_chat.SOURCE_PATH.read_text(encoding="utf-8")
    assert re.fullmatch(r"agents-matrix-connector [0-9a-f]{40}\n", text), text


def test_helpers_reject_what_the_document_rejects():
    post = ("POST", "/chat/v1/messages")
    assert request_errors(*post, {"text": "hi", "room": "!r:hs", "reply_to": "m1"}) == []
    assert request_errors(*post, {"text": ""})  # minLength 1
    assert request_errors(*post, {"text": "x" * 16001})  # maxLength 16000
    assert request_errors(*post, {"text": "hi", "extra": 1})  # additionalProperties false
    assert request_errors(*post, {})  # text required
    assert request_errors("POST", "/chat/v1/reactions", {"to": "m1"})  # key required
    accepted = ("POST", "/chat/v1/messages", 202)
    assert response_errors(*accepted, {"id": "s1", "status": "queued"}) == []
    assert response_errors(*accepted, {"id": 1, "status": "queued"})
    assert response_errors(*accepted, {"id": "s1", "status": "refused"})  # not in the enum
    assert response_errors("POST", "/chat/v1/messages", 418, {"id": "s1", "status": "queued"})
    assert response_errors("POST", "/chat/v1/messages", 403, {"error": {"code": "x"}})  # no message


# -------------------------------------------------------------------------------- ac2: routes


def test_relay_routes_exist_upstream():
    documented = {
        (method.upper(), normalise(path))
        for path, item in fake_chat.DOC["paths"].items()
        for method in item
    }
    app = create_app(settings(), pool=None)  # the real app, not just the router
    relayed = [
        (method.upper(), path)
        for path, item in app.openapi()["paths"].items()
        if path.startswith(RELAY_PREFIX)
        for method in item
    ]
    assert len(relayed) == 10, relayed  # rooms, messages, reactions, status, notes, skills
    assert {(method, path) for method, path in relayed} == {
        (method, f"{RELAY_PREFIX}{p}")
        for method, p in (
            ("GET", "/rooms"),
            ("POST", "/messages"),
            ("POST", "/reactions"),
            ("GET", "/messages/{send_id}"),
            ("GET", "/notes"),
            ("PUT", "/notes"),
            ("GET", "/skills"),
            ("GET", "/skills/{name}"),
            ("PUT", "/skills/{name}"),
            ("DELETE", "/skills/{name}"),
        )
    }
    for method, path in relayed:
        upstream = chat.UPSTREAM_PREFIX + path.removeprefix(RELAY_PREFIX)
        assert (method, normalise(upstream)) in documented, (method, upstream)


# --------------------------------------------------------------------------- ac3: fake answers


def documented_statuses() -> dict[tuple[str, str], list[int]]:
    return {
        key: sorted(int(status) for status in op["responses"])
        for key, op in fake_chat.operations().items()
    }


async def test_fake_chat_matches_vendored_schema():
    fake = FakeChat(CHAT_TOKEN)
    seen: set[tuple[str, str, int]] = set()

    async def call(
        method: str, template: str, path: str | None = None, **kw: Any
    ) -> httpx.Response:
        kw.setdefault("headers", GOOD)
        async with upstream_client(fake) as http:
            r = await http.request(method, path or concrete(template), **kw)
        seen.add((method, template, r.status_code))
        # the fake validated this answer itself; check it again from the outside
        assert response_errors(method, template, r.status_code, r.json()) == [], r.text
        return r

    valid = {
        ("POST", "/chat/v1/messages"): {"text": "hello", "room": "!dm:hs"},
        ("POST", "/chat/v1/reactions"): {"to": "m3", "key": "\U0001f44d"},
        ("PUT", "/chat/v1/notes"): {"body": "likes tea", "expected_version": 0},
        ("PUT", "/chat/v1/skills/{name}"): {"description": "d", "body": "b", "expected_version": 0},
    }
    notes_put = valid[("PUT", "/chat/v1/notes")]
    skill_put = valid[("PUT", "/chat/v1/skills/{name}")]
    # success answers
    assert (await call("GET", "/chat/v1/rooms")).status_code == 200
    sent = await call("POST", "/chat/v1/messages", json=valid[("POST", "/chat/v1/messages")])
    assert sent.status_code == 202
    reply = {"text": "re", "reply_to": "m3"}
    assert (await call("POST", "/chat/v1/messages", json=reply)).status_code == 202
    reaction = valid[("POST", "/chat/v1/reactions")]
    assert (await call("POST", "/chat/v1/reactions", json=reaction)).is_success
    unknown = await call("GET", "/chat/v1/messages/{id}", "/chat/v1/messages/s999")
    assert unknown.status_code == 404
    own = await call("GET", "/chat/v1/messages/{id}", f"/chat/v1/messages/{sent.json()['id']}")
    assert own.status_code == 200
    assert own.json()["id"] == sent.json()["id"] and own.json()["kind"] == "message"

    # the room memory routes: notes and skills round trip through their versions
    assert (await call("GET", "/chat/v1/notes")).json()["version"] == 0
    saved = await call("PUT", "/chat/v1/notes", json=notes_put)
    assert saved.status_code == 200
    assert (saved.json()["version"], saved.json()["body"]) == (1, "likes tea")
    assert (await call("PUT", "/chat/v1/notes", json=notes_put)).status_code == 409
    big = {**notes_put, "expected_version": 1, "body": "x" * 8001}
    assert (await call("PUT", "/chat/v1/notes", json=big)).status_code == 413
    assert (await call("GET", "/chat/v1/skills")).json()["skills"] == []
    made = await call("PUT", "/chat/v1/skills/{name}", json=skill_put)
    assert (made.status_code, made.json()["version"]) == (200, 1)
    assert (await call("PUT", "/chat/v1/skills/{name}", json=skill_put)).status_code == 409
    listed = (await call("GET", "/chat/v1/skills")).json()["skills"]
    assert [s["name"] for s in listed] == ["demo"] and "body" not in listed[0]
    assert (await call("GET", "/chat/v1/skills/{name}")).json()["body"] == "b"
    assert (await call("DELETE", "/chat/v1/skills/{name}")).json()["deleted"] is True
    assert (await call("DELETE", "/chat/v1/skills/{name}")).status_code == 404
    bad = "/chat/v1/skills/Bad_Name"
    bad_name = await call("PUT", "/chat/v1/skills/{name}", bad, json=skill_put)
    assert bad_name.status_code == 422
    for n in range(20):
        path = f"/chat/v1/skills/s{n}"
        assert (await call("PUT", "/chat/v1/skills/{name}", path, json=skill_put)).is_success
    over = await call("PUT", "/chat/v1/skills/{name}", "/chat/v1/skills/s20", json=skill_put)
    assert (over.status_code, over.json()["error"]["code"]) == (409, "skill_limit")

    # error answers the fake produces by itself: bad token, bad session header, bad body
    bad_token = {**GOOD, "Authorization": "Bearer nope"}
    for method, template in fake_chat.operations():
        assert (await call(method, template, headers=bad_token)).status_code == 401
        no_session = {k: v for k, v in GOOD.items() if k != "X-Orch-Session"}
        assert (await call(method, template, headers=no_session)).status_code == 401
        garbled = {**GOOD, "X-Orch-Session": "not-a-uuid"}
        assert (await call(method, template, headers=garbled)).status_code == 401
    for method, template in valid:
        for body in ({}, {**valid[(method, template)], "surplus": 1}):
            r = await call(method, template, json=body)
            assert r.status_code == 422
        r = await call(method, template, content=b"{not json")
        assert r.status_code == 422

    # every other documented status, on request, in the schema the document gives it
    for (method, template), statuses in documented_statuses().items():
        for status_code in statuses:
            if (method, template, status_code) in seen:
                continue
            fake.refuse(f"{method} {template}", status_code, "refused_by_test")
            r = await call(method, template, json=valid.get((method, template)))
            assert r.status_code == status_code
            fake.allow(f"{method} {template}")

    expected = {
        (method, template, status_code)
        for (method, template), statuses in documented_statuses().items()
        for status_code in statuses
    }
    assert seen == expected  # every documented answer of every route was produced and checked


async def test_fake_chat_refuses_what_the_document_does_not_allow():
    fake = FakeChat(CHAT_TOKEN)
    with pytest.raises(ValueError):
        fake.refuse("POST /chat/v1/messages", 418, "teapot")  # status not documented
    with pytest.raises(ValueError):
        fake.refuse("POST /chat/v1/nowhere", 403, "x")  # route not documented
    with pytest.raises(ValueError):
        fake.refuse("POST /chat/v1/messages", 202, "x")  # not an error answer
    async with upstream_client(fake) as http:
        with pytest.raises(AssertionError):
            await http.get("/chat/v1/undocumented", headers=GOOD)
    assert [(c.method, c.path) for c in fake.calls] == [("GET", "/chat/v1/undocumented")]


async def test_fake_chat_records_calls():
    fake = FakeChat(CHAT_TOKEN)
    async with upstream_client(fake) as http:
        await http.post("/chat/v1/messages", json={"text": "hi", "room": "!dm:hs"}, headers=GOOD)
    (call,) = fake.calls
    assert call.method == "POST" and call.path == "/chat/v1/messages"
    assert call.headers["authorization"] == f"Bearer {CHAT_TOKEN}"
    assert call.headers["x-orch-session"] == SESSION
    assert call.json() == {"text": "hi", "room": "!dm:hs"}


# ----------------------------------------------------------------- ac4: what the relay forwards


async def test_relay_requests_match_vendored_schema(monkeypatch):
    rig = make_rig(monkeypatch)
    fake = FakeChat(CHAT_TOKEN)
    rig.app.state.chat_http = upstream_client(fake)
    base = RELAY_PREFIX

    rooms = await rig.call("GET", f"{base}/rooms")
    assert rooms.status_code == 200, rooms.text
    sent = await rig.call("POST", f"{base}/messages", json={"text": "hello", "room": "!dm:hs"})
    assert sent.status_code == 202, sent.text
    replied = await rig.call(
        "POST", f"{base}/messages", json={"text": "quoted", "reply_to": "m3", "room": "!dm:hs"}
    )
    assert replied.status_code == 202, replied.text
    reacted = await rig.call("POST", f"{base}/reactions", json={"to": "m3", "key": "\U0001f44d"})
    assert reacted.status_code == 202, reacted.text
    status = await rig.call("GET", f"{base}/messages/{sent.json()['id']}")
    assert status.status_code == 200, status.text
    assert status.json()["id"] == sent.json()["id"]

    assert [(c.method, c.path) for c in fake.calls] == [
        ("GET", "/chat/v1/rooms"),
        ("POST", "/chat/v1/messages"),
        ("POST", "/chat/v1/messages"),
        ("POST", "/chat/v1/reactions"),
        ("GET", f"/chat/v1/messages/{sent.json()['id']}"),
    ]
    templates = {
        "/chat/v1/rooms": "/chat/v1/rooms",
        "/chat/v1/messages": "/chat/v1/messages",
        "/chat/v1/reactions": "/chat/v1/reactions",
    }
    for call in fake.calls:
        template = templates.get(call.path, "/chat/v1/messages/{id}")
        assert (call.method, template) in fake_chat.operations(), call
        if call.method == "POST":
            assert request_errors(call.method, template, call.json()) == [], call.body
            assert call.headers["content-type"] == "application/json"
        else:
            assert call.body == b""
        assert call.headers["authorization"] == f"Bearer {CHAT_TOKEN}"
        assert call.headers["x-orch-session"] == str(rig.session.id)
        assert call.headers["x-orch-slug"] == "chat-caller"


async def test_relay_passes_a_refusal_of_the_fake_through(monkeypatch):
    rig = make_rig(monkeypatch)
    fake = FakeChat(CHAT_TOKEN)
    fake.refuse("POST /chat/v1/messages", 403, "room_not_allowed", "room !x:hs is not allowed")
    rig.app.state.chat_http = upstream_client(fake)
    r = await rig.call("POST", f"{RELAY_PREFIX}/messages", json={"text": "hi", "room": "!x:hs"})
    assert r.status_code == 403
    assert r.json() == {
        "error": {"code": "room_not_allowed", "message": "room !x:hs is not allowed"}
    }
    # a body the schema rejects comes back as the fake's 422, byte for byte through the relay
    fake.allow("POST /chat/v1/messages")
    r = await rig.call("POST", f"{RELAY_PREFIX}/messages", json={"text": "hi", "bogus": True})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_request"

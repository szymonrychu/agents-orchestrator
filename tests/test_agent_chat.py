"""Chat relay agent routes (design D2, D5, D12): `/agent/v1/chat/*` forwarded to the chat upstream.

The non-DB cases run the real `require_agent` and `_leads` against an in-memory stand-in for the
three repository reads they make, and an `httpx.MockTransport` upstream, so they run without
Docker. The DB-backed cases at the end prove the same identity and leadership against Postgres.
"""

from __future__ import annotations

import asyncio
import json
import re
import secrets
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
from any_mcp.testing import PluginHarness
from any_mcp_tatara_orchestrator import OrchestratorPlugin
from fakes.fake_chat import DEFAULT_ROOM, FakeChat
from fastapi.routing import APIRoute
from test_api_sessions import assert_error

from agents_orchestrator.api import chat
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import leadership
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec
from agents_orchestrator.tokens import derive_tokens

README = (Path(__file__).resolve().parent.parent / "README.md").read_text(encoding="utf-8")
KEY = secrets.token_hex(16)
AGENT_PORT, MAIN_PORT, SERVICE_PORT = 8001, 8000, 8002
CHAT_URL = "http://chat.test:9001"
LEADER_IP, MEMBER_IP = "10.9.0.10", "10.9.0.11"
CHAT_TOKEN = "chat-" + secrets.token_hex(16)

ROUTES = (
    ("GET", "/agent/v1/chat/rooms", "/chat/v1/rooms"),
    ("POST", "/agent/v1/chat/messages", "/chat/v1/messages"),
    ("POST", "/agent/v1/chat/reactions", "/chat/v1/reactions"),
    ("GET", "/agent/v1/chat/messages/s7", "/chat/v1/messages/s7"),
)
BODIES = {
    "/agent/v1/chat/messages": {"text": "hello", "room": "!r:hs", "reply_to": "m3"},
    "/agent/v1/chat/reactions": {"to": "m3", "key": "\U0001f44d"},
}


def settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "database_url": "postgresql://unused/db",
        "allowed_cidrs": "10.1.2.0/24",
        "master_key": KEY,
        "http_port": MAIN_PORT,
        "agent_port": AGENT_PORT,
        "service_port": SERVICE_PORT,
        "chat_url": CHAT_URL,
        "chat_token": CHAT_TOKEN,
    }
    values.update(overrides)
    return Settings(**values)


def agent_token(session_id: uuid.UUID) -> str:
    return derive_tokens(KEY.encode(), session_id).agent


# ------------------------------------------------------------------------- in-memory identity


class _Conn:
    async def __aenter__(self) -> _Conn:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


@dataclass
class FakeDb:
    """Stands in for the pool and the repository reads of `require_agent`, `_session` and
    `_leads`: `sessions_repo.get`, `agents_repo.by_pod_ip` and `leadership.current_leader`."""

    sessions: dict[uuid.UUID, sessions_repo.Session] = field(default_factory=dict)
    agents: dict[tuple[uuid.UUID, str], agents_repo.Agent] = field(default_factory=dict)
    leaders: dict[uuid.UUID, leadership.Leader] = field(default_factory=dict)

    def connection(self, **_: Any) -> _Conn:
        return _Conn()

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def get(_conn: Any, session_id: uuid.UUID) -> sessions_repo.Session | None:
            return self.sessions.get(session_id)

        async def by_pod_ip(_conn: Any, session_id: uuid.UUID, ip: str) -> agents_repo.Agent | None:
            return self.agents.get((session_id, ip))

        async def current_leader(_conn: Any, session_id: uuid.UUID) -> leadership.Leader | None:
            return self.leaders.get(session_id)

        monkeypatch.setattr(sessions_repo, "get", get)
        monkeypatch.setattr(agents_repo, "by_pod_ip", by_pod_ip)
        monkeypatch.setattr(leadership, "current_leader", current_leader)

    def add_agent(self, session_id: uuid.UUID, ordinal: int, ip: str) -> agents_repo.Agent:
        agent = agents_repo.Agent(
            id=uuid.uuid4(),
            session_id=session_id,
            ordinal=ordinal,
            pod_name=f"pod-{ordinal}",
            url=None,
            pod_phase="Running",
            ready=True,
            wrapper_session_id=None,
            wrapper_session_created_at=None,
            gone_at=None,
            last_dispatched_at=None,
            pod_ip=ip,
        )
        self.agents[(session_id, ip)] = agent
        return agent

    def add_session(
        self,
        slug: str,
        *,
        hive: bool,
        leader_ip: str = LEADER_IP,
        created_by_service: str | None = None,
    ) -> sessions_repo.Session:
        spec = {"replicas": 2, "max_replicas": 2} if hive else {"replicas": 1}
        session = sessions_repo.Session(
            id=uuid.uuid4(),
            slug=slug,
            spec=SessionSpec.model_validate(spec),
            generation=1,
            observed_generation=1,
            desired="active",
            status={},
            created_at=datetime.now(UTC),
            created_by_service=created_by_service,
        )
        self.sessions[session.id] = session
        leader = self.add_agent(session.id, 0, leader_ip)
        if hive:
            self.add_agent(session.id, 1, MEMBER_IP)
            self.leaders[session.id] = leadership.Leader(leader.id, leader.pod_name, 1)
        return session


@dataclass
class Upstream:
    """The chat upstream: records every request and answers with `reply`."""

    reply: Any = None
    seen: list[httpx.Request] = field(default_factory=list)

    async def handler(self, request: httpx.Request) -> httpx.Response:
        self.seen.append(request)
        if self.reply is None:
            return httpx.Response(200, json={"ok": True})
        if callable(self.reply):
            return await self.reply(request)
        return self.reply


@dataclass
class Rig:
    db: FakeDb
    upstream: Upstream
    app: Any
    session: sessions_repo.Session

    def client(self, ip: str = LEADER_IP, port: int = AGENT_PORT) -> httpx.AsyncClient:
        transport = httpx.ASGITransport(app=self.app, client=(ip, 4321))
        return httpx.AsyncClient(transport=transport, base_url=f"http://orch:{port}")

    def headers(self, session_id: uuid.UUID | None = None, tok: str | None = None) -> dict:
        sid = session_id or self.session.id
        return {
            "X-Orch-Session": str(sid),
            "Authorization": f"Bearer {tok if tok is not None else agent_token(sid)}",
        }

    async def call(
        self, method: str, path: str, *, ip: str = LEADER_IP, port: int = AGENT_PORT, **kw: Any
    ) -> httpx.Response:
        kw.setdefault("headers", self.headers())
        if method == "POST" and "json" not in kw and "content" not in kw:
            kw["json"] = BODIES[path.split("?")[0]]
        async with self.client(ip, port) as http:
            return await http.request(method, path, **kw)


def make_rig(
    monkeypatch: pytest.MonkeyPatch,
    *,
    hive: bool = True,
    created_by_service: str | None = None,
    **overrides: Any,
) -> Rig:
    db = FakeDb()
    db.install(monkeypatch)
    session = db.add_session("chat-caller", hive=hive, created_by_service=created_by_service)
    upstream = Upstream()
    app = create_app(settings(**overrides), pool=None)
    app.state.pool = db  # the lifespan never runs under ASGITransport, so this pool stays
    app.state.chat_http = httpx.AsyncClient(transport=httpx.MockTransport(upstream.handler))
    return Rig(db, upstream, app, session)


# -------------------------------------------------------------------------------- forwarding


async def test_any_creator_may_use_chat(monkeypatch):
    """The creator does not matter: every kind of session keeps the relay."""
    for service in (None, "agents-webhook", "agents-schedule", "agents-matrix-connector"):
        rig = make_rig(monkeypatch, created_by_service=service)
        for method, path, upstream_path in ROUTES:
            rig.upstream.seen.clear()
            r = await rig.call(method, path)
            assert r.status_code == 200, (service, path, r.text)
            assert rig.upstream.seen[0].url.path == upstream_path


async def test_forwards_with_chat_token_never_agent_token(monkeypatch):
    rig = make_rig(monkeypatch)
    inbound = {
        **rig.headers(),
        "Cookie": "c=1",
        "User-Agent": "inbound-agent/1",
        "X-Forwarded-For": "1.2.3.4",
        "X-Orch-Slug": "forged-slug",
        "X-Extra": "x",
        "Content-Type": "text/plain",
    }
    secret = agent_token(rig.session.id)
    for method, path, upstream_path in ROUTES:
        rig.upstream.seen.clear()
        kw: dict[str, Any] = {"headers": inbound}
        if method == "POST":
            kw["content"] = json.dumps(BODIES[path]).encode()
        r = await rig.call(method, path + "?a=1&b=x%20y", **kw)
        assert r.status_code == 200, r.text
        (sent,) = rig.upstream.seen
        assert sent.method == method
        assert sent.url.scheme == "http" and sent.url.host == "chat.test"
        assert sent.url.port == 9001
        assert sent.url.raw_path == (upstream_path + "?a=1&b=x%20y").encode()
        assert sent.headers["authorization"] == f"Bearer {CHAT_TOKEN}"
        assert sent.headers["x-orch-session"] == str(rig.session.id)
        assert sent.headers["x-orch-slug"] == "chat-caller"
        for name, value in sent.headers.items():
            assert secret not in value, name
        names = {k.lower() for k in sent.headers}
        assert not names & {"cookie", "x-forwarded-for", "x-extra"}
        assert sent.headers.get("user-agent") != "inbound-agent/1"
        if method == "POST":
            assert sent.content == json.dumps(BODIES[path]).encode()
            assert sent.headers["content-type"] == "application/json"
        else:
            assert sent.content == b""
            assert "content-type" not in names


async def test_forwards_dot_segment_id_inside_messages_path(monkeypatch):
    rig = make_rig(monkeypatch)
    for raw, encoded in (("%2E%2E", "%2E%2E"), ("%2E", "%2E"), ("a%20b", "a%20b")):
        rig.upstream.seen.clear()
        r = await rig.call("GET", f"/agent/v1/chat/messages/{raw}")
        assert r.status_code == 200, r.text
        assert rig.upstream.seen[0].url.raw_path == f"/chat/v1/messages/{encoded}".encode()


async def test_chat_url_path_prefix_kept(monkeypatch):
    rig = make_rig(monkeypatch, chat_url="https://chat.test/relay/")
    assert (await rig.call("GET", "/agent/v1/chat/rooms")).status_code == 200
    assert str(rig.upstream.seen[0].url) == "https://chat.test/relay/chat/v1/rooms"


async def test_upstream_status_and_body_pass_through(monkeypatch):
    rig = make_rig(monkeypatch)
    cases = [
        ("POST", "/agent/v1/chat/messages", 202, {"id": "s1", "status": "queued"}),
        ("POST", "/agent/v1/chat/reactions", 202, {"id": "s2", "status": "queued"}),
        (
            "GET",
            "/agent/v1/chat/rooms",
            200,
            {"rooms": [{"room_id": "!r:hs", "name": "Me", "kind": "dm", "bound": True}]},
        ),
        (
            "POST",
            "/agent/v1/chat/messages",
            403,
            {"error": {"code": "room_not_allowed", "message": "room !x:hs is not allowed"}},
        ),
        (
            "POST",
            "/agent/v1/chat/reactions",
            422,
            {"error": {"code": "bad_reference", "message": "m99 is unknown"}},
        ),
        (
            "POST",
            "/agent/v1/chat/messages",
            429,
            {"error": {"code": "rate_limited", "message": "at most 20 sends per minute"}},
        ),
        ("GET", "/agent/v1/chat/messages/s1", 200, {"id": "s1", "status": "sent"}),
        (
            "GET",
            "/agent/v1/chat/messages/s9",
            404,
            {"error": {"code": "not_found", "message": "x"}},
        ),
    ]
    for method, path, status, body in cases:
        raw = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode()
        rig.upstream.reply = httpx.Response(status, content=raw)
        r = await rig.call(method, path)
        assert r.status_code == status, (path, r.text)
        assert r.content == raw
        assert r.headers["content-type"] == "application/json"


async def test_upstream_failure_is_chat_unavailable(monkeypatch):
    rig = make_rig(monkeypatch)
    leak = "upstream-secret-" + secrets.token_hex(8)

    async def refused(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(f"refused {leak}", request=request)

    async def timed_out(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout(f"timeout {leak}", request=request)

    async def slow(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(5)
        return httpx.Response(202, json={"id": "s1", "status": "queued"})

    async def not_json(request: httpx.Request) -> httpx.Response:
        return httpx.Response(502, content=f"<html>bad gateway {leak}</html>".encode())

    async def empty(request: httpx.Request) -> httpx.Response:
        return httpx.Response(204)

    monkeypatch.setattr(chat, "TIMEOUT_SECONDS", 0.05)
    for reply in (refused, timed_out, slow, not_json, empty):
        for method, path, _ in ROUTES:
            rig.upstream.reply = reply
            r = await rig.call(method, path)
            assert_error(r, 503, "chat_unavailable")
            assert leak not in r.text


async def test_unconfigured(monkeypatch):
    rig = make_rig(monkeypatch, chat_url="", chat_token="")
    for method, path, _ in ROUTES:
        assert_error(await rig.call(method, path), 503, "chat_unconfigured")
    assert rig.upstream.seen == []


async def test_body_cap(monkeypatch):
    rig = make_rig(monkeypatch)
    cap = chat.MAX_BODY_BYTES
    assert cap == 256 * 1024

    def padded(size: int) -> bytes:
        head = b'{"text":"'
        tail = b'"}'
        return head + b"a" * (size - len(head) - len(tail)) + tail

    for path in ("/agent/v1/chat/messages", "/agent/v1/chat/reactions"):
        assert_error(
            await rig.call("POST", path, content=padded(cap + 1)), 413, "payload_too_large"
        )

        async def chunks():
            for _ in range(cap // 4096 + 1):
                yield b"a" * 4096

        assert_error(await rig.call("POST", path, content=chunks()), 413, "payload_too_large")
    assert rig.upstream.seen == []
    r = await rig.call("POST", "/agent/v1/chat/messages", content=padded(cap))
    assert r.status_code == 200, r.text
    assert len(rig.upstream.seen[0].content) == cap


def _escape_all(value: str) -> str:
    """JSON string literal with every character escaped (UTF-16 surrogate pairs above BMP)."""
    units = value.encode("utf-16-be")
    return (
        '"' + "".join(f"\\u{units[i]:02x}{units[i + 1]:02x}" for i in range(0, len(units), 2)) + '"'
    )


async def test_max_text_passes_cap(monkeypatch):
    rig = make_rig(monkeypatch)
    text = "\U0001f600" * 16000
    room = "!" + "r" * 254
    raw = json.dumps({"text": text, "room": room, "reply_to": "m123"}, ensure_ascii=False).encode()
    escaped = (
        f'{{"text": {_escape_all(text)}, "room": {_escape_all(room)}, '
        f'"reply_to": {_escape_all("m123")}}}'
    ).encode()
    assert len(raw) > 64000
    assert len(escaped) > 192000
    for body in (raw, escaped):
        rig.upstream.seen.clear()
        r = await rig.call("POST", "/agent/v1/chat/messages", content=body)
        assert r.status_code == 200, r.text
        (sent,) = rig.upstream.seen
        assert sent.content == body
        assert json.loads(sent.content)["text"] == text


# ------------------------------------------------------------------------- identity and ports


async def test_leader_only_and_identity(monkeypatch):
    rig = make_rig(monkeypatch)
    other = rig.db.add_session("other", hive=False, leader_ip="10.9.0.20")
    for method, path, _ in ROUTES:
        assert_error(await rig.call(method, path, ip=MEMBER_IP), 403, "not_leader")
        bad = rig.headers(tok="x" + agent_token(rig.session.id))
        assert_error(await rig.call(method, path, headers=bad), 401, "unauthorized")
        assert_error(await rig.call(method, path, headers={}), 401, "unauthorized")
        assert_error(await rig.call(method, path, ip="10.9.9.9"), 403, "forbidden")
        foreign = rig.headers(other.id)
        assert_error(await rig.call(method, path, headers=foreign), 403, "forbidden")
    rig.db.leaders.pop(rig.session.id)
    assert_error(await rig.call("GET", "/agent/v1/chat/rooms"), 403, "not_leader")
    assert rig.upstream.seen == []
    alone_rig = make_rig(monkeypatch, hive=False)
    assert (await alone_rig.call("GET", "/agent/v1/chat/rooms")).status_code == 200
    assert alone_rig.upstream.seen[0].headers["x-orch-slug"] == "chat-caller"


async def test_not_leader_checked_before_configuration(monkeypatch):
    rig = make_rig(monkeypatch, chat_url="", chat_token="")
    assert_error(await rig.call("GET", "/agent/v1/chat/rooms", ip=MEMBER_IP), 403, "not_leader")
    big = b"a" * (chat.MAX_BODY_BYTES + 1)
    r = await rig.call("POST", "/agent/v1/chat/messages", content=big)
    assert_error(r, 503, "chat_unconfigured")


async def test_chat_routes_only_on_agent_port(monkeypatch):
    rig = make_rig(monkeypatch)
    service = {"Authorization": "Bearer " + secrets.token_hex(16)}
    for method, path, _ in ROUTES:
        for port, ip, headers in (
            (MAIN_PORT, "10.1.2.3", rig.headers()),
            (MAIN_PORT, LEADER_IP, rig.headers()),
            (SERVICE_PORT, "10.1.2.3", service),
            (SERVICE_PORT, LEADER_IP, rig.headers()),
        ):
            r = await rig.call(method, path, ip=ip, port=port, headers=headers)
            assert r.status_code in (403, 404), (port, path, r.text)
    assert rig.upstream.seen == []


def test_client_owned_per_app():
    app = create_app(settings(), pool=None)
    first = chat.client(app)
    assert chat.client(app) is first
    assert first.timeout.read == chat.TIMEOUT_SECONDS
    asyncio.run(chat.close_client(app))
    assert app.state.chat_http is None
    assert first.is_closed


def test_readme_documents_chat_routes():
    agent_api = README.split("\n## Agent API\n", 1)[1].split("\n## ", 1)[0]
    assert "\n### Chat\n" in agent_api
    section = agent_api.split("\n### Chat\n", 1)[1].split("\n### ", 1)[0]
    documented = set(
        re.findall(r"^#### `(GET|POST) (/agent/v1/chat/\S+)`\s*$", section, re.MULTILINE)
    )
    app = create_app(settings(), pool=None)
    served = {
        (method, route.path)
        for route in chat.router.routes
        if isinstance(route, APIRoute)
        for method in route.methods
    }
    assert served == {
        ("GET", "/agent/v1/chat/rooms"),
        ("POST", "/agent/v1/chat/messages"),
        ("POST", "/agent/v1/chat/reactions"),
        ("GET", "/agent/v1/chat/messages/{send_id}"),
    }
    assert documented == served
    assert {f"{m} {p}" for m, p in served} <= {
        f"{m.upper()} {p}" for p, ops in app.openapi()["paths"].items() for m in ops
    }
    for needle in (
        "not_leader",
        "chat_unconfigured",
        "chat_unavailable",
        "payload_too_large",
        "X-Orch-Slug",
        "ORCH_CHAT_TOKEN",
        "unchanged",
    ):
        assert needle in section, needle


# ------------------------------------------------------- the released plugin against the routes


@dataclass
class Target:
    """An app whose chat upstream is a `FakeChat`, and the session the plugin speaks for."""

    app: Any
    fake: FakeChat
    session_id: uuid.UUID
    slug: str
    leader_ip: str
    member_ip: str

    def __post_init__(self) -> None:
        transport = httpx.ASGITransport(app=self.fake.app)
        self.app.state.chat_http = httpx.AsyncClient(transport=transport)

    def plugin(self, ip: str) -> PluginHarness:
        """The released orchestrator plugin, calling this app from the pod IP `ip`."""
        config = {
            "base_url": f"http://orch:{AGENT_PORT}",
            "token": agent_token(self.session_id),
            "session": str(self.session_id),
            "agent": "a0",
            "slug": self.slug,
        }
        transport = httpx.ASGITransport(app=self.app, client=(ip, 4321))
        return PluginHarness(OrchestratorPlugin, config=config, transport=transport)

    async def direct(self, method: str, path: str, **kw: Any) -> httpx.Response:
        """The same route called without the plugin, as the leader."""
        headers = {
            "X-Orch-Session": str(self.session_id),
            "Authorization": f"Bearer {agent_token(self.session_id)}",
        }
        transport = httpx.ASGITransport(app=self.app, client=(self.leader_ip, 4321))
        async with httpx.AsyncClient(
            transport=transport, base_url=f"http://orch:{AGENT_PORT}"
        ) as http:
            return await http.request(method, path, headers=headers, **kw)


def memory_target(monkeypatch: pytest.MonkeyPatch) -> Target:
    rig = make_rig(monkeypatch)
    fake = FakeChat(CHAT_TOKEN)
    return Target(rig.app, fake, rig.session.id, rig.session.slug, LEADER_IP, MEMBER_IP)


async def check_chat_tools_parity(target: Target) -> None:
    """The four chat tools return what the routes return, and a member's calls are refused."""
    fake = target.fake
    async with target.plugin(target.leader_ip) as h:
        rooms = await h.call("chat_rooms")
        sent = await h.call("chat_send", {"text": "hello from the plugin", "room": DEFAULT_ROOM})
        reacted = await h.call("chat_react", {"to": "m3", "key": "\U0001f44d"})
        status = await h.call("chat_status", {"id": "s1"})
        missing = await h.call("chat_status", {"id": "s99"})
    for result in (rooms, sent, reacted, status):
        assert not result.is_error, result.text
    direct_rooms = await target.direct("GET", "/agent/v1/chat/rooms")
    assert rooms.structured == direct_rooms.json() == {"rooms": fake.rooms}
    assert sent.structured == {"id": "s1", "status": "queued"}
    assert reacted.structured == {"id": "s2", "status": "queued"}
    body = {"text": "hello from the plugin", "room": DEFAULT_ROOM}
    direct_send = await target.direct("POST", "/agent/v1/chat/messages", json=body)
    assert direct_send.status_code == 202
    assert direct_send.json() == {"id": "s3", "status": "queued"}
    direct_react = await target.direct(
        "POST", "/agent/v1/chat/reactions", json={"to": "m3", "key": "\U0001f44d"}
    )
    assert direct_react.json() == {"id": "s4", "status": "queued"}
    direct_status = await target.direct("GET", "/agent/v1/chat/messages/s1")
    assert status.structured == direct_status.json()
    assert status.structured["id"] == "s1" and status.structured["kind"] == "message"
    # an unknown id is the upstream's 404, passed through by the relay and the plugin
    direct_missing = await target.direct("GET", "/agent/v1/chat/messages/s99")
    assert direct_missing.status_code == 404
    assert missing.is_error
    assert missing.structured["error"]["status"] == 404
    assert missing.structured["error"]["code"] == direct_missing.json()["error"]["code"]
    # the plugin sent the same bodies as the direct calls and the same identity headers
    plugin_calls = [c for c in fake.calls if c.method == "POST"][:2]
    assert plugin_calls[0].json() == {"text": "hello from the plugin", "room": DEFAULT_ROOM}
    assert plugin_calls[1].json() == {"to": "m3", "key": "\U0001f44d"}
    for call in fake.calls:
        assert call.headers["x-orch-session"] == str(target.session_id)
        assert call.headers["x-orch-slug"] == target.slug
    # only the leader's calls succeed
    fake.calls.clear()
    async with target.plugin(target.member_ip) as h:
        for name, args in (
            ("chat_rooms", {}),
            ("chat_send", {"text": "x"}),
            ("chat_react", {"to": "m1", "key": "x"}),
            ("chat_status", {"id": "s1"}),
        ):
            refused = await h.call(name, args)
            assert refused.is_error, name
            assert refused.structured["error"]["code"] == "not_leader", refused.text
    assert fake.calls == []


async def test_chat_tools_parity(monkeypatch):
    """The released plugin drives chat_rooms, chat_send, chat_react and chat_status against the
    real agent routes and the contract fake of the chat upstream (design D16)."""
    await check_chat_tools_parity(memory_target(monkeypatch))


async def test_chat_send_max_text_through_plugin(monkeypatch):
    """16000 four-byte characters through the released plugin and the real relay reach the
    upstream byte for byte (interrogate Act 6, design D5)."""
    target = memory_target(monkeypatch)
    text = "\U0001f600" * 16000
    async with target.plugin(target.leader_ip) as h:
        sent = await h.call("chat_send", {"text": text})
    assert not sent.is_error, sent.text
    assert sent.structured == {"id": "s1", "status": "queued"}
    (call,) = target.fake.calls
    assert call.path == "/chat/v1/messages"
    assert call.json()["text"] == text
    assert len(call.body) > 64000


# ------------------------------------------------------------------- DB-backed (CI, Docker)


async def test_leader_only_and_identity_db(pool, dsn, monkeypatch):
    from factories import insert_agent, insert_session

    async def exec_(query: str, params: tuple) -> None:
        async with pool.connection() as conn:
            await conn.execute(query, params)

    row = await insert_session(pool, slug="chat-db", spec={"replicas": 2, "max_replicas": 2})
    a0 = await insert_agent(pool, row["id"], ordinal=0, ready=True)
    a1 = await insert_agent(pool, row["id"], ordinal=1, ready=True)
    for agent, ip in ((a0, LEADER_IP), (a1, MEMBER_IP)):
        await exec_("UPDATE agents SET pod_ip = %s WHERE id = %s", (ip, agent["id"]))
    await exec_(
        "INSERT INTO session_leaders (session_id, agent_id, epoch, since, reason)"
        " VALUES (%s, %s, 1, now(), 'start')",
        (row["id"], a0["id"]),
    )
    upstream = Upstream()
    app = create_app(settings(database_url=dsn), pool)
    app.state.chat_http = httpx.AsyncClient(transport=httpx.MockTransport(upstream.handler))
    sid = row["id"]
    good = {"X-Orch-Session": str(sid), "Authorization": f"Bearer {agent_token(sid)}"}

    async def call(ip: str, headers: dict) -> httpx.Response:
        transport = httpx.ASGITransport(app=app, client=(ip, 4321))
        async with httpx.AsyncClient(
            transport=transport, base_url=f"http://orch:{AGENT_PORT}"
        ) as http:
            return await http.post("/agent/v1/chat/messages", json={"text": "hi"}, headers=headers)

    assert_error(await call(MEMBER_IP, good), 403, "not_leader")
    assert_error(await call(LEADER_IP, {**good, "Authorization": "Bearer x"}), 401, "unauthorized")
    assert_error(await call("10.9.9.9", good), 403, "forbidden")
    assert upstream.seen == []
    r = await call(LEADER_IP, good)
    assert r.status_code == 200, r.text
    (sent,) = upstream.seen
    assert sent.headers["x-orch-slug"] == "chat-db"
    assert sent.headers["x-orch-session"] == str(sid)
    assert sent.headers["authorization"] == f"Bearer {CHAT_TOKEN}"


async def test_chat_tools_parity_db(pool, dsn):
    """The same parity over the DB-backed identity and leadership reads."""
    from factories import insert_agent, insert_session

    row = await insert_session(pool, slug="chat-parity", spec={"replicas": 2, "max_replicas": 2})
    a0 = await insert_agent(pool, row["id"], ordinal=0, ready=True)
    a1 = await insert_agent(pool, row["id"], ordinal=1, ready=True)
    async with pool.connection() as conn:
        for agent, ip in ((a0, LEADER_IP), (a1, MEMBER_IP)):
            await conn.execute("UPDATE agents SET pod_ip = %s WHERE id = %s", (ip, agent["id"]))
        await conn.execute(
            "INSERT INTO session_leaders (session_id, agent_id, epoch, since, reason)"
            " VALUES (%s, %s, 1, now(), 'start')",
            (row["id"], a0["id"]),
        )
    app = create_app(settings(database_url=dsn), pool)
    target = Target(app, FakeChat(CHAT_TOKEN), row["id"], "chat-parity", LEADER_IP, MEMBER_IP)
    await check_chat_tools_parity(target)

"""Taskboard relay agent routes: `/agent/v1/tasks/*` forwarded to the taskboard upstream.

Runs the real `require_agent` against the in-memory identity stand-in of the chat relay tests and
an `httpx.MockTransport` upstream, so it needs no Docker.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import secrets
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from test_agent_chat import AGENT_PORT, KEY, LEADER_IP, FakeDb, agent_token
from test_api_sessions import assert_error

from agents_orchestrator.api import tasks
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.settings import Settings

TB_URL = "http://taskboard.test:9002"
TB_TOKEN = "tb-" + secrets.token_hex(16)


def settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "database_url": "postgresql://unused/db",
        "allowed_cidrs": "10.1.2.0/24",
        "master_key": KEY,
        "http_port": 8000,
        "agent_port": AGENT_PORT,
        "service_port": 8002,
        "taskboard_url": TB_URL,
        "taskboard_token": TB_TOKEN,
    }
    values.update(overrides)
    return Settings(**values)


@dataclass
class Upstream:
    reply: Any = None
    seen: list[httpx.Request] = field(default_factory=list)

    async def handler(self, request: httpx.Request) -> httpx.Response:
        self.seen.append(request)
        if self.reply is None:
            return httpx.Response(200, json={"items": [], "next_cursor": None})
        if callable(self.reply):
            return await self.reply(request)
        return self.reply


@dataclass
class Rig:
    app: Any
    upstream: Upstream
    session: sessions_repo.Session
    db: FakeDb | None = None

    def db_touched(self) -> list:
        assert self.db is not None
        return self.db.touched

    async def call(self, method: str, path: str, **kw: Any) -> httpx.Response:
        sid = self.session.id
        headers = {"X-Orch-Session": str(sid), "Authorization": f"Bearer {agent_token(sid)}"}
        headers.update(kw.pop("headers", {}))
        transport = httpx.ASGITransport(app=self.app, client=(LEADER_IP, 4321))
        async with httpx.AsyncClient(
            transport=transport, base_url=f"http://orch:{AGENT_PORT}"
        ) as c:
            return await c.request(method, path, headers=headers, **kw)


def make_rig(
    monkeypatch: pytest.MonkeyPatch,
    *,
    labels: list[str] | None = None,
    hive: bool = True,
    created_by_service: str | None = None,
    **overrides: Any,
) -> Rig:
    db = FakeDb()
    db.install(monkeypatch)
    session = db.add_session("tb-caller", hive=hive, created_by_service=created_by_service)
    session = dataclasses.replace(session, labels=labels or [])
    db.sessions[session.id] = session
    upstream = Upstream()
    app = create_app(settings(**overrides), pool=None)
    app.state.pool = db
    app.state.taskboard_http = httpx.AsyncClient(transport=httpx.MockTransport(upstream.handler))
    return Rig(app, upstream, session, db)


ROUTES = (
    ("GET", "/agent/v1/tasks", "/taskboard/api/v1/tasks"),
    ("POST", "/agent/v1/tasks", "/taskboard/api/v1/tasks"),
    ("GET", "/agent/v1/tasks/T-1", "/taskboard/api/v1/tasks/T-1"),
    ("PATCH", "/agent/v1/tasks/T-1", "/taskboard/api/v1/tasks/T-1"),
    ("PUT", "/agent/v1/tasks/T-1", "/taskboard/api/v1/tasks/T-1"),
    ("GET", "/agent/v1/tasks/T-1/comments", "/taskboard/api/v1/tasks/T-1/comments"),
    ("POST", "/agent/v1/tasks/T-1/comments", "/taskboard/api/v1/tasks/T-1/comments"),
    ("GET", "/agent/v1/tasks/T-1/todo", "/taskboard/api/v1/tasks/T-1/todo"),
    ("POST", "/agent/v1/tasks/T-1/todo", "/taskboard/api/v1/tasks/T-1/todo"),
    ("PATCH", "/agent/v1/tasks/T-1/todo/3", "/taskboard/api/v1/tasks/T-1/todo/3"),
    ("PUT", "/agent/v1/tasks/T-1/deps", "/taskboard/api/v1/tasks/T-1/deps"),
    ("GET", "/agent/v1/tasks/T-1/decisions", "/taskboard/api/v1/tasks/T-1/decisions"),
    ("POST", "/agent/v1/tasks/T-1/decisions", "/taskboard/api/v1/tasks/T-1/decisions"),
    (
        "POST",
        "/agent/v1/tasks/T-1/decisions/4/withdraw",
        "/taskboard/api/v1/tasks/T-1/decisions/4/withdraw",
    ),
)


def kwargs(method: str) -> dict[str, Any]:
    return {} if method == "GET" else {"json": {"title": "x"}}


async def test_forwards_verified_session_and_slug(monkeypatch):
    rig = make_rig(monkeypatch)
    secret = agent_token(rig.session.id)
    for method, path, upstream_path in ROUTES:
        rig.upstream.seen.clear()
        r = await rig.call(method, path + "?cursor=c1&status=open", **kwargs(method))
        assert r.status_code == 200, (path, r.text)
        (sent,) = rig.upstream.seen
        assert sent.method == method
        assert sent.url.host == "taskboard.test" and sent.url.port == 9002
        assert sent.url.raw_path == (upstream_path + "?cursor=c1&status=open").encode()
        assert sent.headers["authorization"] == f"Bearer {TB_TOKEN}"
        assert sent.headers["x-orch-session"] == str(rig.session.id)
        assert sent.headers["x-orch-slug"] == "tb-caller"
        for name, value in sent.headers.items():
            assert secret not in value, name
        if method != "GET":
            assert sent.content == json.dumps({"title": "x"}, separators=(",", ":")).encode()
            assert sent.headers["content-type"] == "application/json"


async def test_inbound_x_orch_headers_are_replaced(monkeypatch):
    rig = make_rig(monkeypatch)
    inbound = {
        "X-Orch-Slug": "forged",
        "X-Orch-Extra": "x",
        "X-Orch-Admin": "1",
        "Cookie": "c=1",
        "X-Forwarded-For": "1.2.3.4",
    }
    r = await rig.call("GET", "/agent/v1/tasks", headers=inbound)
    assert r.status_code == 200
    (sent,) = rig.upstream.seen
    assert sent.headers["x-orch-slug"] == "tb-caller"
    names = {k.lower() for k in sent.headers}
    assert not names & {"x-orch-extra", "x-orch-admin", "cookie", "x-forwarded-for"}


async def test_upstream_status_and_body_pass_through(monkeypatch):
    rig = make_rig(monkeypatch)
    raw = json.dumps({"error": {"code": "stale", "message": "x"}}).encode()
    rig.upstream.reply = httpx.Response(412, content=raw)
    r = await rig.call("PATCH", "/agent/v1/tasks/T-1", json={"version": 1})
    assert r.status_code == 412 and r.content == raw


async def test_trigger_session_is_not_refused(monkeypatch):
    for service in ("agents-webhook", "agents-schedule", "agents-matrix-connector"):
        rig = make_rig(monkeypatch, created_by_service=service)
        assert (await rig.call("GET", "/agent/v1/tasks")).status_code == 200


async def test_no_leader_check(monkeypatch):
    """A hive member that is not the leader may use the relay."""
    rig = make_rig(monkeypatch, hive=True)
    transport = httpx.ASGITransport(app=rig.app, client=("10.9.0.11", 4321))
    sid = rig.session.id
    headers = {"X-Orch-Session": str(sid), "Authorization": f"Bearer {agent_token(sid)}"}
    async with httpx.AsyncClient(transport=transport, base_url="http://orch:8001") as c:
        r = await c.get("/agent/v1/tasks", headers=headers)
    assert r.status_code == 200, r.text


async def test_labels_mine_expands_to_session_labels(monkeypatch):
    rig = make_rig(monkeypatch, labels=["team-a", "infra"])
    r = await rig.call("GET", "/agent/v1/tasks?labels=mine&label=extra&status=open")
    assert r.status_code == 200
    (sent,) = rig.upstream.seen
    assert sent.url.params.multi_items() == [
        ("label", "team-a"),
        ("label", "infra"),
        ("label", "extra"),
        ("status", "open"),
    ]
    assert "labels" not in sent.url.params


async def test_labels_mine_unlabelled_session_empty(monkeypatch):
    rig = make_rig(monkeypatch, labels=[])
    r = await rig.call("GET", "/agent/v1/tasks?labels=mine")
    assert r.status_code == 200
    assert r.json() == {"items": [], "next_cursor": None}
    assert rig.upstream.seen == []


async def test_labels_other_value_passes_through(monkeypatch):
    rig = make_rig(monkeypatch, labels=["a"])
    await rig.call("GET", "/agent/v1/tasks?labels=other")
    assert rig.upstream.seen[0].url.params["labels"] == "other"


async def test_unconfigured_503(monkeypatch):
    rig = make_rig(monkeypatch, taskboard_url="", taskboard_token="")
    for method, path, _ in ROUTES:
        assert_error(await rig.call(method, path, **kwargs(method)), 503, "taskboard_unconfigured")
    assert rig.upstream.seen == []


@pytest.mark.parametrize(
    "path",
    [
        "/agent/v1/tasks/T-1/decisions/4/answer",
        "/agent/v1/tasks/stream",
        "/agent/v1/tasks/T-1/stream",
        "/agent/v1/tasks/T-1/decisions/4",
        "/agent/v1/tasks/T-1/other",
        "/agent/v1/tasks/..%2Fadmin",
        "/agent/v1/tasks/T-1/todo/3/x",
        "/agent/v1/tasks/T-1/deps/x",
        "/agent/v1/tasks/T-1/",
    ],
)
async def test_unlisted_paths_are_404(monkeypatch, path):
    rig = make_rig(monkeypatch)
    for method in ("GET", "POST", "PATCH"):
        assert_error(await rig.call(method, path, **kwargs(method)), 404, "not_found")
    assert rig.upstream.seen == []


async def test_answer_path_is_404(monkeypatch):
    rig = make_rig(monkeypatch)
    r = await rig.call("POST", "/agent/v1/tasks/T-1/decisions/4/answer", json={"answer": "y"})
    assert_error(r, 404, "not_found")
    assert rig.upstream.seen == []


async def test_stream_path_is_404(monkeypatch):
    rig = make_rig(monkeypatch)
    assert_error(await rig.call("GET", "/agent/v1/tasks/stream"), 404, "not_found")
    assert rig.upstream.seen == []


async def test_body_cap_413(monkeypatch):
    rig = make_rig(monkeypatch)
    cap = tasks.MAX_BODY_BYTES
    assert cap == 256 * 1024
    assert_error(
        await rig.call("POST", "/agent/v1/tasks", content=b"a" * (cap + 1)),
        413,
        "payload_too_large",
    )

    async def chunks():
        for _ in range(cap // 4096 + 1):
            yield b"a" * 4096

    assert_error(
        await rig.call("PATCH", "/agent/v1/tasks/T-1", content=chunks()), 413, "payload_too_large"
    )
    assert rig.upstream.seen == []
    r = await rig.call("POST", "/agent/v1/tasks", content=b"a" * cap)
    assert r.status_code == 200


async def test_upstream_error_503(monkeypatch):
    rig = make_rig(monkeypatch)
    leak = "upstream-secret-" + secrets.token_hex(8)

    async def refused(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(f"refused {leak}", request=request)

    async def slow(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(5)
        return httpx.Response(200, json={})

    async def not_json(request: httpx.Request) -> httpx.Response:
        return httpx.Response(502, content=f"<html>{leak}</html>".encode())

    monkeypatch.setattr(tasks, "TIMEOUT_SECONDS", 0.05)
    for reply in (refused, slow, not_json):
        rig.upstream.reply = reply
        r = await rig.call("GET", "/agent/v1/tasks")
        assert_error(r, 503, "taskboard_unavailable")
        assert leak not in r.text


async def test_no_agent_token_leaks_upstream(monkeypatch):
    rig = make_rig(monkeypatch)
    secret = agent_token(rig.session.id)
    await rig.call("POST", "/agent/v1/tasks", json={"title": "x"})
    (sent,) = rig.upstream.seen
    assert secret not in str(sent.url)
    assert secret not in sent.content.decode()
    assert all(secret not in v for v in sent.headers.values())


async def test_requires_agent_credentials(monkeypatch):
    rig = make_rig(monkeypatch)
    r = await rig.call("GET", "/agent/v1/tasks", headers={"Authorization": "Bearer wrong"})
    assert r.status_code == 401
    assert rig.upstream.seen == []


async def test_task_write_touches_session(monkeypatch):
    """A 2xx answer to POST, PUT or PATCH touches the caller's session; a GET or any other
    status never does."""
    rig = make_rig(monkeypatch)
    for method, path, _ in ROUTES:
        for status in (200, 201, 204, 400, 404, 409, 500, 503):
            rig.db_touched().clear()
            rig.upstream.reply = httpx.Response(status, json={})
            await rig.call(method, path, **kwargs(method))
            write = method != "GET" and status < 300
            assert rig.db_touched() == ([rig.session.id] if write else []), (method, path, status)


def test_every_relayed_write_method_stamps_the_update():
    """The relay's write routes are exactly the methods that stamp `last_update_at` (spec 3.2:
    POST, PUT, PATCH); reads never do."""
    methods = {m for route in tasks.router.routes for m in route.methods}
    assert methods - {"GET"} == tasks._WRITES == {"POST", "PUT", "PATCH"}


async def test_task_touch_failure_does_not_fail_the_relay(monkeypatch, caplog):
    rig = make_rig(monkeypatch)

    async def broken(_conn, _session_id):
        raise RuntimeError("db down")

    monkeypatch.setattr(sessions_repo, "touch_update", broken)
    rig.upstream.reply = httpx.Response(201, json={"key": "T-1"})
    r = await rig.call("POST", "/agent/v1/tasks", json={"title": "x"})
    assert r.status_code == 201 and r.json() == {"key": "T-1"}
    [record] = [r for r in caplog.records if r.name == "agents_orchestrator.api.forward"]
    assert record.levelno == logging.ERROR and record.exc_info is not None
    assert "could not stamp last_update_at" in record.getMessage()


async def _db_app(pool, dsn, upstream: Upstream):
    from factories import insert_agent, insert_session

    row = await insert_session(pool, slug=f"tb-{uuid.uuid4().hex[:8]}", spec={"replicas": 1})
    agent = await insert_agent(pool, row["id"], ordinal=0, ready=True)
    async with pool.connection() as conn:
        await conn.execute("UPDATE agents SET pod_ip = %s WHERE id = %s", (LEADER_IP, agent["id"]))
        cur = await conn.execute(
            "UPDATE sessions SET last_update_at = now() - interval '5 h' WHERE id = %s"
            " RETURNING last_update_at",
            (row["id"],),
        )
        old = (await cur.fetchone())[0]
    app = create_app(settings(database_url=dsn), pool)
    app.state.taskboard_http = httpx.AsyncClient(transport=httpx.MockTransport(upstream.handler))
    return app, row["id"], old


async def _call_db(app, sid, method: str, path: str) -> httpx.Response:
    headers = {"X-Orch-Session": str(sid), "Authorization": f"Bearer {agent_token(sid)}"}
    transport = httpx.ASGITransport(app=app, client=(LEADER_IP, 4321))
    async with httpx.AsyncClient(transport=transport, base_url=f"http://orch:{AGENT_PORT}") as c:
        return await c.request(method, path, headers=headers, **kwargs(method))


async def _last_update(pool, sid):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT last_update_at FROM sessions WHERE id = %s", (sid,))
        return (await cur.fetchone())[0]


async def test_task_write_touches_last_update(pool, dsn):
    for method, path in (
        ("POST", "/agent/v1/tasks"),
        ("PUT", "/agent/v1/tasks/T-1"),
        ("PATCH", "/agent/v1/tasks/T-1"),
    ):
        app, sid, old = await _db_app(pool, dsn, Upstream(reply=httpx.Response(201, json={})))
        assert (await _call_db(app, sid, method, path)).status_code == 201
        new = await _last_update(pool, sid)
        assert new > old and abs((datetime.now(UTC) - new).total_seconds()) < 5, method


async def test_task_read_and_client_error_do_not_touch(pool, dsn):
    for method, path, status in (
        ("GET", "/agent/v1/tasks", 200),
        ("POST", "/agent/v1/tasks", 422),
        ("PATCH", "/agent/v1/tasks/T-1", 404),
        ("POST", "/agent/v1/tasks/T-1/comments", 500),
    ):
        app, sid, old = await _db_app(pool, dsn, Upstream(reply=httpx.Response(status, json={})))
        assert (await _call_db(app, sid, method, path)).status_code == status
        assert await _last_update(pool, sid) == old, (method, path, status)


# ------------------------------------------------------------ projects summary relay (T-120)


def _scoped(rig: Rig, **changes: Any) -> None:
    session = dataclasses.replace(rig.session, **changes)
    assert rig.db is not None
    rig.db.sessions[session.id] = session


async def test_projects_summary_relay_path_query_and_default_scope(monkeypatch):
    rig = make_rig(monkeypatch)
    r = await rig.call("GET", "/agent/v1/projects/summary?project=alpha")
    assert r.status_code == 200
    (sent,) = rig.upstream.seen
    assert sent.method == "GET"
    assert sent.url.raw_path == b"/taskboard/api/v1/projects/summary?project=alpha"
    assert sent.headers["authorization"] == f"Bearer {TB_TOKEN}"
    assert sent.headers["x-orch-session"] == str(rig.session.id)
    assert sent.headers["x-orch-projects"] == "default"


async def test_projects_summary_superleader_sends_star(monkeypatch):
    rig = make_rig(monkeypatch)
    spec = rig.session.spec.model_copy(update={"session_messaging": True})
    _scoped(rig, spec=spec)
    await rig.call("GET", "/agent/v1/projects/summary", headers={"X-Orch-Projects": "a"})
    (sent,) = rig.upstream.seen
    assert sent.headers.get_list("x-orch-projects") == ["*"]
    assert sent.url.raw_path == b"/taskboard/api/v1/projects/summary"


async def test_projects_summary_granted_session_sends_home_and_extra(monkeypatch):
    rig = make_rig(monkeypatch)
    _scoped(rig, project="home", extra_projects=("extra",))
    await rig.call("GET", "/agent/v1/projects/summary")
    (sent,) = rig.upstream.seen
    assert sent.headers["x-orch-projects"] == "home,extra"


async def test_projects_summary_passes_upstream_answer_unchanged(monkeypatch):
    rig = make_rig(monkeypatch)
    rig.upstream.reply = httpx.Response(403, json={"error": {"code": "forbidden"}})
    r = await rig.call("GET", "/agent/v1/projects/summary?project=x")
    assert r.status_code == 403
    assert r.json() == {"error": {"code": "forbidden"}}


async def test_projects_summary_unconfigured_503(monkeypatch):
    rig = make_rig(monkeypatch, taskboard_url="", taskboard_token="")
    assert_error(await rig.call("GET", "/agent/v1/projects/summary"), 503, "taskboard_unconfigured")
    assert rig.upstream.seen == []


def test_projects_listing_is_not_shadowed_by_the_summary_relay(monkeypatch):
    rig = make_rig(monkeypatch)
    paths = rig.app.openapi()["paths"]
    listing = paths["/agent/v1/projects"]["get"]["operationId"]
    summary = paths["/agent/v1/projects/summary"]["get"]["operationId"]
    assert listing.startswith("projects_") and summary.startswith("projects_summary_")

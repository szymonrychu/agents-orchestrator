"""Service port (D10): bearer service tokens on ORCH_SERVICE_PORT, a fixed route list."""

from __future__ import annotations

import hashlib
import logging
import os
import uuid

import httpx
import pytest
import pytest_asyncio
from factories import insert_session
from test_api_sessions import assert_error, make_settings

from agents_orchestrator.api import service_auth
from agents_orchestrator.api.app import create_app
from agents_orchestrator.settings import Settings, SettingsError

DB = "postgresql://u:p@h/db"
DEFAULT_ID = "00000000-0000-0000-0000-000000000001"
TOKEN = "s3cret-webhook-token-value"
OTHER_TOKEN = "s3cret-schedule-token-value"
OUTSIDE = ("203.0.113.9", 4444)  # not in make_settings' 10.1.2.0/24
INSIDE = ("10.1.2.3", 5555)


def sha(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


TOKENS = f"agents-webhook:{sha(TOKEN)},agents-schedule:{sha(OTHER_TOKEN)}"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for k in list(os.environ):
        if k.startswith("ORCH_"):
            monkeypatch.delenv(k)
    monkeypatch.setenv("ORCH_DATABASE_URL", DB)


def client(app, port: int, peer=OUTSIDE, token: str | None = TOKEN) -> httpx.AsyncClient:
    headers = {"Authorization": f"Bearer {token}"} if token is not None else {}
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=peer),
        base_url=f"http://orch:{port}",
        headers=headers,
    )


@pytest_asyncio.fixture
async def app(pool, dsn):
    return create_app(make_settings(dsn, service_tokens=TOKENS), pool)


async def test_allowed_routes_with_token(app, caplog):
    caplog.set_level(logging.INFO)
    async with client(app, 8002) as c:
        r = await c.get("/api/v1/meta")
        assert r.status_code == 200, r.text
        r = await c.get("/api/v1/presets")
        assert r.status_code == 200, r.text
        assert any(p["id"] == DEFAULT_ID for p in r.json())
        r = await c.get(f"/api/v1/presets/{DEFAULT_ID}")
        assert r.status_code == 200, r.text
        r = await c.post("/api/v1/sessions", json={"preset_id": DEFAULT_ID, "slug": "wh-1"})
        assert r.status_code == 201, r.text
        session_id = r.json()["id"]
        r = await c.get("/api/v1/sessions")
        assert r.status_code == 200, r.text
        r = await c.get(f"/api/v1/sessions/{session_id}")
        assert r.status_code == 200, r.text
        r = await c.post(f"/api/v1/sessions/{session_id}/messages", json={"text": "hi"})
        assert r.status_code == 202, r.text
        message_id = r.json()["message_id"]
        r = await c.get(f"/api/v1/sessions/{session_id}/messages")
        assert r.status_code == 200, r.text
        r = await c.get(f"/api/v1/messages/{message_id}")
        assert r.status_code == 200, r.text
        assert r.json()["text"] == "hi"
    lines = [rec.getMessage() for rec in caplog.records if rec.name == service_auth.log.name]
    assert any("service=agents-webhook" in m and "/api/v1/meta" in m for m in lines), lines
    assert all(TOKEN not in rec.getMessage() for rec in caplog.records)


@pytest.mark.parametrize(
    ("method", "path", "allowed"),
    [
        ("GET", "/api/v1/stream", True),
        ("POST", "/api/v1/stream", False),
        ("HEAD", "/api/v1/stream", False),
        ("GET", "/api/v1/stream/x", False),
        ("GET", "/api/v1/stream/", False),
        ("GET", "/api/v1/sessions/x/events/stream", False),
    ],
)
def test_stream_route_is_get_only_and_exact(method, path, allowed):
    assert service_auth.route_allowed(method, path) is allowed


async def test_token_required(app, caplog, monkeypatch):
    caplog.set_level(logging.DEBUG)
    async with client(app, 8002, token=None) as c:
        assert (await c.get("/healthz")).status_code == 200
        assert (await c.get("/readyz")).status_code == 200
        r = await c.get("/api/v1/meta")
        assert_error(r, 401, "unauthorized")
        assert r.headers["www-authenticate"] == "Bearer"
        r = await c.get("/api/v1/meta", headers={"Authorization": f"Basic {TOKEN}"})
        assert_error(r, 401, "unauthorized")
        r = await c.get("/api/v1/meta", headers={"Authorization": "Bearer "})
        assert_error(r, 401, "unauthorized")
    async with client(app, 8002, token="wrong-token") as c:
        r = await c.get("/api/v1/meta")
        assert_error(r, 401, "unauthorized")
        assert r.headers["www-authenticate"] == "Bearer"
        assert "wrong-token" not in r.text
        r = await c.get(f"/api/v1/presets/{DEFAULT_ID}")
        assert_error(r, 401, "unauthorized")
    # a hash is not a token: presenting the configured hash itself is refused
    async with client(app, 8002, token=sha(TOKEN)) as c:
        assert_error(await c.get("/api/v1/meta"), 401, "unauthorized")
    # the second configured service authenticates as itself
    async with client(app, 8002, token=OTHER_TOKEN) as c:
        assert (await c.get("/api/v1/meta")).status_code == 200
    assert any("service=agents-schedule" in rec.getMessage() for rec in caplog.records)
    for rec in caplog.records:
        msg = rec.getMessage()
        assert TOKEN not in msg and "wrong-token" not in msg and OTHER_TOKEN not in msg

    # every configured hash is compared, even when the first one already matched
    calls = []
    real = service_auth.hmac.compare_digest

    def spy(a, b):
        calls.append(1)
        return real(a, b)

    monkeypatch.setattr(service_auth.hmac, "compare_digest", spy)
    async with client(app, 8002) as c:
        assert (await c.get("/api/v1/meta")).status_code == 200
    assert len(calls) == 2


async def test_route_allowlist(app, pool):
    cl = await insert_session(pool)
    async with client(app, 8002) as c:
        for method, path in [
            ("DELETE", f"/api/v1/sessions/{cl['id']}"),
            ("PUT", f"/api/v1/presets/{DEFAULT_ID}"),
            ("DELETE", f"/api/v1/presets/{DEFAULT_ID}"),
            ("POST", "/api/v1/presets"),
            ("POST", f"/api/v1/presets/{DEFAULT_ID}/copy"),
            ("POST", f"/api/v1/agents/{uuid.uuid4()}/restart"),
            ("POST", f"/api/v1/sessions/{cl['id']}/scale"),
            ("GET", f"/api/v1/sessions/{cl['id']}/agents"),
            ("GET", f"/api/v1/sessions/{cl['id']}/events/stream"),
            ("GET", "/api/v1/openapi.json"),
            ("GET", "/api/v1/docs"),
            ("DELETE", f"/api/v1/messages/{uuid.uuid4()}"),
            ("POST", f"/api/v1/messages/{uuid.uuid4()}/retry"),
            ("HEAD", "/api/v1/meta"),
            ("GET", "/api/v1/sessions/"),
            ("GET", "/api/v1/meta/extra"),
        ]:
            r = await c.request(method, path, json={} if method in ("PUT", "POST") else None)
            assert r.status_code == 403, (method, path, r.status_code)
            if method != "HEAD":
                assert_error(r, 403, "forbidden")
        # the refused DELETE never reached the route
        assert (await c.get(f"/api/v1/sessions/{cl['id']}")).status_code == 200
        r = await c.get("/agent/v1/me")
        assert_error(r, 404, "not_found")
    async with client(app, 8002, token=None) as c:
        assert_error(await c.get("/agent/v1/me"), 404, "not_found")

    sent: list[dict] = []

    async def receive():
        return {"type": "websocket.connect"}

    async def send(message):
        sent.append(message)

    scope = {
        "type": "websocket",
        "asgi": {"version": "3.0"},
        "scheme": "ws",
        "path": f"/api/v1/agents/{uuid.uuid4()}/terminals/t1/attach",
        "raw_path": b"/",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"authorization", f"Bearer {TOKEN}".encode())],
        "client": OUTSIDE,
        "server": ("orch", 8002),
        "subprotocols": [],
        "state": {},
    }
    await app(scope, receive, send)
    assert sent == [{"type": "websocket.close", "code": 1008}]


async def test_gate_records_the_authenticated_service_name_per_request():
    """The gate hands the authenticated service name to the app through the request state; a
    request on another port, or one presenting a forged header, carries none."""
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse
    from starlette.routing import Route

    async def who(request):
        return JSONResponse({"service": getattr(request.state, "service", None)})

    inner = Starlette(routes=[Route("/api/v1/meta", who)])
    hashes = {"agents-webhook": sha(TOKEN), "agents-schedule": sha(OTHER_TOKEN)}
    gated = service_auth.ServiceGate(inner, 8002, hashes)
    for token, name in ((TOKEN, "agents-webhook"), (OTHER_TOKEN, "agents-schedule")):
        async with client(gated, 8002, token=token) as c:
            r = await c.get("/api/v1/meta", headers={"X-Orch-Service": "agents-matrix-connector"})
            assert r.json() == {"service": name}
    async with client(gated, 8000, token=None) as c:
        assert (await c.get("/api/v1/meta")).json() == {"service": None}


async def test_session_created_on_the_service_port_records_its_service(app, pool):
    """A session a service creates stores that service's name; one created on the main port
    stores none."""
    async with client(app, 8002) as c:
        r = await c.post("/api/v1/sessions", json={"preset_id": DEFAULT_ID, "slug": "wh-1"})
        assert r.status_code == 201, r.text
        assert r.json()["created_by_service"] == "agents-webhook"
    async with client(app, 8002, token=OTHER_TOKEN) as c:
        r = await c.post("/api/v1/sessions", json={"preset_id": DEFAULT_ID, "slug": "sch-1"})
        assert r.status_code == 201, r.text
    async with client(app, 8000, peer=INSIDE, token=None) as c:
        r = await c.post("/api/v1/sessions", json={"preset_id": DEFAULT_ID, "slug": "user-1"})
        assert r.status_code == 201, r.text
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT slug, created_by_service FROM sessions ORDER BY slug")
        rows = await cur.fetchall()
    assert rows == [("sch-1", "agents-schedule"), ("user-1", None), ("wh-1", "agents-webhook")]


async def test_spec_session_created_on_the_service_port_records_its_service(app, pool):
    async with client(app, 8002) as c:
        r = await c.post("/api/v1/sessions", json={"spec": {"replicas": 1}, "slug": "wh-spec"})
        assert r.status_code == 201, r.text
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT created_by_service FROM sessions WHERE slug='wh-spec'")
        assert (await cur.fetchone())[0] == "agents-webhook"


async def test_main_port_unchanged(app):
    async with client(app, 8000) as c:
        assert_error(await c.get("/api/v1/meta"), 403, "forbidden")
    async with client(app, 8000, peer=INSIDE, token=None) as c:
        assert (await c.get("/api/v1/meta")).status_code == 200
        assert_error(await c.get("/agent/v1/me"), 404, "not_found")
    async with client(app, 8000, peer=INSIDE, token="anything-at-all") as c:
        assert (await c.get("/api/v1/meta")).status_code == 200
        assert (await c.get("/api/v1/sessions")).status_code == 200
    # the agent port does not serve the service routes either
    async with client(app, 8001) as c:
        assert_error(await c.get("/api/v1/meta"), 404, "not_found")


def _problem(monkeypatch, value: str, file_value: str | None = None, tmp_path=None) -> str:
    monkeypatch.setenv("ORCH_SERVICE_TOKENS", value)
    if file_value is not None:
        f = tmp_path / "tokens"
        f.write_text(file_value)
        monkeypatch.setenv("ORCH_SERVICE_TOKENS_FILE", str(f))
    with pytest.raises(SettingsError) as e:
        Settings()
    assert "ORCH_SERVICE_TOKENS" in e.value.problems
    return str(e.value)


def test_service_tokens_validation(monkeypatch, tmp_path):
    s = Settings()
    assert s.service_port == 8002
    assert s.service_tokens == []
    assert s.service_token_hashes() == {}

    monkeypatch.setenv("ORCH_SERVICE_TOKENS", TOKENS)
    s = Settings()
    assert s.service_token_hashes() == {
        "agents-webhook": sha(TOKEN),
        "agents-schedule": sha(OTHER_TOKEN),
    }

    # the file wins over the variable
    f = tmp_path / "tokens"
    f.write_text(f"from-file:{sha('x')}\n")
    monkeypatch.setenv("ORCH_SERVICE_TOKENS_FILE", str(f))
    assert Settings().service_token_hashes() == {"from-file": sha("x")}
    monkeypatch.delenv("ORCH_SERVICE_TOKENS_FILE")

    good = sha(TOKEN)
    for bad in [
        "no-colon-here",
        f"Upper:{good}",
        f"-leading:{good}",
        f":{good}",
        f"{'a' * 64}:{good}",
        f"svc:{good.upper()}",
        f"svc:{good[:-1]}",
        f"svc:{good}0",
        f"svc:{'g' * 64}",
        f"svc:{good}:extra",
        f"svc a:{good}",
    ]:
        msg = _problem(monkeypatch, f"ok:{sha('y')},{bad}")
        assert bad not in msg and good not in msg and good.upper() not in msg, msg

    msg = _problem(monkeypatch, f"svc:{good},svc:{sha('z')}")
    assert good not in msg and sha("z") not in msg

    # a malformed file is the same error, still without echoing the content
    msg = _problem(monkeypatch, "", file_value=f"bad entry {good}", tmp_path=tmp_path)
    assert good not in msg
    monkeypatch.delenv("ORCH_SERVICE_TOKENS_FILE")
    monkeypatch.setenv("ORCH_SERVICE_TOKENS_FILE", str(tmp_path / "missing"))
    with pytest.raises(SettingsError) as e:
        Settings()
    assert "ORCH_SERVICE_TOKENS_FILE" in e.value.problems
    monkeypatch.delenv("ORCH_SERVICE_TOKENS_FILE")
    monkeypatch.delenv("ORCH_SERVICE_TOKENS")

    monkeypatch.setenv("ORCH_SERVICE_PORT", "8000")
    with pytest.raises(SettingsError, match="ORCH_SERVICE_PORT"):
        Settings()
    monkeypatch.setenv("ORCH_SERVICE_PORT", "8001")
    with pytest.raises(SettingsError, match="ORCH_SERVICE_PORT"):
        Settings()
    monkeypatch.setenv("ORCH_SERVICE_PORT", "9002")
    assert Settings().service_port == 9002


async def asgi_status(app, method: str, path: str, token: str | None = TOKEN) -> int:
    """One request with a raw scope path, bypassing httpx URL normalisation."""
    sent: list[dict] = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    headers = [(b"authorization", f"Bearer {token}".encode())] if token else []
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": headers,
        "client": OUTSIDE,
        "server": ("orch", 8002),
        "state": {},
    }
    await app(scope, receive, send)
    return next(m["status"] for m in sent if m["type"] == "http.response.start")


async def test_route_matching_is_exact(app):
    cid = str(uuid.uuid4())
    for path in [
        "/api/v1/meta\n",
        "/api/v1/sessions\n",
        f"/api/v1/sessions/{cid}\n",
        "/api/v1/messages/1\n",
        "/api/v1/sessions/..",
        "/api/v1/sessions/../messages",
        "/api/v1/presets/..",
        "/api/v1/messages/..",
        "/api/v1/presets/not-a-uuid",
        "/api/v1/sessions/not-a-uuid/messages",
        "/api/v1/messages/abc",
        f"/api/v1/messages/{cid}",
    ]:
        assert not service_auth.route_allowed("GET", path), path
        assert await asgi_status(app, "GET", path) == 403, path
    assert service_auth.route_allowed("GET", f"/api/v1/sessions/{cid}")
    assert service_auth.route_allowed("GET", f"/api/v1/presets/{cid.upper()}")
    assert service_auth.route_allowed("GET", "/api/v1/messages/42")
    async with client(app, 8002) as c:
        assert_error(await c.get("/api/v1/meta%0A"), 403, "forbidden")


async def test_health_unauthenticated_only_for_get_and_head(app):
    for path in ("/healthz", "/readyz"):
        assert await asgi_status(app, "GET", path, token=None) == 200
        assert await asgi_status(app, "HEAD", path, token=None) != 401
        for method in ("POST", "PUT", "DELETE", "PATCH", "OPTIONS"):
            assert await asgi_status(app, method, path, token=None) == 401, (method, path)
            assert await asgi_status(app, method, path) == 403, (method, path)


async def test_logged_path_cannot_inject_lines(app, caplog):
    caplog.set_level(logging.INFO)
    injected = "/api/v1/meta\nservice request service=admin method=GET path=/x status=200"
    assert await asgi_status(app, "GET", injected, token=None) == 401
    assert await asgi_status(app, "GET", injected) == 403
    lines = [rec.getMessage() for rec in caplog.records if rec.name == service_auth.log.name]
    assert len(lines) == 2, lines
    assert all("\n" not in m for m in lines), lines

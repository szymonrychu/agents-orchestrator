"""Origin allowlist for WebSocket upgrades and mutating HTTP routes (D5)."""

from __future__ import annotations

import httpx
import pytest
from fastapi import Depends, FastAPI
from test_api_sessions import make_settings

from agents_orchestrator.api import errors
from agents_orchestrator.api.app import create_app
from agents_orchestrator.api.origin import OriginAllowlist, require_allowed_origin
from agents_orchestrator.settings import Settings, SettingsError

DB = "postgresql://u:p@h/db"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    import os

    for k in list(os.environ):
        if k.startswith("ORCH_"):
            monkeypatch.delenv(k)
    monkeypatch.setenv("ORCH_DATABASE_URL", DB)


def test_settings_parse_and_validate(monkeypatch):
    assert Settings().allowed_origins == []
    monkeypatch.setenv("ORCH_ALLOWED_ORIGINS", "HTTP://Localhost:5173, https://Orch.Example.com")
    assert Settings().allowed_origins == ["http://localhost:5173", "https://orch.example.com"]
    for bad in (
        "http://localhost:5173/",
        "http://localhost/path",
        "localhost:5173",
        "ftp://host",
        "http://",
        "http://user@host",
        "*",
        "null",
    ):
        monkeypatch.setenv("ORCH_ALLOWED_ORIGINS", f"http://ok.example.com,{bad}")
        with pytest.raises(SettingsError, match="ORCH_ALLOWED_ORIGINS"):
            Settings()


def test_default_ports_are_dropped(monkeypatch):
    from agents_orchestrator.api.origin import origin_allowed
    from agents_orchestrator.settings import normalise_origin

    assert normalise_origin("https://Host:443") == "https://host"
    assert normalise_origin("http://host:80") == "http://host"
    assert normalise_origin("https://host:80") == "https://host:80"
    assert normalise_origin("http://host:443") == "http://host:443"
    assert normalise_origin("https://host:443/") is None
    assert normalise_origin("https://host:443?x=1") is None
    monkeypatch.setenv("ORCH_ALLOWED_ORIGINS", "https://orch.example.com:443,http://l:80")
    allowed = Settings().allowed_origins
    assert allowed == ["https://orch.example.com", "http://l"]
    assert origin_allowed("https://orch.example.com", allowed)
    assert origin_allowed("https://orch.example.com:443", allowed)
    assert origin_allowed("http://l", allowed)


async def _ws(app, origin: str | None) -> list[dict]:
    headers = [(b"host", b"orch")]
    if origin is not None:
        headers.append((b"origin", origin.encode()))
    scope = {"type": "websocket", "path": "/ws", "headers": headers, "client": ("10.0.0.1", 1)}
    sent: list[dict] = []

    async def receive():
        return {"type": "websocket.connect"}

    async def send(message):
        sent.append(message)

    await app(scope, receive, send)
    return sent


def _inner():
    calls: list[str] = []

    async def inner(scope, receive, send):
        calls.append(scope["type"])
        await send({"type": "websocket.accept"})

    return inner, calls


async def test_websocket_origin_rule():
    inner, calls = _inner()
    app = OriginAllowlist(inner, ["http://localhost:5173"])

    assert await _ws(app, "http://localhost:5173") == [{"type": "websocket.accept"}]
    assert await _ws(app, "HTTP://LOCALHOST:5173") == [{"type": "websocket.accept"}]
    assert await _ws(app, None) == [{"type": "websocket.accept"}]
    assert len(calls) == 3
    for bad in ("http://evil.example", "null", "http://localhost:5174", "http://localhost:5173/x"):
        assert await _ws(app, bad) == [{"type": "websocket.close", "code": 1008}], bad
    assert len(calls) == 3  # refused before the app ran, so before accept


async def test_create_app_wires_origin_and_source_allowlists():
    app = create_app(
        make_settings(allowed_cidrs="10.0.0.0/8", allowed_origins=["http://localhost:5173"])
    )
    refused = [{"type": "websocket.close", "code": 1008}]
    # no websocket route is registered, so an upgrade that passes both middlewares is not 1008
    assert await _ws(app, "http://evil.example") == refused
    assert await _ws(app, "null") == refused
    assert await _ws(app, "http://localhost:5173") != refused
    assert await _ws(app, None) != refused

    # the source allowlist still runs: a peer outside the CIDR is refused even with a good Origin
    scope = {
        "type": "websocket",
        "path": "/ws",
        "headers": [(b"origin", b"http://localhost:5173")],
        "client": ("192.168.1.1", 1),
    }
    sent: list[dict] = []

    async def receive():
        return {"type": "websocket.connect"}

    async def send(message):
        sent.append(message)

    await app(scope, receive, send)
    assert sent == refused

    # the default empty list refuses every Origin-bearing upgrade
    default = create_app(make_settings(allowed_cidrs="10.0.0.0/8"))
    assert await _ws(default, "http://localhost:5173") == refused


async def test_empty_list_fails_closed():
    inner, calls = _inner()
    app = OriginAllowlist(inner, [])
    assert await _ws(app, "http://localhost:5173") == [{"type": "websocket.close", "code": 1008}]
    assert await _ws(app, "null") == [{"type": "websocket.close", "code": 1008}]
    assert calls == []
    assert await _ws(app, None) == [{"type": "websocket.accept"}]


async def test_non_websocket_scopes_pass_through():
    seen: list[str] = []

    async def inner(scope, receive, send):
        seen.append(scope["type"])

    app = OriginAllowlist(inner, [])
    for kind in ("http", "lifespan"):
        await app({"type": kind, "headers": [(b"origin", b"http://evil.example")]}, None, None)
    assert seen == ["http", "lifespan"]


def _http_app(origins: list[str]) -> FastAPI:
    app = FastAPI()
    errors.install(app)
    app.state.settings = Settings(allowed_origins=origins)

    @app.post("/guarded", dependencies=[Depends(require_allowed_origin)])
    async def guarded() -> dict:
        return {"ok": True}

    return app


async def _post(app, origin: str | None) -> httpx.Response:
    headers = {} if origin is None else {"Origin": origin}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://orch"
    ) as c:
        return await c.post("/guarded", headers=headers)


async def test_http_dependency():
    app = _http_app(["http://localhost:5173"])
    assert (await _post(app, "http://localhost:5173")).status_code == 200
    assert (await _post(app, None)).status_code == 200
    for bad in ("http://evil.example", "null"):
        r = await _post(app, bad)
        assert r.status_code == 403, bad
        assert r.json()["error"]["code"] == "forbidden_origin"
    r = await _post(_http_app([]), "http://localhost:5173")
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden_origin"

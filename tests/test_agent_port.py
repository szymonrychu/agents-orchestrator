"""Agent port listener: two sockets, one server, path gate by accepting port."""

from __future__ import annotations

import asyncio
import socket

import httpx
import pytest
from fastapi import FastAPI
from test_api_agent import client_for as agent_client
from test_api_agent import make_settings

from agents_orchestrator import cli
from agents_orchestrator.api.allowlist import PortGate, SourceAllowlist
from agents_orchestrator.api.app import create_app
from agents_orchestrator.settings import Settings

DB = "postgresql://u:p@h/db"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    import os

    for k in list(os.environ):
        if k.startswith("ORCH_"):
            monkeypatch.delenv(k)
    monkeypatch.setenv("ORCH_DATABASE_URL", DB)


def make_app() -> FastAPI:
    import ipaddress

    app = FastAPI()

    @app.get("/agent/v1/ping")
    async def ping():
        return {"ok": True}

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app.get("/api/v1/x")
    async def x():
        return {"x": 1}

    app.add_middleware(
        SourceAllowlist, networks=[ipaddress.ip_network("10.0.0.0/8")], skip_port=8001
    )
    app.add_middleware(PortGate, agent_port=8001)
    return app


async def get(url: str, client=("203.0.113.9", 1234)):
    transport = httpx.ASGITransport(app=make_app(), client=client)
    async with httpx.AsyncClient(transport=transport) as c:
        return await c.get(url)


def test_agent_settings(monkeypatch):
    s = Settings()
    assert s.agent_port == 8001
    assert s.agent_api_url is None
    monkeypatch.setenv("ORCH_AGENT_PORT", "9001")
    monkeypatch.setenv("ORCH_AGENT_API_URL", "http://agents.ns:8001")
    s = Settings()
    assert s.agent_port == 9001
    assert s.agent_api_url == "http://agents.ns:8001"


def test_agent_port_serves_only_agent_paths():
    async def run():
        ok = await get("http://h:8001/agent/v1/ping")
        health = await get("http://h:8001/healthz")
        other = await get("http://h:8001/api/v1/x")
        return ok, health, other

    ok, health, other = asyncio.run(run())
    assert ok.status_code == 200  # source 203.0.113.9 is outside the allowlist
    assert health.status_code == 200
    assert other.status_code == 404
    assert other.json()["error"]["code"] == "not_found"


async def test_proposal_main_routes_are_not_on_the_agent_port(pool, dsn):
    """The human's proposal routes exist on the main port and answer 404 on the agent port."""
    app = create_app(make_settings(dsn), pool)
    served = app.openapi()["paths"]
    for method, path in [
        ("GET", "/api/v1/proposals"),
        ("GET", "/api/v1/proposals/{proposal_id}"),
        ("POST", "/api/v1/proposals/{proposal_id}/review"),
    ]:
        assert method.lower() in served[path], path
        async with agent_client(app) as http:
            r = await http.request(method, path.replace("{proposal_id}", "1"), json={})
        assert r.status_code == 404, (method, path)
        assert r.json()["error"]["code"] == "not_found"


def test_main_port_unchanged():
    async def run():
        return (
            await get("http://h:8000/agent/v1/ping", client=("10.1.1.1", 1)),
            await get("http://h:8000/api/v1/x", client=("203.0.113.9", 1)),
            await get("http://h:8000/api/v1/x", client=("10.1.1.1", 1)),
        )

    agent, refused, allowed = asyncio.run(run())
    assert agent.status_code == 404
    assert agent.json()["error"]["code"] == "not_found"
    assert refused.status_code == 403
    assert allowed.status_code == 200


def _free_port(host: str = "127.0.0.1", family: int = socket.AF_INET) -> int:
    with socket.socket(family) as probe:
        probe.bind((host, 0))
        return probe.getsockname()[1]


def test_three_sockets_bound():
    a, b, c = _free_port(), _free_port(), _free_port()
    settings = Settings(http_host="127.0.0.1", http_port=a, agent_port=b, service_port=c)
    socks = cli.bind_sockets(settings)
    try:
        assert sorted(s.getsockname()[1] for s in socks) == sorted([a, b, c])
        config = cli.build_uvicorn_config(settings)
        assert config.proxy_headers is False
    finally:
        for s in socks:
            s.close()


def test_bind_sockets_ipv6_host():
    if not socket.has_ipv6:
        pytest.skip("no ipv6")
    with socket.socket(socket.AF_INET6) as probe:
        try:
            probe.bind(("::1", 0))
        except OSError:
            pytest.skip("ipv6 loopback unavailable")
        a = probe.getsockname()[1]
    b = _free_port("::1", socket.AF_INET6)
    c = _free_port("::1", socket.AF_INET6)
    socks = cli.bind_sockets(Settings(http_host="::1", http_port=a, agent_port=b, service_port=c))
    try:
        assert all(s.family == socket.AF_INET6 for s in socks)
    finally:
        for s in socks:
            s.close()


def test_bind_sockets_closes_first_when_second_bind_fails(monkeypatch):
    created: list[socket.socket] = []
    real = socket.socket

    class Spy(real):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            created.append(self)

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        a = probe.getsockname()[1]
    with real() as holder:
        holder.bind(("127.0.0.1", 0))
        holder.listen(1)
        b = holder.getsockname()[1]
        monkeypatch.setattr(socket, "socket", Spy)
        with pytest.raises(OSError):
            cli.bind_sockets(Settings(http_host="127.0.0.1", http_port=a, agent_port=b))
    assert len(created) == 2
    assert all(s.fileno() == -1 for s in created)

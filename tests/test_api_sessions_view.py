"""Session provenance in the session view: which service or superleader created a session."""

from __future__ import annotations

import hashlib

import httpx
import pytest
from fakes.fake_kube import FakeKube
from test_admin_sessions import P, S, boss  # noqa: F401  (boss is a fixture)
from test_api_agent import KEY, headers
from test_api_agent import client_for as agent_client
from test_api_sessions import client_for as main_client
from test_api_sessions import make_settings

from agents_orchestrator.api.app import create_app

TOKEN = "s3cret-webhook-token-value"
DEFAULT_ID = "00000000-0000-0000-0000-000000000001"


@pytest.fixture
def app(pool, dsn):
    digest = hashlib.sha256(TOKEN.encode()).hexdigest()
    settings = make_settings(dsn, service_tokens=f"agents-webhook:{digest}", master_key=KEY)
    return create_app(settings, pool, kube=FakeKube(namespace="agents"))


def service_client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=("203.0.113.9", 4444)),
        base_url="http://orch:8002",
        headers={"Authorization": f"Bearer {TOKEN}"},
    )


async def views(http, session_id: str) -> tuple[dict, dict]:
    """The session as the list and as the detail route serve it."""
    listed = {s["id"]: s for s in (await http.get("/api/v1/sessions")).json()}
    detail = (await http.get(f"/api/v1/sessions/{session_id}")).json()
    return listed[session_id], detail


async def test_service_created_session_names_its_service_and_no_session(app):
    async with service_client(app) as http:
        r = await http.post("/api/v1/sessions", json={"preset_id": DEFAULT_ID, "slug": "wh-1"})
        assert r.status_code == 201, r.text
        created = r.json()
        listed, detail = await views(http, created["id"])
    for view in (created, listed, detail):
        assert view["created_by_service"] == "agents-webhook"
        assert view["created_by_session"] is None


async def test_ui_created_session_has_no_provenance(app):
    async with main_client(app) as http:
        r = await http.post("/api/v1/sessions", json={"preset_id": DEFAULT_ID, "slug": "ui-1"})
        assert r.status_code == 201, r.text
        listed, detail = await views(http, r.json()["id"])
    for view in (r.json(), listed, detail):
        assert view["created_by_service"] is None
        assert view["created_by_session"] is None


async def test_superleader_created_session_names_the_creating_session(app, boss):  # noqa: F811
    async with agent_client(app, boss.leader_ip) as http:
        preset = (
            await http.post(P, headers=headers(boss.id), json={"name": "worker", "spec": {}})
        ).json()
        r = await http.post(
            S, headers=headers(boss.id), json={"preset_id": preset["id"], "slug": "child-1"}
        )
        assert r.status_code == 201, r.text
    async with main_client(app) as http:
        listed, detail = await views(http, r.json()["id"])
    for view in (r.json(), listed, detail):
        assert view["created_by_service"] is None
        assert view["created_by_session"] == str(boss.id)

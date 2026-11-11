"""POST /agent/v1/admin/projects: a superleader creates a project, as the main port does."""

from __future__ import annotations

from fakes.fake_kube import FakeKube
from psycopg.rows import dict_row
from test_agent_session_messaging import alone
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error
from test_api_sessions import client_for as main_client

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app

URL = f"{ADMIN_PREFIX}/projects"
BODY = {"slug": "alpha", "name": "Alpha", "description": "first"}


async def post(pool, dsn, who, body):
    app = create_app(make_settings(dsn), pool, kube=FakeKube(namespace="agents"))
    async with client_for(app, who.leader_ip) as http:
        return app, await http.post(URL, headers=headers(who.id), json=body)


async def test_superleader_creates_project_and_audit_row(pool, dsn):
    boss = await alone(pool, "pj-boss", superleader=True)
    app, r = await post(pool, dsn, boss, BODY)
    assert r.status_code == 201, r.text
    body = r.json()
    assert {k: body[k] for k in ("slug", "name", "description", "archived")} == {
        **BODY,
        "archived": False,
    }
    async with main_client(app) as main:
        assert (await main.get("/api/v1/projects/alpha")).json()["name"] == "Alpha"
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT data FROM events WHERE session_id = %s AND type = 'admin_call'", (boss.id,)
        )
        rows = await cur.fetchall()
    assert len(rows) == 1
    data = rows[0]["data"]
    assert (data["method"], data["path"], data["status"]) == ("POST", URL, 201)
    assert data["fields"] == ["name", "slug"]


async def test_non_superleader_is_refused(pool, dsn):
    leader = await alone(pool, "pj-leader")
    _, r = await post(pool, dsn, leader, BODY)
    assert_error(r, 403, "not_superleader")


async def test_duplicate_slug_is_409(pool, dsn):
    boss = await alone(pool, "pj-dup", superleader=True)
    app, first = await post(pool, dsn, boss, BODY)
    assert first.status_code == 201
    async with client_for(app, boss.leader_ip) as http:
        r = await http.post(URL, headers=headers(boss.id), json=BODY)
    assert_error(r, 409, "project_exists")


async def test_invalid_slug_is_422(pool, dsn):
    boss = await alone(pool, "pj-bad", superleader=True)
    _, r = await post(pool, dsn, boss, {"slug": "Not A Slug!", "name": "x"})
    assert_error(r, 422, "invalid_request")

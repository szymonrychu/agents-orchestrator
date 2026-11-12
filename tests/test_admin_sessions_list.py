"""GET /agent/v1/admin/sessions: a superleader lists every session as the redacted SessionView
(unmanageable-preset and raw-spec sessions included), with the main port's filters and errors."""

from __future__ import annotations

from factories import attach_preset
from psycopg.rows import dict_row
from test_agent_session_messaging import alone, hive, other_project
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error
from test_api_sessions import client_for as main_client

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app

S = f"{ADMIN_PREFIX}/sessions"


async def list_as(app, caller, **params):
    async with client_for(app, caller.leader_ip) as http:
        return await http.get(S, headers=headers(caller.id), params=params)


def slugs(r) -> list[str]:
    assert r.status_code == 200, r.text
    return sorted(s["slug"] for s in r.json())


async def test_admin_sessions_lists_every_session(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    boss = await alone(pool, "ls-boss", superleader=True)
    hidden = await hive(pool, "ls-hidden")
    await attach_preset(pool, hidden.id, manageable=False)
    raw = await hive(pool, "ls-raw")
    other = await alone(pool, "ls-other-boss", superleader=True)
    r = await list_as(app, boss)
    assert slugs(r) == sorted(["ls-boss", "ls-hidden", "ls-raw", "ls-other-boss"])
    body = {s["slug"]: s for s in r.json()}
    assert body[hidden.slug]["id"] == str(hidden.id)
    assert body[raw.slug]["id"] == str(raw.id)
    assert body[other.slug]["spec"]["session_messaging"] is True
    assert "vault" in body[raw.slug]


async def test_admin_sessions_project_filter(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    boss = await alone(pool, "pf-boss", superleader=True)
    proj = await other_project(pool, "pf-proj")
    await hive(pool, "pf-in", project=proj)
    await hive(pool, "pf-out")
    other_boss = await alone(pool, "pf-oboss", superleader=True, project=proj)
    assert slugs(await list_as(app, boss, project=proj)) == ["pf-in", other_boss.slug]
    both = await list_as(app, boss, project=proj, superleaders="include")
    assert slugs(both) == sorted(["pf-in", "pf-oboss", "pf-boss"])
    assert slugs(await list_as(app, boss, slug="pf-out")) == ["pf-out"]


async def test_admin_sessions_unknown_project_422(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    boss = await alone(pool, "up-boss", superleader=True)
    assert_error(await list_as(app, boss, project="nope"), 422, "unknown_project")


async def test_admin_sessions_label_filter(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    boss = await alone(pool, "lf-boss", superleader=True)
    a = await hive(pool, "lf-a")
    await hive(pool, "lf-b")
    async with pool.connection() as conn:
        await conn.execute("UPDATE sessions SET labels = '{x}' WHERE id = %s", (a.id,))
    assert slugs(await list_as(app, boss, label="x")) == ["lf-a"]
    assert_error(await list_as(app, boss, label="bad label"), 422, "invalid_request")


async def test_admin_sessions_regular_leader_403(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    lead = await hive(pool, "rl-lead")
    r = await list_as(app, lead)
    assert r.status_code == 403, r.text


async def test_admin_sessions_audit_row_present(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    boss = await alone(pool, "au-boss", superleader=True)
    assert (await list_as(app, boss)).status_code == 200
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT data FROM events WHERE session_id = %s AND type = 'admin_call'", (boss.id,)
        )
        rows = [r["data"] for r in await cur.fetchall()]
    assert rows[-1]["status"] == 200 and rows[-1]["method"] == "GET" and rows[-1]["path"] == S


async def test_main_port_list_sessions_unchanged(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    await hive(pool, "mp-a")
    await alone(pool, "mp-boss", superleader=True)
    async with main_client(app) as http:
        r = await http.get("/api/v1/sessions")
        assert slugs(r) == ["mp-a", "mp-boss"]
        bad = await http.get("/api/v1/sessions", params={"project": "nope"})
        assert_error(bad, 422, "unknown_project")

"""Whole-session restart: one restart command per live agent, in one transaction (D12)."""

from __future__ import annotations

import uuid

import pytest_asyncio
from factories import insert_agent, insert_session
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app


@pytest_asyncio.fixture
async def http(pool, dsn):
    async with client_for(create_app(make_settings(dsn), pool)) as c:
        yield c


async def test_restart_enqueues_one_command_per_live_agent(http, pool):
    cl = await insert_session(pool)
    other = await insert_session(pool)
    a = await insert_agent(pool, cl["id"], ordinal=0)
    b = await insert_agent(pool, cl["id"], ordinal=1)
    gone = await insert_agent(pool, cl["id"], ordinal=2)
    foreign = await insert_agent(pool, other["id"], ordinal=0)
    async with pool.connection() as conn:
        await conn.execute("UPDATE agents SET gone_at = now() WHERE id = %s", (gone["id"],))
    r = await http.post(f"/api/v1/sessions/{cl['id']}/restart")
    assert r.status_code == 202, r.text
    ids = r.json()["command_ids"]
    assert len(ids) == 2
    got = {}
    for cid in ids:
        c = (await http.get(f"/api/v1/commands/{cid}")).json()
        assert (c["kind"], c["status"]) == ("restart", "queued")
        got[c["agent_id"]] = cid
    assert set(got) == {str(a["id"]), str(b["id"])}
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT count(*) FROM commands WHERE agent_id = ANY(%s)", ([gone["id"], foreign["id"]],)
        )
        assert (await cur.fetchone())[0] == 0


async def test_restart_with_no_agents_returns_empty_list(http, pool):
    cl = await insert_session(pool)
    r = await http.post(f"/api/v1/sessions/{cl['id']}/restart")
    assert r.status_code == 202
    assert r.json() == {"command_ids": []}


async def test_restart_missing_and_deleting(http, pool):
    assert_error(await http.post(f"/api/v1/sessions/{uuid.uuid4()}/restart"), 404, "not_found")
    cl = await insert_session(pool, desired="deleting")
    await insert_agent(pool, cl["id"])
    assert_error(await http.post(f"/api/v1/sessions/{cl['id']}/restart"), 409, "session_deleting")
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT count(*) FROM commands")
        assert (await cur.fetchone())[0] == 0

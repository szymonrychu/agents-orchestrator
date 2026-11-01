"""O8: admin restart routes. A restart of another session or agent needs no claim; a restart of
the caller's own session claims the self-restart cooldown and queues one note (D12, D13)."""

from __future__ import annotations

import asyncio

import pytest
from fakes.fake_kube import FakeKube
from psycopg.rows import dict_row
from test_agent_session_messaging import alone, hive
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app

S = f"{ADMIN_PREFIX}/sessions"


@pytest.fixture
async def boss(pool):
    return await hive(pool, "rs-boss", superleader=True)


@pytest.fixture
def app(pool, dsn):
    settings = make_settings(dsn).model_copy(update={"self_restart_cooldown_seconds": 600})
    return create_app(settings, pool, kube=FakeKube(namespace="agents"))


@pytest.fixture
def call(app, boss):
    async def go(method, path, **kw):
        async with client_for(app, boss.leader_ip) as http:
            return await http.request(method, path, headers=headers(boss.id), **kw)

    return go


async def q(pool, query, params=()):
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        return list(await cur.fetchall())


async def agents_of(pool, session_id):
    return await q(
        pool,
        "SELECT id, pod_name FROM agents WHERE session_id = %s ORDER BY ordinal",
        (session_id,),
    )


async def test_restart_of_another_session_needs_no_claim(call, pool, boss):
    other = await hive(pool, "rs-other")
    r = await call("POST", f"{S}/{other.slug}/restart")
    assert r.status_code == 202, r.text
    ids = r.json()["command_ids"]
    assert len(ids) == 2
    for cid in ids:
        c = await call("GET", f"{ADMIN_PREFIX}/commands/{cid}")
        assert c.status_code == 200, c.text
        assert (c.json()["kind"], c.json()["status"]) == ("restart", "queued")
    # as often as asked, and nothing is claimed or noted
    assert (await call("POST", f"{S}/{other.slug}/restart")).status_code == 202
    assert (await q(pool, "SELECT last_self_restart_at FROM sessions WHERE id = %s", (boss.id,)))[
        0
    ]["last_self_restart_at"] is None
    assert await q(pool, "SELECT 1 FROM messages") == []


async def test_restart_of_another_sessions_agent(call, pool):
    other = await hive(pool, "rs-other-agent")
    pod = (await agents_of(pool, other.id))[1]["pod_name"]
    r = await call("POST", f"{S}/{other.slug}/agents/{pod}/restart")
    assert r.status_code == 202, r.text
    (cmd,) = await q(pool, "SELECT agent_id, kind FROM commands")
    assert cmd["kind"] == "restart"
    assert cmd["agent_id"] == (await agents_of(pool, other.id))[1]["id"]
    assert_error(await call("POST", f"{S}/{other.slug}/agents/nope/restart"), 404, "not_found")
    assert_error(await call("POST", f"{S}/nope/restart"), 404, "not_found")
    assert_error(await call("POST", f"{S}/nope/agents/{pod}/restart"), 404, "not_found")


async def test_deleting_session_is_refused(call, pool):
    gone = await hive(pool, "rs-deleting", desired="deleting")
    assert_error(await call("POST", f"{S}/{gone.slug}/restart"), 409, "session_deleting")


async def test_own_session_restart_claims_and_notes_once(call, pool, boss):
    r = await call("POST", f"{S}/{boss.slug}/restart")
    assert r.status_code == 202, r.text
    assert len(r.json()["command_ids"]) == 2
    assert (await q(pool, "SELECT last_self_restart_at FROM sessions WHERE id = %s", (boss.id,)))[
        0
    ]["last_self_restart_at"] is not None
    notes = await q(
        pool, "SELECT text, lane, sender_session_id FROM messages WHERE session_id = %s", (boss.id,)
    )
    assert len(notes) == 1  # one for the session, not one per agent
    assert "self-initiated" in notes[0]["text"].lower()
    assert "600" in notes[0]["text"]
    assert notes[0]["lane"] == "priority"


async def test_second_own_restart_inside_the_window_is_refused(call, pool, boss):
    assert (await call("POST", f"{S}/{boss.slug}/restart")).status_code == 202
    r = await call("POST", f"{S}/{boss.slug}/restart")
    assert_error(r, 429, "restart_cooldown")
    assert 0 < r.json()["error"]["retry_after"] <= 600
    assert r.headers["retry-after"] == str(r.json()["error"]["retry_after"])
    # an own-agent restart shares the same cooldown
    pod = (await agents_of(pool, boss.id))[0]["pod_name"]
    assert_error(
        await call("POST", f"{S}/{boss.slug}/agents/{pod}/restart"), 429, "restart_cooldown"
    )
    assert len(await q(pool, "SELECT 1 FROM commands")) == 2
    assert len(await q(pool, "SELECT 1 FROM messages")) == 1


async def test_own_agent_restart_claims_and_notes(call, pool, boss):
    pod = (await agents_of(pool, boss.id))[1]["pod_name"]
    r = await call("POST", f"{S}/{boss.slug}/agents/{pod}/restart")
    assert r.status_code == 202, r.text
    assert len(await q(pool, "SELECT 1 FROM commands")) == 1
    (note,) = await q(pool, "SELECT text FROM messages WHERE session_id = %s", (boss.id,))
    assert pod in note["text"]


async def test_concurrent_own_restarts_one_wins_and_notes_once(app, pool, boss):
    async def go():
        async with client_for(app, boss.leader_ip) as http:
            return await http.post(f"{S}/{boss.slug}/restart", headers=headers(boss.id))

    results = await asyncio.gather(*(go() for _ in range(6)))
    assert sorted(r.status_code for r in results) == [202] + [429] * 5
    assert len(await q(pool, "SELECT 1 FROM messages")) == 1
    assert len(await q(pool, "SELECT 1 FROM commands")) == 2


async def test_failed_enqueue_leaves_the_claim_unspent(call, pool, boss, monkeypatch):
    from agents_orchestrator.db import queue

    async def boom(*a, **k):
        raise RuntimeError("queue down")

    monkeypatch.setattr(queue, "enqueue_message", boom)
    with pytest.raises(RuntimeError):
        await call("POST", f"{S}/{boss.slug}/restart")
    assert (await q(pool, "SELECT last_self_restart_at FROM sessions WHERE id = %s", (boss.id,)))[
        0
    ]["last_self_restart_at"] is None
    assert await q(pool, "SELECT 1 FROM commands") == []


async def test_command_status_and_missing(call, pool):
    assert_error(await call("GET", f"{ADMIN_PREFIX}/commands/999999"), 404, "not_found")


async def test_non_superleader_is_refused(pool, dsn, app):
    plain = await alone(pool, "rs-plain")
    async with client_for(app, plain.leader_ip) as http:
        r = await http.post(f"{S}/rs-plain/restart", headers=headers(plain.id))
    assert_error(r, 403, "not_superleader")

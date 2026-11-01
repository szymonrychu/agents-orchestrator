"""O8: admin restart routes. A restart of another session or agent needs no claim; a restart of
the caller's own session claims the self-restart cooldown and queues one note (D12, D13)."""

from __future__ import annotations

import asyncio

import pytest
from factories import attach_preset
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
    await attach_preset(pool, other.id)
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
    await attach_preset(pool, other.id)
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
    await attach_preset(pool, gone.id)
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


async def finish_restarts(pool, n, status="done"):
    from agents_orchestrator.db import queue

    for _ in range(n):
        async with pool.connection() as conn:
            cmd = await queue.claim_command(conn, ["restart"])
            assert cmd is not None
            assert await queue.complete_command(conn, cmd.id, status, {})


async def held(pool, session_id):
    (row,) = await q(
        pool,
        "SELECT (not_before IS NOT NULL AND not_before > now()) AS held FROM messages"
        " WHERE session_id = %s",
        (session_id,),
    )
    return row["held"]


async def replace_pods(pool, session_id):
    """The replacement pods are up: ready, with a wrapper session attached after the restart."""
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE agents SET ready = true, wrapper_session_created_at = clock_timestamp()"
            " WHERE session_id = %s",
            (session_id,),
        )


async def release(pool):
    from agents_orchestrator.db import queue

    async with pool.connection() as conn:
        await queue.release_ready_notes(conn)


async def test_self_note_stays_held_while_the_old_pod_terminates(call, pool, boss):
    from agents_orchestrator.db import queue

    assert (await call("POST", f"{S}/{boss.slug}/restart")).status_code == 202
    assert await held(pool, boss.id)
    async with pool.connection() as conn:
        assert await queue.claim_and_pick(conn) is None  # not deliverable while held
    await finish_restarts(pool, 2)  # the pods are deleted; the old ones are still terminating
    await release(pool)
    assert await held(pool, boss.id)


async def test_self_note_is_released_when_every_replacement_is_up(call, pool, boss):
    await call("POST", f"{S}/{boss.slug}/restart")
    await finish_restarts(pool, 2)
    first = (await agents_of(pool, boss.id))[0]["id"]
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE agents SET wrapper_session_created_at = clock_timestamp() WHERE id = %s",
            (first,),
        )
    await release(pool)
    assert await held(pool, boss.id)  # one replacement still missing
    await replace_pods(pool, boss.id)
    await release(pool)
    assert not await held(pool, boss.id)


async def test_a_resumed_replacement_releases_the_note(call, pool, boss):
    pod = (await agents_of(pool, boss.id))[0]["pod_name"]
    await call("POST", f"{S}/{boss.slug}/agents/{pod}/restart")
    await finish_restarts(pool, 1)
    await release(pool)
    assert await held(pool, boss.id)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE agents SET wrapper_resumed_at = clock_timestamp() WHERE session_id = %s"
            " AND pod_name = %s",
            (boss.id, pod),
        )
    await release(pool)
    assert not await held(pool, boss.id)


async def test_a_failed_restart_releases_the_note(call, pool, boss):
    pod = (await agents_of(pool, boss.id))[0]["pod_name"]
    await call("POST", f"{S}/{boss.slug}/agents/{pod}/restart")
    await finish_restarts(pool, 1, "failed")  # no pod was replaced
    await release(pool)
    assert not await held(pool, boss.id)


async def test_a_gone_agent_does_not_hold_the_note(call, pool, boss):
    await call("POST", f"{S}/{boss.slug}/restart")
    await finish_restarts(pool, 2)
    async with pool.connection() as conn:
        await conn.execute("UPDATE agents SET gone_at = now() WHERE session_id = %s", (boss.id,))
    await release(pool)
    assert not await held(pool, boss.id)


async def test_commands_are_scoped_to_the_issuing_superleader(call, pool, app, boss):
    other = await hive(pool, "rs-victim")
    await attach_preset(pool, other.id)
    mine = (await call("POST", f"{S}/{other.slug}/restart")).json()["command_ids"][0]
    assert (await call("GET", f"{ADMIN_PREFIX}/commands/{mine}")).status_code == 200
    # a command made on the main port, and one issued by another superleader, are not visible
    async with pool.connection() as conn:
        from agents_orchestrator.db import queue

        main_cmd = await queue.enqueue_command(
            conn, (await agents_of(pool, other.id))[0]["id"], "restart"
        )
    assert_error(await call("GET", f"{ADMIN_PREFIX}/commands/{main_cmd}"), 404, "not_found")
    rival = await alone(pool, "rs-rival", superleader=True)
    async with client_for(app, rival.leader_ip) as http:
        r = await http.get(f"{ADMIN_PREFIX}/commands/{mine}", headers=headers(rival.id))
    assert_error(r, 404, "not_found")


async def test_restart_hidden_session_404(call, pool):
    other = await hive(pool, "rs-hidden")
    await attach_preset(pool, other.id, manageable=False)
    assert_error(await call("POST", f"{S}/{other.slug}/restart"), 404, "not_found")
    assert await q(pool, "SELECT 1 FROM commands") == []


async def test_restart_agent_hidden_session_404(call, pool):
    other = await hive(pool, "rs-hidden-agent")
    await attach_preset(pool, other.id, manageable=False)
    pod = (await agents_of(pool, other.id))[0]["pod_name"]
    assert_error(await call("POST", f"{S}/{other.slug}/agents/{pod}/restart"), 404, "not_found")
    assert await q(pool, "SELECT 1 FROM commands") == []


async def test_self_restart_with_hidden_preset_still_works(call, pool, boss):
    await attach_preset(pool, boss.id, manageable=False)
    assert (await call("POST", f"{S}/{boss.slug}/restart")).status_code == 202
    assert_error(await call("POST", f"{S}/{boss.slug}/restart"), 429, "restart_cooldown")

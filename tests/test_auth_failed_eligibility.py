"""An agent whose claude shows a re-login screen (T-225) is out of dispatch and leadership."""

from __future__ import annotations

from factories import insert_agent, set_agent_status
from test_api_sessions import client_for, make_settings
from test_leadership_pass import free_ago, hive, leader_row, lpass, run, set_leader
from test_queue_leader_routing import _agent as queue_agent
from test_queue_leader_routing import _claim, _enqueue
from test_sessions_repo import _make_leader, _refused, _scalable

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import leadership, status


async def _fail(pool, agent_id) -> None:
    async with pool.connection() as conn:
        await status.set_auth_failed(conn, agent_id, "login")


async def _past_grace(pool, session_id) -> None:
    await run(
        pool,
        "UPDATE session_leaders SET unready_since = now() - make_interval(secs => %s)"
        " WHERE session_id = %s",
        (leadership.LEADER_GRACE_SECONDS + 1, session_id),
    )


async def test_claim_skips_auth_failed_agent(pool):
    from factories import insert_session

    c = await insert_session(pool, spec={"replicas": 2})
    failed = await queue_agent(pool, c["id"], free_ago=100, ordinal=0)
    live = await queue_agent(pool, c["id"], free_ago=1, ordinal=1, state="booting")
    await _fail(pool, failed["id"])
    await _enqueue(pool, c["id"], "u")
    assert await _claim(pool) is None

    await free_ago(pool, live["id"], 1, state="idle")
    claim = await _claim(pool)
    assert claim is not None and claim.agent_id == live["id"]


async def test_auth_failed_agent_never_initial_leader(pool):
    c, (a0, a1, _a2) = await hive(pool)
    await _fail(pool, a0["id"])
    change = await lpass(pool, c["id"])
    assert change is not None
    assert (change.agent_id, change.epoch, change.reason) == (a1["id"], 1, "initial")

    only, (b0, b1) = await hive(pool, 2)
    for b in (b0, b1):
        await _fail(pool, b["id"])
    async with pool.connection() as conn:
        assert only["id"] not in await leadership.leadership_candidates(conn)
    assert await lpass(pool, only["id"]) is None
    assert await leader_row(pool, only["id"]) is None


async def test_auth_failed_agent_never_successor(pool):
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    # the failed peer was free the longest, the healthy one is busy
    await free_ago(pool, a1["id"], 1000)
    await free_ago(pool, a2["id"], 10, state="busy")
    await _fail(pool, a1["id"])
    await run(pool, "UPDATE agents SET gone_at = now() WHERE id = %s", (a0["id"],))
    change = await lpass(pool, c["id"])
    assert change is not None
    assert (change.agent_id, change.reason) == (a2["id"], "leader_lost")


async def test_auth_failed_leader_replaced_after_grace(pool):
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await _fail(pool, a0["id"])
    await _fail(pool, a2["id"])
    async with pool.connection() as conn:
        assert c["id"] in await leadership.leadership_candidates(conn)
    # first sight records unready_since, no takeover inside the grace
    assert await lpass(pool, c["id"]) is None
    row = await leader_row(pool, c["id"])
    assert row["unready_since"] is not None and row["agent_id"] == a0["id"]
    await _past_grace(pool, c["id"])
    change = await lpass(pool, c["id"])
    assert change is not None
    assert (change.agent_id, change.epoch, change.reason) == (a1["id"], 4, "leader_lost")


async def test_all_agents_failed_leader_stays(pool):
    c, (a0, a1) = await hive(pool, 2)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    for a in (a0, a1):
        await _fail(pool, a["id"])
    assert await lpass(pool, c["id"]) is None
    await _past_grace(pool, c["id"])
    assert await lpass(pool, c["id"]) is None
    row = await leader_row(pool, c["id"])
    assert (row["agent_id"], row["epoch"]) == (a0["id"], 3)


async def test_ready_with_session_excludes_auth_failed(pool, dsn):
    from factories import insert_session

    c = await insert_session(pool, spec={"replicas": 2})
    ok = await insert_agent(pool, c["id"], ready=True, wrapper_session_id="w-a")
    failed = await insert_agent(pool, c["id"], ready=True, wrapper_session_id="w-b")
    await set_agent_status(pool, ok["id"], state="idle")
    await set_agent_status(pool, failed["id"], state="idle")
    await _fail(pool, failed["id"])
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as http:
        summary = (await http.get(f"/api/v1/sessions/{c['id']}")).json()["summary"]
    assert summary["ready"] == 2
    assert summary["ready_with_session"] == 1


async def test_set_replicas_counts_auth_failed_ineligible(pool):
    c = await _scalable(pool, replicas=4)
    zero = await insert_agent(pool, c["id"], ordinal=0, ready=True, wrapper_session_id="w0")
    two = await insert_agent(pool, c["id"], ordinal=2, ready=True, wrapper_session_id="w2")
    await _make_leader(pool, c["id"], two["id"])
    await set_agent_status(pool, zero["id"], state="idle")
    await _fail(pool, zero["id"])
    assert await _refused(pool, c["id"], 2, caller_agent_id=two["id"]) == "leader_would_leave"
    assert await _refused(pool, c["id"], 2) == "leader_would_leave"


async def test_new_pod_snapshot_makes_agent_eligible_again(pool):
    from factories import insert_session

    c = await insert_session(pool, spec={"replicas": 1})
    agent = await queue_agent(pool, c["id"], free_ago=100, ordinal=0)
    await _fail(pool, agent["id"])
    await _enqueue(pool, c["id"], "u")
    assert await _claim(pool) is None

    async with pool.connection() as conn:
        await status.upsert_status(
            conn,
            agent["id"],
            state="idle",
            wrapper_idle_since=None,
            context=None,
            current_turn_id=None,
            last_seq=0,
            auth=(None, None),
        )
    claim = await _claim(pool)
    assert claim is not None and claim.agent_id == agent["id"]

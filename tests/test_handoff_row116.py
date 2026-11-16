# ruff: noqa: F811  (the `env` fixture imported from test_handoff_pass is a parameter name)
"""Row-116 class (spec 5.11, step 12): a `limit` leadership request exists only for a `moving`
handoff row. A compaction closes the row before any move, its queued request is rejected
`superseded` in the same transaction, and the leadership pass rejects any other queued `limit`
request `superseded`, so no stale handoff ever reaches a successor and the agent is never
cleared because of it."""

from __future__ import annotations

import pytest
from factories import insert_event
from test_handoff_pass import LOW, STRONG, Env, env, fake  # noqa: F401 (fixtures)

from agents_orchestrator.db import handoffs, leadership, queue


async def leader_of(env: Env, session_id):
    got = await env.rows(
        "SELECT agent_id FROM session_leaders WHERE session_id = %s", (session_id,)
    )
    return got[0]["agent_id"]


async def cycle_to_moving(env: Env, c: dict, a: dict) -> int:
    """Pressure, an accepted gate-clear submit and the pass's own limit request (queued)."""
    await env.gauge(a["id"], STRONG, state="busy", current_turn_id="t-busy")
    assert await env.step(c["id"]) == [f"pressuring:{a['id']}"]
    assert await env.submit(a)
    assert await env.step(c["id"]) == [f"moving:{a['id']}"]
    row = await env.row(a["id"])
    assert row.phase == "moving" and row.leadership_request_id is not None
    return row.leadership_request_id


async def compaction(env: Env, c: dict, a: dict) -> None:
    await insert_event(env.pool, c["id"], a["id"], type="compacted", data={})
    await env.gauge(a["id"], LOW, state="busy", current_turn_id="t-busy")


async def test_row116_sequence_regression(env):
    c, agents = await env.hive(3, used=LOW)
    a0, a1, a2 = agents
    requests = []
    for _ in range(2):
        rid = await cycle_to_moving(env, c, a0)
        requests.append(rid)
        assert await env.lpass(c["id"]) is None  # the leader is never idle: the request waits
        await compaction(env, c, a0)
        # the dispatcher runs the leadership pass first: still waiting at the boundary
        assert await env.lpass(c["id"]) is None
        assert await env.step(c["id"]) == [f"done:{a0['id']}:compacted"]
        got = await env.rows(
            "SELECT status, reject_reason FROM leadership_requests WHERE id = %s", (rid,)
        )
        assert (got[0]["status"], got[0]["reject_reason"]) == ("rejected", "superseded")
    # the leader finally goes idle: nothing moves, nothing is cleared
    await env.idle_for(a0["id"], 600)
    assert await env.lpass(c["id"]) is None
    assert await env.step(c["id"]) == []
    assert await leader_of(env, c["id"]) == a0["id"]
    rows = await env.rows(
        "SELECT phase, close_reason FROM agent_handoffs WHERE agent_id = %s ORDER BY id",
        (a0["id"],),
    )
    assert [(r["phase"], r["close_reason"]) for r in rows] == [("done", "compacted")] * 2
    assert {r["status"] for r in await env.requests(c["id"])} == {"rejected"}
    assert await env.commands(a0["id"]) == []
    # no stale handoff text ever reaches a successor
    for other in (a1, a2):
        assert await env.messages(other["id"]) == []
        assert (
            await env.rows(
                "SELECT id FROM agent_handoffs WHERE successor_agent_id = %s", (other["id"],)
            )
            == []
        )


@pytest.mark.parametrize("row_phase", [None, "pressuring", "done"])
async def test_limit_request_without_moving_row_rejected(env, row_phase):
    c, agents = await env.hive(3, used=LOW)
    a0 = agents[0]
    leader = await leader_of(env, c["id"])
    async with env.pool.connection() as conn:
        epoch = (await leadership.current_leader(conn, c["id"])).epoch
        rid = await leadership.insert_request(
            conn, c["id"], "handover", leader, None, "limit", epoch
        )
        if row_phase is not None:
            row = await handoffs.insert(conn, c["id"], a0["id"], "pressure", "pressuring")
            await env.run(
                "UPDATE agent_handoffs SET leadership_request_id = %s WHERE id = %s",
                (rid, row.id),
            )
            if row_phase == "done":
                await env.run(
                    "UPDATE agent_handoffs SET phase = 'done', close_reason = 'compacted',"
                    " closed_at = now() WHERE id = %s",
                    (row.id,),
                )
    await env.idle_for(a0["id"], 600)  # at the boundary: it would apply if it were live
    assert await env.lpass(c["id"]) is None
    got = await env.rows(
        "SELECT status, reject_reason FROM leadership_requests WHERE id = %s", (rid,)
    )
    assert (got[0]["status"], got[0]["reject_reason"]) == ("rejected", "superseded")
    assert await leader_of(env, c["id"]) == a0["id"]


@pytest.mark.parametrize("how", ["close", "clear"])
async def test_a_row_leaving_moving_rejects_its_queued_request(env, how):
    c, agents = await env.hive(3, used=LOW)
    a0 = agents[0]
    rid = await cycle_to_moving(env, c, a0)
    row = await env.row(a0["id"])
    async with env.pool.connection() as conn:
        if how == "close":
            assert await handoffs.close(conn, row.id, "cancelled", "shutdown")
        else:
            command_id = await queue.enqueue_command(conn, a0["id"], "clear")
            assert await handoffs.start_clear(conn, row.id, ["moving"], command_id)
    got = await env.rows(
        "SELECT status, reject_reason FROM leadership_requests WHERE id = %s", (rid,)
    )
    assert (got[0]["status"], got[0]["reject_reason"]) == ("rejected", "superseded")

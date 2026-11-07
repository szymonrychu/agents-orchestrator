"""A dead agent is never eligible as leader (design D4): every call site of the eligibility."""

from __future__ import annotations

from factories import insert_agent, insert_session, set_agent_status
from test_leadership_pass import (
    decided,
    free_ago,
    hive,
    leader_row,
    lpass,
    request,
    run,
    set_leader,
    shrink,
)
from test_queue_leader_routing import _agent as queue_agent
from test_queue_leader_routing import _claim, _enqueue, _lead
from test_sessions_repo import _make_leader, _refused, _scalable

from agents_orchestrator.db import leadership, sessions


async def dead_and_busy(pool, dead_id, live_id) -> None:
    """The dead peer was free the longest, the live one is busy: only the exclusion skips it."""
    await free_ago(pool, dead_id, 1000, state="dead")
    await free_ago(pool, live_id, 10, state="busy")


# ---------------------------------------------------------------- AC1


async def test_dead_agent_never_initial_leader(pool):
    c, (a0, a1, _a2) = await hive(pool)
    await set_agent_status(pool, a0["id"], state="dead")
    change = await lpass(pool, c["id"])
    assert change is not None
    assert (change.agent_id, change.epoch, change.reason) == (a1["id"], 1, "initial")

    # no live agent: nobody leads, and the dead one is not a candidate for a first leader
    only, (b0, b1) = await hive(pool, 2)
    for b in (b0, b1):
        await set_agent_status(pool, b["id"], state="dead")
    async with pool.connection() as conn:
        assert only["id"] not in await leadership.leadership_candidates(conn)
    assert await lpass(pool, only["id"]) is None
    assert await leader_row(pool, only["id"]) is None

    # an agent without a status row yet is still eligible
    fresh = await insert_session(pool, spec={"replicas": 1, "max_replicas": 4})
    await insert_agent(pool, fresh["id"], ordinal=0, ready=True, wrapper_session_id="w0")
    change = await lpass(pool, fresh["id"])
    assert change is not None and change.reason == "initial"


# ---------------------------------------------------------------- AC2


async def test_dead_agent_never_handover_target(pool):
    # a queued handover with no named target
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await dead_and_busy(pool, a1["id"], a2["id"])
    await request(pool, c["id"], "handover", a0["id"], None, "full", 3)
    change = await lpass(pool, c["id"])
    assert change is not None
    assert (change.agent_id, change.epoch, change.reason) == (a2["id"], 4, "handover: full")

    # a queued handover naming a dead target is rejected, the leader stays
    c, (a0, a1, _a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await set_agent_status(pool, a1["id"], state="dead")
    named = await request(pool, c["id"], "handover", a0["id"], a1["id"], "full", 3)
    assert await lpass(pool, c["id"]) is None
    assert await decided(pool, named) == ("rejected", "target_not_ready")
    assert (await leader_row(pool, c["id"]))["agent_id"] == a0["id"]

    # a queued handover with only dead peers has no peer to take it
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    for a in (a1, a2):
        await set_agent_status(pool, a["id"], state="dead")
    none = await request(pool, c["id"], "handover", a0["id"], None, "full", 3)
    assert await lpass(pool, c["id"]) is None
    assert await decided(pool, none) == ("rejected", "no_peers")

    # scale-down: the leader leaves, the staying dead agent is skipped for the staying live one
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a2["id"], epoch=3)
    await dead_and_busy(pool, a0["id"], a1["id"])
    await shrink(pool, c["id"], 2)
    change = await lpass(pool, c["id"])
    assert change is not None
    assert (change.agent_id, change.reason) == (a1["id"], "scale_down")

    # scale-down with only a dead staying agent: the leader stays and the next pass retries
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a2["id"], epoch=3)
    for a in (a0, a1):
        await set_agent_status(pool, a["id"], state="dead")
    await shrink(pool, c["id"], 2)
    assert await lpass(pool, c["id"]) is None
    assert (await leader_row(pool, c["id"]))["agent_id"] == a2["id"]

    # lost leader (gone)
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await dead_and_busy(pool, a1["id"], a2["id"])
    await run(pool, "UPDATE agents SET gone_at = now() WHERE id = %s", (a0["id"],))
    change = await lpass(pool, c["id"])
    assert change is not None
    assert (change.agent_id, change.reason) == (a2["id"], "leader_lost")


# ---------------------------------------------------------------- AC3


async def test_make_leader_to_dead_agent_rejected(pool):
    c, (a0, a1, _a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await set_agent_status(pool, a1["id"], state="dead")
    rid = await request(pool, c["id"], "user", None, a1["id"], "user", None)
    assert await lpass(pool, c["id"]) is None
    assert await decided(pool, rid) == ("rejected", "target_not_ready")
    row = await leader_row(pool, c["id"])
    assert (row["agent_id"], row["epoch"]) == (a0["id"], 3)

    # the same target is accepted once it is live again
    await set_agent_status(pool, a1["id"], state="idle")
    again = await request(pool, c["id"], "user", None, a1["id"], "user", None)
    change = await lpass(pool, c["id"])
    assert change is not None and (change.agent_id, change.reason) == (a1["id"], "user")
    assert await decided(pool, again) == ("applied", None)


# ---------------------------------------------------------------- AC4


async def test_dead_leader_replaced_after_grace(pool):
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await dead_and_busy(pool, a1["id"], a2["id"])
    await set_agent_status(pool, a0["id"], state="dead")
    async with pool.connection() as conn:
        assert c["id"] in await leadership.leadership_candidates(conn)
    # first sight records unready_since, no takeover inside the grace
    assert await lpass(pool, c["id"]) is None
    row = await leader_row(pool, c["id"])
    assert row["unready_since"] is not None and (row["agent_id"], row["epoch"]) == (a0["id"], 3)
    assert await lpass(pool, c["id"]) is None
    # past the grace: a live agent takes over, never the dead peer
    await run(
        pool,
        "UPDATE session_leaders SET unready_since = now() - make_interval(secs => %s)"
        " WHERE session_id = %s",
        (leadership.LEADER_GRACE_SECONDS + 1, c["id"]),
    )
    change = await lpass(pool, c["id"])
    assert change is not None
    assert (change.agent_id, change.epoch, change.reason) == (a2["id"], 4, "leader_lost")
    assert (await leader_row(pool, c["id"]))["unready_since"] is None

    # a leader that is live again before the grace ends clears the mark
    c, (a0, _a1, _a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await set_agent_status(pool, a0["id"], state="dead")
    assert await lpass(pool, c["id"]) is None
    assert (await leader_row(pool, c["id"]))["unready_since"] is not None
    await set_agent_status(pool, a0["id"], state="idle")
    assert await lpass(pool, c["id"]) is None
    row = await leader_row(pool, c["id"])
    assert (row["agent_id"], row["epoch"], row["unready_since"]) == (a0["id"], 3, None)

    # with only dead agents the dead leader stays and the move is retried
    c, (a0, a1) = await hive(pool, 2)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    for a in (a0, a1):
        await set_agent_status(pool, a["id"], state="dead")
    await run(
        pool,
        "UPDATE session_leaders SET unready_since = now() - interval '1 hour'"
        " WHERE session_id = %s",
        (c["id"],),
    )
    assert await lpass(pool, c["id"]) is None
    assert (await leader_row(pool, c["id"]))["agent_id"] == a0["id"]


# ---------------------------------------------------------------- AC5


async def test_claim_hold_ignores_dead_peer(pool):
    c = await insert_session(pool, spec={"replicas": 1, "max_replicas": 4})
    await queue_agent(pool, c["id"], free_ago=100, ordinal=0, state="dead")
    leader = await queue_agent(pool, c["id"], free_ago=1, ordinal=1)
    await _lead(pool, c["id"], leader["id"])
    await _enqueue(pool, c["id"], "u")
    claim = await _claim(pool)
    assert claim is not None
    assert claim.agent_id == leader["id"]


# ---------------------------------------------------------------- AC6


async def test_set_replicas_counts_dead_agent_ineligible(pool):
    c = await _scalable(pool, replicas=4)
    zero = await insert_agent(pool, c["id"], ordinal=0, ready=True, wrapper_session_id="w0")
    two = await insert_agent(pool, c["id"], ordinal=2, ready=True, wrapper_session_id="w2")
    await _make_leader(pool, c["id"], two["id"])
    await set_agent_status(pool, zero["id"], state="dead")
    before = await run_spec(pool, c["id"])
    assert await _refused(pool, c["id"], 2, caller_agent_id=two["id"]) == "leader_would_leave"
    assert await _refused(pool, c["id"], 2) == "leader_would_leave"
    assert await run_spec(pool, c["id"]) == before

    # live again: the staying agent can take over, the scale-down is accepted
    await set_agent_status(pool, zero["id"], state="idle")
    async with pool.connection() as conn:
        out = await sessions.set_replicas(conn, c["id"], 2, caller_agent_id=two["id"])
    assert (out.replicas, out.previous_replicas) == (2, 4)


async def run_spec(pool, session_id) -> dict:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT spec FROM sessions WHERE id = %s", (session_id,))
        row = await cur.fetchone()
    return row[0]

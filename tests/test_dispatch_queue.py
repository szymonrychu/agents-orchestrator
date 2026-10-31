"""Queue SQL: round-robin claim, reservation, FIFO, retries, ambiguous sends, commands, checks."""

from __future__ import annotations

import asyncio
import json
import random
import uuid

import psycopg
import pytest
from factories import insert_agent, insert_message, insert_session, set_agent_status

from agents_orchestrator.db import queue as q

# ---------------------------------------------------------------- helpers


async def _ready_agent(pool, session_id, *, free_ago: float = 0, state: str = "idle", **kw):
    """A ready agent with a session whose free_since is `free_ago` seconds before now()."""
    kw.setdefault("ready", True)
    kw.setdefault("wrapper_session_id", f"s-{uuid.uuid4().hex[:6]}")
    kw.setdefault("url", f"http://agent-{uuid.uuid4().hex[:6]}:8080")
    agent = await insert_agent(pool, session_id, **kw)
    await set_agent_status(pool, agent["id"], state=state)
    await _set_free(pool, agent["id"], free_ago, state)
    return agent


async def _set_free(pool, agent_id, ago: float = 0, state: str = "idle") -> None:
    """What the ingester does on a transition into idle: free_since from the database clock."""
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE agent_status SET state=%s, free_since = now() - make_interval(secs => %s)"
            " WHERE agent_id=%s",
            (state, ago, agent_id),
        )


async def _exec(pool, query: str, params: tuple = ()) -> None:
    async with pool.connection() as conn:
        await conn.execute(query, params)


async def _msg(pool, message_id) -> dict:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT * FROM messages WHERE id=%s", (message_id,))
        cols = [d.name for d in cur.description]
        return dict(zip(cols, await cur.fetchone(), strict=True))


async def _agent(pool, agent_id) -> dict:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT * FROM agents WHERE id=%s", (agent_id,))
        cols = [d.name for d in cur.description]
        return dict(zip(cols, await cur.fetchone(), strict=True))


async def _enqueue(pool, session_id, text: str = "hi", target=None) -> int:
    async with pool.connection() as conn:
        return await q.enqueue_message(conn, session_id, text, target, lane="idle")


async def _claim(pool):
    async with pool.connection() as conn:
        return await q.claim_and_pick(conn)


async def _call(pool, fn, *args):
    async with pool.connection() as conn:
        return await fn(conn, *args)


async def _connect(dsn: str) -> psycopg.AsyncConnection:
    return await psycopg.AsyncConnection.connect(dsn)


async def _begin(conn: psycopg.AsyncConnection) -> None:
    """Open a transaction the queue function joins (as a savepoint) instead of committing."""
    await conn.execute("SELECT 1")


# ---------------------------------------------------------------- round-robin pick


async def test_claim_picks_oldest_free_since(pool):
    cl = await insert_session(pool)
    cid = cl["id"]
    await _ready_agent(pool, cid, free_ago=10)
    oldest = await _ready_agent(pool, cid, free_ago=30)
    await _ready_agent(pool, cid, free_ago=20)
    # Older free_since, but each fails one pick condition.
    await _ready_agent(pool, cid, free_ago=100, ready=False)
    await _ready_agent(pool, cid, free_ago=100, wrapper_session_id=None)
    await _ready_agent(pool, cid, free_ago=100, state="busy")
    await _ready_agent(pool, cid, free_ago=100, state="booting")
    gone = await _ready_agent(pool, cid, free_ago=100)
    await _exec(pool, "UPDATE agents SET gone_at=now() WHERE id=%s", (gone["id"],))
    mid = await _enqueue(pool, cid, "task")

    claim = await _claim(pool)

    assert claim is not None
    assert claim.message_id == mid
    assert claim.agent_id == oldest["id"]
    assert claim.agent_url == oldest["url"]
    assert claim.wrapper_session_id == oldest["wrapper_session_id"]
    assert claim.session_id == cid
    assert claim.text == "task"
    assert claim.attempts == 0
    assert claim.previous_dispatched_at is None
    m = await _msg(pool, mid)
    assert m["status"] == "dispatching"
    assert m["agent_id"] == oldest["id"]
    assert m["dispatched_at"] is not None
    assert m["heartbeat_at"] == m["dispatched_at"]
    assert claim.dispatched_at == m["dispatched_at"]
    a = await _agent(pool, oldest["id"])
    assert a["last_dispatched_at"] == m["dispatched_at"]


async def test_target_agent_restricts_pick(pool):
    cid = (await insert_session(pool))["id"]
    await _ready_agent(pool, cid, free_ago=60)
    newest = await _ready_agent(pool, cid, free_ago=1)
    mid = await _enqueue(pool, cid, "for you", newest["id"])

    claim = await _claim(pool)

    assert claim.message_id == mid
    assert claim.agent_id == newest["id"]
    assert claim.target_agent_id == newest["id"]


async def test_busy_agents_leave_message_queued(pool):
    busy_session = (await insert_session(pool))["id"]
    free_session = (await insert_session(pool))["id"]
    await _ready_agent(pool, busy_session, state="busy")
    await _ready_agent(pool, busy_session, state="busy")
    other = await _ready_agent(pool, free_session)
    stuck = await _enqueue(pool, busy_session, "first")
    moving = await _enqueue(pool, free_session, "second")

    claim = await _claim(pool)

    assert claim.message_id == moving
    assert claim.agent_id == other["id"]
    assert (await _msg(pool, stuck))["status"] == "queued"
    assert await _claim(pool) is None
    assert (await _msg(pool, stuck))["status"] == "queued"


async def test_deleting_session_is_not_dispatched(pool):
    # A deleting session is being exported and torn down (D15): a turn started now would miss
    # the discard-hook history, so its queued messages stay queued while other sessions move.
    deleting = (await insert_session(pool, desired="deleting"))["id"]
    active = (await insert_session(pool))["id"]
    await _ready_agent(pool, deleting, free_ago=60)
    other = await _ready_agent(pool, active)
    held = await _enqueue(pool, deleting, "first")
    moving = await _enqueue(pool, active, "second")

    claim = await _claim(pool)

    assert (claim.message_id, claim.agent_id) == (moving, other["id"])
    assert await _claim(pool) is None
    assert (await _msg(pool, held))["status"] == "queued"


async def test_reservation_rule_blocks_redispatch_before_idle_seen(pool):
    cid = (await insert_session(pool))["id"]
    agent = await _ready_agent(pool, cid, free_ago=5)
    m1 = await _enqueue(pool, cid, "one")
    m2 = await _enqueue(pool, cid, "two")

    c1 = await _claim(pool)
    assert c1.message_id == m1
    assert await _call(pool, q.mark_sent, m1, "turn-1") is True
    # The ingester has not yet seen busy: state is still idle with the old free_since.
    assert await _claim(pool) is None
    assert (await _msg(pool, m2))["status"] == "queued"

    await _set_free(pool, agent["id"])  # the turn ended: a newer free_since
    c2 = await _claim(pool)
    assert c2.message_id == m2
    assert c2.agent_id == agent["id"]
    assert c2.previous_dispatched_at == c1.dispatched_at
    m = await _msg(pool, m1)
    assert m["status"] == "sent"
    assert m["turn_id"] == "turn-1"


async def test_two_dispatchers_never_double_dispatch(pool, dsn):
    cid = (await insert_session(pool))["id"]
    agents = [await _ready_agent(pool, cid, free_ago=i) for i in range(5)]
    ids = [await _enqueue(pool, cid, f"m{i}") for i in range(50)]
    claims: list = []

    async def dispatcher(seed: int) -> None:
        rnd = random.Random(seed)
        conn = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
        try:
            while len(claims) < len(ids):
                claim = await q.claim_and_pick(conn)
                if claim is None:
                    await asyncio.sleep(0.002)
                    continue
                claims.append(claim)
                await asyncio.sleep(rnd.random() * 0.004)
                assert await q.mark_sent(conn, claim.message_id, f"t{claim.message_id}")
                await asyncio.sleep(rnd.random() * 0.004)
                # The turn ends: the ingester records a new idle transition.
                await conn.execute(
                    "UPDATE agent_status SET free_since = now() WHERE agent_id=%s",
                    (claim.agent_id,),
                )
        finally:
            await conn.close()

    await asyncio.wait_for(asyncio.gather(dispatcher(1), dispatcher(2)), timeout=60)

    message_ids = [c.message_id for c in claims]
    assert len(message_ids) == len(set(message_ids)) == 50
    assert sorted(message_ids) == ids
    per_free = [(c.agent_id, c.free_since) for c in claims]
    assert len(per_free) == len(set(per_free))
    assert {c.agent_id for c in claims} <= {a["id"] for a in agents}
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT count(*) FROM messages WHERE status='sent' AND session_id=%s", (cid,)
        )
        assert (await cur.fetchone())[0] == 50


async def test_fifo_per_session(pool):
    cid = (await insert_session(pool))["id"]
    a1 = await _ready_agent(pool, cid, free_ago=3)
    a2 = await _ready_agent(pool, cid, free_ago=2)
    ids = [await _enqueue(pool, cid, f"m{i}") for i in range(6)]
    order = []
    for _ in range(6):
        claim = await _claim(pool)
        assert claim is not None
        order.append(claim.message_id)
        await _call(pool, q.mark_sent, claim.message_id, f"t{claim.message_id}")
        await _set_free(pool, claim.agent_id)
    assert order == ids
    assert {a1["id"], a2["id"]} >= {(await _msg(pool, i))["agent_id"] for i in ids}


# ---------------------------------------------------------------- guards, retry, backoff


async def test_cancel_and_retry_guards(pool):
    cid = (await insert_session(pool))["id"]
    agent = await _ready_agent(pool, cid)
    queued = await _enqueue(pool, cid)
    assert await _call(pool, q.retry_message, queued) is False
    assert await _call(pool, q.cancel_message, queued) is True
    assert (await _msg(pool, queued))["status"] == "cancelled"
    assert await _call(pool, q.cancel_message, queued) is False
    assert await _call(pool, q.retry_message, queued) is False

    dispatching = await _enqueue(pool, cid)
    claim = await _claim(pool)
    assert claim.message_id == dispatching
    assert await _call(pool, q.cancel_message, dispatching) is False
    assert await _call(pool, q.retry_message, dispatching) is False

    failed = (
        await insert_message(
            pool,
            cid,
            status="failed",
            attempts=5,
            error="ambiguous",
            agent_id=agent["id"],
        )
    )["id"]
    await _exec(
        pool,
        "UPDATE messages SET not_before=now() + interval '1 hour', ambiguous_at=now() WHERE id=%s",
        (failed,),
    )
    assert await _call(pool, q.cancel_message, failed) is False
    assert await _call(pool, q.retry_message, failed) is True
    m = await _msg(pool, failed)
    assert m["status"] == "queued"
    assert m["attempts"] == 0
    assert m["error"] is None
    assert m["not_before"] is None
    assert m["ambiguous_at"] is None
    assert await _call(pool, q.retry_message, failed) is False


async def _backoff_seconds(conn, message_id) -> float | None:
    cur = await conn.execute(
        "SELECT extract(epoch from not_before - now())::float FROM messages WHERE id=%s",
        (message_id,),
    )
    return (await cur.fetchone())[0]


async def test_requeue_with_backoff_and_fail_at_max_attempts(pool, dsn):
    cid = (await insert_session(pool))["id"]
    agent = await _ready_agent(pool, cid)
    mid = (await insert_message(pool, cid, status="dispatching", agent_id=agent["id"]))["id"]
    conn = await _connect(dsn)
    try:
        # base 2, cap 5: attempts 1..3 back off 2, 4, 5 (capped); attempt 4 fails.
        for attempt, expected in [(1, 2.0), (2, 4.0), (3, 5.0)]:
            async with conn.transaction():
                status = await q.record_failure(conn, mid, "unavailable", 4, 2, 5)
                assert status == "queued"
                assert await _backoff_seconds(conn, mid) == pytest.approx(expected)
            m = await _msg(pool, mid)
            assert m["attempts"] == attempt
            assert m["error"] == "unavailable"
            assert m["status"] == "queued"
            # Not dispatching any more: the guard loses.
            assert await q.record_failure(conn, mid, "unavailable", 4, 2, 5) is None
            assert (await _msg(pool, mid))["attempts"] == attempt
            await _exec(pool, "UPDATE messages SET status='dispatching' WHERE id=%s", (mid,))
        assert await q.record_failure(conn, mid, "session_gone", 4, 2, 5) == "failed"
        await conn.commit()
    finally:
        await conn.close()
    m = await _msg(pool, mid)
    assert m["status"] == "failed"
    assert m["attempts"] == 4
    assert m["error"] == "session_gone"

    # A 409 requeue keeps attempts and sets no backoff.
    m2 = (await insert_message(pool, cid, status="dispatching", attempts=2))["id"]
    assert await _call(pool, q.requeue, m2) is True
    row = await _msg(pool, m2)
    assert (row["status"], row["attempts"], row["not_before"]) == ("queued", 2, None)
    assert await _call(pool, q.requeue, m2) is False


async def test_stale_dispatching_moves_to_verifying(pool):
    cid = (await insert_session(pool))["id"]
    agent = await _ready_agent(pool, cid, free_ago=200)
    m1 = await _enqueue(pool, cid, "slow")
    claim = await _claim(pool)
    assert claim.message_id == m1
    await _exec(
        pool,
        "UPDATE messages SET dispatched_at = now() - interval '120 seconds',"
        " heartbeat_at = NULL WHERE id=%s",
        (m1,),
    )
    fresh_session = (await insert_session(pool))["id"]
    fresh_agent = await _ready_agent(pool, fresh_session)
    await _enqueue(pool, fresh_session, "fresh")
    fresh = await _claim(pool)
    assert fresh.agent_id == fresh_agent["id"]
    before = (await _agent(pool, agent["id"]))["last_dispatched_at"]

    assert await _call(pool, q.verify_stale_dispatching, 60) == 1

    m = await _msg(pool, m1)
    assert m["status"] == "verifying"
    assert m["error"] == "dispatcher_lost"
    assert m["ambiguous_at"] is not None
    assert (await _msg(pool, fresh.message_id))["status"] == "dispatching"
    # The reservation is kept: the agent is not picked for the next message.
    assert (await _agent(pool, agent["id"]))["last_dispatched_at"] == before
    await _enqueue(pool, cid, "next")
    assert await _claim(pool) is None
    assert await _call(pool, q.verify_stale_dispatching, 60) == 0


async def test_heartbeat_defers_stale_cutoff(pool):
    cid = (await insert_session(pool))["id"]
    old = "now() - interval '120 seconds'"
    beating = (await insert_message(pool, cid, status="dispatching"))["id"]
    silent = (await insert_message(pool, cid, status="dispatching"))["id"]
    await _exec(
        pool,
        f"UPDATE messages SET dispatched_at={old}, heartbeat_at={old} WHERE id = ANY(%s)",
        ([beating, silent],),
    )

    assert await _call(pool, q.heartbeat_dispatching, beating) is True
    assert await _call(pool, q.verify_stale_dispatching, 60) == 1

    assert (await _msg(pool, beating))["status"] == "dispatching"
    m = await _msg(pool, silent)
    assert (m["status"], m["error"]) == ("verifying", "dispatcher_lost")
    # A heartbeat on a row that is no longer dispatching loses its guard.
    assert await _call(pool, q.heartbeat_dispatching, silent) is False


async def test_backoff_holds_session_head_fifo(pool):
    held = (await insert_session(pool))["id"]
    other = (await insert_session(pool))["id"]
    await _ready_agent(pool, held, free_ago=5)
    await _ready_agent(pool, held, free_ago=4)
    other_agent = await _ready_agent(pool, other)
    head = (await insert_message(pool, held, attempts=1, error="unavailable"))["id"]
    await _exec(
        pool, "UPDATE messages SET not_before = now() + interval '1 hour' WHERE id=%s", (head,)
    )
    later = await _enqueue(pool, held, "later")
    elsewhere = await _enqueue(pool, other, "elsewhere")

    claim = await _claim(pool)
    assert claim.message_id == elsewhere
    assert claim.agent_id == other_agent["id"]
    assert await _claim(pool) is None
    assert (await _msg(pool, head))["status"] == "queued"
    assert (await _msg(pool, later))["status"] == "queued"

    await _exec(
        pool, "UPDATE messages SET not_before = now() - interval '1 second' WHERE id=%s", (head,)
    )
    first = await _claim(pool)
    assert first.message_id == head
    assert first.attempts == 1
    second = await _claim(pool)
    assert second.message_id == later


# ---------------------------------------------------------------- release and interleaving


async def test_release_makes_agent_pickable_after_non_accept(pool):
    cid = (await insert_session(pool))["id"]
    agent = await _ready_agent(pool, cid, free_ago=30)

    async def outcome_409(conn, mid):
        assert await q.requeue(conn, mid) is True

    async def outcome_unavailable(conn, mid):
        assert await q.record_failure(conn, mid, "unavailable", 5, 0, 60) == "queued"

    async def outcome_gone(conn, mid):
        assert await q.record_failure(conn, mid, "session_gone", 5, 0, 60) == "queued"

    async def outcome_config_error(conn, mid):
        assert await q.fail_message(conn, mid, "config_error") is True

    async def outcome_rejected(conn, mid):
        assert await q.fail_message(conn, mid, "rejected") is True

    outcomes = [outcome_409, outcome_unavailable, outcome_gone, outcome_config_error]
    outcomes.append(outcome_rejected)
    for outcome in outcomes:
        mid = await _enqueue(pool, cid, outcome.__name__)
        claim = await _claim(pool)
        assert claim.message_id == mid, outcome.__name__
        assert claim.agent_id == agent["id"]
        async with pool.connection() as conn:
            await q.release_agent(conn, claim.agent_id, claim.previous_dispatched_at)
            await outcome(conn, mid)
        restored = (await _agent(pool, agent["id"]))["last_dispatched_at"]
        assert restored == claim.previous_dispatched_at, outcome.__name__
        # The very next claim picks the same agent again, no new free_since needed.
        nxt = await _claim(pool)
        expect = mid if outcome in (outcome_409, outcome_unavailable, outcome_gone) else None
        if expect is None:
            assert nxt is None
            assert (await _msg(pool, mid))["status"] == "failed"
            probe = await _enqueue(pool, cid, "probe")
            nxt = await _claim(pool)
            assert nxt.message_id == probe
        else:
            assert nxt.message_id == expect, outcome.__name__
        assert nxt.agent_id == agent["id"], outcome.__name__
        async with pool.connection() as conn:
            await q.release_agent(conn, nxt.agent_id, nxt.previous_dispatched_at)
            assert await q.mark_sent(conn, nxt.message_id, f"t{nxt.message_id}") is True
        await _exec(pool, "UPDATE agents SET last_dispatched_at = NULL WHERE id=%s", (agent["id"],))


async def test_release_is_guarded_by_claim_time(pool):
    cid = (await insert_session(pool))["id"]
    agent = await _ready_agent(pool, cid, free_ago=30)
    await _enqueue(pool, cid)
    claim = await _claim(pool)
    # Another dispatcher has since re-reserved the agent: a late release must not undo it.
    await _exec(pool, "UPDATE agents SET last_dispatched_at = now() WHERE id=%s", (agent["id"],))
    later = (await _agent(pool, agent["id"]))["last_dispatched_at"]
    async with pool.connection() as conn:
        released = await q.release_agent(
            conn, agent["id"], claim.previous_dispatched_at, claimed_at=claim.dispatched_at
        )
    assert released is False
    assert (await _agent(pool, agent["id"]))["last_dispatched_at"] == later


async def test_forced_interleaving_never_double_picks_agent(pool, dsn):
    cid = (await insert_session(pool))["id"]
    x = await _ready_agent(pool, cid, free_ago=30)
    y = await _ready_agent(pool, cid, free_ago=10)
    m1 = await _enqueue(pool, cid, "one")
    m2 = await _enqueue(pool, cid, "two")
    a = await _connect(dsn)
    b = await _connect(dsn)
    try:
        await _begin(a)
        ca = await q.claim_and_pick(a)
        assert (ca.message_id, ca.agent_id) == (m1, x["id"])
        # A has written x.last_dispatched_at and holds x's row, but has not committed.

        await _begin(b)
        cb = await q.claim_and_pick(b)
        assert cb is None or cb.agent_id != x["id"]
        # The agent pick on its own, bypassing the session lock, re-evaluates x and skips it.
        pick = await q._pick_agent(b, cid, None)
        assert pick is not None
        assert pick.agent_id == y["id"]
        await b.rollback()

        # B starts its transaction before A commits; its next statement sees A's write.
        await _begin(b)
        await a.commit()
        cb = await q.claim_and_pick(b)
        assert cb is not None
        assert (cb.message_id, cb.agent_id) == (m2, y["id"])
        await b.commit()

        # x is reserved by A's committed claim until a newer free_since.
        pick = await q._pick_agent(b, cid, None)
        assert pick is None
        await b.rollback()
    finally:
        await a.close()
        await b.close()
    assert (await _agent(pool, x["id"]))["last_dispatched_at"] == ca.dispatched_at


async def test_advisory_lock_keeps_fifo_under_two_dispatchers(pool, dsn):
    cid = (await insert_session(pool))["id"]
    x = await _ready_agent(pool, cid, free_ago=30)
    y = await _ready_agent(pool, cid, free_ago=10)
    other = (await insert_session(pool))["id"]
    z = await _ready_agent(pool, other)
    m1 = await _enqueue(pool, cid, "one")
    m2 = await _enqueue(pool, cid, "two")
    m3 = await _enqueue(pool, other, "elsewhere")
    a = await _connect(dsn)
    b = await _connect(dsn)
    try:
        await _begin(a)
        ca = await q.claim_and_pick(a)
        assert (ca.message_id, ca.agent_id) == (m1, x["id"])

        # While A's claim of session 1 is open, B never takes m2; other sessions still dispatch.
        await _begin(b)
        cb = await q.claim_and_pick(b)
        assert (cb.message_id, cb.agent_id) == (m3, z["id"])
        assert await q.claim_and_pick(b) is None
        await b.commit()
        assert (await _msg(pool, m2))["status"] == "queued"

        await a.commit()
        cb = await q.claim_and_pick(b)
        await b.commit()
        assert (cb.message_id, cb.agent_id) == (m2, y["id"])
    finally:
        await a.close()
        await b.close()


# ---------------------------------------------------------------- targeted messages


async def test_busy_target_does_not_block_head_of_line(pool):
    cid = (await insert_session(pool))["id"]
    busy = await _ready_agent(pool, cid, free_ago=60, state="busy")
    booting = await _ready_agent(pool, cid, free_ago=60, state="booting")
    free = await _ready_agent(pool, cid, free_ago=5)
    to_busy = await _enqueue(pool, cid, "for busy", busy["id"])
    to_booting = await _enqueue(pool, cid, "for booting", booting["id"])
    untargeted = await _enqueue(pool, cid, "anyone")

    claim = await _claim(pool)

    assert (claim.message_id, claim.agent_id) == (untargeted, free["id"])
    assert (await _msg(pool, to_busy))["status"] == "queued"
    assert (await _msg(pool, to_booting))["status"] == "queued"
    await _set_free(pool, busy["id"])
    claim = await _claim(pool)
    assert (claim.message_id, claim.agent_id) == (to_busy, busy["id"])


async def test_gone_target_fails_with_target_gone(pool):
    cid = (await insert_session(pool))["id"]
    gone = await _ready_agent(pool, cid, free_ago=60)
    await _exec(pool, "UPDATE agents SET gone_at=now() WHERE id=%s", (gone["id"],))
    free = await _ready_agent(pool, cid, free_ago=5)
    to_gone = await _enqueue(pool, cid, "for gone", gone["id"])
    to_missing = await _enqueue(pool, cid, "for nobody", uuid.uuid4())
    untargeted = await _enqueue(pool, cid, "anyone")

    claim = await _claim(pool)

    assert (claim.message_id, claim.agent_id) == (untargeted, free["id"])
    for mid in (to_gone, to_missing):
        m = await _msg(pool, mid)
        assert (m["status"], m["error"]) == ("failed", "target_gone")


# ---------------------------------------------------------------- ambiguous sends


async def test_ambiguous_keeps_reservation_until_failed(pool):
    cid = (await insert_session(pool))["id"]
    agent = await _ready_agent(pool, cid, free_ago=30)
    m1 = await _enqueue(pool, cid, "maybe")
    c1 = await _claim(pool)
    assert c1.message_id == m1
    assert await _call(pool, q.mark_verifying, m1, "ambiguous") is True
    assert await _call(pool, q.mark_verifying, m1, "ambiguous") is False
    m = await _msg(pool, m1)
    assert (m["status"], m["error"]) == ("verifying", "ambiguous")
    assert m["ambiguous_at"] is not None

    m2 = await _enqueue(pool, cid, "next")
    assert await _claim(pool) is None
    assert await _call(pool, q.fail_ambiguous, m1) is True
    assert (await _msg(pool, m1))["status"] == "failed"
    # the failed message released its reservation, so the agent is pickable again
    c2 = await _claim(pool)
    assert (c2.message_id, c2.agent_id) == (m2, agent["id"])


async def test_verifying_transitions_and_guarded_release(pool):
    cid = (await insert_session(pool))["id"]
    agent = await _ready_agent(pool, cid, free_ago=30)

    async def claim_verifying(text: str):
        mid = await _enqueue(pool, cid, text)
        claim = await _claim(pool)
        assert claim.message_id == mid
        assert await _call(pool, q.mark_verifying, mid, "ambiguous") is True
        return claim

    # Started: verifying to sent; the reservation stays.
    started = await claim_verifying("started")
    assert await _call(pool, q.resolve_started, started.message_id, "turn-9") is True
    assert await _call(pool, q.resolve_started, started.message_id, "turn-9") is False
    m = await _msg(pool, started.message_id)
    assert (m["status"], m["turn_id"], m["error"]) == ("sent", "turn-9", None)
    assert (await _agent(pool, agent["id"]))["last_dispatched_at"] == started.dispatched_at
    await _exec(pool, "UPDATE agents SET last_dispatched_at = NULL WHERE id=%s", (agent["id"],))

    # Not started, reservation still this message's: requeued with backoff, reservation cleared.
    ns = await claim_verifying("not started")
    async with pool.connection() as conn, conn.transaction():
        assert await q.resolve_not_started(conn, ns.message_id, 2, 2, 60) == "queued"
        assert await _backoff_seconds(conn, ns.message_id) == pytest.approx(2.0)
    m = await _msg(pool, ns.message_id)
    assert (m["status"], m["attempts"]) == ("queued", 1)
    assert (await _agent(pool, agent["id"]))["last_dispatched_at"] is None
    assert await _call(pool, q.resolve_not_started, ns.message_id, 2, 2, 60) is None

    # Not started at max attempts, while another claim now owns the reservation: fails,
    # and the newer reservation is left alone.
    await _exec(pool, "UPDATE messages SET not_before = NULL WHERE id=%s", (ns.message_id,))
    again = await _claim(pool)
    assert again.message_id == ns.message_id
    assert await _call(pool, q.mark_verifying, again.message_id, "dispatcher_lost") is True
    await _exec(pool, "UPDATE agents SET last_dispatched_at = now() WHERE id=%s", (agent["id"],))
    newer = (await _agent(pool, agent["id"]))["last_dispatched_at"]
    assert newer != again.dispatched_at
    assert await _call(pool, q.resolve_not_started, ns.message_id, 2, 2, 60) == "failed"
    m = await _msg(pool, ns.message_id)
    assert (m["status"], m["error"], m["attempts"]) == ("failed", "ambiguous_not_started", 2)
    assert (await _agent(pool, agent["id"]))["last_dispatched_at"] == newer

    # No answer by the deadline: failed as ambiguous.
    await _exec(pool, "UPDATE agents SET last_dispatched_at = NULL WHERE id=%s", (agent["id"],))
    amb = await claim_verifying("no answer")
    assert await _call(pool, q.fail_ambiguous, amb.message_id) is True
    assert await _call(pool, q.fail_ambiguous, amb.message_id) is False
    m = await _msg(pool, amb.message_id)
    assert (m["status"], m["error"]) == ("failed", "ambiguous")


async def test_list_verifying_age_from_db_clock(pool):
    cid = (await insert_session(pool))["id"]
    agent = await _ready_agent(pool, cid)
    mid = await _enqueue(pool, cid, "check me")
    claim = await _claim(pool)
    assert await _call(pool, q.mark_verifying, mid, "ambiguous") is True
    await _exec(
        pool, "UPDATE messages SET ambiguous_at = now() - interval '42 seconds' WHERE id=%s", (mid,)
    )
    await insert_message(pool, cid, status="dispatching")

    rows = await _call(pool, q.list_verifying, 50)

    assert len(rows) == 1
    v = rows[0]
    assert v.message_id == mid
    assert v.session_id == cid
    assert v.agent_id == agent["id"]
    assert v.agent_url == agent["url"]
    assert v.wrapper_session_id == agent["wrapper_session_id"]
    assert v.dispatched_at == claim.dispatched_at
    assert v.text == "check me"
    assert v.error == "ambiguous"
    assert 42 <= v.age_seconds < 52
    assert await _call(pool, q.list_verifying, 0) == []


# ---------------------------------------------------------------- notifications


async def test_transitions_notify(pool, dsn):
    cid = (await insert_session(pool))["id"]
    await _ready_agent(pool, cid)
    listener = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    try:
        await listener.execute("LISTEN message_queued")
        await listener.execute("LISTEN status")
        mid = await _enqueue(pool, cid)
        claim = await _claim(pool)
        await _call(pool, q.mark_sent, claim.message_id, "t1")
        got = [
            (n.channel, json.loads(n.payload))
            async for n in listener.notifies(timeout=2.0, stop_after=4)
        ]
    finally:
        await listener.close()
    assert ("message_queued", {"session_id": str(cid)}) in got
    assert got.count(("status", {"session_id": str(cid)})) >= 3
    assert mid == claim.message_id


# ---------------------------------------------------------------- commands and checks


async def test_commands_and_checks_lifecycle(pool):
    cid = (await insert_session(pool))["id"]
    agent = await _ready_agent(pool, cid)
    async with pool.connection() as conn:
        interrupt = await q.enqueue_command(conn, agent["id"], "interrupt")
        restart = await q.enqueue_command(conn, agent["id"], "restart")
        compact = await q.enqueue_command(conn, agent["id"], "compact")
        with pytest.raises(ValueError):
            await q.enqueue_command(conn, agent["id"], "explode")

    async with pool.connection() as conn:
        cmd = await q.claim_command(conn, ["restart", "rebootstrap"])
    assert (cmd.id, cmd.kind, cmd.status) == (restart, "restart", "running")
    assert (cmd.agent_id, cmd.session_id) == (agent["id"], cid)
    assert (cmd.agent_url, cmd.wrapper_session_id) == (agent["url"], agent["wrapper_session_id"])
    assert cmd.started_at is not None
    assert await _call(pool, q.claim_command, ["restart", "rebootstrap"]) is None

    first = await _call(pool, q.claim_command, ["interrupt", "compact"])
    second = await _call(pool, q.claim_command, ["interrupt", "compact"])
    assert (first.id, second.id) == (interrupt, compact)
    assert await _call(pool, q.claim_command, ["interrupt", "compact"]) is None

    assert await _call(pool, q.complete_command, restart, "done", {"deleted": "agent-0"}) is True
    assert await _call(pool, q.complete_command, restart, "failed", None) is False
    assert await _call(pool, q.complete_command, interrupt, "failed", {"error": "x"}) is True
    async with pool.connection() as conn:
        rows = await (
            await conn.execute(
                "SELECT id, status, result, finished_at FROM commands ORDER BY id",
            )
        ).fetchall()
    by_id = {r[0]: r for r in rows}
    assert by_id[restart][1:3] == ("done", {"deleted": "agent-0"})
    assert by_id[restart][3] is not None
    assert by_id[interrupt][1:3] == ("failed", {"error": "x"})
    assert by_id[compact][1] == "running"
    with pytest.raises(ValueError):
        await _call(pool, q.complete_command, compact, "queued", None)

    # Checks.
    async with pool.connection() as conn:
        c1 = await q.enqueue_check(conn, cid, "pytest -q")
        c2 = await q.enqueue_check(conn, cid, "ruff check .")
        c3 = await q.enqueue_check(conn, cid, "make")
    check = await _call(pool, q.claim_check)
    assert (check.id, check.session_id, check.command, check.status) == (
        c1,
        cid,
        "pytest -q",
        "running",
    )
    assert await _call(pool, q.set_check_job, c1, "agent-x-check-1") is True
    assert await _call(pool, q.complete_check, c1, "passed", 0, "ok\n") is True
    assert await _call(pool, q.complete_check, c1, "failed", 1, "") is False
    assert await _call(pool, q.set_check_job, c1, "late") is False
    second_check = await _call(pool, q.claim_check)
    assert second_check.id == c2
    assert await _call(pool, q.cancel_checks, cid) == 1  # only c3 is still queued
    assert await _call(pool, q.claim_check) is None
    assert await _call(pool, q.complete_check, c2, "failed", 2, "boom") is True
    with pytest.raises(ValueError):
        await _call(pool, q.complete_check, c3, "running", None, None)
    async with pool.connection() as conn:
        rows = await (
            await conn.execute(
                "SELECT id, status, job_name, exit_code, log_tail, started_at, finished_at"
                " FROM checks ORDER BY id"
            )
        ).fetchall()
    assert rows[0][:5] == (c1, "passed", "agent-x-check-1", 0, "ok\n")
    assert rows[0][5] is not None and rows[0][6] is not None
    assert rows[1][:5] == (c2, "failed", None, 2, "boom")
    assert rows[2][1] == "cancelled"
    assert rows[2][6] is not None

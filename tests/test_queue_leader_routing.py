"""Leader-only routing: an untargeted message behaves as if targeted to the session leader."""

from __future__ import annotations

import uuid

from factories import insert_agent, insert_session, set_agent_status

from agents_orchestrator.db import queue as q


async def _agent(pool, session_id, *, free_ago: float, state: str = "idle", **kw):
    agent = await insert_agent(
        pool,
        session_id,
        ready=True,
        wrapper_session_id=f"s-{uuid.uuid4().hex[:6]}",
        url=f"http://agent-{uuid.uuid4().hex[:6]}:8080",
        **kw,
    )
    await set_agent_status(pool, agent["id"], state=state)
    await _exec(
        pool,
        "UPDATE agent_status SET free_since = now() - make_interval(secs => %s) WHERE agent_id=%s",
        (free_ago, agent["id"]),
    )
    return agent


async def _exec(pool, query: str, params: tuple = ()) -> None:
    async with pool.connection() as conn:
        await conn.execute(query, params)


async def _lead(pool, session_id, agent_id, epoch: int = 1) -> None:
    await _exec(
        pool,
        "INSERT INTO session_leaders (session_id, agent_id, epoch, since, reason)"
        " VALUES (%s, %s, %s, now(), 'initial')"
        " ON CONFLICT (session_id) DO UPDATE SET agent_id = EXCLUDED.agent_id,"
        " epoch = EXCLUDED.epoch",
        (session_id, agent_id, epoch),
    )


async def _enqueue(pool, session_id, text: str = "hi", target=None) -> int:
    async with pool.connection() as conn:
        return await q.enqueue_message(conn, session_id, text, target, lane="idle")


async def _claim(pool):
    async with pool.connection() as conn:
        return await q.claim_and_pick(conn)


async def _status(pool, message_id) -> str:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT status FROM messages WHERE id=%s", (message_id,))
        return (await cur.fetchone())[0]


async def test_untargeted_goes_to_leader(pool):
    c = await insert_session(pool)
    other = await _agent(pool, c["id"], free_ago=100, ordinal=0)
    leader = await _agent(pool, c["id"], free_ago=1, ordinal=1)
    await _lead(pool, c["id"], leader["id"])
    await _enqueue(pool, c["id"])
    claim = await _claim(pool)
    assert claim is not None
    assert claim.agent_id == leader["id"]
    assert claim.agent_id != other["id"]


async def test_busy_leader_holds_untargeted_only(pool):
    c = await insert_session(pool)
    member = await _agent(pool, c["id"], free_ago=10, ordinal=0)
    leader = await _agent(pool, c["id"], free_ago=10, ordinal=1, state="busy")
    await _lead(pool, c["id"], leader["id"])
    first = await _enqueue(pool, c["id"], "u1")
    second = await _enqueue(pool, c["id"], "u2")
    targeted = await _enqueue(pool, c["id"], "t", target=member["id"])
    claim = await _claim(pool)
    assert claim is not None
    assert claim.message_id == targeted
    assert claim.agent_id == member["id"]
    assert await _claim(pool) is None
    assert await _status(pool, first) == "queued"
    assert await _status(pool, second) == "queued"


async def test_explicit_target_wins(pool):
    c = await insert_session(pool)
    member = await _agent(pool, c["id"], free_ago=5, ordinal=0)
    leader = await _agent(pool, c["id"], free_ago=50, ordinal=1)
    await _lead(pool, c["id"], leader["id"])
    mid = await _enqueue(pool, c["id"], "t", target=member["id"])
    claim = await _claim(pool)
    assert claim is not None
    assert (claim.message_id, claim.agent_id) == (mid, member["id"])


async def test_gone_leader_waits(pool):
    c = await insert_session(pool)
    await _agent(pool, c["id"], free_ago=50, ordinal=0)
    leader = await _agent(pool, c["id"], free_ago=50, ordinal=1)
    await _lead(pool, c["id"], leader["id"])
    await _exec(pool, "UPDATE agents SET gone_at = now() WHERE id=%s", (leader["id"],))
    mid = await _enqueue(pool, c["id"])
    assert await _claim(pool) is None
    assert await _status(pool, mid) == "queued"


async def test_new_leader_after_handover(pool):
    c = await insert_session(pool)
    old = await _agent(pool, c["id"], free_ago=50, ordinal=0)
    new = await _agent(pool, c["id"], free_ago=5, ordinal=1)
    await _lead(pool, c["id"], old["id"])
    await _enqueue(pool, c["id"], "a")
    first = await _claim(pool)
    assert first is not None
    assert first.agent_id == old["id"]
    await _lead(pool, c["id"], new["id"], epoch=2)
    await _enqueue(pool, c["id"], "b")
    second = await _claim(pool)
    assert second is not None
    assert second.agent_id == new["id"]


async def test_held_untargeted_backoff_does_not_starve_targeted(pool):
    c = await insert_session(pool)
    member = await _agent(pool, c["id"], free_ago=10, ordinal=0)
    leader = await _agent(pool, c["id"], free_ago=10, ordinal=1, state="busy")
    await _lead(pool, c["id"], leader["id"])
    await _enqueue(pool, c["id"], "u1")
    waiting = await _enqueue(pool, c["id"], "u2")
    await _exec(
        pool,
        "UPDATE messages SET not_before = now() + interval '1 hour' WHERE id=%s",
        (waiting,),
    )
    targeted = await _enqueue(pool, c["id"], "t", target=member["id"])
    claim = await _claim(pool)
    assert claim is not None
    assert claim.message_id == targeted


async def _compacted(pool, agent_id) -> None:
    """Stamp a compaction reported after any term that began just now."""
    await _exec(
        pool,
        "UPDATE agent_status SET compaction_at = now() + interval '5 seconds' WHERE agent_id=%s",
        (agent_id,),
    )


async def test_compacted_leader_holds_untargeted(pool):
    c = await insert_session(pool, spec={"replicas": 2, "max_replicas": 4})
    leader = await _agent(pool, c["id"], free_ago=1, ordinal=0)
    member = await _agent(pool, c["id"], free_ago=100, ordinal=1)
    await _lead(pool, c["id"], leader["id"])
    await _compacted(pool, leader["id"])
    untargeted = await _enqueue(pool, c["id"], "u")
    targeted = await _enqueue(pool, c["id"], "t", target=member["id"])
    claim = await _claim(pool)
    assert claim is not None
    assert (claim.message_id, claim.agent_id) == (targeted, member["id"])
    assert await _claim(pool) is None
    assert await _status(pool, untargeted) == "queued"


async def test_leaving_leader_holds_untargeted(pool):
    c = await insert_session(pool, spec={"replicas": 1, "max_replicas": 4})
    stays = await _agent(pool, c["id"], free_ago=100, ordinal=0)
    leader = await _agent(pool, c["id"], free_ago=1, ordinal=1)
    await _lead(pool, c["id"], leader["id"])
    untargeted = await _enqueue(pool, c["id"], "u")
    targeted = await _enqueue(pool, c["id"], "t", target=stays["id"])
    claim = await _claim(pool)
    assert claim is not None
    assert (claim.message_id, claim.agent_id) == (targeted, stays["id"])
    assert await _claim(pool) is None
    assert await _status(pool, untargeted) == "queued"


async def test_compacted_leader_without_peer_keeps_leading(pool):
    c = await insert_session(pool, spec={"replicas": 2, "max_replicas": 4})
    leader = await _agent(pool, c["id"], free_ago=1, ordinal=0)
    peer = await _agent(pool, c["id"], free_ago=100, ordinal=1)
    await _exec(pool, "UPDATE agents SET ready = false WHERE id=%s", (peer["id"],))
    await _lead(pool, c["id"], leader["id"])
    await _compacted(pool, leader["id"])
    await _enqueue(pool, c["id"], "u")
    claim = await _claim(pool)
    assert claim is not None
    assert claim.agent_id == leader["id"]


async def test_compaction_before_the_term_does_not_hold(pool):
    c = await insert_session(pool, spec={"replicas": 2, "max_replicas": 4})
    leader = await _agent(pool, c["id"], free_ago=1, ordinal=0)
    await _agent(pool, c["id"], free_ago=100, ordinal=1)
    await _lead(pool, c["id"], leader["id"])
    await _exec(
        pool,
        "UPDATE agent_status SET compaction_at = now() - interval '5 seconds' WHERE agent_id=%s",
        (leader["id"],),
    )
    await _enqueue(pool, c["id"], "u")
    claim = await _claim(pool)
    assert claim is not None
    assert claim.agent_id == leader["id"]

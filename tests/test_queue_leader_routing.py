"""Leader-only routing: an untargeted message behaves as if targeted to the session leader."""

from __future__ import annotations

import uuid

from factories import insert_agent, insert_session, set_agent_status
from fakes.fake_wrapper import FakeWrapper

from agents_orchestrator.db import queue as q
from agents_orchestrator.dispatcher import Dispatcher
from agents_orchestrator.settings import Settings
from agents_orchestrator.tokens import derive_tokens
from agents_orchestrator.wrapper.client import WrapperClient

KEY = "k" * 32


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


async def _enqueue(pool, session_id, text: str = "hi", target=None, kind: str = "turn") -> int:
    async with pool.connection() as conn:
        return await q.enqueue_message(conn, session_id, text, target, kind=kind)


async def _command(pool, session_id, text: str = "/compact", target=None) -> int:
    return await _enqueue(pool, session_id, text, target, kind="command")


async def _sent_command(pool, message_id: int, wrapper_command_id: str = "cmd_1") -> None:
    async with pool.connection() as conn:
        assert await q.mark_sent_command(conn, message_id, wrapper_command_id)


async def _last_dispatched(pool, agent_id):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT last_dispatched_at FROM agents WHERE id=%s", (agent_id,))
        return (await cur.fetchone())[0]


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


# ---------------------------------------------------------------- kind=command messages (D10)


async def test_command_claim_keeps_reservation_free(pool):
    c = await insert_session(pool)
    agent = await _agent(pool, c["id"], free_ago=10)
    cmd = await _command(pool, c["id"], "/exit")
    claim = await _claim(pool)
    assert claim is not None
    assert (claim.message_id, claim.kind, claim.agent_id) == (cmd, "command", agent["id"])
    assert await _last_dispatched(pool, agent["id"]) is None
    async with pool.connection() as conn:
        assert await q.fail_message(conn, cmd, "command_denied")  # the wrapper refused it

    prose = await _enqueue(pool, c["id"], "next")
    claim = await _claim(pool)
    assert claim is not None
    assert (claim.message_id, claim.kind, claim.agent_id) == (prose, "turn", agent["id"])
    assert await _last_dispatched(pool, agent["id"]) == claim.dispatched_at


async def test_command_skips_leader_holds(pool):
    # a busy leader holds untargeted prose; a command overtakes it to the leader
    c = await insert_session(pool)
    await _agent(pool, c["id"], free_ago=10, ordinal=0)
    leader = await _agent(pool, c["id"], free_ago=10, ordinal=1, state="busy")
    await _lead(pool, c["id"], leader["id"])
    u1 = await _enqueue(pool, c["id"], "u1")
    u2 = await _enqueue(pool, c["id"], "u2")
    cmd = await _command(pool, c["id"])
    claim = await _claim(pool)
    assert claim is not None
    assert (claim.message_id, claim.kind, claim.agent_id) == (cmd, "command", leader["id"])
    await _sent_command(pool, cmd)
    assert await _claim(pool) is None  # the prose still waits for the busy leader
    await _exec(
        pool,
        "UPDATE agent_status SET state = 'idle', free_since = now() WHERE agent_id=%s",
        (leader["id"],),
    )
    claim = await _claim(pool)
    assert claim is not None
    assert (claim.message_id, claim.agent_id) == (u1, leader["id"])  # prose order unchanged
    assert await _status(pool, u2) == "queued"

    # a pending handover holds untargeted prose; a command still goes to the current leader
    c2 = await insert_session(pool, spec={"replicas": 2, "max_replicas": 4})
    leader2 = await _agent(pool, c2["id"], free_ago=1, ordinal=0)
    await _agent(pool, c2["id"], free_ago=100, ordinal=1)
    await _lead(pool, c2["id"], leader2["id"])
    await _compacted(pool, leader2["id"])
    held = await _enqueue(pool, c2["id"], "u")
    cmd2 = await _command(pool, c2["id"], "/context")
    claim = await _claim(pool)
    assert claim is not None
    assert (claim.message_id, claim.agent_id) == (cmd2, leader2["id"])
    await _sent_command(pool, cmd2, "cmd_2")
    assert await _claim(pool) is None
    assert await _status(pool, held) == "queued"

    # backoff is not exempt: a command waiting out its not_before waits, and so does a command
    # behind an untargeted head in backoff
    c3 = await insert_session(pool)
    leader3 = await _agent(pool, c3["id"], free_ago=10)
    await _lead(pool, c3["id"], leader3["id"])
    own = await _command(pool, c3["id"])
    await _exec(
        pool, "UPDATE messages SET not_before = now() + interval '1 hour' WHERE id=%s", (own,)
    )
    assert await _claim(pool) is None
    await _exec(pool, "UPDATE messages SET kind = 'turn' WHERE id=%s", (own,))
    behind = await _command(pool, c3["id"], "/context")
    assert await _claim(pool) is None
    assert await _status(pool, behind) == "queued"
    await _exec(pool, "UPDATE messages SET status = 'cancelled' WHERE session_id=%s", (c3["id"],))

    # a gone leader is not exempt: the command waits for the successor
    c4 = await insert_session(pool)
    successor = await _agent(pool, c4["id"], free_ago=10, ordinal=0)
    gone = await _agent(pool, c4["id"], free_ago=10, ordinal=1)
    await _lead(pool, c4["id"], gone["id"])
    await _exec(pool, "UPDATE agents SET gone_at = now() WHERE id=%s", (gone["id"],))
    cmd4 = await _command(pool, c4["id"])
    assert await _claim(pool) is None
    assert await _status(pool, cmd4) == "queued"
    await _lead(pool, c4["id"], successor["id"], epoch=2)
    claim = await _claim(pool)
    assert claim is not None
    assert (claim.message_id, claim.agent_id) == (cmd4, successor["id"])


async def test_session_messages_wait_for_dispatching_command(pool, dsn):
    c = await insert_session(pool)
    agent = await _agent(pool, c["id"], free_ago=10)
    c1 = await _command(pool, c["id"], "/compact")
    c2 = await _command(pool, c["id"], "/context")
    p3 = await _enqueue(pool, c["id"], "prose")
    first = await _claim(pool)
    assert first is not None
    assert (first.message_id, first.agent_id) == (c1, agent["id"])
    assert await _claim(pool) is None  # a later command and later prose wait behind c1
    await _sent_command(pool, c1, "cmd_1")
    second = await _claim(pool)
    assert second is not None
    assert (second.message_id, second.kind) == (c2, "command")
    assert await _claim(pool) is None
    await _sent_command(pool, c2, "cmd_2")
    third = await _claim(pool)
    assert third is not None
    assert (third.message_id, third.kind) == (p3, "turn")
    async with pool.connection() as conn:
        assert await q.mark_sent(conn, p3, "turn-x")

    # through the dispatcher: the fake wrapper receives them in id order
    cid = uuid.uuid4()
    fake = FakeWrapper(token=derive_tokens(KEY.encode(), cid).auth)
    await fake.start()
    d = Dispatcher(pool, Settings(database_url=dsn, master_key=KEY))
    try:
        await insert_session(pool, id=cid)
        client = WrapperClient(fake.base_url, fake.token)
        wsid = (await client.create_session("s", "/w")).wrapper_session_id
        await client.aclose()
        agent = await insert_agent(
            pool, cid, ready=True, wrapper_session_id=wsid, url=fake.base_url
        )
        await set_agent_status(pool, agent["id"], state="idle")
        await _exec(
            pool,
            "UPDATE agent_status SET free_since = now() - interval '10 seconds' WHERE agent_id=%s",
            (agent["id"],),
        )
        ids = [
            await _command(pool, cid, "/compact keep it"),
            await _command(pool, cid, "/context"),
            await _enqueue(pool, cid, "after the commands"),
        ]
        for n in range(len(ids)):
            await d.drain()  # one at a time: each waits for the one before it to be sent
            assert [await _status(pool, i) for i in ids] == ["sent"] * (n + 1) + ["queued"] * (
                len(ids) - n - 1
            )
        posts = [
            p for m, p in fake.requests if m == "POST" and p.startswith(f"/v1/sessions/{wsid}/")
        ]
        assert posts == [
            f"/v1/sessions/{wsid}/commands",
            f"/v1/sessions/{wsid}/commands",
            f"/v1/sessions/{wsid}/turns",
        ]
        records = fake.sessions[wsid].commands
        assert [(r["name"], r["args"]) for r in records] == [
            ("compact", "keep it"),
            ("context", ""),
        ]
        assert fake.turn_texts == ["after the commands"]
    finally:
        await d.aclose()
        await fake.stop()


async def test_untargeted_command_without_leader_prefers_idle_free_longest(pool):
    # no leader, no target: an idle agent before a busy one (even one with an older free_since),
    # and among idle agents the one free the longest (the leadership pass's order)
    c = await insert_session(pool)
    await _agent(pool, c["id"], free_ago=1000, ordinal=0, state="busy")
    await _agent(pool, c["id"], free_ago=10, ordinal=1)
    longest = await _agent(pool, c["id"], free_ago=100, ordinal=2)
    cmd = await _command(pool, c["id"])
    claim = await _claim(pool)
    assert claim is not None
    assert (claim.message_id, claim.kind, claim.agent_id) == (cmd, "command", longest["id"])

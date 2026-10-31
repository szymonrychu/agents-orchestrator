"""Dispatcher loop against a real Postgres and the in-process fake wrapper."""

from __future__ import annotations

import asyncio
import time
import uuid

import pytest_asyncio
from factories import (
    insert_agent,
    insert_command,
    insert_message,
    insert_session,
    set_agent_status,
)
from fakes.fake_wrapper import FakeWrapper
from psycopg.rows import dict_row

from agents_orchestrator.db import queue as q
from agents_orchestrator.dispatcher import Dispatcher
from agents_orchestrator.settings import Settings
from agents_orchestrator.tokens import derive_tokens
from agents_orchestrator.wrapper.client import WrapperClient

KEY = "k" * 32


def make_settings(dsn: str, **kw) -> Settings:
    kw.setdefault("retry_backoff_base_seconds", 0.2)
    kw.setdefault("retry_backoff_max_seconds", 0.2)
    return Settings(database_url=dsn, master_key=KEY, **kw)


class Env:
    def __init__(self, pool, dsn, session, fake, token):
        self.pool, self.dsn, self.session, self.fake, self.token = pool, dsn, session, fake, token
        self.cid = session["id"]

    def dispatcher(self, **kw) -> Dispatcher:
        return Dispatcher(self.pool, make_settings(self.dsn, **kw))

    def send_only_dispatcher(self, **kw) -> Dispatcher:
        """A dispatcher whose verifier pass does nothing, to pin the hand-off to verifying.

        The verifier itself is covered in tests/test_ambiguous_verify.py.
        """
        d = self.dispatcher(**kw)

        async def no_verify() -> int:
            return 0

        d.verify_pending = no_verify
        return d

    async def agent(self, free_ago: float = 0, state: str = "idle", fake=None, session=None):
        fake = fake or self.fake
        if session is None:
            client = WrapperClient(fake.base_url, self.token)
            session = (
                await client.create_session(f"s{uuid.uuid4().hex[:4]}", "/w")
            ).wrapper_session_id
        agent = await insert_agent(
            self.pool, self.cid, ready=True, wrapper_session_id=session, url=fake.base_url
        )
        await set_agent_status(self.pool, agent["id"], state=state)
        await self.set_free(agent["id"], free_ago, state)
        return agent

    async def set_free(self, agent_id, ago: float = 0, state: str = "idle") -> None:
        """What the ingester does on a transition into idle."""
        await self.exec(
            "UPDATE agent_status SET state=%s, free_since = now() - make_interval(secs => %s)"
            " WHERE agent_id=%s",
            (state, ago, agent_id),
        )

    async def exec(self, query: str, params: tuple = ()) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(query, params)

    async def rows(self, query: str, params: tuple = ()) -> list[dict]:
        async with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, params)
            return await cur.fetchall()

    async def message(self, id: int) -> dict:
        return (await self.rows("SELECT * FROM messages WHERE id=%s", (id,)))[0]

    async def agent_row(self, id) -> dict:
        return (await self.rows("SELECT * FROM agents WHERE id=%s", (id,)))[0]

    async def msg(self, **kw) -> dict:
        return await insert_message(self.pool, self.cid, **kw)


async def make_env(pool, dsn, spec=None, token=None) -> Env:
    cid = uuid.uuid4()
    auth = derive_tokens(KEY.encode(), cid).auth
    fake = FakeWrapper(token=token or auth)
    await fake.start()
    session = await insert_session(pool, id=cid, spec=spec or {})
    return Env(pool, dsn, session, fake, auth)


@pytest_asyncio.fixture
async def env(pool, dsn):
    e = await make_env(pool, dsn)
    yield e
    await e.fake.stop()


async def test_round_robin_by_free_since(env):
    a = await env.agent(free_ago=10)
    b = await env.agent(free_ago=30)
    c = await env.agent(free_ago=20)
    ms = [await env.msg(text=f"m{i}") for i in range(3)]
    d = env.dispatcher()
    await d.drain()
    await d.aclose()
    got = [(await env.message(m["id"]))["agent_id"] for m in ms]
    assert got == [b["id"], c["id"], a["id"]]
    assert {(await env.message(m["id"]))["status"] for m in ms} == {"sent"}
    assert env.fake.received_turns == 3


async def test_message_waits_until_agent_free(env):
    agent = await env.agent()
    m1, m2 = await env.msg(text="one"), await env.msg(text="two")
    d = env.dispatcher()
    await d.drain()
    assert (await env.message(m1["id"]))["status"] == "sent"
    await d.drain()
    assert (await env.message(m2["id"]))["status"] == "queued"
    env.fake.set_session_state("ready")
    await env.set_free(agent["id"])  # the ingester sees the turn end
    await d.drain()
    await d.aclose()
    assert (await env.message(m2["id"]))["status"] == "sent"
    assert env.fake.turn_texts == ["one", "two"]


async def test_409_requeues(env):
    agent = await env.agent()
    m = await env.msg()
    env.fake.set_turn_status(409)
    d = env.dispatcher()
    await d.drain()
    row = await env.message(m["id"])
    assert (row["status"], row["attempts"], row["not_before"]) == ("queued", 0, None)
    assert (await env.agent_row(agent["id"]))["last_dispatched_at"] is None
    env.fake.set_turn_status(None)
    await d.drain()
    await d.aclose()
    assert (await env.message(m["id"]))["status"] == "sent"


async def test_503_retried_with_backoff_then_fails_at_max_attempts(pool, dsn):
    env = await make_env(pool, dsn, spec={"max_attempts": 3})
    try:
        await env.agent()
        m = await env.msg()
        env.fake.set_turn_status(503)
        d = env.dispatcher(retry_backoff_base_seconds=1, retry_backoff_max_seconds=1)
        await d.drain()
        row = await env.message(m["id"])
        assert (row["status"], row["attempts"], row["error"]) == ("queued", 1, "unavailable")
        assert row["not_before"] is not None
        await d.drain()  # still inside the backoff: not touched
        assert (await env.message(m["id"]))["attempts"] == 1
        await asyncio.sleep(1.1)
        await d.drain()
        assert (await env.message(m["id"]))["attempts"] == 2
        await asyncio.sleep(1.1)
        await d.drain()
        row = await env.message(m["id"])
        assert (row["status"], row["attempts"], row["error"]) == ("failed", 3, "unavailable")

        # and an outage that ends is retried automatically, with no user action
        m2 = await env.msg(text="again")
        await d.drain()
        assert (await env.message(m2["id"]))["status"] == "queued"
        env.fake.set_turn_status(None)
        await asyncio.sleep(1.1)
        await d.drain()
        await d.aclose()
        assert (await env.message(m2["id"]))["status"] == "sent"
    finally:
        await env.fake.stop()


async def test_two_dispatchers_no_double_send(env):
    for _ in range(3):
        await env.agent()
    for i in range(6):
        await env.msg(text=f"m{i}")
    d1, d2 = env.dispatcher(), env.dispatcher()
    await asyncio.gather(d1.drain(), d2.drain(), d1.drain(), d2.drain())
    await d1.aclose()
    await d2.aclose()
    ids = [m["orch_message_id"] for m in env.fake.turn_metadata]
    assert len(ids) == 3 and len(set(ids)) == 3
    assert env.fake.received_turns == 3


async def test_notify_loss_recovered_by_poll(env):
    await env.agent()
    d = env.dispatcher(dispatch_poll_seconds=0.2)
    stop = asyncio.Event()
    task = asyncio.create_task(d.run(stop))
    try:
        await asyncio.sleep(0.5)  # listening, nothing to do
        m = await env.msg()  # inserted by SQL: no NOTIFY
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if (await env.message(m["id"]))["status"] == "sent":
                break
            await asyncio.sleep(0.1)
        assert (await env.message(m["id"]))["status"] == "sent"
    finally:
        stop.set()
        await asyncio.wait_for(task, 5)


async def test_interrupt_and_compact_commands(env):
    agent = await env.agent()
    client = WrapperClient(env.fake.base_url, env.token)
    session = (await env.agent_row(agent["id"]))["wrapper_session_id"]
    await client.submit_turn(session, "work")  # a running turn to interrupt
    ci = await insert_command(env.pool, agent["id"], kind="interrupt")
    cc = await insert_command(env.pool, agent["id"], kind="compact")
    d = env.dispatcher()
    await d.drain()
    rows = {r["id"]: r for r in await env.rows("SELECT * FROM commands")}
    assert rows[ci["id"]]["status"] == "done" and rows[cc["id"]]["status"] == "done"
    assert rows[cc["id"]]["result"]["response"]["state"] == "queued"
    assert (await client.get_session(session)).state == "ready"
    # a command the wrapper refuses completes as failed with the error
    bad = await insert_agent(
        env.pool, env.cid, ready=True, wrapper_session_id="nope", url=env.fake.base_url
    )
    cf = await insert_command(env.pool, bad["id"], kind="interrupt")
    await d.drain()
    await d.aclose()
    row = (await env.rows("SELECT * FROM commands WHERE id=%s", (cf["id"],)))[0]
    assert row["status"] == "failed" and "404" in row["result"]["error"]


async def test_every_non_accept_status_releases_agent(pool, dsn):
    expect = {
        404: ("queued", "session_gone"),
        401: ("failed", "config_error"),
        422: ("failed", "rejected"),
        503: ("queued", "unavailable"),
    }
    for code, (status, error) in expect.items():
        env = await make_env(pool, dsn)
        try:
            agent = await env.agent()
            m1 = await env.msg(text="first")
            env.fake.set_turn_status(code)
            d = env.dispatcher()
            await d.drain()
            row = await env.message(m1["id"])
            assert (row["status"], row["error"]) == (status, error), code
            assert (await env.agent_row(agent["id"]))["last_dispatched_at"] is None, code
            env.fake.set_turn_status(None)
            m2 = await env.msg(text="second")
            await asyncio.sleep(0.35)  # past the backoff of a retried head
            await d.drain()
            await d.aclose()
            sent = [r for r in (await env.message(m1["id"]), await env.message(m2["id"]))]
            assert any(r["status"] == "sent" and r["agent_id"] == agent["id"] for r in sent), code
        finally:
            await env.fake.stop()


async def test_ambiguous_send_moves_to_verifying_without_resend(env):
    agent = await env.agent()
    m = await env.msg()
    env.fake.delay_turn_response(3)  # the turn is recorded, the answer comes too late
    d = env.send_only_dispatcher(wrapper_send_timeout_seconds=0.5, dispatching_stale_seconds=2)
    await d.drain()
    row = await env.message(m["id"])
    assert (row["status"], row["error"]) == ("verifying", "ambiguous")
    assert row["ambiguous_at"] is not None
    assert (await env.agent_row(agent["id"]))["last_dispatched_at"] is not None
    await d.drain()
    await d.drain()
    await d.aclose()
    assert env.fake.received_turns == 1
    assert (await env.message(m["id"]))["status"] == "verifying"
    assert (await env.agent_row(agent["id"]))["last_dispatched_at"] is not None


async def test_wrapper_500_goes_through_verifier(env):
    agent = await env.agent()
    m = await env.msg()
    env.fake.fail_turn_after_receive(500)  # the turn is recorded, then the wrapper answers 500
    d = env.dispatcher()
    await d.drain()
    await d.drain()
    await d.aclose()
    row = await env.message(m["id"])
    assert row["ambiguous_at"] is not None  # it went through verifying
    assert (row["status"], row["turn_id"]) == ("sent", env.fake.last_turn_id)
    assert row["attempts"] == 0
    assert env.fake.received_turns == 1
    assert (await env.agent_row(agent["id"]))["last_dispatched_at"] == row["dispatched_at"]


async def test_slow_send_heartbeats_and_times_out(env):
    await env.agent()
    m = await env.msg()
    env.fake.delay_turn_response(5)
    d = env.send_only_dispatcher(
        wrapper_send_timeout_seconds=1.0,
        dispatching_heartbeat_seconds=0.2,
        dispatching_stale_seconds=2.0,
    )
    started = time.monotonic()
    task = asyncio.create_task(d.drain())
    await asyncio.sleep(0.7)
    row = await env.message(m["id"])
    assert row["status"] == "dispatching"
    assert row["heartbeat_at"] > row["dispatched_at"]
    await task
    await d.aclose()
    assert time.monotonic() - started < 3.0  # the send timeout bounds it
    row = await env.message(m["id"])
    assert (row["status"], row["error"]) == ("verifying", "ambiguous")  # not dispatcher_lost


async def test_agent_not_wedged_after_failed_send(env):
    agent = await env.agent()
    m1 = await env.msg(text="doomed")
    env.fake.set_turn_status(503)
    d = env.dispatcher()
    await d.drain()
    assert (await env.message(m1["id"]))["error"] == "unavailable"
    async with env.pool.connection() as conn:
        assert await q.cancel_message(conn, m1["id"])
    env.fake.set_turn_status(None)
    m2 = await env.msg(text="next")
    await d.drain()
    await d.aclose()
    row = await env.message(m2["id"])
    assert (row["status"], row["agent_id"]) == ("sent", agent["id"])


async def test_two_dispatchers_keep_fifo_per_session(env):
    for _ in range(2):
        await env.agent()
    ms = [await env.msg(text=f"m{i}") for i in range(5)]
    d1, d2 = env.dispatcher(), env.dispatcher()
    await asyncio.gather(d1.drain(), d2.drain(), d1.drain(), d2.drain())
    await d1.aclose()
    await d2.aclose()
    statuses = [(await env.message(m["id"]))["status"] for m in ms]
    assert statuses == ["sent", "sent", "queued", "queued", "queued"]
    ids = sorted(m["orch_message_id"] for m in env.fake.turn_metadata)
    assert ids == [ms[0]["id"], ms[1]["id"]]


async def test_session_gone_enqueues_rebootstrap(env):
    agent = await env.agent(session="missing-session")
    m = await env.msg()
    d = env.dispatcher()
    await d.drain()
    await d.aclose()
    row = await env.message(m["id"])
    assert (row["status"], row["error"], row["attempts"]) == ("queued", "session_gone", 1)
    cmds = await env.rows("SELECT * FROM commands WHERE agent_id=%s", (agent["id"],))
    assert [(c["kind"], c["status"]) for c in cmds] == [("rebootstrap", "queued")]


async def test_submit_metadata_and_permanent_failures(pool, dsn):
    env = await make_env(pool, dsn)
    try:
        await env.agent()
        m = await env.msg()
        env.fake.set_turn_status(503)
        d = env.dispatcher()
        await d.drain()
        env.fake.set_turn_status(None)
        await asyncio.sleep(0.35)
        await d.drain()
        assert env.fake.turn_metadata == [{"orch_message_id": m["id"], "orch_attempt": 2}]
        await d.aclose()

        # 422 and 401 fail at once and are not retried
        for code, error in ((422, "rejected"), (401, "config_error")):
            e2 = await make_env(pool, dsn)
            try:
                await e2.agent()
                bad = await e2.msg()
                e2.fake.set_turn_status(code)
                d2 = e2.dispatcher()
                await d2.drain()
                await asyncio.sleep(0.35)
                await d2.drain()
                await d2.aclose()
                row = await e2.message(bad["id"])
                assert (row["status"], row["error"], row["attempts"]) == ("failed", error, 0)
            finally:
                await e2.fake.stop()
    finally:
        await env.fake.stop()


async def test_stale_dispatching_row_goes_to_verifier_not_resent(env):
    """A crash between claim commit and send leaves a dispatching row (D24)."""
    agent = await env.agent()
    m = await env.msg(status="dispatching", agent_id=agent["id"], attempts=0)
    await env.exec(
        "UPDATE messages SET dispatched_at = now() - interval '1 hour', heartbeat_at = NULL"
        " WHERE id=%s",
        (m["id"],),
    )
    d = env.dispatcher(dispatching_stale_seconds=30)
    await d.drain()
    await d.aclose()
    row = await env.message(m["id"])
    assert (row["status"], row["error"]) == ("verifying", "dispatcher_lost")
    assert env.fake.received_turns == 0


async def test_notify_wakes_dispatcher_on_both_channels(env):
    await env.agent()
    await env.agent()
    d = env.dispatcher(dispatch_poll_seconds=60)  # the poll cannot be what wakes it
    stop = asyncio.Event()
    task = asyncio.create_task(d.run(stop))
    try:
        await asyncio.sleep(0.5)  # initial drain done, listening
        for channel in ("message_queued", "agent_free"):
            m = await env.msg()
            await env.exec("SELECT pg_notify(%s, '{}')", (channel,))
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if (await env.message(m["id"]))["status"] == "sent":
                    break
                await asyncio.sleep(0.05)
            assert (await env.message(m["id"]))["status"] == "sent", channel
    finally:
        stop.set()
        await asyncio.wait_for(task, 5)


async def test_at_most_eight_sends_in_flight(env):
    fakes = [FakeWrapper(token=env.token) for _ in range(9)]
    for f in fakes:
        await f.start()
    await env.agent()
    for f in fakes:
        await env.agent(fake=f)
    for i in range(10):
        await env.msg(text=f"m{i}")
    d = env.dispatcher()
    state = {"now": 0, "peak": 0, "timeouts": 0}
    reached = asyncio.Event()
    real = d._send

    async def counting(claim):
        state["now"] += 1
        state["peak"] = max(state["peak"], state["now"])
        if state["now"] >= 8:
            reached.set()
        try:
            # hold every send until 8 are in flight (deterministic, no sleep overlap)
            try:
                await asyncio.wait_for(reached.wait(), 10)
            except TimeoutError:
                state["timeouts"] += 1
            await real(claim)
        finally:
            state["now"] -= 1

    d._send = counting
    await d.drain()
    await d.aclose()
    for f in fakes:
        await f.stop()
    assert state["timeouts"] == 0, "cap never reached 8 concurrent sends"
    assert state["peak"] == 8
    assert env.fake.received_turns + sum(f.received_turns for f in fakes) == 10


# ---------------------------------------------------------------- lanes (D2)


async def test_idle_lane_waits_for_agent_free(env):
    await env.agent(state="busy")
    m = await env.msg(lane="idle")
    d = env.dispatcher()
    await d.drain()
    await d.aclose()
    assert (await env.message(m["id"]))["status"] == "queued"
    assert env.fake.received_turns == 0


async def test_regular_and_priority_bypass_agent_free(env):
    agent = await env.agent()
    env.fake.set_session_state("busy")
    await env.set_free(agent["id"], 0, "busy")  # the agent is mid-turn
    reg = await env.msg(text="regular", lane="regular")
    pri = await env.msg(text="priority", lane="priority")
    d = env.dispatcher()
    await d.drain()
    await d.aclose()
    assert (await env.message(reg["id"]))["status"] == "sent"
    assert (await env.message(pri["id"]))["status"] == "sent"
    assert env.fake.turn_texts == ["regular", "priority"]
    assert env.fake.turn_lanes == ["regular", "priority"]


async def test_idle_head_does_not_block_a_later_regular_message(env):
    await env.agent(state="busy")
    idle = await env.msg(text="later", lane="idle")
    reg = await env.msg(text="now", lane="regular")
    d = env.dispatcher()
    await d.drain()
    await d.aclose()
    assert (await env.message(idle["id"]))["status"] == "queued"
    assert (await env.message(reg["id"]))["status"] == "sent"


async def test_lane_is_sent_to_the_wrapper(env):
    await env.agent()
    await env.msg(lane="idle")
    d = env.dispatcher()
    await d.drain()
    await d.aclose()
    assert env.fake.turn_lanes == ["idle"]


async def test_dialog_open_fails_regular_message_with_reason(env):
    agent = await env.agent()
    m = await env.msg(lane="regular")
    env.fake.set_turn_status(409, "dialog_open")
    d = env.dispatcher()
    await d.drain()
    await d.aclose()
    row = await env.message(m["id"])
    assert (row["status"], row["error"]) == ("failed", "dialog_open")
    assert (await env.agent_row(agent["id"]))["last_dispatched_at"] is None


async def test_booting_and_busy_409_requeue_without_an_attempt(env):
    await env.agent()
    m = await env.msg(lane="priority")
    d = env.dispatcher()
    for code in ("booting", "busy"):
        env.fake.set_turn_status(409, code)
        await d.drain()
        row = await env.message(m["id"])
        assert (row["status"], row["attempts"]) == ("queued", 0), code
    await d.aclose()


async def test_dead_409_is_retried_with_backoff(env):
    await env.agent()
    m = await env.msg(lane="regular")
    env.fake.set_turn_status(409, "dead")
    d = env.dispatcher()
    await d.drain()
    await d.aclose()
    row = await env.message(m["id"])
    assert (row["status"], row["attempts"]) == ("queued", 1)

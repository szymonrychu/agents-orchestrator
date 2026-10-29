"""Dispatcher loop against a real Postgres and the in-process fake wrapper."""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import UTC, datetime

import httpx
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
from psycopg_pool import AsyncConnectionPool
from test_api_sessions import client_for
from test_api_sessions import make_settings as api_settings

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import queue as q
from agents_orchestrator.dispatcher import Dispatcher
from agents_orchestrator.dispatcher.session_frame import frame
from agents_orchestrator.settings import Settings
from agents_orchestrator.tokens import derive_tokens
from agents_orchestrator.wrapper.client import WrapperClient
from agents_orchestrator.wrapper.models import CommandAccepted

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

    async def queue(self, text: str, kind: str = "turn", sender: dict | None = None) -> int:
        """Enqueue as the api does; `sender` is the sending session row of a session message."""
        async with self.pool.connection() as conn:
            return await q.enqueue_message(
                conn,
                self.cid,
                text,
                kind=kind,
                sender_session_id=sender["id"] if sender else None,
                sender_slug=sender["slug"] if sender else None,
            )

    def wrapper_commands(self, agent: dict) -> list[dict]:
        """The CommandRecords the fake wrapper queued on the agent's session, oldest first."""
        return self.fake.sessions[agent["wrapper_session_id"]].commands

    async def not_before_ahead(self, id: int) -> float:
        """Seconds from now (database clock) until the message's not_before."""
        rows = await self.rows(
            "SELECT extract(epoch from not_before - now())::float8 AS ahead FROM messages"
            " WHERE id=%s",
            (id,),
        )
        return rows[0]["ahead"]


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


async def test_compact_command_outcome_mapping():
    """`_run_command` without a database: the wrapper record on accept, the reason otherwise."""
    cid = uuid.uuid4()
    fake = FakeWrapper(token=derive_tokens(KEY.encode(), cid).auth)
    await fake.start()
    dsn = "postgresql://unused"
    d = Dispatcher(AsyncConnectionPool(dsn, open=False), make_settings(dsn))
    try:
        client = WrapperClient(fake.base_url, fake.token)
        session = (await client.create_session("c", "/w")).wrapper_session_id
        await client.aclose()

        def cmd(wrapper_session_id: str | None = session) -> q.Command:
            now = datetime.now(UTC)
            return q.Command(
                1, uuid.uuid4(), "compact", "running", now, now, cid, fake.base_url,
                wrapper_session_id,
            )  # fmt: skip

        status, result = await d._run_command(cmd())
        assert status == "done"
        record = result["response"]
        assert (record["name"], record["args"], record["state"]) == ("compact", "", "queued")
        assert record["id"].startswith("cmd_")
        fake.commands_client = ["context"]
        status, result = await d._run_command(cmd())
        assert status == "failed" and "403 command_not_allowed" in result["error"]
        fake.commands_client = ["*"]
        status, result = await d._run_command(cmd("ses_nope"))
        assert status == "failed" and "404" in result["error"]
        assert await d._run_command(cmd(None)) == ("failed", {"error": "agent has no session"})
    finally:
        await d.aclose()
        await fake.stop()


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


# ---------------------------------------------------------------- kind=command messages (D10)


async def test_reload_plugins_from_chat_runs_as_command_and_frees_agent(env):
    """The reported case: '/reload-plugins' from the chat never leaves the session busy."""
    agent = await env.agent()
    wsid = agent["wrapper_session_id"]
    url = f"/api/v1/sessions/{env.cid}/messages"
    d = env.dispatcher()
    async with client_for(create_app(api_settings(env.dsn), env.pool)) as http:
        r = await http.post(url, json={"text": "/reload-plugins"})
        assert r.status_code == 202, r.text
        mid = r.json()["message_id"]
        await d.drain()
        records = env.wrapper_commands(agent)
        assert [(c["name"], c["args"]) for c in records] == [("reload-plugins", "")]
        row = await env.message(mid)
        assert (row["status"], row["wrapper_command_id"], row["turn_id"], row["error"]) == (
            "sent",
            records[0]["id"],
            None,
            None,
        )
        assert ("POST", f"/v1/sessions/{wsid}/commands") in env.fake.requests
        assert not [p for m, p in env.fake.requests if m == "POST" and p.endswith("/turns")]
        assert env.fake.received_turns == 0
        assert records[0]["state"] == "completed"
        assert env.fake.sessions[wsid].state == "ready"
        assert (await env.agent_row(agent["id"]))["last_dispatched_at"] is None

        r = await http.post(url, json={"text": "and now some prose"})
        assert r.status_code == 202, r.text
        prose = r.json()["message_id"]
        await d.drain()
        row = await env.message(prose)
        assert (row["status"], row["agent_id"]) == ("sent", agent["id"])
        assert env.fake.turn_texts == ["and now some prose"]
        await env.fake.complete_turn(row["turn_id"])
        client = WrapperClient(env.fake.base_url, env.token)
        assert (await client.get_turn(wsid, row["turn_id"])).state == "completed"
        assert (await client.get_session(wsid)).state == "ready"
        await client.aclose()
    await d.aclose()


async def test_command_message_sent_unframed_to_commands(env):
    agent = await env.agent()
    boss = await insert_session(env.pool)
    cmd = await env.queue("/compact keep <the> plan", kind="command", sender=boss)
    d = env.dispatcher()
    await d.drain()
    records = env.wrapper_commands(agent)
    assert [(c["name"], c["args"]) for c in records] == [("compact", "keep <the> plan")]
    row = await env.message(cmd)
    assert (row["status"], row["wrapper_command_id"]) == ("sent", records[0]["id"])
    assert env.fake.received_turns == 0

    prose = await env.queue("status please", sender=boss)
    await d.drain()
    await d.aclose()
    assert env.fake.turn_texts == [frame("status please", boss["slug"], prose)]
    assert env.fake.turn_metadata[0]["orch_sender_session"] == boss["slug"]


async def test_command_message_dispatched_while_agent_busy(env):
    agent = await env.agent()
    work = await env.queue("long work")
    d = env.dispatcher()
    await d.drain()
    turn_id = (await env.message(work))["turn_id"]
    assert turn_id
    # the ingester saw the turn start
    await env.exec("UPDATE agent_status SET state='busy' WHERE agent_id=%s", (agent["id"],))
    waiting = await env.queue("prose for after the turn")
    cmd = await env.queue("/compact", kind="command")
    await d.drain()
    row = await env.message(cmd)
    records = env.wrapper_commands(agent)
    assert [(c["name"], c["state"], c["turn_id"]) for c in records] == [
        ("compact", "queued", turn_id)
    ]
    assert (row["status"], row["wrapper_command_id"]) == ("sent", records[0]["id"])
    assert (await env.message(waiting))["status"] == "queued"
    assert env.fake.turn_texts == ["long work"]

    await env.fake.complete_turn(turn_id)  # the wrapper runs the command after the turn
    assert records[0]["state"] == "completed"
    await env.set_free(agent["id"])  # the ingester sees the session idle again
    await d.drain()
    await d.aclose()
    assert (await env.message(waiting))["status"] == "sent"
    assert env.fake.turn_texts == ["long work", "prose for after the turn"]


async def test_command_args_exclude_run_marker(env):
    agent = await env.agent()
    text = "/review-pr 12\n\n<!-- agents-webhook:42 -->"
    mid = await env.queue(text, kind="command")
    d = env.dispatcher()
    await d.drain()
    await d.aclose()
    records = env.wrapper_commands(agent)
    assert [(c["name"], c["args"]) for c in records] == [("review-pr", "12")]
    row = await env.message(mid)
    assert (row["status"], row["text"]) == ("sent", text)


async def test_command_busy_requeues_with_backoff(pool, dsn):
    base = 1.0
    # 429 command_queue_full: the wrapper's command queue is full behind a running turn
    env = await make_env(pool, dsn)
    try:
        agent = await env.agent()
        wsid = agent["wrapper_session_id"]
        client = WrapperClient(env.fake.base_url, env.token)
        turn_id = (await client.submit_turn(wsid, "busy")).turn_id
        for i in range(4):
            assert isinstance(await client.command(wsid, "context", str(i)), CommandAccepted)
        await client.aclose()
        mid = await env.queue("/compact", kind="command")
        d = env.dispatcher(retry_backoff_base_seconds=base, retry_backoff_max_seconds=base)
        await d.drain()
        row = await env.message(mid)
        assert (row["status"], row["attempts"]) == ("queued", 0)
        assert base - 0.5 < await env.not_before_ahead(mid) <= base
        await d.drain()  # inside the backoff: not sent again
        assert (await env.message(mid))["status"] == "queued"
        assert len(env.wrapper_commands(agent)) == 4
        assert (await env.agent_row(agent["id"]))["last_dispatched_at"] is None

        await env.fake.complete_turn(turn_id)  # the queued commands run, the queue empties
        await asyncio.sleep(base + 0.1)
        await d.drain()
        await d.aclose()
        row = await env.message(mid)
        assert (row["status"], row["attempts"]) == ("sent", 0)
        assert env.wrapper_commands(agent)[-1]["name"] == "compact"
    finally:
        await env.fake.stop()

    # 409 booting
    env = await make_env(pool, dsn)
    try:
        agent = await env.agent()
        env.fake.set_booting(True)
        mid = await env.queue("/compact", kind="command")
        d = env.dispatcher(retry_backoff_base_seconds=base, retry_backoff_max_seconds=base)
        await d.drain()
        row = await env.message(mid)
        assert (row["status"], row["attempts"]) == ("queued", 0)
        assert base - 0.5 < await env.not_before_ahead(mid) <= base
        assert env.wrapper_commands(agent) == []
        env.fake.set_booting(False)
        await asyncio.sleep(base + 0.1)
        await d.drain()
        await d.aclose()
        row = await env.message(mid)
        assert (row["status"], row["attempts"]) == ("sent", 0)
    finally:
        await env.fake.stop()


async def test_command_refusal_fails_with_wrapper_code(pool, dsn):
    env = await make_env(pool, dsn)
    try:
        agent = await env.agent()
        d = env.dispatcher()
        cases = [
            ("/exit", "command_denied"),
            ("/CLEAR now", "command_denied"),
            ("/ is root", "invalid_command"),
            ("/", "invalid_command"),
        ]
        for text, error in cases:
            mid = await env.queue(text, kind="command")
            await d.drain()
            await asyncio.sleep(0.35)  # past any backoff: a refusal is never retried
            await d.drain()
            row = await env.message(mid)
            assert (row["status"], row["error"], row["attempts"]) == ("failed", error, 0), text
        env.fake.commands_client = ["context"]
        mid = await env.queue("/compact", kind="command")
        await d.drain()
        await d.aclose()
        row = await env.message(mid)
        assert (row["status"], row["error"]) == ("failed", "command_not_allowed")
        assert env.wrapper_commands(agent) == []
        assert env.fake.received_turns == 0
        assert (await env.agent_row(agent["id"]))["last_dispatched_at"] is None
    finally:
        await env.fake.stop()

    # 401: the session's derived token does not open this wrapper
    env = await make_env(pool, dsn, token="not-the-derived-token")
    try:
        client = WrapperClient(env.fake.base_url, env.fake.token)
        await env.agent(session=(await client.create_session("s", "/w")).wrapper_session_id)
        await client.aclose()
        mid = await env.queue("/compact", kind="command")
        d = env.dispatcher()
        await d.drain()
        await d.aclose()
        row = await env.message(mid)
        assert (row["status"], row["error"], row["attempts"]) == ("failed", "config_error", 0)
    finally:
        await env.fake.stop()


async def test_command_session_gone_counts_attempt_and_rebootstraps(env):
    agent = await env.agent(session="missing-session")
    mid = await env.queue("/compact", kind="command")
    d = env.dispatcher()
    await d.drain()
    await d.aclose()
    row = await env.message(mid)
    assert (row["status"], row["error"], row["attempts"]) == ("queued", "session_gone", 1)
    assert row["not_before"] is not None
    cmds = await env.rows("SELECT * FROM commands WHERE agent_id=%s", (agent["id"],))
    assert [(c["kind"], c["status"]) for c in cmds] == [("rebootstrap", "queued")]
    assert (await env.agent_row(agent["id"]))["last_dispatched_at"] is None


class _LostCommandAnswer(httpx.AsyncBaseTransport):
    """Forwards every request; a POST to /commands reaches the wrapper, then its answer is lost
    to a read timeout or replaced by a 500 or 502."""

    def __init__(self, fault: str | int) -> None:
        self.fault = fault
        self.inner = httpx.AsyncHTTPTransport()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await self.inner.handle_async_request(request)
        if request.method != "POST" or not request.url.path.endswith("/commands"):
            return response
        await response.aread()
        await response.aclose()
        if self.fault == "timeout":
            raise httpx.ReadTimeout("the answer never came", request=request)
        body = {"error": {"code": "internal", "message": "lost"}}
        return httpx.Response(int(self.fault), json=body)

    async def aclose(self) -> None:
        await self.inner.aclose()


async def test_command_ambiguous_fails_without_verify(env):
    agent = await env.agent()
    for i, fault in enumerate(("timeout", 500, 502)):
        mid = await env.queue(f"/compact pass {i}", kind="command")
        d = env.dispatcher()
        d._http = httpx.AsyncClient(transport=_LostCommandAnswer(fault))
        await d.drain()
        row = await env.message(mid)
        assert (row["status"], row["error"], row["attempts"]) == ("failed", "ambiguous", 0), fault
        assert row["ambiguous_at"] is None, fault  # it never entered verifying
        assert row["wrapper_command_id"] is None, fault
        await d.drain()  # never resent
        await d.aclose()
        assert [c["args"] for c in env.wrapper_commands(agent)] == [
            f"pass {n}" for n in range(i + 1)
        ], fault
    assert env.fake.received_turns == 0

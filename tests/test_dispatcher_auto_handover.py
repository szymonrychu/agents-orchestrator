"""A running Dispatcher hands a leaving leader over on a NOTIFY, never on the poll."""

from __future__ import annotations

import asyncio
import time
import uuid

import pytest_asyncio
from factories import insert_agent, insert_message, insert_session, set_agent_status
from fakes.fake_wrapper import FakeWrapper
from psycopg.rows import dict_row

from agents_orchestrator.db import leadership, sessions
from agents_orchestrator.db.pool import notify
from agents_orchestrator.dispatcher import Dispatcher
from agents_orchestrator.settings import Settings
from agents_orchestrator.tokens import derive_tokens
from agents_orchestrator.wrapper.client import WrapperClient

KEY = "k" * 32


class Env:
    def __init__(self, pool, dsn, session, fake, token) -> None:
        self.pool, self.dsn, self.session, self.fake, self.token = pool, dsn, session, fake, token
        self.cid = session["id"]

    async def agent(self, ordinal: int, *, state: str = "idle", free_ago: float = 0) -> dict:
        client = WrapperClient(self.fake.base_url, self.token)
        try:
            sid = (await client.create_session(f"s{uuid.uuid4().hex[:4]}", "/w")).wrapper_session_id
        finally:
            await client.aclose()
        agent = await insert_agent(
            self.pool,
            self.cid,
            ordinal=ordinal,
            ready=True,
            wrapper_session_id=sid,
            url=self.fake.base_url,
        )
        await set_agent_status(self.pool, agent["id"], state=state)
        await self.exec(
            "UPDATE agent_status SET free_since = now() - make_interval(secs => %s)"
            " WHERE agent_id=%s",
            (free_ago, agent["id"]),
        )
        return agent

    async def exec(self, query: str, params: tuple = ()) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(query, params)

    async def rows(self, query: str, params: tuple = ()) -> list[dict]:
        async with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, params)
            return await cur.fetchall()

    async def lead(self, agent_id) -> None:
        await self.exec(
            "INSERT INTO session_leaders (session_id, agent_id, epoch, since, reason)"
            " VALUES (%s, %s, 1, now() - interval '1 minute', 'initial')",
            (self.cid, agent_id),
        )

    async def leader(self) -> dict:
        return (await self.rows("SELECT * FROM session_leaders WHERE session_id=%s", (self.cid,)))[
            0
        ]

    async def wait_for(self, check, what: str, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if await check():
                return
            await asyncio.sleep(0.05)
        raise AssertionError(f"timed out: {what}")


@pytest_asyncio.fixture
async def env(pool, dsn):
    cid = uuid.uuid4()
    auth = derive_tokens(KEY.encode(), cid).auth
    fake = FakeWrapper(token=auth)
    await fake.start()
    session = await insert_session(pool, id=cid, spec={"replicas": 3, "max_replicas": 4})
    yield Env(pool, dsn, session, fake, auth)
    await fake.stop()


async def running(env: Env):
    """A dispatcher whose poll (60 s) cannot be what wakes it; returns (stop, task)."""
    settings = Settings(database_url=env.dsn, master_key=KEY, dispatch_poll_seconds=60)
    stop = asyncio.Event()
    task = asyncio.create_task(Dispatcher(env.pool, settings).run(stop))
    await asyncio.sleep(0.5)  # the first drain is done and the listener is up
    return stop, task


async def finish(stop, task) -> None:
    stop.set()
    await asyncio.wait_for(task, 5)


async def test_idle_compaction_does_not_move(env):
    leader = await env.agent(0)
    await env.agent(1, free_ago=100)
    await env.lead(leader["id"])
    stop, task = await running(env)
    try:
        await env.exec(
            "UPDATE agent_status SET compaction_at = now() WHERE agent_id=%s", (leader["id"],)
        )
        async with env.pool.connection() as conn:
            await notify(conn, leadership.CHANNEL, {"session_id": str(env.cid)})
        await asyncio.sleep(1.0)
    finally:
        await finish(stop, task)
    row = await env.leader()
    assert (row["agent_id"], row["epoch"]) == (leader["id"], 1)


async def test_scale_down_moves_without_poll(env):
    stays = await env.agent(0, free_ago=100)
    leader = await env.agent(1)
    await env.exec(
        "UPDATE sessions SET spec = jsonb_set(spec, '{replicas}', '2') WHERE id=%s", (env.cid,)
    )
    await env.lead(leader["id"])
    stop, task = await running(env)
    try:
        async with env.pool.connection() as conn:
            await sessions.set_replicas(conn, env.cid, 1)

        async def moved() -> bool:
            return (await env.leader())["agent_id"] == stays["id"]

        await env.wait_for(moved, "leadership moved to the agent that stays")
    finally:
        await finish(stop, task)
    row = await env.leader()
    assert (row["epoch"], row["reason"]) == (2, "scale_down")


async def test_compacted_leader_takes_next_message(env):
    leader = await env.agent(0, state="busy")
    await env.agent(1, free_ago=100)
    await env.lead(leader["id"])
    await env.exec(
        "UPDATE agent_status SET compaction_at = now() WHERE agent_id=%s", (leader["id"],)
    )
    message = await insert_message(env.pool, env.cid, text="next")
    stop, task = await running(env)
    try:
        status = (await env.rows("SELECT status FROM messages WHERE id=%s", (message["id"],)))[0]
        assert status["status"] == "queued"  # the leader is busy
        assert (await env.leader())["agent_id"] == leader["id"]
        # what the ingester does when the turn ends
        await env.exec(
            "UPDATE agent_status SET state='idle', free_since=now() WHERE agent_id=%s",
            (leader["id"],),
        )
        async with env.pool.connection() as conn:
            await notify(conn, "agent_free", {})

        async def sent() -> bool:
            row = (await env.rows("SELECT status FROM messages WHERE id=%s", (message["id"],)))[0]
            return row["status"] == "sent"

        await env.wait_for(sent, "the message is sent")
    finally:
        await finish(stop, task)
    row = (await env.rows("SELECT agent_id FROM messages WHERE id=%s", (message["id"],)))[0]
    assert row["agent_id"] == leader["id"]
    assert (await env.leader())["agent_id"] == leader["id"]

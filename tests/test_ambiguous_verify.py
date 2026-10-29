"""Ambiguous-send verifier (D24) against a real Postgres and the in-process fake wrapper.

A message whose send outcome was ambiguous is checked against the original agent's wrapper
before any automatic retry, so a message is never pasted into a session twice.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest_asyncio
from factories import insert_agent, insert_message, insert_session, set_agent_status
from fakes.fake_wrapper import FakeWrapper
from psycopg.rows import dict_row

from agents_orchestrator.db import queue as q
from agents_orchestrator.db.pool import open_pool
from agents_orchestrator.dispatcher import Dispatcher
from agents_orchestrator.dispatcher.verify import Verifier
from agents_orchestrator.settings import Settings
from agents_orchestrator.tokens import derive_tokens
from agents_orchestrator.wrapper.client import WrapperClient

KEY = "k" * 32
SETTLE = 1.0
DEADLINE = 2.0


def make_settings(dsn: str, **kw) -> Settings:
    kw.setdefault("retry_backoff_base_seconds", 0.2)
    kw.setdefault("retry_backoff_max_seconds", 0.2)
    kw.setdefault("wrapper_send_timeout_seconds", 0.5)
    kw.setdefault("dispatching_stale_seconds", 1.0)
    kw.setdefault("ambiguous_settle_seconds", SETTLE)
    kw.setdefault("ambiguous_deadline_seconds", DEADLINE)
    return Settings(database_url=dsn, master_key=KEY, **kw)


class Env:
    def __init__(self, pool, dsn, cid, fake, token):
        self.pool, self.dsn, self.cid, self.fake, self.token = pool, dsn, cid, fake, token
        self.settings = make_settings(dsn)
        self.client = WrapperClient(fake.base_url, token)

    def dispatcher(self, **kw) -> Dispatcher:
        return Dispatcher(self.pool, make_settings(self.dsn, **kw) if kw else self.settings)

    def verifier(self, pool=None) -> Verifier:
        return Verifier(pool or self.pool, self.settings, WrapperClient)

    async def agent(self, session: str | None = None) -> dict:
        if session is None:
            session = (await self.client.create_session("s", "/w")).wrapper_session_id
        agent = await insert_agent(
            self.pool, self.cid, ready=True, wrapper_session_id=session, url=self.fake.base_url
        )
        await set_agent_status(self.pool, agent["id"], state="idle")
        await self.exec(
            "UPDATE agent_status SET free_since = now() - interval '10 seconds' WHERE agent_id=%s",
            (agent["id"],),
        )
        return agent

    async def verifying(self, agent: dict, age: float = 0.0, error: str = "ambiguous") -> dict:
        """A message the dispatcher handed to the verifier `age` seconds ago, agent reserved."""
        m = await insert_message(
            self.pool, self.cid, status="verifying", agent_id=agent["id"], error=error
        )
        await self.exec(
            "UPDATE messages SET dispatched_at = now() - make_interval(secs => %s + 1),"
            " ambiguous_at = now() - make_interval(secs => %s) WHERE id=%s",
            (age, age, m["id"]),
        )
        await self.exec(
            "UPDATE agents SET last_dispatched_at = (SELECT dispatched_at FROM messages"
            " WHERE id=%s) WHERE id=%s",
            (m["id"], agent["id"]),
        )
        return await self.message(m["id"])

    async def age(self, message_id: int, seconds: float) -> None:
        """Move ambiguous_at back, as if `seconds` had passed on the database clock."""
        await self.exec(
            "UPDATE messages SET ambiguous_at = now() - make_interval(secs => %s) WHERE id=%s",
            (seconds, message_id),
        )

    async def submit(self, session: str, metadata: dict) -> str:
        """A turn the wrapper accepted, as the lost dispatcher or a peer would have caused."""
        outcome = await self.client.submit_turn(session, "work", metadata)
        return outcome.turn_id

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


@pytest_asyncio.fixture
async def env(pool, dsn):
    cid = uuid.uuid4()
    token = derive_tokens(KEY.encode(), cid).auth
    fake = FakeWrapper(token=token)
    await fake.start()
    await insert_session(pool, id=cid, spec={})
    e = Env(pool, dsn, cid, fake, token)
    yield e
    await e.client.aclose()
    await fake.stop()


async def test_started_turn_resolves_to_sent(env):
    agent = await env.agent()
    m = await insert_message(env.pool, env.cid, text="once")
    env.fake.reset_connection_after_receive()  # the turn is recorded, then the reset
    d = env.dispatcher()
    await d.drain()  # send -> ambiguous -> verifying -> the verifier runs in the same pass
    await d.aclose()
    row = await env.message(m["id"])
    assert row["ambiguous_at"] is not None  # it went through verifying
    assert (row["status"], row["turn_id"], row["error"]) == ("sent", env.fake.last_turn_id, None)
    assert row["attempts"] == 0
    # the reservation stays: the turn is running on this agent
    assert (await env.agent_row(agent["id"]))["last_dispatched_at"] == row["dispatched_at"]
    assert env.fake.received_turns == 1
    assert env.fake.turn_metadata == [{"orch_message_id": m["id"], "orch_attempt": 1}]


async def test_not_started_requeued_after_settle_and_sent_once(env):
    agent = await env.agent()
    m = await insert_message(env.pool, env.cid, text="exactly once")
    env.fake.reset_connection_before_receive()  # the request dies before a turn is recorded
    d = env.dispatcher(retry_backoff_base_seconds=1.0, retry_backoff_max_seconds=1.0)
    await d.drain()
    row = await env.message(m["id"])
    assert (row["status"], row["error"]) == ("verifying", "ambiguous")
    assert env.fake.received_turns == 0
    env.fake.reset_connection_before_receive(False)

    await d.drain()  # inside the settle window: no decision, no resend
    row = await env.message(m["id"])
    assert (row["status"], row["attempts"]) == ("verifying", 0)
    assert env.fake.received_turns == 0

    await env.age(m["id"], SETTLE + 0.1)
    await d.drain()  # past the settle window: definitely not started
    row = await env.message(m["id"])
    assert (row["status"], row["attempts"], row["error"]) == (
        "queued",
        1,
        "ambiguous_not_started",
    )
    assert row["not_before"] is not None
    # the reservation was this message's, so it is cleared
    assert (await env.agent_row(agent["id"]))["last_dispatched_at"] is None

    await d.drain()  # still inside the backoff
    assert (await env.message(m["id"]))["status"] == "queued"
    assert env.fake.received_turns == 0

    await asyncio.sleep(1.1)
    await d.drain()
    await d.drain()
    await d.aclose()
    row = await env.message(m["id"])
    assert (row["status"], row["turn_id"]) == ("sent", env.fake.last_turn_id)
    assert env.fake.received_turns == 1
    assert env.fake.turn_texts == ["exactly once"]
    assert env.fake.turn_metadata == [{"orch_message_id": m["id"], "orch_attempt": 2}]


async def test_not_started_keeps_a_newer_reservation(env):
    """The reservation is cleared only while it is still this message's (D23 a)."""
    agent = await env.agent()
    m = await env.verifying(agent, age=SETTLE + 0.1)
    await env.exec("UPDATE agents SET last_dispatched_at = now() WHERE id=%s", (agent["id"],))
    newer = (await env.agent_row(agent["id"]))["last_dispatched_at"]
    assert await env.verifier().verify_once() == 1
    assert (await env.message(m["id"]))["status"] == "queued"
    assert (await env.agent_row(agent["id"]))["last_dispatched_at"] == newer


async def test_no_decision_inside_settle_window(env):
    agent = await env.agent()
    fresh = await env.verifying(agent, age=0)
    almost = await env.verifying(agent, age=SETTLE - 0.4)
    reserved = (await env.agent_row(agent["id"]))["last_dispatched_at"]
    v = env.verifier()
    assert await v.verify_once() == 0
    assert await v.verify_once() == 0
    for before in (fresh, almost):
        after = await env.message(before["id"])
        assert after == before  # no column changed
    assert (await env.agent_row(agent["id"]))["last_dispatched_at"] == reserved
    assert env.fake.received_turns == 0


async def test_unknown_outcome_waits_then_fails_at_deadline(env):
    reachable = await env.agent()
    lost = await env.agent(session="no-such-session")  # the wrapper answers 404
    unreachable_m = await env.verifying(reachable, age=SETTLE + 0.1)
    missing_m = await env.verifying(lost, age=SETTLE + 0.1)
    await env.fake.set_unreachable(True)
    v = env.verifier()
    d = env.dispatcher()
    try:
        assert await v.verify_once() == 0
        await d.drain()  # the wired verifier and the claim: still nothing re-sent
        for m in (unreachable_m, missing_m):
            row = await env.message(m["id"])
            assert (row["status"], row["attempts"], row["error"]) == ("verifying", 0, "ambiguous")

        for m in (unreachable_m, missing_m):
            await env.age(m["id"], DEADLINE + 0.1)
        assert await v.verify_once() == 2
        for m in (unreachable_m, missing_m):
            row = await env.message(m["id"])
            assert (row["status"], row["error"], row["attempts"]) == ("failed", "ambiguous", 0)
            assert row["turn_id"] is None

        await env.fake.set_unreachable(False)
        await d.drain()
        for m in (unreachable_m, missing_m):
            assert (await env.message(m["id"]))["status"] == "failed"  # no automatic resend
        assert env.fake.received_turns == 0

        # only the user's explicit Retry sends it again
        async with env.pool.connection() as conn:
            assert await q.retry_message(conn, unreachable_m["id"])
        await d.drain()
        assert (await env.message(unreachable_m["id"]))["status"] == "sent"
        assert env.fake.received_turns == 1
    finally:
        await d.aclose()


async def test_ambiguous_fail_keeps_newer_reservation(env):
    agent = await env.agent()
    m = await env.verifying(agent, age=DEADLINE + 0.1)
    await env.exec("UPDATE agents SET last_dispatched_at = now() WHERE id=%s", (agent["id"],))
    newer = (await env.agent_row(agent["id"]))["last_dispatched_at"]
    async with env.pool.connection() as conn:
        assert await q.fail_ambiguous(conn, m["id"])
        assert await q.retry_message(conn, m["id"])
    assert (await env.agent_row(agent["id"]))["last_dispatched_at"] == newer


async def test_stale_dispatching_resolved_by_verifier(env):
    agent = await env.agent()
    session = agent["wrapper_session_id"]
    m = await insert_message(env.pool, env.cid, status="dispatching", agent_id=agent["id"])
    await env.exec(
        "UPDATE messages SET dispatched_at = now() - interval '1 hour', heartbeat_at = NULL"
        " WHERE id=%s",
        (m["id"],),
    )
    # the lost dispatcher's send did reach the wrapper, and that turn has since completed
    turn_id = await env.submit(session, {"orch_message_id": m["id"], "orch_attempt": 1})
    await env.fake.complete_turn(turn_id)
    d = env.dispatcher()
    await d.drain()
    await d.aclose()
    row = await env.message(m["id"])
    assert row["ambiguous_at"] is not None
    assert (row["status"], row["turn_id"], row["error"]) == ("sent", turn_id, None)
    assert env.fake.received_turns == 1  # only the lost dispatcher's send


async def test_two_verifiers_resolve_once(env):
    agent = await env.agent()
    session = agent["wrapper_session_id"]
    started = await env.verifying(agent, age=0)
    turn_id = await env.submit(session, {"orch_message_id": started["id"]})
    await env.fake.complete_turn(turn_id)
    other = await env.agent()
    not_started = await env.verifying(other, age=SETTLE + 0.1)

    second_pool = await open_pool(env.dsn)
    try:
        v1, v2 = env.verifier(), env.verifier(second_pool)
        wins = await asyncio.gather(v1.verify_once(), v2.verify_once())
    finally:
        await second_pool.close()
    assert sum(wins) == 2  # one resolution per message across both verifiers
    row = await env.message(started["id"])
    assert (row["status"], row["turn_id"]) == ("sent", turn_id)
    row = await env.message(not_started["id"])
    assert (row["status"], row["attempts"]) == ("queued", 1)  # counted once, not twice
    assert env.fake.received_turns == 1


async def test_matches_on_message_id_not_any_turn(env):
    agent = await env.agent()
    session = agent["wrapper_session_id"]
    m = await env.verifying(agent, age=SETTLE + 0.1)
    # older turns of other messages, then a peer turn running right now on this agent
    for other in (m["id"] + 100, m["id"] + 101):
        await env.fake.complete_turn(await env.submit(session, {"orch_message_id": other}))
    peer_turn = await env.submit(session, {})
    snap = await env.client.get_session(session)
    assert snap.current_turn_id == peer_turn
    assert await env.verifier().verify_once() == 1
    row = await env.message(m["id"])
    assert (row["status"], row["turn_id"], row["attempts"]) == ("queued", None, 1)

    # and the message's own turn is found among the newest turns behind a running peer turn
    await env.fake.complete_turn(peer_turn)
    own = await env.verifying(agent, age=0)
    own_turn = await env.submit(session, {"orch_message_id": own["id"]})
    await env.fake.complete_turn(own_turn)
    peer_turn = await env.submit(session, {"orch_message_id": None})
    assert (await env.client.get_session(session)).current_turn_id == peer_turn
    assert await env.verifier().verify_once() == 1
    row = await env.message(own["id"])
    assert (row["status"], row["turn_id"]) == ("sent", own_turn)


def test_readme_documents_delivery_and_retries():
    import re
    from pathlib import Path

    readme = (Path(__file__).parent.parent / "README.md").read_text()
    match = re.search(r"^## Delivery and retries\s*$", readme, re.MULTILINE)
    assert match, "README has no '## Delivery and retries' section"
    body = re.split(r"^## ", readme[match.end() :], maxsplit=1, flags=re.MULTILINE)[0]
    for outcome in ("accepted", "busy", "unavailable", "gone", "config error", "rejected"):
        assert re.search(rf"^\| {outcome} \|", body, re.MULTILINE), outcome
    assert re.search(r"^\| ambiguous \|", body, re.MULTILINE)
    for needle in (
        "ORCH_RETRY_BACKOFF_BASE_SECONDS",
        "ORCH_RETRY_BACKOFF_MAX_SECONDS",
        "ORCH_AMBIGUOUS_SETTLE_SECONDS",
        "ORCH_AMBIGUOUS_DEADLINE_SECONDS",
        "max_attempts",
        "`verifying`",
        "orch_message_id",
        "never twice",
        "settle window",
    ):
        assert needle in body, needle


async def test_stale_command_row_fails_ambiguous(env):
    """A command has no turn to look for: a stale or verifying command row fails `ambiguous` at
    once and is never resent (D10)."""
    agent = await env.agent()
    m = await insert_message(
        env.pool, env.cid, text="/compact", status="dispatching", agent_id=agent["id"]
    )
    await env.exec(
        "UPDATE messages SET kind = 'command', dispatched_at = now() - interval '1 hour',"
        " heartbeat_at = NULL WHERE id=%s",
        (m["id"],),
    )
    d = env.dispatcher()
    await d.drain()
    row = await env.message(m["id"])
    assert row["ambiguous_at"] is not None  # handed to the verifier, which failed it
    assert (row["status"], row["error"], row["attempts"]) == ("failed", "ambiguous", 0)
    await d.drain()
    await d.aclose()
    assert (await env.message(m["id"]))["status"] == "failed"
    assert env.fake.sessions[agent["wrapper_session_id"]].commands == []
    assert env.fake.received_turns == 0

    # a verifying command row fails inside the settle window, with no wrapper lookup
    v = await env.verifying(agent, age=0)
    await env.exec("UPDATE messages SET kind = 'command' WHERE id=%s", (v["id"],))
    await env.fake.set_unreachable(True)
    assert await env.verifier().verify_once() == 1
    row = await env.message(v["id"])
    assert (row["status"], row["error"]) == ("failed", "ambiguous")


def test_readme_documents_command_dispatch():
    import re
    from pathlib import Path

    readme = (Path(__file__).parent.parent / "README.md").read_text()

    def section(title: str) -> str:
        match = re.search(rf"^## {re.escape(title)}\s*$", readme, re.MULTILINE)
        assert match, f"README has no '## {title}' section"
        return re.split(r"^## ", readme[match.end() :], maxsplit=1, flags=re.MULTILINE)[0]

    delivery = section("Delivery and retries")
    for needle in (
        "`/commands`",
        "`wrapper_command_id`",
        "`command_queue_full`",
        "`command_denied`",
        "`command_not_allowed`",
        "`invalid_command`",
        "`config_error`",
        "`ambiguous`",
        "run marker",
        "Retry",
    ):
        assert needle in delivery, needle
    architecture = section("Architecture").lower()
    for needle in ("`last_dispatched_at`", "per-session order", "`dispatching`", "overtake"):
        assert needle in architecture, needle
    api = section("API")
    post = api.split("### `POST /api/v1/sessions/{session_id}/messages`", 1)[1].split("\n### ")[0]
    assert "`kind`" in post and "slash" in post
    listing = api.split("### `GET /api/v1/sessions/{session_id}/messages`", 1)[1].split("\n### ")[0]
    for field in ("`kind`", "`wrapper_command_id`"):
        assert field in listing, field

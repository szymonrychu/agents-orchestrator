"""Cooperative context-pressure handoff (`dispatcher/handoff.handoff_pass`) against Postgres and
the fake wrapper (its pressure policy and in-flight inventory).

Time moves through SQL: free_since, discuss_since and command times are pushed into the past.
Submits are written the way `api/agent.handoff_submit` writes them (the route has its own tests).
"""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from types import SimpleNamespace

import psycopg
import pytest
from factories import (
    insert_agent,
    insert_event,
    insert_message,
    insert_session,
    set_agent_status,
)
from fakes import fake_wrapper
from fakes.fake_wrapper import FakeWrapper
from psycopg.rows import dict_row

from agents_orchestrator import handoff_gate, handoff_texts
from agents_orchestrator.db import handoffs, handover, leadership, queue
from agents_orchestrator.db.queue import _CLAIM_LOCK_NAMESPACE
from agents_orchestrator.dispatcher import Dispatcher, handoff, hive
from agents_orchestrator.settings import Settings
from agents_orchestrator.wrapper.client import WrapperClient
from agents_orchestrator.wrapper.models import Inflight

KEY = "k" * 32
TOKEN = "test-token"
WINDOW = 200000
# compact 95 (the default), 200k window: L2 (strong) = min(85%, 95% - 60k) = 130k tokens,
# L3 (critical) = min(90%, 95% - 25k) = 165k tokens
STRONG = 140000
CRITICAL = 170000
LOW = 20000
HANDOFF = {
    "doing": "wiring the cooperative handoff into the dispatcher",
    "done": ["commit abc123 on feat/forced-handoff"],
    "in_flight": [],
    "next": ["open the draft PR", "watch CI"],
    "open_requests": [],
    "results": ["tests in tests/test_handoff_pass.py"],
    "gotchas": ["reinstall the wrapper after mise run lint"],
}
TASK = {"id": "bf3a", "description": "CI watch"}


@pytest.fixture
async def fake(monkeypatch):
    monkeypatch.setattr(fake_wrapper, "MAX_SESSIONS", 200)
    f = FakeWrapper(token=TOKEN)
    await f.start()
    try:
        yield f
    finally:
        await f.stop()


class Env:
    def __init__(self, pool, dsn: str, fake: FakeWrapper) -> None:
        self.pool, self.dsn, self.fake = pool, dsn, fake
        self.settings = Settings(database_url=dsn, master_key=KEY)
        self.policy = handoff.HandoffPolicy.from_settings(self.settings)
        self.clients: list[WrapperClient] = []
        self.fetch = handoff.inflight_fetcher(self.factory, 5.0)
        self.state = hive.HiveState()
        self.clock = 1000.0

    def factory(self, session_id, url) -> WrapperClient:
        client = WrapperClient(url, TOKEN)
        self.clients.append(client)
        return client

    async def run(self, sql: str, params: tuple = ()) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(sql, params)

    async def rows(self, sql: str, params: tuple = ()) -> list[dict]:
        async with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            return await cur.fetchall()

    async def step(self, session_id) -> list[str]:
        async with self.pool.connection() as conn:
            return await handoff.handoff_pass(
                conn, session_id, self.policy, self.settings, self.fetch
            )

    async def push(self, advance: float = 100.0) -> None:
        self.clock += advance
        await hive.push_once(
            self.pool, self.settings, self.factory, self.state, clock=lambda: self.clock
        )

    def policy_of(self, agent: dict) -> dict | None:
        return self.fake.pressure.get(agent["wrapper_session_id"])

    async def wsid(self) -> str:
        client = WrapperClient(self.fake.base_url, TOKEN)
        try:
            return (
                await client.create_session(f"h{uuid.uuid4().hex[:6]}", "/w")
            ).wrapper_session_id
        finally:
            await client.aclose()

    async def agent(self, session_id, ordinal: int) -> dict:
        return await insert_agent(
            self.pool,
            session_id,
            ordinal=ordinal,
            ready=True,
            wrapper_session_id=await self.wsid(),
            url=self.fake.base_url,
        )

    async def gauge(self, agent_id, used: int, **kw) -> None:
        kw.setdefault("state", "idle")
        await set_agent_status(
            self.pool,
            agent_id,
            context_used_tokens=used,
            context_window_tokens=WINDOW,
            context_pct=100 * used / WINDOW,
            **kw,
        )

    async def solo(self, used: int = STRONG) -> tuple[dict, dict]:
        c = await insert_session(self.pool, spec={})
        a = await self.agent(c["id"], 0)
        await self.gauge(a["id"], used)
        return c, a

    async def hive(self, n: int = 3, used: int = STRONG) -> tuple[dict, list[dict]]:
        c = await insert_session(self.pool, spec={"replicas": n, "max_replicas": 4})
        agents = []
        for i in range(n):
            a = await self.agent(c["id"], i)
            await self.gauge(a["id"], LOW)
            agents.append(a)
        async with self.pool.connection() as conn:
            await leadership.leadership_pass(conn, c["id"])
        await self.gauge(agents[0]["id"], used)
        return c, agents

    async def lpass(self, session_id):
        async with self.pool.connection() as conn:
            return await leadership.leadership_pass(conn, session_id)

    async def row(self, agent_id) -> handoffs.Handoff | None:
        async with self.pool.connection() as conn:
            return await handoffs.open_row(conn, agent_id)

    async def last_row(self, agent_id) -> dict:
        return (
            await self.rows(
                "SELECT * FROM agent_handoffs WHERE agent_id = %s ORDER BY id DESC LIMIT 1",
                (agent_id,),
            )
        )[0]

    async def messages(self, agent_id, notice: str | None = None) -> list[dict]:
        return await self.rows(
            "SELECT * FROM messages WHERE target_agent_id = %s"
            " AND (%s::text IS NULL OR notice = %s) ORDER BY id",
            (agent_id, notice, notice),
        )

    async def commands(self, agent_id) -> list[dict]:
        return await self.rows(
            "SELECT * FROM commands WHERE agent_id = %s AND kind = 'clear' ORDER BY id",
            (agent_id,),
        )

    async def requests(self, session_id) -> list[dict]:
        return await self.rows(
            "SELECT * FROM leadership_requests WHERE session_id = %s ORDER BY id", (session_id,)
        )

    def inflight(self, agent: dict, **fields) -> None:
        self.fake.set_inflight(agent["wrapper_session_id"], **fields)

    async def submit(self, agent: dict, *, to: str | None = None, abandon=()) -> bool:
        """What the submit route stores for a text that passed the quality check: the handoff,
        the wrapper's inventory with the gate's verdict, a gate block when it blocks. Opens a
        voluntary row when none is open. True when the gate was clear."""
        inflight = Inflight.model_validate(self.fake.inflight.get(agent["wrapper_session_id"], {}))
        gate = handoff_gate.evaluate(inflight, [SimpleNamespace(**a) for a in abandon])
        snap = {
            **inflight.model_dump(mode="json"),
            "waiting_on": gate.waiting_on,
            "abandoned": gate.abandoned,
        }
        stored = {**HANDOFF, "to": to, "abandon": list(abandon), "abandoned": gate.abandoned}
        async with self.pool.connection() as conn:
            row = await handoffs.open_row(conn, agent["id"])
            if row is None:
                row = await handoffs.open_pressure(
                    conn, agent["session_id"], agent["id"], 0, "work", "voluntary"
                )
            assert await handoffs.submit(conn, row.id, stored, snap, None)
            if not gate.clear:
                await handoffs.record_gate_block(conn, row.id, snap)
        return gate.clear

    async def idle_for(self, agent_id, seconds: int) -> None:
        await self.run(
            "UPDATE agent_status SET state = 'idle', current_turn_id = NULL,"
            " free_since = now() - make_interval(secs => %s) WHERE agent_id = %s",
            (seconds, agent_id),
        )

    async def busy(self, agent_id) -> None:
        await self.run(
            "UPDATE agent_status SET state = 'busy', current_turn_id = 't-busy'"
            " WHERE agent_id = %s",
            (agent_id,),
        )

    async def cleared(self, session_id, agent_id) -> None:
        await insert_event(self.pool, session_id, agent_id, type="session_cleared", data={})

    async def finish_command(self, command_id: int, status: str, result: dict) -> None:
        await self.run(
            "UPDATE commands SET status = %s, result = %s::jsonb, finished_at = now()"
            " WHERE id = %s",
            (status, psycopg.types.json.Jsonb(result), command_id),
        )

    async def to_clearing(self, c: dict, a: dict) -> dict:
        """A solo agent under pressure with an accepted gate-clear submit, cleared once idle."""
        assert await self.step(c["id"]) == [f"pressuring:{a['id']}"]
        assert await self.submit(a)
        assert await self.step(c["id"]) == [f"clearing:{a['id']}"]
        (cmd,) = await self.commands(a["id"])
        return cmd

    async def to_returning(self, c: dict, a: dict) -> dict:
        cmd = await self.to_clearing(c, a)
        await self.finish_command(cmd["id"], "done", {"response": {"id": "c1"}})
        await self.cleared(c["id"], a["id"])
        assert await self.step(c["id"]) == [f"returning:{a['id']}"]
        (ret,) = await self.messages(a["id"], "handoff_return")
        return ret


@pytest.fixture
async def env(pool, dsn, fake):
    e = Env(pool, dsn, fake)
    try:
        yield e
    finally:
        for client in e.clients:
            await client.aclose()


# ---------------------------------------------------------------- settings


def test_removed_handover_settings_are_gone(dsn, monkeypatch):
    for name in (
        "handover_notify_seconds",
        "handover_extension_seconds",
        "handover_idle_proceed_seconds",
    ):
        assert name not in Settings.model_fields
        monkeypatch.setenv(f"ORCH_{name.upper()}", "77")
    s = Settings(database_url=dsn, master_key=KEY)  # an old env is ignored, not an alias
    assert not hasattr(s, "handover_notify_seconds")
    assert s.agent_shutdown_idle_proceed_seconds == 60


def test_quiet_settings_defaults_and_bounds(dsn):
    s = Settings(database_url=dsn, master_key=KEY)
    assert (s.handover_quiet_seconds, s.handover_quiet_cap_seconds) == (60, 600)
    with pytest.raises(ValueError):
        Settings(
            database_url=dsn,
            master_key=KEY,
            handover_quiet_seconds=900,
            handover_quiet_cap_seconds=600,
        )


def test_clear_settings_defaults_and_bounds(dsn):
    s = Settings(database_url=dsn, master_key=KEY)
    assert (s.handover_clear_timeout_seconds, s.handover_clear_failed_block_seconds) == (
        180,
        1800,
    )
    with pytest.raises(ValueError):
        Settings(database_url=dsn, master_key=KEY, handover_clear_timeout_seconds=5)


# ---------------------------------------------------------------- open and pressure


async def test_l1_opens_pressuring_row_and_pushes_policy(env, dsn):
    env.settings = Settings(database_url=dsn, master_key=KEY, handover_budget_tokens=50000)
    c, a = await env.solo(used=100000)  # past start + 50k, below L2 (130k)
    await env.run(
        "INSERT INTO agent_limits (agent_id, start_context_tokens, since)"
        " VALUES (%s, 0, now() - interval '2 hours')",
        (a["id"],),
    )
    assert await env.step(c["id"]) == [f"pressuring:{a['id']}"]
    row = await env.row(a["id"])
    assert (row.trigger, row.phase, row.level, row.profile) == (
        "pressure",
        "pressuring",
        1,
        "work",
    )
    assert await env.messages(a["id"]) == []  # pressure is a policy, never a message
    assert await env.step(c["id"]) == []  # one row
    await env.push()
    policy = env.policy_of(a)
    assert policy is not None and policy["version"] == 1
    assert policy["handoff_state"] == "none" and policy["profile"] == "work"
    assert policy["thresholds"]["l1_tokens"] == 50000
    assert policy["thresholds"]["l2_tokens"] == 130000


async def test_below_l1_opens_nothing_and_pushes_nothing(env):
    c, a = await env.solo(used=LOW)
    assert await env.step(c["id"]) == []
    assert await env.row(a["id"]) is None
    await env.push()
    assert env.policy_of(a) is None


async def test_level_rises_to_l3_no_clear_no_message(env):
    c, a = await env.solo(used=STRONG)
    assert await env.step(c["id"]) == [f"pressuring:{a['id']}"]
    assert (await env.row(a["id"])).level == 2
    await env.gauge(a["id"], CRITICAL)
    assert await env.step(c["id"]) == [f"level:{a['id']}:3"]
    row = await env.row(a["id"])
    assert (row.phase, row.level, row.l2_tokens_at) == ("pressuring", 3, STRONG)
    await env.idle_for(a["id"], 3600)  # idle for an hour: still nothing is forced
    for _ in range(3):
        assert await env.step(c["id"]) == []
    assert await env.commands(a["id"]) == []
    assert await env.messages(a["id"]) == []
    assert await env.rows("SELECT * FROM commands WHERE agent_id = %s", (a["id"],)) == []


async def test_member_never_gets_a_row(env):
    c, (a0, a1, a2) = await env.hive()
    for a in (a1, a2):
        await env.gauge(a["id"], CRITICAL)
    assert await env.step(c["id"]) == [f"pressuring:{a0['id']}"]
    await env.push()
    for a in (a1, a2):
        assert await env.row(a["id"]) is None
        assert await env.messages(a["id"]) == []
        assert env.policy_of(a) is None


async def test_stopping_or_not_live_agent_opens_nothing(env):
    c, a = await env.solo()
    await env.run("UPDATE agent_status SET stopping_since = now() WHERE agent_id = %s", (a["id"],))
    assert await env.step(c["id"]) == []
    await env.run("UPDATE agent_status SET stopping_since = NULL WHERE agent_id = %s", (a["id"],))
    await env.run("UPDATE agents SET ready = false WHERE id = %s", (a["id"],))
    assert await env.step(c["id"]) == []
    assert await env.row(a["id"]) is None


async def test_chat_activity_profiles_the_row_chat(env):
    c, a = await env.solo()
    await env.run(
        "INSERT INTO chat_activity (session_id, agent_id, direction) VALUES (%s, %s, 'out')",
        (c["id"], a["id"]),
    )
    await env.step(c["id"])
    assert (await env.row(a["id"])).profile == "chat"
    await env.push()
    assert env.policy_of(a)["profile"] == "chat"


# ---------------------------------------------------------------- submitted


async def test_submitted_clear_solo_waits_for_idle_then_clears(env):
    c, a = await env.solo()
    assert await env.step(c["id"]) == [f"pressuring:{a['id']}"]
    await env.busy(a["id"])
    assert await env.submit(a)
    assert await env.step(c["id"]) == []  # a running turn: never interrupted
    assert await env.commands(a["id"]) == []
    await env.idle_for(a["id"], 1)
    await env.run(
        "UPDATE agent_status SET background_subagents = jsonb_build_object('x', now()::text)"
        " WHERE agent_id = %s",
        (a["id"],),
    )
    assert await env.step(c["id"]) == []  # a background subagent runs
    await env.run(
        "UPDATE agent_status SET background_subagents = '{}' WHERE agent_id = %s", (a["id"],)
    )
    assert await env.step(c["id"]) == [f"clearing:{a['id']}"]
    (cmd,) = await env.commands(a["id"])
    row = await env.row(a["id"])
    assert (row.phase, row.clear_command_id, row.clear_attempts) == ("clearing", cmd["id"], 1)
    assert await env.step(c["id"]) == []  # no return before the clear happened
    await env.finish_command(cmd["id"], "done", {"response": {"id": "c1"}})
    await env.cleared(c["id"], a["id"])
    assert await env.step(c["id"]) == [f"returning:{a['id']}"]
    (ret,) = await env.messages(a["id"], "handoff_return")
    assert ret["text"] == handoff_texts.return_text(row.handoff, cleared=True)
    assert ret["text"].startswith("Your context was cleared after your accepted handoff.")
    assert "continue with: open the draft PR" in ret["text"]
    assert "Doing: wiring the cooperative handoff into the dispatcher" in ret["text"]
    assert (ret["lane"], ret["target_agent_id"]) == ("regular", a["id"])
    await env.run("UPDATE messages SET status = 'sent' WHERE id = %s", (ret["id"],))
    assert await env.step(c["id"]) == [f"done:{a['id']}"]
    last = await env.last_row(a["id"])
    assert (last["phase"], last["close_reason"]) == ("done", "returned")
    assert last["cleared_at"] is not None


async def test_submitted_blocked_stays_and_pushes_waiting_on(env):
    c, a = await env.solo()
    await env.step(c["id"])
    env.inflight(a, background_tasks=[TASK])
    assert not await env.submit(a)
    await env.idle_for(a["id"], 600)
    for _ in range(2):
        assert await env.step(c["id"]) == []  # the stored inventory is current
    row = await env.row(a["id"])
    assert row.phase == "submitted"
    assert row.inflight["waiting_on"] == ["task:bf3a (CI watch)"]
    assert await env.commands(a["id"]) == []
    await env.push()
    policy = env.policy_of(a)
    assert policy["handoff_state"] == "blocked"
    assert policy["waiting_on"] == ["task:bf3a (CI watch)"]
    assert "{waiting_on}" in policy["texts"]["l2"]["full"]
    # a second task starts: the stored inventory follows, the policy too
    env.inflight(a, background_tasks=[TASK, {"id": "c9", "description": "tail logs"}])
    assert await env.step(c["id"]) == [f"blocked:{a['id']}"]
    await env.push()
    assert env.policy_of(a)["waiting_on"] == ["task:bf3a (CI watch)", "task:c9 (tail logs)"]
    assert env.policy_of(a)["version"] == 2


async def test_blocker_clears_tells_agent_to_resubmit(env):
    c, a = await env.solo()
    await env.step(c["id"])
    env.inflight(a, background_tasks=[TASK])
    assert not await env.submit(a)
    await env.step(c["id"])
    env.inflight(a)  # the task reported
    assert await env.step(c["id"]) == [f"resubmit:{a['id']}"]
    row = await env.row(a["id"])
    assert row.phase == "submitted" and row.inflight["resubmit"] is True
    assert row.inflight["waiting_on"] == []
    for _ in range(2):
        assert await env.step(c["id"]) == []  # idle, gate clear: still waits for the resubmit
    assert await env.commands(a["id"]) == []
    await env.push()
    policy = env.policy_of(a)
    assert policy["handoff_state"] == "submitted"
    assert handoff_texts.resubmit_text() in policy["texts"]["l2"]["full"]
    assert await env.submit(a)  # the agent re-reads the results and submits again
    assert await env.step(c["id"]) == [f"clearing:{a['id']}"]


async def test_abandoned_task_does_not_block_but_still_holds_the_idle_clear(env):
    c, a = await env.solo()
    await env.step(c["id"])
    env.inflight(a, background_tasks=[TASK])
    abandon = [{"item": "task:bf3a", "why": "stale CI watcher, the PR is merged"}]
    assert await env.submit(a, abandon=abandon)
    assert await env.step(c["id"]) == []  # still running: the agent is not idle yet
    env.inflight(a)
    assert await env.step(c["id"]) == [f"clearing:{a['id']}"]


async def test_hive_submit_inserts_limit_request_moving(env):
    c, (a0, a1, a2) = await env.hive()
    assert await env.step(c["id"]) == [f"pressuring:{a0['id']}"]
    assert await env.submit(a0, to=a2["pod_name"])
    assert await env.step(c["id"]) == [f"moving:{a0['id']}"]
    (lr,) = await env.requests(c["id"])
    assert (lr["kind"], lr["reason"], lr["from_agent_id"], lr["to_agent_id"], lr["epoch"]) == (
        "handover",
        handoff.LIMIT_REASON,
        a0["id"],
        a2["id"],
        1,
    )
    row = await env.row(a0["id"])
    assert (row.phase, row.leadership_request_id) == ("moving", lr["id"])
    assert await env.commands(a0["id"]) == []  # no clear before the discussion
    assert await env.step(c["id"]) == []  # waits for the leadership pass, one request
    assert len(await env.requests(c["id"])) == 1
    change = await env.lpass(c["id"])
    assert change is not None and change.agent_id == a2["id"]
    row = await env.row(a0["id"])
    assert (row.phase, row.successor_agent_id) == ("discussing", a2["id"])


async def test_hive_submit_to_an_ineligible_pod_lets_the_pass_pick(env):
    c, (a0, a1, a2) = await env.hive()
    await env.step(c["id"])
    await env.run("UPDATE agents SET ready = false WHERE id = %s", (a2["id"],))
    assert await env.submit(a0, to=a2["pod_name"])
    assert await env.step(c["id"]) == [f"moving:{a0['id']}"]
    (lr,) = await env.requests(c["id"])
    assert lr["to_agent_id"] is None


async def test_rejected_limit_request_falls_back_to_solo_order(env):
    c, (a0, a1, a2) = await env.hive()
    await env.step(c["id"])
    assert await env.submit(a0)
    await env.step(c["id"])
    await env.run(
        "UPDATE leadership_requests SET status = 'rejected', reject_reason = 'no_peers',"
        " decided_at = now()"
    )
    env.inflight(a0, background_tasks=[TASK])
    assert await env.step(c["id"]) == []  # still the leader, but the gate blocks
    env.inflight(a0)
    assert await env.step(c["id"]) == [f"clearing:{a0['id']}"]
    assert len(await env.commands(a0["id"])) == 1


async def test_single_live_hive_follows_solo_order(env):
    c, (a0, a1) = await env.hive(2)
    await env.run("UPDATE agents SET ready = false WHERE id = %s", (a1["id"],))
    await env.step(c["id"])
    assert await env.submit(a0)
    assert await env.step(c["id"]) == [f"clearing:{a0['id']}"]
    assert await env.requests(c["id"]) == []
    (cmd,) = await env.commands(a0["id"])
    await env.finish_command(cmd["id"], "done", {"response": {"id": "c1"}})
    await env.cleared(c["id"], a0["id"])
    assert await env.step(c["id"]) == [f"returning:{a0['id']}"]
    (ret,) = await env.messages(a0["id"], "handoff_return")
    assert "open the draft PR" in ret["text"]


async def test_solo_voluntary_submit_below_l1_clears_and_returns(env):
    c, a = await env.solo(used=LOW)
    assert await env.step(c["id"]) == []
    assert await env.submit(a)  # the route opens a voluntary row at level 0
    assert await env.step(c["id"]) == [f"clearing:{a['id']}"]
    (cmd,) = await env.commands(a["id"])
    await env.finish_command(cmd["id"], "done", {"response": {"id": "c1"}})
    await env.cleared(c["id"], a["id"])
    assert await env.step(c["id"]) == [f"returning:{a['id']}"]
    last = await env.last_row(a["id"])
    assert last["trigger"] == "voluntary"


async def test_stale_voluntary_pressuring_row_is_closed(env):
    # a rejected or lost voluntary submit left a level-0 row in pressuring (step 10 review)
    c, a = await env.solo(used=LOW)
    async with env.pool.connection() as conn:
        await handoffs.open_pressure(conn, c["id"], a["id"], 0, "work", "voluntary")
    assert await env.step(c["id"]) == [f"cancelled:{a['id']}:stale"]
    assert await env.row(a["id"]) is None
    # at L1 or above the same row is an ordinary pressure row and stays
    async with env.pool.connection() as conn:
        await handoffs.open_pressure(conn, c["id"], a["id"], 0, "work", "voluntary")
    await env.gauge(a["id"], STRONG)
    assert await env.step(c["id"]) == [f"level:{a['id']}:2"]
    assert (await env.row(a["id"])).phase == "pressuring"


async def test_inflight_timeout_skips_the_session(env):
    c, a = await env.solo()
    await env.step(c["id"])
    assert await env.submit(a)

    async def slow(session_id, url, wsid):
        raise TimeoutError

    env.fetch = slow
    assert await env.step(c["id"]) == []
    assert (await env.row(a["id"])).phase == "submitted"
    assert await env.commands(a["id"]) == []


async def test_unreachable_wrapper_blocks_the_gate(env):
    c, a = await env.solo()
    await env.step(c["id"])
    assert await env.submit(a)
    await env.run("UPDATE agents SET url = 'http://127.0.0.1:1' WHERE id = %s", (a["id"],))
    assert await env.step(c["id"]) == [f"blocked:{a['id']}"]
    assert (await env.row(a["id"])).inflight["waiting_on"] == [handoff_gate.UNREACHABLE]
    assert await env.commands(a["id"]) == []


# ---------------------------------------------------------------- discussing


async def _discussing(env, *, quiet_ago: int, since_ago: int):
    c, (a0, a1, a2) = await env.hive(used=LOW)
    async with env.pool.connection() as conn:
        row = await handoffs.insert(conn, c["id"], a0["id"], "voluntary", "discussing")
    await env.run(
        "UPDATE agent_handoffs SET discuss_since = now() - make_interval(secs => %s) WHERE id = %s",
        (since_ago, row.id),
    )
    await env.run(
        "UPDATE sessions SET created_at = now() - interval '1 day',"
        " last_update_at = now() - interval '1 day' WHERE id = %s",
        (c["id"],),
    )
    for a in (a0, a1, a2):
        await env.idle_for(a["id"], quiet_ago)
    return c, (a0, a1, a2)


async def test_discussing_clears_after_quiet(env):
    c, (a0, a1, a2) = await _discussing(env, quiet_ago=30, since_ago=30)
    assert await env.step(c["id"]) == []  # quiet for 30 s only
    for a in (a0, a1, a2):
        await env.idle_for(a["id"], 61)
    await env.run("UPDATE agent_handoffs SET discuss_since = now() - interval '61 seconds'")
    assert await env.step(c["id"]) == [f"clearing:{a0['id']}"]
    (cmd,) = await env.commands(a0["id"])
    await env.finish_command(cmd["id"], "done", {"response": {"id": "c1"}})
    await env.cleared(c["id"], a0["id"])
    assert await env.step(c["id"]) == [f"done:{a0['id']}"]
    last = await env.last_row(a0["id"])
    assert (last["phase"], last["close_reason"]) == ("done", "cleared")
    assert last["cleared_at"] is not None
    assert await env.messages(a0["id"], "handoff_return") == []  # hive order: no return


async def test_voluntary_handover_clears_after_quiet_window(env):
    # m890 end to end: request -> apply (no clear) -> discussing -> quiet 60 s -> clear -> done
    c, (a0, a1, a2) = await env.hive(used=LOW)
    async with env.pool.connection() as conn:
        await leadership.insert_request(
            conn, c["id"], "handover", a0["id"], a1["id"], "context full", 1
        )
    change = await env.lpass(c["id"])
    assert change is not None and change.agent_id == a1["id"]
    assert await env.commands(a0["id"]) == []  # the apply no longer clears
    row = await env.row(a0["id"])
    assert (row.trigger, row.phase, row.handoff_text) == ("voluntary", "discussing", "context full")
    assert await env.step(c["id"]) == []  # the window is open
    for a in (a0, a1, a2):
        await env.idle_for(a["id"], 61)
    await env.run(
        "UPDATE sessions SET created_at = now() - interval '1 day',"
        " last_update_at = now() - interval '1 day' WHERE id = %s",
        (c["id"],),
    )
    await env.run("UPDATE agent_handoffs SET discuss_since = now() - interval '61 seconds'")
    assert await env.step(c["id"]) == [f"clearing:{a0['id']}"]
    (cmd,) = await env.commands(a0["id"])
    await env.finish_command(cmd["id"], "done", {"response": {"id": "c1"}})
    await env.cleared(c["id"], a0["id"])
    assert await env.step(c["id"]) == [f"done:{a0['id']}"]


async def test_discussing_cap_drops_only_the_session_quiet_requirement(env):
    c, (a0, a1, a2) = await _discussing(env, quiet_ago=700, since_ago=300)
    await env.run("UPDATE agent_status SET state = 'busy' WHERE agent_id = %s", (a1["id"],))
    assert await env.step(c["id"]) == []
    await env.run("UPDATE agent_handoffs SET discuss_since = now() - interval '601 seconds'")
    assert await env.step(c["id"]) == [f"clearing:{a0['id']}"]


async def test_ex_leader_not_cleared_while_inflight_after_quiet_cap(env):
    c, (a0, a1, a2) = await _discussing(env, quiet_ago=700, since_ago=601)
    env.inflight(a0, subagents=1, worktrees=[{"path": "/workdir/code/x", "dirty": 2}])
    assert await env.step(c["id"]) == [f"blocked:{a0['id']}"]
    for _ in range(2):
        assert await env.step(c["id"]) == []  # quiet and capped, still not cleared
    assert await env.commands(a0["id"]) == []
    row = await env.row(a0["id"])
    assert row.phase == "discussing"
    assert row.inflight["waiting_on"] == [
        "subagent:1 running",
        "worktree:/workdir/code/x dirty 2",
    ]
    await env.push()  # the ex-leader is a member now, but keeps its pressure until cleared
    policy = env.policy_of(a0)
    assert policy["handoff_state"] == "accepted"
    assert policy["waiting_on"] == row.inflight["waiting_on"]
    env.inflight(a0)
    assert await env.step(c["id"]) == [f"clearing:{a0['id']}"]


async def test_ex_leader_cleared_when_quiet_and_gate_clear(env):
    c, (a0, a1, a2) = await _discussing(env, quiet_ago=61, since_ago=61)
    env.inflight(a0, crons=[{"id": "c1", "schedule": "*/5 * * * *", "prompt": "check"}])
    assert await env.step(c["id"]) == [f"clearing:{a0['id']}"]  # crons never block


async def test_ex_leader_policy_removed_once_its_row_closed(env):
    c, (a0, a1, a2) = await env.hive(used=STRONG)
    await env.step(c["id"])
    assert await env.submit(a0)
    await env.step(c["id"])
    await env.push()
    assert env.policy_of(a0)["handoff_state"] == "accepted"
    assert (await env.lpass(c["id"])).agent_id != a0["id"]
    assert (await env.row(a0["id"])).phase == "discussing"
    await env.push()
    assert env.policy_of(a0) is not None  # a member now, its row still open: pressure stays
    await env.run("UPDATE agent_handoffs SET discuss_since = now() - interval '601 seconds'")
    assert await env.step(c["id"]) == [f"clearing:{a0['id']}"]
    (cmd,) = await env.commands(a0["id"])
    await env.finish_command(cmd["id"], "done", {"response": {"id": "c1"}})
    await env.cleared(c["id"], a0["id"])
    assert await env.step(c["id"]) == [f"done:{a0['id']}"]
    await env.push()
    assert env.policy_of(a0) is None  # a member gets no pressure


# ---------------------------------------------------------------- closing before moving


async def test_compaction_closes_pressuring_and_submitted_compacted(env):
    c, a = await env.solo()
    await env.step(c["id"])
    await insert_event(env.pool, c["id"], a["id"], type="compacted", data={"trigger": "auto"})
    assert await env.step(c["id"]) == [f"done:{a['id']}:compacted"]
    last = await env.last_row(a["id"])
    assert (last["phase"], last["close_reason"]) == ("done", "compacted")
    # the gauge still shows the pre-compaction context: no new row until it is re-read
    assert await env.step(c["id"]) == []
    # submitted (with a nudge still queued): the gauge dropped below L1 (a manual /compact)
    c2, a2 = await env.solo()
    await env.step(c2["id"])
    nudge = await insert_message(
        env.pool,
        c2["id"],
        text="nudge",
        target_agent_id=a2["id"],
        lane="regular",
        notice="handoff_nudge",
    )
    env.inflight(a2, background_tasks=[TASK])
    assert not await env.submit(a2)
    await env.gauge(a2["id"], LOW)
    assert await env.step(c2["id"]) == [f"done:{a2['id']}:compacted"]
    assert (await env.rows("SELECT status FROM messages WHERE id = %s", (nudge["id"],)))[0][
        "status"
    ] == "cancelled"
    assert await env.commands(a2["id"]) == []
    # the next L1 crossing starts a new cycle
    await env.gauge(a2["id"], STRONG)
    assert await env.step(c2["id"]) == [f"pressuring:{a2['id']}"]


async def test_compacting_alone_does_not_close(env):
    c, a = await env.solo()
    await env.step(c["id"])
    await insert_event(env.pool, c["id"], a["id"], type="compacting", data={})
    await insert_event(env.pool, c["id"], a["id"], type="compact_failed", data={})
    assert await env.step(c["id"]) == []
    assert (await env.row(a["id"])).phase == "pressuring"


async def test_external_clear_while_pressuring_cancels_cleared(env):
    c, a = await env.solo()
    await env.step(c["id"])
    await env.cleared(c["id"], a["id"])
    assert await env.step(c["id"]) == [f"cancelled:{a['id']}:cleared"]
    last = await env.last_row(a["id"])
    assert (last["phase"], last["close_reason"]) == ("cancelled", "cleared")


async def test_shutdown_and_closing_cancel(env):
    c, a = await env.solo()
    await env.step(c["id"])
    await env.run("UPDATE agent_status SET stopping_since = now() WHERE agent_id = %s", (a["id"],))
    assert await env.step(c["id"]) == [f"cancelled:{a['id']}:shutdown"]
    c2, a2 = await env.solo()
    await env.step(c2["id"])
    await env.run("UPDATE sessions SET desired = 'deleting' WHERE id = %s", (c2["id"],))
    assert await env.step(c2["id"]) == [f"cancelled:{a2['id']}:session_closing"]


async def test_leadership_change_cancels_not_leader(env):
    c, (a0, a1, a2) = await env.hive()
    await env.step(c["id"])
    async with env.pool.connection() as conn:
        await leadership.insert_request(conn, c["id"], "user", None, a1["id"], "test", None)
    await env.lpass(c["id"])
    # the leadership pass closes the row in the move's transaction (handoffs.on_leader_moved)
    assert await env.step(c["id"]) == []
    last = await env.last_row(a0["id"])
    assert (last["phase"], last["close_reason"]) == ("cancelled", "not_leader")
    assert await env.commands(a0["id"]) == []


async def test_limit_request_rejected_after_a_move_closes_not_leader(env):
    c, (a0, a1, a2) = await env.hive()
    await env.step(c["id"])
    assert await env.submit(a0)
    await env.step(c["id"])
    await env.run(
        "UPDATE leadership_requests SET status = 'rejected', reject_reason = 'x',"
        " decided_at = now()"
    )
    await env.run(
        "UPDATE session_leaders SET agent_id = %s, epoch = epoch + 1 WHERE session_id = %s",
        (a1["id"], c["id"]),
    )
    assert await env.step(c["id"]) == [f"cancelled:{a0['id']}:not_leader"]
    assert await env.commands(a0["id"]) == []


# ---------------------------------------------------------------- clearing


async def test_clear_retry_requires_gate(env):
    c, a = await env.solo()
    cmd = await env.to_clearing(c, a)
    await env.finish_command(
        cmd["id"], "failed", {"error": "WrapperError: 409 subagents_running: /clear is refused"}
    )
    env.inflight(a, background_tasks=[TASK])  # work started again after the submit
    for _ in range(2):
        assert await env.step(c["id"]) == []
    assert len(await env.commands(a["id"])) == 1
    env.inflight(a)
    assert await env.step(c["id"]) == [f"clear_retry:{a['id']}"]
    assert len(await env.commands(a["id"])) == 2


async def test_clear_refused_subagents_running_is_retried(env):
    c, a = await env.solo()
    cmd = await env.to_clearing(c, a)
    # refused at the wrapper's POST while a background subagent runs
    await env.finish_command(
        cmd["id"], "failed", {"error": "WrapperError: 409 subagents_running: /clear is refused"}
    )
    await env.run(
        "UPDATE agent_status SET background_subagents = jsonb_build_object('x', now()::text)"
        " WHERE agent_id = %s",
        (a["id"],),
    )
    assert await env.step(c["id"]) == []  # waits for the background work
    await env.run(
        "UPDATE agent_status SET background_subagents = '{}' WHERE agent_id = %s", (a["id"],)
    )
    assert await env.step(c["id"]) == [f"clear_retry:{a['id']}"]
    cmds = await env.commands(a["id"])
    assert len(cmds) == 2
    row = await env.row(a["id"])
    assert (row.clear_command_id, row.clear_attempts) == (cmds[1]["id"], 2)
    # accepted, then failed asynchronously in the wrapper: seen through its command_failed event
    await env.finish_command(cmds[1]["id"], "done", {"response": {"id": "w-2"}})
    await insert_event(
        env.pool,
        c["id"],
        a["id"],
        type="command_failed",
        data={"record": {"id": "w-2", "name": "clear", "error": "subagents_running"}},
    )
    assert await env.step(c["id"]) == [f"clear_retry:{a['id']}"]
    # after 5 attempts the row closes clear_failed and the solo agent still gets its handoff
    await env.run("UPDATE agent_handoffs SET clear_attempts = 5 WHERE agent_id = %s", (a["id"],))
    last_cmd = (await env.commands(a["id"]))[-1]
    await env.finish_command(last_cmd["id"], "failed", {"error": "409 subagents_running"})
    assert await env.step(c["id"]) == [f"cancelled:{a['id']}:clear_failed"]
    last = await env.last_row(a["id"])
    assert (last["phase"], last["close_reason"]) == ("cancelled", "clear_failed")
    (ret,) = await env.messages(a["id"], "handoff_return")
    assert last["return_message_id"] == ret["id"]
    assert ret["status"] == "queued"
    # still at L2, but no new cycle until the context is cleared or compacted
    assert await env.step(c["id"]) == []
    await insert_event(env.pool, c["id"], a["id"], type="compacted", data={})
    await env.gauge(a["id"], STRONG)
    assert await env.step(c["id"]) == [f"pressuring:{a['id']}"]
    assert len(await env.messages(a["id"], "handoff_return")) == 1


async def test_other_clear_failure_closes_and_returns(env):
    c, a = await env.solo()
    cmd = await env.to_clearing(c, a)
    await env.finish_command(cmd["id"], "failed", {"error": "agent has no session"})
    assert await env.step(c["id"]) == [f"cancelled:{a['id']}:clear_failed"]
    (ret,) = await env.messages(a["id"], "handoff_return")
    assert "cleared" not in ret["text"].split("\n")[0].replace("clear after", "")
    assert ret["text"].startswith("The clear after your accepted handoff failed")
    assert "Doing: wiring the cooperative handoff" in ret["text"]


async def test_clearing_without_session_cleared_times_out_and_retries(env):
    # the wrapper accepted /clear and then restarted: no session_cleared, no command_failed
    c, a = await env.solo()
    cmd = await env.to_clearing(c, a)
    await env.finish_command(cmd["id"], "done", {"response": {"id": "c1"}})
    await env.idle_for(a["id"], 600)
    assert await env.step(c["id"]) == []  # accepted just now: not yet
    await env.run(
        "UPDATE commands SET finished_at = now() - make_interval(secs => %s) WHERE id = %s",
        (env.settings.handover_clear_timeout_seconds + 1, cmd["id"]),
    )
    await env.busy(a["id"])
    assert await env.step(c["id"]) == []  # a running turn may still be ahead of the clear
    await env.idle_for(a["id"], env.settings.handover_clear_timeout_seconds + 1)
    assert await env.step(c["id"]) == [f"clear_retry:{a['id']}"]
    assert (await env.row(a["id"])).clear_attempts == 2
    await env.run("UPDATE agent_handoffs SET clear_attempts = 5 WHERE agent_id = %s", (a["id"],))
    last_cmd = (await env.commands(a["id"]))[-1]
    await env.run(
        "UPDATE commands SET status = 'done', finished_at = now() - make_interval(secs => %s)"
        " WHERE id = %s",
        (env.settings.handover_clear_timeout_seconds + 1, last_cmd["id"]),
    )
    assert await env.step(c["id"]) == [f"cancelled:{a['id']}:clear_failed"]
    assert len(await env.messages(a["id"], "handoff_return")) == 1


@pytest.mark.parametrize("status", ["queued", "running"])
async def test_clear_command_never_finished_times_out(env, status):
    # the dispatcher died between claiming the clear and completing it (running), or never
    # claimed it (queued): the row must not hang in clearing, holding the solo agent's messages
    c, a = await env.solo()
    cmd = await env.to_clearing(c, a)
    stale = env.settings.handover_clear_timeout_seconds + 1
    await env.run(
        "UPDATE commands SET status = %s,"
        " started_at = CASE WHEN %s = 'running' THEN now() END WHERE id = %s",
        (status, status, cmd["id"]),
    )
    await env.idle_for(a["id"], 600)
    assert await env.step(c["id"]) == []  # queued or claimed just now: not yet
    await env.run(
        "UPDATE commands SET created_at = now() - make_interval(secs => %s),"
        " started_at = started_at - make_interval(secs => %s) WHERE id = %s",
        (stale, stale, cmd["id"]),
    )
    assert await env.step(c["id"]) == [f"clear_retry:{a['id']}"]
    first, retry = await env.commands(a["id"])
    assert first["status"] == "failed"
    assert first["result"] == {"error": "clear_timeout"}
    assert retry["status"] == "queued"
    await env.run("UPDATE agent_handoffs SET clear_attempts = 5 WHERE agent_id = %s", (a["id"],))
    await env.run(
        "UPDATE commands SET status = %s, created_at = now() - make_interval(secs => %s),"
        " started_at = CASE WHEN %s = 'running' THEN now() - make_interval(secs => %s) END"
        " WHERE id = %s",
        (status, stale, status, stale, retry["id"]),
    )
    assert await env.step(c["id"]) == [f"cancelled:{a['id']}:clear_failed"]
    assert len(await env.messages(a["id"], "handoff_return")) == 1


async def test_clear_failed_block_expires(env):
    c, a = await env.solo()
    cmd = await env.to_clearing(c, a)
    await env.finish_command(cmd["id"], "failed", {"error": "agent has no session"})
    assert await env.step(c["id"]) == [f"cancelled:{a['id']}:clear_failed"]
    assert await env.step(c["id"]) == []
    await env.run(
        "UPDATE agent_handoffs SET closed_at = now() - make_interval(secs => %s)"
        " WHERE agent_id = %s",
        (env.settings.handover_clear_failed_block_seconds + 1, a["id"]),
    )
    assert await env.step(c["id"]) == [f"pressuring:{a['id']}"]


async def test_return_sent_once_across_pod_restart_during_clearing(env):
    c, a = await env.solo()
    cmd = await env.to_clearing(c, a)
    await env.finish_command(cmd["id"], "done", {"response": {"id": "c1"}})
    # the pod restarts: a new wrapper session, the cleared event is replayed and ingested
    await env.run(
        "UPDATE agents SET wrapper_session_id = %s WHERE id = %s", (await env.wsid(), a["id"])
    )
    await env.cleared(c["id"], a["id"])
    await insert_event(
        env.pool, c["id"], a["id"], wrapper_session_id="w0-new", type="session_cleared", data={}
    )
    env.policy = handoff.HandoffPolicy.from_settings(env.settings)  # a fresh dispatcher
    results = await asyncio.gather(env.step(c["id"]), env.step(c["id"]))
    assert sorted(results, key=len) == [[], [f"returning:{a['id']}"]]
    assert await env.step(c["id"]) == []
    assert len(await env.messages(a["id"], "handoff_return")) == 1


async def test_single_live_hive_leader_not_reopened_on_pre_clear_gauge(env):
    # after an L3 clear the leader's gauge still shows the old context until the wrapper
    # reports again; it must read as no gauge (no new row) and the L1 baseline resets
    c, (a0, a1) = await env.hive(2)
    await env.run("UPDATE agents SET ready = false WHERE id = %s", (a1["id"],))
    (before,) = await env.rows(
        "SELECT start_context_tokens FROM session_leaders WHERE session_id = %s", (c["id"],)
    )
    assert before["start_context_tokens"] is not None
    await env.step(c["id"])
    assert await env.submit(a0)
    assert await env.step(c["id"]) == [f"clearing:{a0['id']}"]
    (cmd,) = await env.commands(a0["id"])
    await env.finish_command(cmd["id"], "done", {"response": {"id": "c1"}})
    await env.cleared(c["id"], a0["id"])
    assert await env.step(c["id"]) == [f"returning:{a0['id']}"]
    (ret,) = await env.messages(a0["id"], "handoff_return")
    await env.run("UPDATE messages SET status = 'sent' WHERE id = %s", (ret["id"],))
    assert await env.step(c["id"]) == [f"done:{a0['id']}"]
    assert await env.step(c["id"]) == []  # the L2 gauge predates the clear
    (after,) = await env.rows(
        "SELECT start_context_tokens FROM session_leaders WHERE session_id = %s", (c["id"],)
    )
    assert after["start_context_tokens"] is None
    (session,) = await env.rows(
        "SELECT id, slug, spec, status FROM sessions WHERE id = %s", (c["id"],)
    )
    async with env.pool.connection() as conn:
        plan = await hive._read(conn, session, env.settings)
    assert plan is not None and plan.fill_start is None  # not recorded from the stale gauge
    await env.gauge(a0["id"], LOW)  # the first reading after the clear
    async with env.pool.connection() as conn:
        plan = await hive._read(conn, session, env.settings)
    assert plan is not None and plan.fill_start == LOW


# ---------------------------------------------------------------- the pressure policy push


async def test_policy_repushed_after_wrapper_restart(env):
    c, a = await env.solo()
    await env.step(c["id"])
    await env.push()
    assert env.policy_of(a)["version"] == 1
    puts = len([r for r in env.fake.requests if r[0] == "PUT"])
    await env.push(advance=10)  # nothing changed: no PUT
    assert len([r for r in env.fake.requests if r[0] == "PUT"]) == puts
    env.fake.pressure.clear()  # the wrapper restarted and lost the policy
    await env.push(advance=20)  # before the periodic check
    assert env.policy_of(a) is None
    await env.push(advance=61)
    policy = env.policy_of(a)
    assert policy is not None and policy["version"] == 2  # max(none, 1) + 1
    assert policy["handoff_state"] == "none"
    await env.push(advance=61)  # the check finds the same version: no PUT
    assert env.policy_of(a)["version"] == 2


async def test_policy_version_seeded_from_the_wrapper(env):
    c, a = await env.solo()
    await env.step(c["id"])
    env.fake.pressure[a["wrapper_session_id"]] = {"version": 41, "handoff_state": "none"}
    await env.push()
    assert env.policy_of(a)["version"] == 42


async def test_older_wrapper_404_skipped_no_crash(env, caplog):
    c, a = await env.solo()
    c2, b = await env.solo()
    await env.step(c["id"])
    await env.step(c2["id"])
    env.fake.disable_pressure()  # 409 pressure_disabled
    await env.run("UPDATE agents SET wrapper_session_id = 'ses_gone' WHERE id = %s", (b["id"],))
    with caplog.at_level(logging.INFO, logger="agents_orchestrator.dispatcher.hive"):
        await env.push()
        await env.push(advance=100)
    lines = [r.getMessage() for r in caplog.records if "pressure policy" in r.getMessage()]
    assert len([x for x in lines if str(a["id"]) in x]) == 1  # logged once per agent
    assert len([x for x in lines if str(b["id"]) in x]) == 1
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    # the pass carries on without pressure: an accepted gate-clear submit still clears
    assert await env.submit(a)
    assert await env.step(c["id"]) == [f"clearing:{a['id']}"]


async def test_no_handoff_request_or_notice_messages_ever_queued(env):
    c, a = await env.solo()
    await env.to_returning(c, a)
    h, (b0, b1, b2) = await env.hive()
    await env.step(h["id"])
    await env.gauge(b0["id"], CRITICAL)
    for _ in range(3):
        await env.step(h["id"])
    assert await env.submit(b0)
    await env.step(h["id"])
    await env.lpass(h["id"])
    await env.step(h["id"])
    await env.push()
    msgs = await env.rows("SELECT notice, text FROM messages")
    assert not [m for m in msgs if m["notice"] in ("handoff_notice", "handoff_request")]
    texts = [m["text"] for m in msgs] + [
        t for p in env.fake.pressure.values() for v in p["texts"].values() for t in v.values()
    ]
    for text in texts:
        assert not re.search(r"minute|deadline", text, re.I), text
        assert not re.search(r"\bin \d+", text), text


# ---------------------------------------------------------------- queue gate


async def _claim(env) -> int | None:
    async with env.pool.connection() as conn:
        claim = await queue.claim_and_pick(conn)
    return claim.message_id if claim is not None else None


async def _status(env, message_id: int) -> str:
    (row,) = await env.rows("SELECT status FROM messages WHERE id = %s", (message_id,))
    return row["status"]


async def test_messages_flow_while_pressuring_and_submitted(env):
    c, a = await env.solo()
    await env.step(c["id"])
    work = await insert_message(
        env.pool, c["id"], text="work", target_agent_id=a["id"], lane="regular"
    )
    assert await _claim(env) == work["id"]  # pressuring
    await env.run("UPDATE messages SET status = 'sent' WHERE id = %s", (work["id"],))
    env.inflight(a, background_tasks=[TASK])
    assert not await env.submit(a)
    more = await insert_message(env.pool, c["id"], text="more", lane="regular")
    assert await _claim(env) == more["id"]  # submitted, blocked


async def test_solo_clearing_holds_other_messages(env):
    c, a = await env.solo()
    await env.to_clearing(c, a)
    work = await insert_message(env.pool, c["id"], text="user work", lane="regular")
    targeted = await insert_message(
        env.pool, c["id"], text="peer work", target_agent_id=a["id"], lane="regular"
    )
    assert await _claim(env) is None  # clearing: held
    (cmd,) = await env.commands(a["id"])
    await env.finish_command(cmd["id"], "done", {"response": {"id": "c1"}})
    await env.cleared(c["id"], a["id"])
    assert await env.step(c["id"]) == [f"returning:{a['id']}"]
    (ret,) = await env.messages(a["id"], "handoff_return")
    assert await _claim(env) == ret["id"]  # the return goes first
    assert await _claim(env) is None
    await env.run("UPDATE messages SET status = 'sent' WHERE id = %s", (ret["id"],))
    assert await env.step(c["id"]) == [f"done:{a['id']}"]
    assert {await _claim(env), await _claim(env)} == {work["id"], targeted["id"]}


async def test_hive_member_messages_are_not_held(env):
    # only the subject's own messages wait; a member keeps getting its targeted work
    c, (a0, a1) = await env.hive(2)
    await env.run("UPDATE agents SET ready = false WHERE id = %s", (a1["id"],))
    await env.step(c["id"])
    assert await env.submit(a0)
    assert await env.step(c["id"]) == [f"clearing:{a0['id']}"]  # the only live agent
    await env.run("UPDATE agents SET ready = true WHERE id = %s", (a1["id"],))
    member = await insert_message(
        env.pool, c["id"], text="member", target_agent_id=a1["id"], lane="regular"
    )
    own = await insert_message(
        env.pool, c["id"], text="own", target_agent_id=a0["id"], lane="regular"
    )
    assert await _claim(env) == member["id"]
    assert await _claim(env) is None
    assert await _status(env, own["id"]) == "queued"


@pytest.mark.parametrize("event", ["compacted", "session_cleared"])
async def test_nudge_cancelled_after_reset(env, event):
    c, a = await env.solo()
    await env.step(c["id"])
    nudge = await insert_message(
        env.pool,
        c["id"],
        text="nudge",
        target_agent_id=a["id"],
        lane="regular",
        notice="handoff_nudge",
    )
    await insert_event(env.pool, c["id"], a["id"], type=event, data={})
    assert await _claim(env) is None  # the drain runs before any pass
    assert await _status(env, nudge["id"]) == "cancelled"


async def test_nudge_sent_only_while_pressuring(env):
    c, a = await env.solo()
    await env.step(c["id"])
    first = await insert_message(
        env.pool,
        c["id"],
        text="nudge",
        target_agent_id=a["id"],
        lane="regular",
        notice="handoff_nudge",
    )
    assert await _claim(env) == first["id"]
    await env.run("UPDATE messages SET status = 'sent' WHERE id = %s", (first["id"],))
    env.inflight(a, background_tasks=[TASK])
    assert not await env.submit(a)
    late = await insert_message(
        env.pool,
        c["id"],
        text="nudge",
        target_agent_id=a["id"],
        lane="regular",
        notice="handoff_nudge",
    )
    assert await _claim(env) is None
    assert await _status(env, late["id"]) == "cancelled"


async def test_removed_notice_kinds_are_always_cancelled(env):
    c, a = await env.solo()
    await env.step(c["id"])
    ids = []
    for kind in ("handoff_notice", "handoff_request"):
        m = await insert_message(
            env.pool, c["id"], text="old", target_agent_id=a["id"], lane="regular", notice=kind
        )
        ids.append(m["id"])
    assert await _claim(env) is None
    assert [await _status(env, i) for i in ids] == ["cancelled", "cancelled"]


# ---------------------------------------------------------------- lock and dispatcher


async def test_pass_skipped_without_claim_lock(env, dsn):
    c, a = await env.solo()
    async with await psycopg.AsyncConnection.connect(dsn) as holder, holder.transaction():
        await holder.execute(
            "SELECT pg_advisory_xact_lock(%s, hashtext(%s::text))",
            (_CLAIM_LOCK_NAMESPACE, c["id"]),
        )
        assert await env.step(c["id"]) == []
        assert await env.row(a["id"]) is None
    assert await env.step(c["id"]) == [f"pressuring:{a['id']}"]


async def test_handoff_sessions_lists_subjects_and_open_rows(env):
    c, a = await env.solo()
    h, _ = await env.hive()
    closing, b = await env.solo()
    async with env.pool.connection() as conn:
        await handoffs.open_pressure(conn, closing["id"], b["id"], 2, "work")
    await env.run("UPDATE sessions SET desired = 'deleting' WHERE id = %s", (closing["id"],))
    async with env.pool.connection() as conn:
        got = await handoff.handoff_sessions(conn)
    assert {c["id"], h["id"], closing["id"]} <= set(got)


async def test_dispatcher_runs_the_pass_after_leadership(env):
    d = Dispatcher(env.pool, env.settings)
    try:
        c, a = await env.solo()
        await d._drain_leadership()
        assert (await env.row(a["id"])).phase == "pressuring"
        assert d._push_wake.is_set()
        # throttled: a second wake within the interval runs no pass
        assert await env.submit(a)
        await d._drain_leadership()
        assert (await env.row(a["id"])).phase == "submitted"
        d._handoff_at = None
        # the pass reads the inventory through the dispatcher's fetcher (the fake's token here)
        d._inflight = env.fetch
        await d._drain_leadership()
        assert (await env.row(a["id"])).phase == "clearing"
    finally:
        await d.aclose()


async def test_failing_session_list_does_not_stop_message_drain(env, monkeypatch):
    d = Dispatcher(env.pool, env.settings)

    async def boom(conn):
        raise RuntimeError("db hiccup")

    monkeypatch.setattr(handoff, "handoff_sessions", boom)
    try:
        await d._drain_leadership()  # logged, not raised
    finally:
        await d.aclose()


async def test_open_lost_to_a_concurrent_row_is_skipped(env, monkeypatch):
    # a row opened between the pass's read and its insert (the submit route's voluntary row)
    # must not abort the pass
    c, a = await env.solo()
    assert await env.submit(a)

    async def no_rows(conn, session_id):
        return []

    monkeypatch.setattr(handoff.handoffs, "open_rows", no_rows)
    assert await env.step(c["id"]) == []
    assert (await env.row(a["id"])).trigger == "voluntary"


async def test_handoff_pass_skips_parked_session(env):
    c, a = await env.solo()
    parked, b = await env.solo()
    await env.run(
        "UPDATE sessions SET parked_at = now(), parked_reason = 'turns' WHERE id = %s",
        (parked["id"],),
    )
    async with env.pool.connection() as conn:
        got = await handoff.handoff_sessions(conn)
    assert c["id"] in got and parked["id"] not in got
    d = Dispatcher(env.pool, env.settings)
    try:
        await d._drain_leadership()
    finally:
        await d.aclose()
    assert (await env.row(a["id"])).phase == "pressuring"
    assert await env.rows("SELECT id FROM agent_handoffs WHERE agent_id = %s", (b["id"],)) == []


# ---------------------------------------------------------------- pod shutdown


async def _shutdown(env, session_id, agent_id, text: str | None, status: str = "completed"):
    """The wrapper's SIGTERM routine as the ingester stores it: the agent is stopping, its pod
    unready, and the handoff turn's outcome is an event."""
    await env.run("UPDATE agent_status SET stopping_since = now() WHERE agent_id = %s", (agent_id,))
    await env.run("UPDATE agents SET ready = false WHERE id = %s", (agent_id,))
    await insert_event(env.pool, session_id, agent_id, type="shutdown_started", data={})
    return await _shutdown_handoff(env, session_id, agent_id, text, status)


async def _shutdown_handoff(env, session_id, agent_id, text, status="completed"):
    return await insert_event(
        env.pool,
        session_id,
        agent_id,
        type="shutdown_handoff",
        data={"turn_id": "t-sd", "status": status, "final_text": text},
    )


async def _next_pod(env, agent_id) -> None:
    """The agent's next pod is ready and its stream attached (the mark is cleared)."""
    await env.run("UPDATE agent_status SET stopping_since = NULL WHERE agent_id = %s", (agent_id,))
    await env.run("UPDATE agents SET ready = true WHERE id = %s", (agent_id,))
    await env.idle_for(agent_id, 1)


async def _leader_lost(env, session_id) -> None:
    await env.lpass(session_id)  # marks unready_since
    await env.run(
        "UPDATE session_leaders SET unready_since = now() - interval '1 hour'"
        " WHERE session_id = %s",
        (session_id,),
    )
    await env.lpass(session_id)


async def _deliver(env) -> list:
    async def no_decisions(session_id, slug, projects):
        return []

    async with env.pool.connection() as conn:
        return await handover.deliver_handover(
            conn,
            budget_bytes=16384,
            output_chars=500,
            decisions=no_decisions,
            shutdown_hold_seconds=330,
        )


async def test_shutdown_solo_next_pod_clears_then_returns(env):
    c, a = await env.solo(LOW)
    work = await insert_message(env.pool, c["id"], text="user work", lane="regular")
    await _shutdown(env, c["id"], a["id"], "solo shutdown text")
    assert await env.step(c["id"]) == [f"shutdown_handed:{a['id']}"]
    row = await env.row(a["id"])
    assert (row.trigger, row.phase, row.handoff_text) == (
        "shutdown",
        "handed",
        "solo shutdown text",
    )
    assert await env.step(c["id"]) == []
    assert await env.commands(a["id"]) == []
    assert await _claim(env) is None
    await env.run("UPDATE agents SET ready = true WHERE id = %s", (a["id"],))
    assert await env.step(c["id"]) == []
    assert await env.commands(a["id"]) == []

    await _next_pod(env, a["id"])
    assert await _claim(env) is None  # held: the clear and the return go first
    assert await env.step(c["id"]) == [f"clearing:{a['id']}"]
    assert await _claim(env) is None
    (cmd,) = await env.commands(a["id"])
    await env.finish_command(cmd["id"], "done", {"response": {"id": "c1"}})
    await env.cleared(c["id"], a["id"])
    assert await env.step(c["id"]) == [f"returning:{a['id']}"]
    (ret,) = await env.messages(a["id"], "handoff_return")
    assert ret["text"] == (
        "Handoff from your previous context (pod shutdown, cleared just now):\n\nsolo shutdown text"
    )
    assert await _claim(env) == ret["id"]
    await env.run("UPDATE messages SET status = 'sent' WHERE id = %s", (ret["id"],))
    assert await env.step(c["id"]) == [f"done:{a['id']}"]
    last = await env.last_row(a["id"])
    assert (last["trigger"], last["phase"], last["close_reason"]) == (
        "shutdown",
        "done",
        "returned",
    )
    assert await _claim(env) == work["id"]


async def test_shutdown_leader_handoff_goes_to_successor_and_old_agent_is_cleared(env):
    c, (a0, a1, a2) = await env.hive(used=LOW)
    await env.gauge(a2["id"], 60000)  # a1 is the successor (lowest context)
    await env.run("UPDATE agent_status SET stopping_since = now() WHERE agent_id = %s", (a0["id"],))
    await insert_event(env.pool, c["id"], a0["id"], type="shutdown_started", data={})
    await env.lpass(c["id"])
    async with env.pool.connection() as conn:
        leader = await leadership.current_leader(conn, c["id"])
    assert leader.agent_id == a1["id"]
    assert await _deliver(env) == []
    await _shutdown_handoff(env, c["id"], a0["id"], "leader shutdown text")
    assert await env.step(c["id"]) == [f"shutdown_handed:{a0['id']}"]
    row = await env.row(a0["id"])
    assert (row.phase, row.successor_agent_id) == ("handed", a1["id"])

    assert await _deliver(env) == [c["id"]]
    (notice,) = await env.messages(a1["id"], "handover")
    assert notice["text"].startswith(
        f"Handoff from {a0['pod_name']} (written by it):\nleader shutdown text\n\n"
    )
    assert (await env.row(a0["id"])).delivered_at is not None
    assert await _deliver(env) == []

    assert await env.step(c["id"]) == []
    await _next_pod(env, a0["id"])
    assert await env.step(c["id"]) == [f"clearing:{a0['id']}"]
    (cmd,) = await env.commands(a0["id"])
    await env.finish_command(cmd["id"], "done", {"response": {"id": "c1"}})
    await env.cleared(c["id"], a0["id"])
    assert await env.step(c["id"]) == [f"done:{a0['id']}"]
    last = await env.last_row(a0["id"])
    assert (last["phase"], last["close_reason"]) == ("done", "cleared")
    assert await env.messages(a0["id"], "handoff_return") == []
    assert len(await env.messages(a1["id"])) == 1


async def test_shutdown_leader_text_follows_a_notice_already_sent(env):
    c, (a0, a1, a2) = await env.hive(used=LOW)
    await env.gauge(a2["id"], 60000)
    await env.run("UPDATE agent_status SET stopping_since = now() WHERE agent_id = %s", (a0["id"],))
    await env.run("UPDATE agents SET ready = false WHERE id = %s", (a0["id"],))
    await _leader_lost(env, c["id"])
    await env.run(
        "UPDATE agent_status SET stopping_since = now() - interval '1 hour' WHERE agent_id = %s",
        (a0["id"],),
    )
    assert await _deliver(env) == [c["id"]]
    await env.run("UPDATE agent_status SET stopping_since = now() WHERE agent_id = %s", (a0["id"],))
    await _shutdown_handoff(env, c["id"], a0["id"], "late text")
    assert await env.step(c["id"]) == [
        f"shutdown_handed:{a0['id']}",
        f"handoff_sent:{a1['id']}",
    ]
    texts = [m["text"] for m in await env.messages(a1["id"])]
    assert len(texts) == 2 and texts[1] == (
        f"Handoff from {a0['pod_name']} (written by it before its pod shut down):\nlate text"
    )
    assert await env.step(c["id"]) == []
    assert await _deliver(env) == []
    assert len(await env.messages(a1["id"])) == 2


async def test_shutdown_member_resumes_as_today(env):
    c, (a0, a1, a2) = await env.hive(used=LOW)
    await _shutdown(env, c["id"], a1["id"], "member text")
    assert await env.step(c["id"]) == [f"shutdown_member:{a1['id']}"]
    assert await env.row(a1["id"]) is None
    last = await env.last_row(a1["id"])
    assert (last["trigger"], last["phase"], last["close_reason"], last["handoff_text"]) == (
        "shutdown",
        "done",
        "member",
        "member text",
    )
    assert await env.step(c["id"]) == []
    await _next_pod(env, a1["id"])
    assert await env.step(c["id"]) == []
    assert await env.commands(a1["id"]) == []
    assert await env.messages(a1["id"]) == []
    assert await env.messages(a0["id"]) == []


@pytest.mark.parametrize(
    ("text", "status"),
    [("   ", "completed"), (None, "running"), (None, "not_started"), (None, "failed")],
)
async def test_shutdown_without_text_does_nothing(env, text, status):
    c, a = await env.solo(LOW)
    await _shutdown(env, c["id"], a["id"], text, status)
    assert await env.step(c["id"]) == []
    assert await env.rows("SELECT * FROM agent_handoffs WHERE agent_id = %s", (a["id"],)) == []
    await _next_pod(env, a["id"])
    assert await env.step(c["id"]) == []
    assert await env.commands(a["id"]) == []
    assert await env.messages(a["id"]) == []


async def test_sigterm_during_open_row_closes_it_shutdown(env):
    # pressuring, and clearing (the clear queued): both close at SIGTERM
    c, a = await env.solo()
    await env.step(c["id"])
    await env.run("UPDATE agent_status SET stopping_since = now() WHERE agent_id = %s", (a["id"],))
    assert await env.step(c["id"]) == [f"cancelled:{a['id']}:shutdown"]

    c2, a2 = await env.solo()
    await env.to_clearing(c2, a2)
    await env.run("UPDATE agent_status SET stopping_since = now() WHERE agent_id = %s", (a2["id"],))
    assert await env.step(c2["id"]) == [f"cancelled:{a2['id']}:shutdown"]
    (cmd,) = await env.commands(a2["id"])
    assert cmd["status"] == "cancelled"  # the queued clear never runs on the dying pod
    await _shutdown(env, c2["id"], a2["id"], "shutdown text")
    assert await env.step(c2["id"]) == [f"shutdown_handed:{a2['id']}"]


async def test_returning_row_survives_sigterm_and_keeps_its_return(env):
    c, a = await env.solo()
    ret = await env.to_returning(c, a)
    await env.run("UPDATE agent_status SET stopping_since = now() WHERE agent_id = %s", (a["id"],))
    assert await env.step(c["id"]) == []  # the return waits for the next pod
    assert await _claim(env) is None
    await _shutdown(env, c["id"], a["id"], "shutdown text")
    assert await env.step(c["id"]) == [
        f"cancelled:{a['id']}:shutdown",
        f"shutdown_handed:{a['id']}",
    ]
    closed = (
        await env.rows(
            "SELECT * FROM agent_handoffs WHERE agent_id = %s AND trigger = 'pressure'", (a["id"],)
        )
    )[0]
    assert (closed["phase"], closed["close_reason"]) == ("cancelled", "shutdown")
    assert await _status(env, ret["id"]) == "queued"  # still delivered, after the new return


# ---------------------------------------------------------------- fix round 1 (decision 716)


async def _quiet_hive(env, c, agents) -> None:
    """Every agent idle and the session quiet long past the discussion cap."""
    await env.run(
        "UPDATE sessions SET created_at = now() - interval '1 day',"
        " last_update_at = now() - interval '1 day' WHERE id = %s",
        (c["id"],),
    )
    for a in agents:
        await env.idle_for(a["id"], 700)


@pytest.mark.parametrize("before", ["pressuring", "refused", "accepted"])
async def test_rollout_move_never_clears_the_ex_leader(env, before):
    # a rollout move is not the row's own limit request: the row closes, no clear ever follows
    c, (a0, a1, a2) = await env.hive()
    assert await env.step(c["id"]) == [f"pressuring:{a0['id']}"]
    if before == "refused":
        env.inflight(a0, background_tasks=[TASK])
        assert not await env.submit(a0)
    elif before == "accepted":
        assert await env.submit(a0)  # the pass has not moved it yet
    async with env.pool.connection() as conn:
        await leadership.insert_request(conn, c["id"], "handover", a0["id"], a1["id"], "rollout", 1)
    assert (await env.lpass(c["id"])).agent_id == a1["id"]
    last = await env.last_row(a0["id"])
    assert (last["phase"], last["close_reason"]) == ("cancelled", "not_leader")
    env.inflight(a0)
    await _quiet_hive(env, c, (a0, a1, a2))
    for _ in range(3):
        await env.step(c["id"])
    assert await env.commands(a0["id"]) == []
    assert await env.row(a0["id"]) is None


def _interleave(monkeypatch, between) -> None:
    """Run `between` (committed on its own connection) right after the pass read its rows."""
    real = handoffs.open_rows
    done = []

    async def open_rows(conn, session_id):
        rows = await real(conn, session_id)
        if not done:
            done.append(True)
            await between()
        return rows

    monkeypatch.setattr(handoffs, "open_rows", open_rows)


async def test_blocked_resubmit_after_the_read_stops_the_limit_request(env, monkeypatch):
    c, (a0, a1, a2) = await env.hive()
    await env.step(c["id"])
    assert await env.submit(a0)  # accepted, gate clear

    async def resubmit() -> None:
        env.inflight(a0, background_tasks=[TASK])
        assert not await env.submit(a0)  # 409 in_flight to the agent

    _interleave(monkeypatch, resubmit)
    assert await env.step(c["id"]) == []
    assert await env.requests(c["id"]) == []
    row = await env.row(a0["id"])
    assert row.phase == "submitted"
    assert row.inflight["waiting_on"] == ["task:bf3a (CI watch)"]


async def test_blocked_resubmit_after_the_read_stops_the_solo_clear(env, monkeypatch):
    c, a = await env.solo()
    await env.step(c["id"])
    assert await env.submit(a)

    async def resubmit() -> None:
        env.inflight(a, background_tasks=[TASK])
        assert not await env.submit(a)

    _interleave(monkeypatch, resubmit)
    assert await env.step(c["id"]) == []
    assert await env.commands(a["id"]) == []
    row = await env.row(a["id"])
    assert (row.phase, row.inflight["waiting_on"]) == ("submitted", ["task:bf3a (CI watch)"])


async def test_accepted_submit_after_the_read_is_not_overwritten_by_resubmit(env, monkeypatch):
    c, a = await env.solo()
    await env.step(c["id"])
    env.inflight(a, background_tasks=[TASK])
    assert not await env.submit(a)
    await env.step(c["id"])
    env.inflight(a)  # the task reported: the pass would ask for a resubmit

    async def fresh() -> None:
        assert await env.submit(a)  # the agent re-submitted first, accepted clear

    _interleave(monkeypatch, fresh)
    assert await env.step(c["id"]) == []
    row = await env.row(a["id"])
    assert row.phase == "submitted"
    assert "resubmit" not in row.inflight and row.inflight["waiting_on"] == []
    monkeypatch.undo()
    assert await env.step(c["id"]) == [f"clearing:{a['id']}"]


async def test_submit_after_the_move_is_refused(env):
    # the other order: the pass moved first, a later submit no longer changes the row
    c, (a0, a1, a2) = await env.hive()
    await env.step(c["id"])
    assert await env.submit(a0)
    assert await env.step(c["id"]) == [f"moving:{a0['id']}"]
    row = await env.row(a0["id"])
    async with env.pool.connection() as conn:
        assert not await handoffs.submit(conn, row.id, HANDOFF, {"waiting_on": []}, None)
    assert (await env.row(a0["id"])).phase == "moving"


async def test_ex_leader_mid_turn_not_cleared_after_quiet_cap(env):
    # the cap drops the session-quiet requirement, never the ex-leader's own turn boundary
    c, (a0, a1, a2) = await _discussing(env, quiet_ago=700, since_ago=601)
    await env.busy(a0["id"])
    for _ in range(2):
        assert await env.step(c["id"]) == []
    assert await env.commands(a0["id"]) == []
    assert (await env.row(a0["id"])).phase == "discussing"
    await env.idle_for(a0["id"], 1)
    assert await env.step(c["id"]) == [f"clearing:{a0['id']}"]

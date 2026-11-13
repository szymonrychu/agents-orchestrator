"""Hive state push: the dispatcher's `push_once` against Postgres and the fake wrapper."""

from __future__ import annotations

import asyncio
import json
import time
import uuid

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

from agents_orchestrator.db import handoffs, leadership, queue
from agents_orchestrator.db.queue import _CLAIM_LOCK_NAMESPACE
from agents_orchestrator.dispatcher import Dispatcher, hive
from agents_orchestrator.handover_transcript import REREAD_RULE
from agents_orchestrator.settings import Settings
from agents_orchestrator.tokens import tokens_for
from agents_orchestrator.wrapper.client import WrapperClient

TOKEN = "test-token"
KEY = "k" * 32
DB0 = "postgresql://u:p@h/db"
OLD = "now() - interval '60 seconds'"


def make_settings(dsn: str) -> Settings:
    return Settings(database_url=dsn, master_key=KEY)


@pytest.fixture
async def fake(monkeypatch):
    monkeypatch.setattr(fake_wrapper, "MAX_SESSIONS", 100)  # one fake hosts every agent session
    f = FakeWrapper(token=TOKEN)
    await f.start()
    try:
        yield f
    finally:
        await f.stop()


class Env:
    def __init__(self, pool, dsn: str, fake: FakeWrapper) -> None:
        self.pool, self.dsn, self.fake = pool, dsn, fake
        self.settings = make_settings(dsn)
        self.state = hive.HiveState()
        self.clock = 1000.0
        self.http_clients: list[WrapperClient] = []

    def factory(self, session_id, url) -> WrapperClient:
        client = WrapperClient(url, TOKEN)
        self.http_clients.append(client)
        return client

    async def push(self, state=None, advance: float = 100.0) -> int:
        """One push_once on a clock that moves `advance` seconds first (past the throttle)."""
        self.clock += advance
        return await hive.push_once(
            self.pool,
            self.settings,
            self.factory,
            state or self.state,
            clock=lambda: self.clock,
        )

    async def session(self, name: str = "s") -> str:
        client = WrapperClient(self.fake.base_url, TOKEN)
        try:
            return (
                await client.create_session(f"{name}{uuid.uuid4().hex[:4]}", "/w")
            ).wrapper_session_id
        finally:
            await client.aclose()

    async def hive(self, n: int = 3, *, spec: dict | None = None, lead: bool = True) -> tuple:
        spec = {"replicas": n, "max_replicas": 4, **(spec or {})}
        c = await insert_session(self.pool, spec=spec)
        agents = []
        for i in range(n):
            agents.append(await self.agent(c["id"], i))
        if lead:
            await self.lpass(c["id"])
            await self.age_leader(c["id"])
        return c, agents

    async def age_leader(self, session_id, seconds: int = 3600) -> None:
        """Backdate the leader's `since`, so the minimum tenure is over."""
        await self.run(
            "UPDATE session_leaders SET since = now() - make_interval(secs => %s)"
            " WHERE session_id = %s",
            (seconds, session_id),
        )

    async def agent(self, session_id, ordinal: int, *, ctx: float | None = None) -> dict:
        sid = await self.session()
        a = await insert_agent(
            self.pool,
            session_id,
            ordinal=ordinal,
            ready=True,
            wrapper_session_id=sid,
            url=self.fake.base_url,
        )
        await set_agent_status(self.pool, a["id"], state="idle", context_pct=ctx)
        return a

    async def limits_start(self, agent_id):
        async with self.pool.connection() as conn:
            return await handoffs.limits_start(conn, agent_id)

    async def lpass(self, session_id):
        async with self.pool.connection() as conn:
            return await leadership.leadership_pass(conn, session_id)

    async def run(self, sql: str, params: tuple = ()) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(sql, params)

    async def rows(self, sql: str, params: tuple = ()) -> list[dict]:
        async with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            return await cur.fetchall()

    async def leader_row(self, session_id) -> dict:
        return (
            await self.rows("SELECT * FROM session_leaders WHERE session_id = %s", (session_id,))
        )[0]

    def notes(self, agent: dict, key: str | None = "hive_state") -> list[str]:
        """Texts pushed to the agent's session: the keyed hive_state ones, or the appended."""
        pushed = self.fake.notes.get(agent["wrapper_session_id"], [])
        return [n["text"] for n in pushed if n["key"] == key]

    async def write_event(
        self, session_id, agent, path: str, *, age: float = 60.0, ts_age: float | None = None
    ) -> dict:
        """A Write call stored `age` s ago; its wrapper ts defaults to the same moment."""
        data = {"tool_use_id": uuid.uuid4().hex, "tool_name": "Write", "tool_input": {}}
        data["tool_input"] = {"file_path": path}
        return await insert_event(
            self.pool,
            session_id,
            agent["id"],
            type="tool_call",
            data=data,
            ts=await self.ago(age if ts_age is None else ts_age),
            created_at=await self.ago(age),
        )

    async def ago(self, seconds: float):
        return (await self.rows("SELECT now() - make_interval(secs => %s) AS t", (seconds,)))[0][
            "t"
        ]


@pytest.fixture
async def env(pool, dsn, fake):
    e = Env(pool, dsn, fake)
    try:
        yield e
    finally:
        for client in e.http_clients:
            await client.aclose()


# ---------------------------------------------------------------- hive_state


async def test_hive_state_on_leader_change(env):
    c, (a0, a1, a2) = await env.hive()
    assert await env.push() == 3
    for agent in (a0, a1, a2):
        (text,) = env.notes(agent)
        assert text.startswith("<hive-state>") and text.rstrip().endswith("</hive-state>")
        assert "in hive" in text and "of up to 4 agents" in text
        assert "Leader: agent-0 (epoch 1)" in text
    assert "You are agent-0 (leader)" in env.notes(a0)[0]
    assert "You are agent-2 (member)" in env.notes(a2)[0]
    assert all(f"agent-{i}" in env.notes(a1)[0] for i in range(3))
    sent = env.fake.notes[a0["wrapper_session_id"]][0]
    assert sent["source"] == "orchestrator" and sent["key"] == "hive_state"
    row = await env.leader_row(c["id"])
    assert row["state_pushed_at"] is not None
    assert set(json.loads(row["state_hash"])) == {str(a["id"]) for a in (a0, a1, a2)}

    # a user override moves the lead: every live agent gets the new state
    async with env.pool.connection() as conn:
        await leadership.insert_request(conn, c["id"], "user", None, a1["id"], "test", None)
    await env.lpass(c["id"])
    assert await env.push() == 3
    for agent in (a0, a1, a2):
        assert len(env.notes(agent)) == 2
        assert "Leader: agent-1 (epoch 2)" in env.notes(agent)[-1]
    assert "You are agent-1 (leader)" in env.notes(a1)[-1]


async def test_hive_state_on_structural_change_only(env):
    c, (a0, a1, a2) = await env.hive()
    await env.push()
    assert await env.push() == 0  # nothing changed
    assert all(len(env.notes(a)) == 1 for a in (a0, a1, a2))

    # busy/idle flips and context changes are pulled with orchestrator_me, never pushed
    await set_agent_status(env.pool, a1["id"], state="busy", context_pct=41)
    assert await env.push() == 0
    await set_agent_status(env.pool, a1["id"], state="idle", context_pct=88)
    assert await env.push() == 0
    await set_agent_status(env.pool, a1["id"], state="busy", context_pct=48)
    assert await env.push() == 0
    assert all(len(env.notes(a)) == 1 for a in (a0, a1, a2))

    await env.run("UPDATE agents SET ready = false WHERE id = %s", (a1["id"],))  # readiness
    assert await env.push() == 3
    # the text carries each member's state and context as of the push
    assert "agent-1 (member, not ready, busy, 48% context)" in env.notes(a0)[-1]
    await env.run("UPDATE agents SET ready = true WHERE id = %s", (a1["id"],))
    assert await env.push() == 3

    a3 = await env.agent(c["id"], 3)  # joins
    assert await env.push() == 4
    assert "agent-3" in env.notes(a0)[-1] and len(env.notes(a3)) == 1

    await env.run("UPDATE agents SET gone_at = now() WHERE id = %s", (a2["id"],))  # leaves
    assert await env.push() == 3
    assert "agent-2" not in env.notes(a0)[-1]
    assert len(env.notes(a2)) == 4  # a gone agent gets nothing more
    assert await env.push() == 0

    await env.run(  # the epoch alone
        "UPDATE session_leaders SET epoch = epoch + 1 WHERE session_id = %s", (c["id"],)
    )
    assert await env.push() == 3
    assert "Leader: agent-0 (epoch 2)" in env.notes(a3)[-1]

    async with env.pool.connection() as conn:  # the leader
        await leadership.insert_request(conn, c["id"], "user", None, a1["id"], "test", None)
    await env.lpass(c["id"])
    assert await env.push() == 3
    assert "Leader: agent-1 (epoch 3)" in env.notes(a0)[-1]
    assert await env.push() == 0


async def test_hive_state_threshold_hint(env):
    # the strong limit in the advice: coalesce(auto_compact_pct, default 95) - 10
    for spec, hard in (({"auto_compact_pct": 80}, 70), ({}, 85), ({"auto_compact_pct": 15}, 5)):
        _, (a0, a1, _a2) = await env.hive(spec=spec)
        await env.push()
        hint = f"and always before {hard}% context."
        assert hint in env.notes(a0)[0] and hint in env.notes(a1)[0]
        assert "instead of compacting" not in env.notes(a0)[0]


async def test_hive_state_points_to_pull_tools(env):
    _, agents = await env.hive()
    await env.push()
    sentence = (
        "Call orchestrator_me for live state and context, orchestrator_activity for what peers did."
    )
    for agent in agents:
        assert sentence in env.notes(agent)[0]


async def test_hive_state_repush_on_session_change(env):
    c, (a0, a1, a2) = await env.hive()
    await env.push()
    new_session = await env.session()  # a fast wrapper restart: same pod, new session
    await env.run(
        "UPDATE agents SET wrapper_session_id = %s WHERE id = %s", (new_session, a1["id"])
    )
    assert await env.push() == 3
    assert len(env.fake.notes[new_session]) == 1
    assert len(env.notes(a0)) == 2


async def test_failed_agent_does_not_repush_others(env):
    c, (a0, a1, a2) = await env.hive()
    await env.run("UPDATE agents SET wrapper_session_id = 'ses_missing' WHERE id = %s", (a2["id"],))
    assert await env.push() == 2
    row = await env.leader_row(c["id"])
    assert row["state_pushed_at"] is None  # not every push succeeded
    assert set(json.loads(row["state_hash"])) == {str(a0["id"]), str(a1["id"])}
    before = len(env.fake.requests)
    assert await env.push() == 0  # a0 and a1 are not pushed again
    posts = [p for m, p in env.fake.requests[before:] if m == "POST"]
    assert posts == ["/v1/sessions/ses_missing/notes"]  # only the failed one is retried
    assert len(env.notes(a0)) == len(env.notes(a1)) == 1


# ---------------------------------------------------------------- no feed


async def test_no_feed_note(env):
    c, (a0, a1, a2) = await env.hive()
    await env.write_event(c["id"], a1, "/work/a.py")
    bash = {"tool_use_id": "b1", "tool_name": "Bash", "tool_input": {"command": "make test"}}
    for agent_id, type_, data, age in (
        (a2["id"], "tool_call", bash, 50),
        (a2["id"], "tool_result", {"tool_use_id": "b1", "tool_name": "Bash", "exit_code": 0}, 49),
        (a0["id"], "turn_completed", {"record": {"final_text": "Done: shipped"}}, 48),
    ):
        when = await env.ago(age)
        await insert_event(
            env.pool, c["id"], agent_id, type=type_, data=data, ts=when, created_at=when
        )
    assert await env.push() == 3  # the keyed hive_state notes only
    await env.write_event(c["id"], a2, "/work/b.py")
    assert await env.push() == 0  # peer activity pushes nothing
    for agent in (a0, a1, a2):
        pushed = env.fake.notes[agent["wrapper_session_id"]]
        assert [n["key"] for n in pushed] == ["hive_state"]
        assert env.notes(agent, key=None) == []
        assert "wrote" not in pushed[0]["text"] and "make test" not in pushed[0]["text"]


# ---------------------------------------------------------------- handover_due

DUE = "handover_due"


async def test_handover_due_sent_to_leader_at_threshold(env):
    c, (a0, a1, a2) = await env.hive()  # default compaction 95, so the threshold is 80
    await set_agent_status(env.pool, a0["id"], state="idle", context_pct=79)
    await set_agent_status(env.pool, a1["id"], state="idle", context_pct=95)
    await env.push()
    assert all(env.notes(a, DUE) == [] for a in (a0, a1, a2))  # below the threshold, or a member

    await set_agent_status(env.pool, a0["id"], state="idle", context_pct=80)
    assert await env.push() == 1  # hive_state is unchanged: only the due note goes out
    (text,) = env.notes(a0, DUE)
    assert text.startswith("<handover-due>") and "Context limit (weak): wrap up." in text
    assert env.notes(a1, DUE) == [] and env.notes(a2, DUE) == []  # members get none
    sent = [n for n in env.fake.notes[a0["wrapper_session_id"]] if n["key"] == DUE]
    assert sent[0]["source"] == "orchestrator"
    assert await env.push() == 0  # one note, not one per tick
    assert len(env.notes(a0, DUE)) == 1


async def test_handover_due_candidates_ranked(env):
    spec = {"replicas": 7, "max_replicas": 8}
    c, agents = await env.hive(7, spec=spec)
    a0, a1, a2, a3, a4, a5, a6 = agents
    await env.run(  # a6 is leaving: its ordinal is not below spec.replicas
        "UPDATE sessions SET spec = jsonb_set(spec, '{replicas}', '6') WHERE id = %s", (c["id"],)
    )
    await env.run("UPDATE agents SET ready = false WHERE id = %s", (a5["id"],))
    await set_agent_status(env.pool, a0["id"], state="busy", context_pct=82)
    await set_agent_status(env.pool, a1["id"], state="busy", context_pct=10)
    await set_agent_status(env.pool, a2["id"], state="idle", context_pct=40)
    await set_agent_status(env.pool, a3["id"], state="idle", context_pct=20.4)
    await set_agent_status(env.pool, a4["id"], state="idle", context_pct=85)  # itself due
    await set_agent_status(env.pool, a5["id"], state="idle", context_pct=5)
    await set_agent_status(env.pool, a6["id"], state="idle", context_pct=1)
    await env.push()
    (text,) = env.notes(a0, DUE)
    lines = [a["pod_name"] for a in (a3, a2, a1)]
    assert "Candidates, best first: " + ", ".join(lines) + "." in text
    for left_out in (a0, a4, a5, a6):  # the leader, an agent itself due, unready, leaving
        assert left_out["pod_name"] not in text.split("Candidates", 1)[1]


async def test_candidates_exclude_no_headroom_and_rank_pending_last(env):
    c, (a0, a1, a2) = await env.hive()
    await env.run(
        "UPDATE sessions SET spec = spec || jsonb_build_object('auto_compact_pct', 25),"
        " status = %s WHERE id = %s",
        (json.dumps({"rollout": {"pending": [1]}}), c["id"]),
    )
    await set_agent_status(env.pool, a0["id"], state="idle", context_pct=12)  # weak: 10..14
    await set_agent_status(env.pool, a1["id"], state="idle", context_pct=2)  # pending: last
    await set_agent_status(env.pool, a2["id"], state="idle", context_pct=8)
    await env.push()
    (text,) = env.notes(a0, DUE)
    assert "Candidates, best first: " + ", ".join(a["pod_name"] for a in (a2, a1)) + "." in text

    await set_agent_status(env.pool, a2["id"], state="idle", context_pct=15)  # no headroom
    await env.push()  # the candidate list changed, so a new note goes out
    text = env.notes(a0, DUE)[-1]
    assert a2["pod_name"] not in text.split("Candidates", 1)[1]


def _levels(start=40000, c=50, budget=150000):
    spec = {"auto_compact_pct": c}
    return hive.levels(
        spec, start, Settings(database_url=DB0, master_key=KEY, handover_budget_tokens=budget)
    )


def test_strong_tokens_rule():
    def strong(c, window):
        return hive.strong_tokens(_levels(c=c), window)

    assert strong(95, 200000) == 130000  # min(85% = 170000, 190000 - 60000)
    assert strong(50, 1000000) == 400000  # min(40% = 400000, 500000 - 60000 = 440000)
    assert strong(95, 50000) == 0  # min(42500, -12500) floored


def test_levels_weak_from_start_plus_budget():
    lv = _levels()  # C 50: strong 40%, weak fallback 35%, weak = 40000 + 150000
    assert (lv.strong_pct, lv.fallback_pct) == (40, 35)
    assert hive.due_level(lv, 189999, 1000000, 19.0, 1200, 1200, False) == "none"
    assert hive.due_level(lv, 190000, 1000000, 19.0, 1200, 1200, False) == "weak"


def test_due_level_weak_is_todays_soft_rule():
    lv = _levels()
    assert hive.due_level(lv, 190000, 1000000, 19.0, 1199, 1200, False) == "none"  # tenure
    assert hive.due_level(lv, 190000, 1000000, 19.0, 1200, 1200, True) == "none"  # rollout
    assert hive.due_level(lv, 190000, 1000000, 19.0, 1200, 1200, False) == "weak"


def test_weak_clamped_to_strong():
    lv = _levels(start=30000)  # window 200000: start + budget 180000, strong tokens 40000
    assert hive.due_level(lv, 39999, 200000, 19.99, 1200, 1200, False) == "none"
    assert hive.due_level(lv, 40000, 200000, 20.0, 1200, 1200, False) == "strong"


def test_due_level_strong_ignores_tenure_and_rollout():
    lv = _levels()
    assert hive.due_level(lv, 400000, 1000000, 40.0, 0, 1200, False) == "strong"
    assert hive.due_level(lv, 400000, 1000000, 40.0, 0, 1200, True) == "strong"
    assert hive.due_level(lv, 399999, 1000000, 39.9, 0, 1200, True) == "none"


def test_null_start_falls_back_to_percent():
    lv = hive.levels({}, None, Settings(database_url=DB0, master_key=KEY))  # C 95: fallback 80
    assert hive.due_level(lv, None, None, 80.0, 1200, 1200, False) == "weak"
    assert hive.due_level(lv, None, None, 79.0, 1200, 1200, False) == "none"
    # tokens known but no start: still the percent rule
    assert hive.due_level(lv, 790000, 1000000, 79.0, 1200, 1200, False) == "none"


def test_due_level_pct_fallback_strong():
    lv = hive.levels({}, None, Settings(database_url=DB0, master_key=KEY))  # C 95: strong 85
    assert hive.due_level(lv, None, None, 85.0, 0, 1200, False) == "strong"
    assert hive.due_level(lv, None, None, 84.9, 0, 1200, False) == "none"


def test_rollout_pending_suppresses_weak_not_strong():
    lv = hive.levels({}, None, Settings(database_url=DB0, master_key=KEY))
    assert hive.due_level(lv, None, None, 82.0, 1200, 1200, True) == "none"
    assert hive.due_level(lv, None, None, 85.0, 1200, 1200, True) == "strong"


async def test_start_tokens_filled_once(env):
    c, (a0, a1, a2) = await env.hive()
    await set_agent_status(
        env.pool,
        a0["id"],
        state="idle",
        context_used_tokens=50000,
        context_window_tokens=1000000,
        context_pct=5,
    )
    await env.push()
    row = await env.leader_row(c["id"])
    assert row["start_context_tokens"] == 50000
    await set_agent_status(
        env.pool,
        a0["id"],
        state="idle",
        context_used_tokens=90000,
        context_window_tokens=1000000,
        context_pct=9,
    )
    await env.push()
    row = await env.leader_row(c["id"])
    assert row["start_context_tokens"] == 50000


async def test_budget_drives_due_note(env):
    c, (a0, a1, a2) = await env.hive(spec={"auto_compact_pct": 50})
    await env.run(
        "UPDATE session_leaders SET start_context_tokens = 40000 WHERE session_id = %s", (c["id"],)
    )
    gauge = dict(state="idle", context_window_tokens=1000000)
    await set_agent_status(env.pool, a0["id"], context_used_tokens=150000, context_pct=15, **gauge)
    await env.push()
    assert env.notes(a0, DUE) == []  # under start + budget
    await set_agent_status(env.pool, a0["id"], context_used_tokens=190000, context_pct=19, **gauge)
    await env.push()
    (text,) = env.notes(a0, DUE)
    assert "Context limit (weak): wrap up." in text and "orchestrator_leadership_handover" in text


async def test_tenure_and_rollout_hold_back_the_weak_note(env):
    c, (a0, a1, a2) = await env.hive()
    await set_agent_status(env.pool, a0["id"], state="idle", context_pct=82)
    await env.age_leader(c["id"], 600)  # under 20 minutes
    await env.push()
    assert env.notes(a0, DUE) == []
    await env.age_leader(c["id"], 1300)
    await env.run(
        "UPDATE sessions SET status = %s WHERE id = %s",
        (json.dumps({"rollout": {"pending": [0]}}), c["id"]),
    )
    await env.push()
    assert env.notes(a0, DUE) == []  # the leader awaits its roll
    await set_agent_status(env.pool, a0["id"], state="idle", context_pct=86)  # strong
    await env.push()
    assert env.notes(a0, DUE) == []  # no weak note at the strong limit
    async with env.pool.connection() as conn:
        subjects = await hive.limit_subjects(conn, env.settings)
    assert [(x.session_id, x.level) for x in subjects if x.session_id == c["id"]] == [
        (c["id"], "strong")
    ]


async def test_hive_state_and_due_text_carry_budget_and_rule(env):
    c, (a0, a1, a2) = await env.hive()
    await env.push()
    text = env.notes(a0)[0]
    assert "150k tokens" in text and "20 minutes" in text and REREAD_RULE in text


async def test_handover_due_resend_rules(env):
    c, (a0, a1, a2) = await env.hive(
        spec={"auto_compact_pct": 72}
    )  # weak 57..62, buckets 11 and 12
    await set_agent_status(env.pool, a0["id"], state="idle", context_pct=58)
    await env.push()
    assert len(env.notes(a0, DUE)) == 1

    await set_agent_status(env.pool, a0["id"], state="idle", context_pct=59)  # same 5% bucket
    # a candidate's state and context, with the order kept
    await set_agent_status(env.pool, a2["id"], state="busy", context_pct=30)
    await env.push()
    assert len(env.notes(a0, DUE)) == 1

    await set_agent_status(env.pool, a0["id"], state="idle", context_pct=60)  # next bucket
    await env.push()
    assert len(env.notes(a0, DUE)) == 2

    await env.run("UPDATE agents SET ready = false WHERE id = %s", (a2["id"],))  # identity
    await env.push()
    assert len(env.notes(a0, DUE)) == 3
    await env.run("UPDATE agents SET ready = true WHERE id = %s", (a2["id"],))
    await env.push()
    assert len(env.notes(a0, DUE)) == 4

    await env.run(  # the epoch alone
        "UPDATE session_leaders SET epoch = epoch + 1 WHERE session_id = %s", (c["id"],)
    )
    await env.push()
    assert len(env.notes(a0, DUE)) == 5 and "Current epoch: 2" in env.notes(a0, DUE)[-1]

    await set_agent_status(env.pool, a0["id"], state="idle", context_pct=50)  # compacted
    await env.push()
    assert len(env.notes(a0, DUE)) == 5
    await set_agent_status(env.pool, a0["id"], state="idle", context_pct=58)  # due again
    await env.push()
    assert len(env.notes(a0, DUE)) == 6

    # a new leader is judged on its own gauge, and the old leader is a member now
    await set_agent_status(env.pool, a1["id"], state="idle", context_pct=10)
    async with env.pool.connection() as conn:
        await leadership.insert_request(conn, c["id"], "user", None, a1["id"], "test", None)
    await env.lpass(c["id"])
    await env.push()
    assert len(env.notes(a0, DUE)) == 6 and env.notes(a1, DUE) == []
    await set_agent_status(env.pool, a1["id"], state="idle", context_pct=58)
    await env.age_leader(c["id"])  # the new leader has led long enough
    await env.push()
    assert len(env.notes(a1, DUE)) == 1 and len(env.notes(a0, DUE)) == 6


async def test_handover_due_no_candidate(env):
    c, (a0, a1) = await env.hive(2)  # scalable: a hive that is not static
    await env.run("UPDATE agents SET ready = false WHERE id = %s", (a1["id"],))
    await set_agent_status(env.pool, a0["id"], state="idle", context_pct=82)
    await env.push()
    (text,) = env.notes(a0, DUE)
    assert "No ready successor; keep working." in text and "Candidates" not in text
    assert "orchestrator_scale" not in text and "mcp__ccw__send_message" not in text

    _, (s0, s1) = await env.hive(2, spec={"static": True, "max_replicas": 2})
    await env.run("UPDATE agents SET ready = false WHERE id = %s", (s1["id"],))
    await set_agent_status(env.pool, s0["id"], state="idle", context_pct=82)
    await env.push()
    (text,) = env.notes(s0, DUE)
    assert "No ready successor; keep working." in text


WEAK_WRAP = (
    "Context limit (weak): wrap up. Finish the step you are in, commit and push, update your "
    "task and todo, and leave notes a successor can act on. Nothing will happen if you keep "
    "working."
)
WELCOME = (
    "At a natural boundary, orchestrator_leadership_handover(to=..., epoch=..., reason=...) "
    "is welcome."
)
SOLO_HANDOFF = (
    "To hand off now on your own terms, call orchestrator_leadership_handover("
    "reason=<your handoff>): your context is cleared and the handoff comes back to you."
)


async def test_weak_note_texts(env):
    c, (a0, a1, a2) = await env.hive()
    await set_agent_status(env.pool, a0["id"], state="idle", context_pct=82)
    await env.push()
    (text,) = env.notes(a0, DUE)
    assert text.startswith("<handover-due>") and text.rstrip().endswith("</handover-due>")
    assert WEAK_WRAP in text and WELCOME in text and "Candidates, best first: " in text
    assert SOLO_HANDOFF not in text

    _, (b0, b1) = await env.hive(2)
    await env.run("UPDATE agents SET ready = false WHERE id = %s", (b1["id"],))
    await set_agent_status(env.pool, b0["id"], state="idle", context_pct=82)
    await env.push()
    (text,) = env.notes(b0, DUE)
    assert WEAK_WRAP in text and "No ready successor; keep working." in text
    assert WELCOME not in text and SOLO_HANDOFF not in text

    solo = await insert_session(env.pool, spec={"replicas": 1})
    s0 = await env.agent(solo["id"], 0)
    await env.run(
        "INSERT INTO agent_limits (agent_id, start_context_tokens, since)"
        " VALUES (%s, 1000, now() - interval '1 hour')",
        (s0["id"],),
    )
    await set_agent_status(env.pool, s0["id"], state="idle", context_pct=82)
    await env.push()
    (text,) = env.notes(s0, DUE)
    assert WEAK_WRAP in text and SOLO_HANDOFF in text
    assert WELCOME not in text and "Candidates" not in text


async def _subjects(env) -> dict:
    async with env.pool.connection() as conn:
        return {x.session_id: x for x in await hive.limit_subjects(conn, env.settings)}


async def test_solo_agent_gets_weak_note(env):
    solo = await insert_session(env.pool, spec={})  # neither replicas nor max_replicas set
    s0 = await env.agent(solo["id"], 0)
    gauge = dict(state="idle", context_window_tokens=1000000)
    await set_agent_status(env.pool, s0["id"], context_used_tokens=50000, context_pct=5, **gauge)
    assert await env.push() == 0
    # the first tick with a gauge records the start context
    assert await env.limits_start(s0["id"]) == 50000
    assert env.notes(s0, DUE) == [] and env.notes(s0) == []  # no hive state for a solo agent

    await set_agent_status(env.pool, s0["id"], context_used_tokens=90000, context_pct=9, **gauge)
    await env.push()
    assert await env.limits_start(s0["id"]) == 50000  # recorded once

    # tenure counts from the recorded start; the budget is start + 150k
    await env.run("UPDATE agent_limits SET since = now() - interval '1 hour'")
    await set_agent_status(env.pool, s0["id"], context_used_tokens=200000, context_pct=20, **gauge)
    assert await env.push() == 1
    assert len(env.notes(s0, DUE)) == 1
    assert (await _subjects(env))[solo["id"]].level == "weak"

    # a clear re-arms the start context: the next gauge is recorded again
    await insert_event(
        env.pool, solo["id"], s0["id"], type="session_cleared", data={}, ts=await env.ago(0)
    )
    await set_agent_status(env.pool, s0["id"], context_used_tokens=30000, context_pct=3, **gauge)
    await env.push()
    assert await env.limits_start(s0["id"]) == 30000
    await env.push()
    assert env.notes(s0, DUE) and len(env.notes(s0, DUE)) == 1
    assert (await _subjects(env))[solo["id"]].level == "none"


async def test_member_never_gets_a_note(env):
    c, (a0, a1) = await env.hive(2)
    for a in (a0, a1):
        await set_agent_status(env.pool, a["id"], state="idle", context_pct=90)
    subjects = await _subjects(env)
    assert [(x.agent_id, x.solo, x.level) for x in subjects.values()] == [
        (a0["id"], False, "strong")
    ]
    await env.push()
    assert env.notes(a1, DUE) == []
    assert await env.limits_start(a1["id"]) is None


async def test_trigger_created_solo_session_is_a_subject(env):
    solo = await insert_session(env.pool, spec={"replicas": 1}, created_by_service="agents-webhook")
    s0 = await env.agent(solo["id"], 0)
    await set_agent_status(env.pool, s0["id"], state="idle", context_pct=90)
    other = await insert_session(env.pool, spec={"replicas": 1}, desired="deleting")
    await env.agent(other["id"], 0)
    gone = await insert_session(env.pool, spec={"replicas": 1})
    g0 = await env.agent(gone["id"], 0)
    await env.run("UPDATE agents SET gone_at = now() WHERE id = %s", (g0["id"],))
    subjects = await _subjects(env)
    assert set(subjects) == {solo["id"]}
    assert (subjects[solo["id"]].solo, subjects[solo["id"]].level) == (True, "strong")


async def test_due_text_has_no_ccw_protocol(env):
    for spec in ({}, {"peers": False}):
        _, (a0, _a1, _a2) = await env.hive(spec=spec)
        await set_agent_status(env.pool, a0["id"], state="idle", context_pct=82)
        await env.push()
        (text,) = env.notes(a0, DUE)
        assert "mcp__ccw__send_message" not in text and '"ready"' not in text
        assert "notify_when_idle" not in text and "peer-idle-notice" not in text


async def test_handover_due_runs_when_hive_state_unchanged(env):
    c, (a0, a1, a2) = await env.hive(spec={"auto_compact_pct": 72})
    await env.push()
    assert len(env.notes(a0)) == 1
    await set_agent_status(env.pool, a0["id"], state="idle", context_pct=58)
    assert await env.push() == 1  # the structure hash is unchanged; the due note still goes
    assert len(env.notes(a0)) == 1 and len(env.notes(a0, DUE)) == 1

    # a failed due push is retried on the next tick
    await set_agent_status(env.pool, a0["id"], state="idle", context_pct=60)
    await env.run("UPDATE agents SET url = 'http://127.0.0.1:1' WHERE id = %s", (a0["id"],))
    assert await env.push() == 0
    await env.run("UPDATE agents SET url = %s WHERE id = %s", (env.fake.base_url, a0["id"]))
    assert await env.push() == 1
    assert len(env.notes(a0, DUE)) == 2


# ---------------------------------------------------------------- rollout_due

ROLLOUT = "rollout_due"


async def set_rollout(
    env, session_id, revision="r1", leader_since: str | None = "t", pending=(0,)
) -> None:
    rollout = {
        "revision": revision,
        "pending": list(pending),
        "since": "t",
        "leader_since": leader_since,
    }
    await env.run(
        "UPDATE sessions SET status = %s WHERE id = %s",
        (json.dumps({"rollout": rollout}), session_id),
    )


async def test_rollout_due_sent_once_per_epoch_and_revision(env):
    c, (a0, a1, a2) = await env.hive()
    await env.push()
    assert env.notes(a0, ROLLOUT) == []
    await set_rollout(env, c["id"])
    assert await env.push() == 1
    (text,) = env.notes(a0, ROLLOUT)
    assert text == hive.render_rollout_due(1, env.settings.rollout_handover_seconds)
    assert text.startswith("<rollout-due>") and text.endswith("</rollout-due>")
    assert "epoch=1" in text and "After 30 minutes" in text
    assert env.notes(a1, ROLLOUT) == [] and env.notes(a2, ROLLOUT) == []
    sent = [n for n in env.fake.notes[a0["wrapper_session_id"]] if n["key"] == ROLLOUT]
    assert sent[0]["source"] == "orchestrator"
    assert await env.push() == 0  # once, not once per tick

    await set_rollout(env, c["id"], revision="r2")  # a newer revision is a new note
    assert await env.push() == 1
    assert len(env.notes(a0, ROLLOUT)) == 2

    await env.run(  # a new epoch is a new note
        "UPDATE session_leaders SET agent_id = %s, epoch = epoch + 1 WHERE session_id = %s",
        (a1["id"], c["id"]),
    )
    await set_rollout(env, c["id"], revision="r2", pending=(1,))  # its pod is the old one
    await env.push()
    assert len(env.notes(a1, ROLLOUT)) == 1
    assert "epoch=2" in env.notes(a1, ROLLOUT)[0]


async def test_no_rollout_note_without_leader_since(env):
    c, (a0, a1, a2) = await env.hive()
    await set_rollout(env, c["id"], leader_since=None)
    await env.push()
    assert all(env.notes(a, ROLLOUT) == [] for a in (a0, a1, a2))
    await set_rollout(env, c["id"])
    await env.push()
    assert len(env.notes(a0, ROLLOUT)) == 1
    await env.run("UPDATE sessions SET status = '{}' WHERE id = %s", (c["id"],))
    await env.push()
    await set_rollout(env, c["id"])  # the window closed and reopened: told again
    await env.push()
    assert len(env.notes(a0, ROLLOUT)) == 2


async def test_no_rollout_note_to_a_leader_whose_pod_is_current(env):
    # the stored status still names the old leader's pod: a new leader on the current revision
    # (a voluntary move inside the window) is never told its pod is the last on the old one
    c, (a0, a1, a2) = await env.hive()
    await env.push()
    await env.run(
        "UPDATE session_leaders SET agent_id = %s, epoch = epoch + 1 WHERE session_id = %s",
        (a1["id"], c["id"]),
    )
    await set_rollout(env, c["id"], pending=(0,))
    await env.push()
    assert all(env.notes(a, ROLLOUT) == [] for a in (a0, a1, a2))
    await set_rollout(env, c["id"], pending=(1,))  # now the leader's own pod is outdated
    await env.push()
    assert len(env.notes(a1, ROLLOUT)) == 1


async def test_rollout_note_retried_after_failure(env):
    c, (a0, a1, a2) = await env.hive()
    await env.push()
    await set_rollout(env, c["id"])
    await env.run("UPDATE agents SET url = 'http://127.0.0.1:1' WHERE id = %s", (a0["id"],))
    assert await env.push() == 0
    await env.run("UPDATE agents SET url = %s WHERE id = %s", (env.fake.base_url, a0["id"]))
    assert await env.push() == 1
    assert len(env.notes(a0, ROLLOUT)) == 1


# ---------------------------------------------------------------- leadership_request

OUTCOME = "leadership_request"


async def handover(env, c, from_agent, to_agent, *, epoch: int = 1) -> int:
    """Queue an agent handover request the way the agent API does."""
    async with env.pool.connection() as conn:
        return await leadership.insert_request(
            conn,
            c["id"],
            "handover",
            from_agent["id"],
            None if to_agent is None else to_agent["id"],
            "test",
            epoch,
        )


async def test_leadership_request_applied_note(env):
    c, (a0, a1, a2) = await env.hive()
    await env.push()
    rid = await handover(env, c, a0, a1)
    await env.lpass(c["id"])
    assert await env.push() == 4  # the new hive_state to all three, plus the outcome to a0
    (text,) = env.notes(a0, OUTCOME)
    assert text == f"handover request {rid} applied: agent-1 leads at epoch 2"
    sent = [n for n in env.fake.notes[a0["wrapper_session_id"]] if n["key"] == OUTCOME]
    assert sent[0]["source"] == "orchestrator"
    assert env.notes(a1, OUTCOME) == [] and env.notes(a2, OUTCOME) == []  # only the requester

    # an untargeted request has no to_agent_id: the note names the leader as of the push
    rid = await handover(env, c, a1, None, epoch=2)
    await env.lpass(c["id"])
    await env.push()
    (text,) = env.notes(a1, OUTCOME)
    leader = (await env.leader_row(c["id"]))["agent_id"]
    pod = (await env.rows("SELECT pod_name FROM agents WHERE id = %s", (leader,)))[0]["pod_name"]
    assert text == f"handover request {rid} applied: {pod} leads at epoch 3"
    assert pod != "agent-1"


async def test_leadership_request_rejected_note(env):
    c, (a0, a1, a2) = await env.hive()
    await env.push()
    await env.run("UPDATE agents SET ready = false WHERE id = %s", (a1["id"],))
    r_target = await handover(env, c, a0, a1)
    r_not_leader = await handover(env, c, a2, a1)
    r_stale = await handover(env, c, a0, a2, epoch=7)
    await env.lpass(c["id"])
    await env.push()
    assert env.notes(a0, OUTCOME) == [
        f"handover request {r_target} rejected: target_not_ready",
        f"handover request {r_stale} rejected: stale_epoch",
    ]
    assert env.notes(a2, OUTCOME) == [f"handover request {r_not_leader} rejected: not_leader"]
    assert env.notes(a1, OUTCOME) == []

    # no_peers: an untargeted request while no other agent is ready
    await env.run("UPDATE agents SET ready = false WHERE id = %s", (a2["id"],))
    r_none = await handover(env, c, a0, None)
    await env.lpass(c["id"])
    await env.push()
    assert env.notes(a0, OUTCOME)[-1] == f"handover request {r_none} rejected: no_peers"
    row = await env.leader_row(c["id"])
    assert row["epoch"] == 1  # no request changed the leader


async def test_leadership_request_once_and_window(env):
    c, (a0, a1, a2) = await env.hive()
    await env.push()
    rid = await handover(env, c, a0, a1)
    async with env.pool.connection() as conn:  # a user request has no requester: no note
        await leadership.insert_request(conn, c["id"], "user", None, a1["id"], "test", None)
    await env.lpass(c["id"])
    await env.push()
    assert len(env.notes(a0, OUTCOME)) == 1
    assert all(env.notes(a, OUTCOME) == [] for a in (a1, a2))
    await env.push()
    await env.push()
    assert len(env.notes(a0, OUTCOME)) == 1  # once per request id

    # a restarted dispatcher (a fresh HiveState) says it once more: at most one duplicate
    assert await env.push(hive.HiveState()) == 1
    assert len(env.notes(a0, OUTCOME)) == 2

    # tracking is pruned with the window
    assert env.state.outcome_sent[c["id"]] == {rid}
    await env.run(
        "UPDATE leadership_requests SET decided_at = now() - make_interval(secs => %s)"
        " WHERE id = %s",
        (hive.OUTCOME_WINDOW_SECONDS + 10, rid),
    )
    await env.push()
    assert env.state.outcome_sent[c["id"]] == set()

    # decided before the window: never sent
    old = await handover(env, c, a1, a0, epoch=2)
    await env.lpass(c["id"])
    await env.run(
        "UPDATE leadership_requests SET decided_at = now() - make_interval(secs => %s)"
        " WHERE id = %s",
        (hive.OUTCOME_WINDOW_SECONDS + 10, old),
    )
    assert await env.push(hive.HiveState()) == 3  # the hive_state for the new leader, no outcome
    assert len(env.notes(a0, OUTCOME)) == 2 and env.notes(a1, OUTCOME) == []


async def test_leadership_request_gone_requester_and_retry(env):
    c, (a0, a1, a2) = await env.hive()
    await env.push()
    rid = await handover(env, c, a0, a1)
    await env.lpass(c["id"])
    await env.run("UPDATE agents SET url = 'http://127.0.0.1:1' WHERE id = %s", (a0["id"],))
    await env.push()
    assert env.notes(a0, OUTCOME) == []  # the push failed
    assert env.state.outcome_sent.get(c["id"], set()) == set()  # and is not remembered
    await env.run("UPDATE agents SET url = %s WHERE id = %s", (env.fake.base_url, a0["id"]))
    await env.push()
    assert env.notes(a0, OUTCOME) == [f"handover request {rid} applied: agent-1 leads at epoch 2"]

    # a requester that is gone gets nothing
    r2 = await handover(env, c, a1, a2, epoch=2)
    await env.lpass(c["id"])
    await env.run("UPDATE agents SET gone_at = now() WHERE id = %s", (a1["id"],))
    await env.push()
    assert env.notes(a1, OUTCOME) == [] and r2 not in env.state.outcome_sent.get(c["id"], set())


# ---------------------------------------------------------------- gating


async def test_throttle_and_lock(env):
    c, (a0, a1, a2) = await env.hive()
    state = hive.HiveState()
    now = [1000.0]

    async def tick() -> int:
        return await hive.push_once(
            env.pool, env.settings, env.factory, state, clock=lambda: now[0]
        )

    assert await tick() == 3
    await env.agent(c["id"], 3)
    now[0] += hive.PUSH_INTERVAL_SECONDS - 1
    assert await tick() == 0  # throttled: one push per session per interval
    now[0] += 2
    assert await tick() == 4

    # another dispatcher holds the session's lock: skipped, and the skip is not a tick
    await env.agent(c["id"], 4)
    now[0] += 100
    holder = await psycopg.AsyncConnection.connect(env.dsn)
    try:
        await holder.execute(
            "SELECT pg_advisory_xact_lock(%s, hashtext(%s::text))", (_CLAIM_LOCK_NAMESPACE, c["id"])
        )
        assert await tick() == 0
        await holder.rollback()
    finally:
        await holder.close()
    assert await tick() == 5


async def test_alone_and_disabled(env):
    solo = await insert_session(env.pool, spec={"replicas": 1})
    s0 = await env.agent(solo["id"], 0)
    await env.write_event(solo["id"], s0, "/solo.py")
    assert await env.push() == 0
    assert env.fake.notes.get(s0["wrapper_session_id"]) is None
    assert (
        await env.rows("SELECT 1 FROM session_leaders WHERE session_id = %s", (solo["id"],)) == []
    )

    c, (a0, a1, a2) = await env.hive()
    await env.write_event(c["id"], a1, "/x.py")
    env.fake.disable_notes()
    assert await env.push() == 0  # 409 notes_disabled everywhere: tolerated, nothing delivered
    assert env.fake.notes == {}
    row = await env.leader_row(c["id"])
    assert row["state_pushed_at"] is not None  # nothing to retry on a wrapper without notes
    env.fake.disable_notes(False)
    assert await env.push() == 0  # and not retried every tick
    assert env.fake.notes == {}


async def test_hanging_wrapper_does_not_block_claim(env, monkeypatch):
    monkeypatch.setattr(hive, "PUSH_TIMEOUT_SECONDS", 1.5)
    c, (a0, a1, a2) = await env.hive()
    msg = await insert_message(env.pool, c["id"])
    env.fake.delay_notes(30)
    task = asyncio.create_task(env.push())
    await asyncio.sleep(0.5)  # the pushes are in flight, no transaction held
    assert not task.done()
    t0 = time.monotonic()
    async with env.pool.connection() as conn:
        claim = await queue.claim_and_pick(conn)
    await env.run("UPDATE agents SET gone_at = now() WHERE id = %s", (a0["id"],))
    change = await env.lpass(c["id"])
    elapsed = time.monotonic() - t0
    assert claim is not None and claim.message_id == msg["id"]
    assert change is not None and change.reason == "leader_lost"
    assert elapsed < 1.0 and not task.done()
    assert await task == 0  # the per-push timeout ended the hang
    row = await env.leader_row(c["id"])
    assert row["state_pushed_at"] is None  # failed pushes are not recorded


async def test_dispatcher_run_pushes_on_leader_changed(env):
    c, agents = await env.hive(lead=False)
    env.fake.token = tokens_for(env.settings, c["id"]).auth  # what the dispatcher presents
    stop = asyncio.Event()
    dispatcher = Dispatcher(env.pool, env.settings)
    task = asyncio.create_task(dispatcher.run(stop))
    try:
        async with asyncio.timeout(15):
            while not env.notes(agents[0]):
                await asyncio.sleep(0.1)
    finally:
        stop.set()
        await task
    assert "Leader: agent-0 (epoch 1)" in env.notes(agents[2])[0]


async def test_concurrent_pushes_claim_once(env):
    c, (a0, a1, a2) = await env.hive()
    await env.write_event(c["id"], a1, "/once.py")  # peer activity adds no note
    env.fake.delay_notes(0.5)  # the second dispatcher reads during the first one's HTTP phase
    first, second = hive.HiveState(), hive.HiveState()
    task = asyncio.create_task(env.push(first))
    await asyncio.sleep(0.25)  # the first is now in its HTTP phase, lock released
    sent = [await env.push(second), await task]
    assert sorted(sent) == [0, 3]  # the 3 hive_state notes, once
    for agent in (a0, a1, a2):
        assert len(env.notes(agent)) == 1
        assert env.notes(agent, key=None) == []


async def test_leadership_pass_is_committed_before_the_push_wake(env, monkeypatch):
    """The push loop is woken after a pass, and it reads through another connection: the
    pass's decision must already be committed when `_drain_leadership` wakes it, or the push
    reads the request as still queued and the requester's outcome note waits for a timer tick."""
    c, (a0, a1, a2) = await env.hive()
    await env.run("UPDATE agents SET ready = false WHERE id = %s", (a1["id"],))
    rid = await handover(env, c, a0, a1)
    dispatcher = Dispatcher(env.pool, env.settings)
    seen: list[tuple[str, bool]] = []
    real_pass = leadership.leadership_pass

    async def spy(conn, session_id, policy):
        change = await real_pass(conn, session_id, policy)
        (row,) = await env.rows("SELECT status FROM leadership_requests WHERE id = %s", (rid,))
        seen.append((row["status"], dispatcher._push_wake.is_set()))
        return change

    monkeypatch.setattr(leadership, "leadership_pass", spy)
    await dispatcher._drain_leadership()
    assert seen == [("rejected", False)]  # decided and visible to others before the wake
    assert dispatcher._push_wake.is_set()


async def test_dispatcher_run_pushes_after_decision(env, monkeypatch):
    monkeypatch.setattr(hive, "PUSH_INTERVAL_SECONDS", 60)  # no timer tick inside the test
    c, (a0, a1, a2) = await env.hive()
    env.fake.token = tokens_for(env.settings, c["id"]).auth
    settings = env.settings.model_copy(update={"dispatch_poll_seconds": 300})  # no poll tick
    stop = asyncio.Event()
    dispatcher = Dispatcher(env.pool, settings)
    task = asyncio.create_task(dispatcher.run(stop))
    try:
        async with asyncio.timeout(15):
            while not env.notes(a2):  # the first push: hive_state everywhere
                await asyncio.sleep(0.1)
        dispatcher.hive.last_tick.clear()  # the per-session 10 s throttle has run out
        # the target goes unready after the API accepted the request: the dispatcher rejects it
        await env.run("UPDATE agents SET ready = false WHERE id = %s", (a1["id"],))
        rid = await handover(env, c, a0, a1)
        async with asyncio.timeout(15):
            while not env.notes(a0, OUTCOME):
                await asyncio.sleep(0.1)
    finally:
        stop.set()
        await task
    assert env.notes(a0, OUTCOME) == [f"handover request {rid} rejected: target_not_ready"]
    assert (await env.leader_row(c["id"]))["epoch"] == 1  # no leader change woke the push

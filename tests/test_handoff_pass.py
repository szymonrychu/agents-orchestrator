"""Strong-limit handoff state machine (`dispatcher/handoff.handoff_pass`) against Postgres.

Time moves through SQL: deadlines, free_since and discuss_since are pushed into the past.
"""

from __future__ import annotations

import asyncio
import uuid

import psycopg
import pytest
from factories import (
    insert_agent,
    insert_event,
    insert_message,
    insert_session,
    insert_turn,
    set_agent_status,
)
from fakes import fake_wrapper
from fakes.fake_wrapper import FakeWrapper
from psycopg.rows import dict_row

from agents_orchestrator.db import handoffs, leadership, queue
from agents_orchestrator.db.queue import _CLAIM_LOCK_NAMESPACE
from agents_orchestrator.dispatcher import Dispatcher, handoff, hive
from agents_orchestrator.handoff_texts import RETURN_EMPTY, strong_notice
from agents_orchestrator.settings import Settings
from agents_orchestrator.wrapper.client import WrapperClient

KEY = "k" * 32
TOKEN = "test-token"
WINDOW = 200000
# compact 95 (the default): strong = min(85% of 200k, 95% of 200k - 60k) = 130k tokens
STRONG = 140000
LOW = 20000


class Env:
    def __init__(self, pool, dsn: str) -> None:
        self.pool = pool
        self.settings = Settings(database_url=dsn, master_key=KEY)
        self.policy = handoff.HandoffPolicy.from_settings(self.settings)

    async def run(self, sql: str, params: tuple = ()) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(sql, params)

    async def rows(self, sql: str, params: tuple = ()) -> list[dict]:
        async with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            return await cur.fetchall()

    async def step(self, session_id) -> list[str]:
        async with self.pool.connection() as conn:
            return await handoff.handoff_pass(conn, session_id, self.policy, self.settings)

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
        a = await insert_agent(self.pool, c["id"], ordinal=0, ready=True, wrapper_session_id="w0")
        await self.gauge(a["id"], used)
        return c, a

    async def hive(self, n: int = 3, used: int = STRONG) -> tuple[dict, list[dict]]:
        c = await insert_session(self.pool, spec={"replicas": n, "max_replicas": 4})
        agents = []
        for i in range(n):
            a = await insert_agent(
                self.pool, c["id"], ordinal=i, ready=True, wrapper_session_id=f"w{i}"
            )
            await self.gauge(a["id"], LOW)
            agents.append(a)
        async with self.pool.connection() as conn:
            await leadership.leadership_pass(conn, c["id"])
        await self.gauge(agents[0]["id"], used)
        return c, agents

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

    async def past_deadline(self, agent_id) -> None:
        await self.run(
            "UPDATE agent_handoffs SET deadline_at = now() - interval '1 second'"
            " WHERE agent_id = %s AND phase IN ('notified', 'extended')",
            (agent_id,),
        )

    async def turn_ends(
        self, message: dict, agent_id, final_text: str | None, *, ago: int = 0
    ) -> str:
        """The message was sent and its turn completed `ago` seconds ago."""
        turn = await insert_turn(
            self.pool, agent_id, status="completed", final_text=final_text, completed_at=None
        )
        await self.run(
            "UPDATE turns SET completed_at = now() - make_interval(secs => %s) WHERE turn_id = %s",
            (ago, turn["turn_id"]),
        )
        await self.run(
            "UPDATE messages SET status = 'sent', agent_id = %s, turn_id = %s WHERE id = %s",
            (agent_id, turn["turn_id"], message["id"]),
        )
        return turn["turn_id"]

    async def idle_for(self, agent_id, seconds: int) -> None:
        await self.run(
            "UPDATE agent_status SET state = 'idle', current_turn_id = NULL,"
            " free_since = now() - make_interval(secs => %s) WHERE agent_id = %s",
            (seconds, agent_id),
        )

    async def to_requested(self, session_id, agent_id) -> dict:
        assert (await self.step(session_id))[0].startswith("notified:")
        await self.past_deadline(agent_id)
        assert any(t.startswith("requested:") for t in await self.step(session_id))
        (req,) = await self.messages(agent_id, "handoff_request")
        return req

    async def cleared(self, session_id, agent_id) -> None:
        await insert_event(self.pool, session_id, agent_id, type="session_cleared", data={})

    async def finish_command(self, command_id: int, status: str, result: dict) -> None:
        await self.run(
            "UPDATE commands SET status = %s, result = %s::jsonb, finished_at = now()"
            " WHERE id = %s",
            (status, psycopg.types.json.Jsonb(result), command_id),
        )


@pytest.fixture
def env(pool, dsn) -> Env:
    return Env(pool, dsn)


# ---------------------------------------------------------------- settings and texts


def test_settings_defaults_and_bounds(dsn):
    s = Settings(database_url=dsn, master_key=KEY)
    assert (
        s.handover_notify_seconds,
        s.handover_extension_seconds,
        s.handover_idle_proceed_seconds,
        s.handover_quiet_seconds,
        s.handover_quiet_cap_seconds,
    ) == (300, 300, 60, 60, 600)
    with pytest.raises(ValueError):
        Settings(database_url=dsn, master_key=KEY, handover_notify_seconds=10)
    with pytest.raises(ValueError):
        Settings(
            database_url=dsn,
            master_key=KEY,
            handover_quiet_seconds=900,
            handover_quiet_cap_seconds=600,
        )


def test_notice_text_verbatim():
    assert strong_notice(5, 5, hive=False) == (
        "Context limit (strong): in 5 minutes you will be asked for a handoff, then your context"
        " is cleared. Finish or park your current step now: commit and push, update your task"
        " and todo. Need more time? Call orchestrator_leadership_handover(extend=true) once for"
        " 5 more minutes."
    )
    assert "then your context is cleared and leadership moves to a successor." in strong_notice(
        5, 5, hive=True
    )


# ---------------------------------------------------------------- 1. open


async def test_strong_opens_notified_row_and_notice_message(env):
    c, a = await env.solo()
    assert await env.step(c["id"]) == [f"notified:{a['id']}"]
    row = await env.row(a["id"])
    assert (row.trigger, row.phase) == ("strong", "notified")
    (left,) = await env.rows(
        "SELECT extract(epoch FROM deadline_at - now()) AS s FROM agent_handoffs WHERE id = %s",
        (row.id,),
    )
    assert 295 <= left["s"] <= 300
    (msg,) = await env.messages(a["id"])
    assert (msg["lane"], msg["notice"], msg["status"]) == ("regular", "handoff_notice", "queued")
    assert msg["target_agent_id"] == a["id"]
    assert msg["text"] == strong_notice(5, 5, hive=False)
    # a second pass opens nothing more
    assert await env.step(c["id"]) == []
    assert len(await env.messages(a["id"])) == 1


async def test_weak_level_opens_nothing(env, dsn):
    env.settings = Settings(database_url=dsn, master_key=KEY, handover_budget_tokens=50000)
    c, a = await env.solo(used=125000)  # past start + 50k, below the strong 130k
    await env.run(
        "INSERT INTO agent_limits (agent_id, start_context_tokens, since)"
        " VALUES (%s, 0, now() - interval '2 hours')",
        (a["id"],),
    )
    async with env.pool.connection() as conn:
        (subject,) = await hive._subject_rows(conn, session_id=c["id"])
    assert hive._row_level(subject, env.settings)[1] == "weak"
    assert await env.step(c["id"]) == []
    assert await env.row(a["id"]) is None
    assert await env.messages(a["id"]) == []


async def test_member_never_gets_a_row(env):
    c, (a0, a1, a2) = await env.hive()
    for a in (a1, a2):
        await env.gauge(a["id"], 180000)
    assert await env.step(c["id"]) == [f"notified:{a0['id']}"]
    for a in (a1, a2):
        assert await env.row(a["id"]) is None
        assert await env.messages(a["id"]) == []


async def test_stopping_or_not_live_agent_opens_nothing(env):
    c, a = await env.solo()
    await env.run("UPDATE agent_status SET stopping_since = now() WHERE agent_id = %s", (a["id"],))
    assert await env.step(c["id"]) == []
    await env.run("UPDATE agent_status SET stopping_since = NULL WHERE agent_id = %s", (a["id"],))
    await env.run("UPDATE agents SET ready = false WHERE id = %s", (a["id"],))
    assert await env.step(c["id"]) == []
    assert await env.row(a["id"]) is None


# ---------------------------------------------------------------- 2. request


async def test_deadline_queues_request_regular_lane(env):
    c, a = await env.solo()
    await env.step(c["id"])
    await env.run("UPDATE agent_status SET state = 'busy' WHERE agent_id = %s", (a["id"],))
    assert await env.step(c["id"]) == []
    await env.past_deadline(a["id"])
    assert await env.step(c["id"]) == [f"requested:{a['id']}"]
    (req,) = await env.messages(a["id"], "handoff_request")
    assert (req["lane"], req["status"], req["target_agent_id"]) == ("regular", "queued", a["id"])
    assert req["text"].startswith("Write your handoff now as your reply")
    row = await env.row(a["id"])
    assert (row.phase, row.request_message_id) == ("requested", req["id"])


async def test_extended_deadline_is_respected(env):
    c, a = await env.solo()
    await env.step(c["id"])
    async with env.pool.connection() as conn:
        assert await handoffs.extend(conn, a["id"], 300) is not None
    (notice,) = await env.messages(a["id"], "handoff_notice")
    await env.turn_ends(notice, a["id"], "ok", ago=120)
    await env.idle_for(a["id"], 120)
    # the original deadline passed, the extended one did not; no early proceed while extended
    await env.run(
        "UPDATE agent_handoffs SET deadline_at = deadline_at - interval '300 seconds'"
        " WHERE agent_id = %s",
        (a["id"],),
    )
    assert await env.step(c["id"]) == []
    assert (await env.row(a["id"])).phase == "extended"
    await env.past_deadline(a["id"])
    assert await env.step(c["id"]) == [f"requested:{a['id']}"]


async def test_early_proceed_after_idle_60s(env):
    c, a = await env.solo()
    await env.step(c["id"])
    (notice,) = await env.messages(a["id"], "handoff_notice")
    await env.turn_ends(notice, a["id"], "parked", ago=30)
    await env.idle_for(a["id"], 30)
    assert await env.step(c["id"]) == []  # idle, but not for 60 s yet
    await env.turn_ends(notice, a["id"], "parked", ago=61)
    await env.idle_for(a["id"], 61)
    await env.run(
        "UPDATE agent_status SET background_subagents = jsonb_build_object('x', now()::text)"
        " WHERE agent_id = %s",
        (a["id"],),
    )
    assert await env.step(c["id"]) == []  # a background subagent still runs
    await env.run(
        "UPDATE agent_status SET background_subagents = '{}' WHERE agent_id = %s", (a["id"],)
    )
    assert await env.step(c["id"]) == [f"requested:{a['id']}"]


async def test_no_early_proceed_while_notice_turn_running(env):
    c, a = await env.solo()
    await env.step(c["id"])
    (notice,) = await env.messages(a["id"], "handoff_notice")
    turn = await insert_turn(env.pool, a["id"], status="running")
    await env.run(
        "UPDATE messages SET status = 'sent', agent_id = %s, turn_id = %s WHERE id = %s",
        (a["id"], turn["turn_id"], notice["id"]),
    )
    await env.idle_for(a["id"], 600)  # even a stale idle gauge does not count
    assert await env.step(c["id"]) == []
    assert (await env.row(a["id"])).phase == "notified"


async def test_request_waits_for_running_turn(env):
    c, a = await env.solo()
    await env.step(c["id"])
    turn = await insert_turn(env.pool, a["id"], status="running")
    await env.run(
        "UPDATE agent_status SET state = 'busy', current_turn_id = %s WHERE agent_id = %s",
        (turn["turn_id"], a["id"]),
    )
    await env.past_deadline(a["id"])
    assert await env.step(c["id"]) == [f"requested:{a['id']}"]
    (req,) = await env.messages(a["id"], "handoff_request")
    assert (req["status"], req["lane"]) == ("queued", "regular")  # never priority
    # nothing is interrupted and the row waits for the request's own turn
    assert await env.rows("SELECT * FROM commands WHERE kind = 'interrupt'") == []
    assert await env.step(c["id"]) == []
    assert (await env.row(a["id"])).phase == "requested"
    assert all(m["lane"] != "priority" for m in await env.messages(a["id"]))


# ---------------------------------------------------------------- 3. handed


async def test_hive_handed_inserts_forced_limit_request(env):
    c, (a0, a1, a2) = await env.hive()
    req = await env.to_requested(c["id"], a0["id"])
    await env.turn_ends(req, a0["id"], "my handoff")
    got = await env.step(c["id"])
    assert got == [f"handed:{a0['id']}", f"handover_requested:{a0['id']}"]
    row = await env.row(a0["id"])
    assert (row.phase, row.handoff_text) == ("handed", "my handoff")
    (lr,) = await env.rows("SELECT * FROM leadership_requests WHERE session_id = %s", (c["id"],))
    assert (lr["kind"], lr["reason"], lr["from_agent_id"], lr["to_agent_id"]) == (
        "handover",
        handoff.LIMIT_REASON,
        a0["id"],
        None,
    )
    assert lr["epoch"] == 1 and lr["status"] == "queued"
    assert await env.commands(a0["id"]) == []  # no clear before the discussion
    # the row waits for the leadership pass; no second request
    assert await env.step(c["id"]) == []
    assert len(await env.rows("SELECT * FROM leadership_requests")) == 1


async def test_rejected_limit_request_falls_back_to_solo_order(env):
    c, (a0, a1, a2) = await env.hive()
    req = await env.to_requested(c["id"], a0["id"])
    await env.turn_ends(req, a0["id"], "my handoff")
    await env.step(c["id"])
    await env.run(
        "UPDATE leadership_requests SET status = 'rejected', reject_reason = 'no_peers',"
        " decided_at = now()"
    )
    assert await env.step(c["id"]) == [f"clearing:{a0['id']}"]
    assert len(await env.commands(a0["id"])) == 1


async def test_single_live_hive_follows_solo_order(env):
    c, (a0, a1) = await env.hive(2)
    await env.run("UPDATE agents SET ready = false WHERE id = %s", (a1["id"],))
    req = await env.to_requested(c["id"], a0["id"])
    await env.turn_ends(req, a0["id"], "alone")
    assert await env.step(c["id"]) == [f"handed:{a0['id']}", f"clearing:{a0['id']}"]
    assert await env.rows("SELECT * FROM leadership_requests") == []
    (cmd,) = await env.commands(a0["id"])
    row = await env.row(a0["id"])
    assert (row.phase, row.clear_command_id, row.clear_attempts) == ("clearing", cmd["id"], 1)
    await env.finish_command(cmd["id"], "done", {"response": {"id": "c1"}})
    await env.cleared(c["id"], a0["id"])
    assert await env.step(c["id"]) == [f"returning:{a0['id']}"]
    (ret,) = await env.messages(a0["id"], "handoff_return")
    assert "alone" in ret["text"]


async def test_solo_handed_enqueues_clear_then_return_after_session_cleared(env):
    c, a = await env.solo()
    req = await env.to_requested(c["id"], a["id"])
    await env.turn_ends(req, a["id"], "solo handoff text")
    assert await env.step(c["id"]) == [f"handed:{a['id']}", f"clearing:{a['id']}"]
    (cmd,) = await env.commands(a["id"])
    # no return before the clear happened
    assert await env.step(c["id"]) == []
    await env.finish_command(cmd["id"], "done", {"response": {"id": "c1"}})
    assert await env.step(c["id"]) == []
    assert await env.messages(a["id"], "handoff_return") == []
    await env.cleared(c["id"], a["id"])
    assert await env.step(c["id"]) == [f"returning:{a['id']}"]
    (ret,) = await env.messages(a["id"], "handoff_return")
    assert ret["text"] == (
        "Handoff from your previous context (strong limit, cleared just now):\n\nsolo handoff text"
    )
    assert (ret["lane"], ret["target_agent_id"]) == ("regular", a["id"])
    row = await env.row(a["id"])
    assert (row.phase, row.return_message_id) == ("returning", ret["id"])
    assert row.cleared_at is not None
    assert await env.step(c["id"]) == []
    await env.run("UPDATE messages SET status = 'sent' WHERE id = %s", (ret["id"],))
    assert await env.step(c["id"]) == [f"done:{a['id']}"]
    last = await env.last_row(a["id"])
    assert (last["phase"], last["close_reason"]) == ("done", "returned")
    assert len(await env.messages(a["id"], "handoff_return")) == 1


# ---------------------------------------------------------------- 4. discussing


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


async def test_discussing_clears_at_cap_while_busy(env):
    c, (a0, a1, a2) = await _discussing(env, quiet_ago=700, since_ago=300)
    await env.run("UPDATE agent_status SET state = 'busy' WHERE agent_id = %s", (a1["id"],))
    assert await env.step(c["id"]) == []
    await env.run("UPDATE agent_handoffs SET discuss_since = now() - interval '601 seconds'")
    assert await env.step(c["id"]) == [f"clearing:{a0['id']}"]


# ---------------------------------------------------------------- cancellation


async def test_compaction_cancels_open_row_and_queued_messages(env):
    # notified: the notice message is cancelled with the row
    c, a = await env.solo()
    await env.step(c["id"])
    await insert_event(env.pool, c["id"], a["id"], type="compacted", data={"trigger": "auto"})
    assert await env.step(c["id"]) == [f"cancelled:{a['id']}:compacted"]
    last = await env.last_row(a["id"])
    assert (last["phase"], last["close_reason"]) == ("cancelled", "compacted")
    (notice,) = await env.messages(a["id"], "handoff_notice")
    assert notice["status"] == "cancelled"
    # the gauge still shows the pre-compaction context: no new row until it is re-read
    assert await env.step(c["id"]) == []
    # requested: the queued request is cancelled and never reaches the compacted context
    c2, a2 = await env.solo()
    await env.to_requested(c2["id"], a2["id"])
    await env.gauge(a2["id"], LOW)  # manual /compact: the gauge dropped below the weak level
    assert await env.step(c2["id"]) == [f"cancelled:{a2['id']}:compacted"]
    (req,) = await env.messages(a2["id"], "handoff_request")
    assert req["status"] == "cancelled"
    # the next strong crossing opens a new row
    await env.gauge(a2["id"], STRONG)
    assert await env.step(c2["id"]) == [f"notified:{a2['id']}"]


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
        await leadership.leadership_pass(conn, c["id"])
    assert await env.step(c["id"]) == [f"cancelled:{a0['id']}:not_leader"]
    (notice,) = await env.messages(a0["id"], "handoff_notice")
    assert notice["status"] == "cancelled"


@pytest.mark.parametrize("variant", ["solo", "hive"])
async def test_empty_final_text_continues(env, variant, caplog):
    if variant == "solo":
        c, a = await env.solo()
    else:
        c, (a, _, _) = await env.hive()
    req = await env.to_requested(c["id"], a["id"])
    await env.turn_ends(req, a["id"], "   ")
    with caplog.at_level("INFO"):
        got = await env.step(c["id"])
    assert got[0] == f"handed:{a['id']}"
    assert "handoff_empty" in caplog.text
    row = await env.row(a["id"])
    assert row.handoff_text is None
    if variant == "hive":
        assert got[1] == f"handover_requested:{a['id']}"
        return
    assert got[1] == f"clearing:{a['id']}"
    (cmd,) = await env.commands(a["id"])
    await env.finish_command(cmd["id"], "done", {"response": {"id": "c1"}})
    await env.cleared(c["id"], a["id"])
    await env.step(c["id"])
    (ret,) = await env.messages(a["id"], "handoff_return")
    assert ret["text"] == RETURN_EMPTY


async def test_failed_request_message_never_hangs(env):
    c, a = await env.solo()
    req = await env.to_requested(c["id"], a["id"])
    await env.run(
        "UPDATE messages SET status = 'failed', error = 'unavailable' WHERE id = %s", (req["id"],)
    )
    assert await env.step(c["id"]) == [f"handed:{a['id']}", f"clearing:{a['id']}"]


async def test_lost_request_turn_never_hangs(env):
    c, a = await env.solo()
    req = await env.to_requested(c["id"], a["id"])
    turn = await insert_turn(env.pool, a["id"], status="running")
    await env.run(
        "UPDATE messages SET status = 'sent', agent_id = %s, turn_id = %s WHERE id = %s",
        (a["id"], turn["turn_id"], req["id"]),
    )
    await env.idle_for(a["id"], 10)
    assert await env.step(c["id"]) == []
    await env.idle_for(a["id"], 61)  # idle with no current turn: the turn record was lost
    assert (await env.step(c["id"]))[0] == f"handed:{a['id']}"


async def test_clear_refused_subagents_running_is_retried(env):
    c, a = await env.solo()
    req = await env.to_requested(c["id"], a["id"])
    await env.turn_ends(req, a["id"], "text")
    await env.step(c["id"])
    (cmd,) = await env.commands(a["id"])
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
    # after 5 attempts the row closes clear_failed and the solo agent still gets its return
    await env.run("UPDATE agent_handoffs SET clear_attempts = 5 WHERE agent_id = %s", (a["id"],))
    last_cmd = (await env.commands(a["id"]))[-1]
    await env.finish_command(last_cmd["id"], "failed", {"error": "409 subagents_running"})
    assert await env.step(c["id"]) == [f"cancelled:{a['id']}:clear_failed"]
    last = await env.last_row(a["id"])
    assert (last["phase"], last["close_reason"]) == ("cancelled", "clear_failed")
    (ret,) = await env.messages(a["id"], "handoff_return")
    assert last["return_message_id"] == ret["id"]
    assert ret["status"] == "queued"
    # still at the strong limit, but no new cycle until the context is cleared or compacted
    assert await env.step(c["id"]) == []
    await insert_event(env.pool, c["id"], a["id"], type="compacted", data={})
    await env.gauge(a["id"], STRONG)
    assert await env.step(c["id"]) == [f"notified:{a['id']}"]
    assert len(await env.messages(a["id"], "handoff_return")) == 1


async def test_other_clear_failure_closes_and_returns(env):
    c, a = await env.solo()
    req = await env.to_requested(c["id"], a["id"])
    await env.turn_ends(req, a["id"], "text")
    await env.step(c["id"])
    (cmd,) = await env.commands(a["id"])
    await env.finish_command(cmd["id"], "failed", {"error": "agent has no session"})
    assert await env.step(c["id"]) == [f"cancelled:{a['id']}:clear_failed"]
    assert len(await env.messages(a["id"], "handoff_return")) == 1


async def test_return_sent_once_across_pod_restart_during_clearing(env):
    c, a = await env.solo()
    req = await env.to_requested(c["id"], a["id"])
    await env.turn_ends(req, a["id"], "text")
    await env.step(c["id"])
    (cmd,) = await env.commands(a["id"])
    await env.finish_command(cmd["id"], "done", {"response": {"id": "c1"}})
    # the pod restarts: a new wrapper session, the cleared event is replayed and ingested
    await env.run("UPDATE agents SET wrapper_session_id = 'w0-new' WHERE id = %s", (a["id"],))
    await env.cleared(c["id"], a["id"])
    await insert_event(
        env.pool, c["id"], a["id"], wrapper_session_id="w0-new", type="session_cleared", data={}
    )
    # a fresh dispatcher (new policy object) and two concurrent passes
    env.policy = handoff.HandoffPolicy.from_settings(env.settings)
    results = await asyncio.gather(env.step(c["id"]), env.step(c["id"]))
    assert sorted(results, key=len) == [[], [f"returning:{a['id']}"]]
    assert await env.step(c["id"]) == []
    assert len(await env.messages(a["id"], "handoff_return")) == 1


# ---------------------------------------------------------------- lock


async def test_pass_skipped_without_claim_lock(env, dsn):
    c, a = await env.solo()
    async with await psycopg.AsyncConnection.connect(dsn) as holder, holder.transaction():
        await holder.execute(
            "SELECT pg_advisory_xact_lock(%s, hashtext(%s::text))",
            (_CLAIM_LOCK_NAMESPACE, c["id"]),
        )
        assert await env.step(c["id"]) == []
        assert await env.row(a["id"]) is None
    assert await env.step(c["id"]) == [f"notified:{a['id']}"]


async def test_handoff_sessions_lists_subjects_and_open_rows(env):
    c, a = await env.solo()
    h, _ = await env.hive()
    closing, b = await env.solo()
    async with env.pool.connection() as conn:
        await handoffs.insert(conn, closing["id"], b["id"], "strong", "notified")
    await env.run("UPDATE sessions SET desired = 'deleting' WHERE id = %s", (closing["id"],))
    async with env.pool.connection() as conn:
        got = await handoff.handoff_sessions(conn)
    assert {c["id"], h["id"], closing["id"]} <= set(got)


# ---------------------------------------------------------------- the keyed note


@pytest.fixture
async def fake(monkeypatch):
    monkeypatch.setattr(fake_wrapper, "MAX_SESSIONS", 100)
    f = FakeWrapper(token=TOKEN)
    await f.start()
    try:
        yield f
    finally:
        await f.stop()


async def test_notice_note_pushed_while_notified(env, fake):
    clients: list[WrapperClient] = []

    def factory(session_id, url) -> WrapperClient:
        client = WrapperClient(url, TOKEN)
        clients.append(client)
        return client

    creator = WrapperClient(fake.base_url, TOKEN)
    try:
        wsid = (await creator.create_session(f"n{uuid.uuid4().hex[:4]}", "/w")).wrapper_session_id
    finally:
        await creator.aclose()
    c = await insert_session(env.pool, spec={})
    a = await insert_agent(
        env.pool, c["id"], ordinal=0, ready=True, wrapper_session_id=wsid, url=fake.base_url
    )
    await env.gauge(a["id"], STRONG)
    state = hive.HiveState()
    clock = [1000.0]

    async def push() -> None:
        clock[0] += 100
        await hive.push_once(env.pool, env.settings, factory, state, clock=lambda: clock[0])

    def notes() -> list[str]:
        return [n["text"] for n in fake.notes.get(wsid, []) if n["key"] == "handoff_notice"]

    try:
        await push()
        assert notes() == []  # strong, but no row yet
        await env.step(c["id"])
        await push()
        assert len(notes()) == 1 and notes()[0].startswith("Context limit (strong)")
        await push()
        assert len(notes()) == 1  # once per row and phase
        async with env.pool.connection() as conn:
            await handoffs.extend(conn, a["id"], 300)
        await push()
        assert len(notes()) == 2 and "in 10 minutes" in notes()[1]
    finally:
        for client in clients:
            await client.aclose()


async def test_dispatcher_runs_the_pass_after_leadership(env):
    d = Dispatcher(env.pool, env.settings)
    try:
        c, a = await env.solo()
        await d._drain_leadership()
        assert (await env.row(a["id"])).phase == "notified"
        assert d._push_wake.is_set()
        # throttled: a second wake within the interval runs no pass
        await env.past_deadline(a["id"])
        await d._drain_leadership()
        assert (await env.row(a["id"])).phase == "notified"
        d._handoff_at = None
        await d._drain_leadership()
        assert (await env.row(a["id"])).phase == "requested"
    finally:
        await d.aclose()


# ---------------------------------------------------------------- fix round 1 (review of step 6)


async def _claim(env) -> int | None:
    async with env.pool.connection() as conn:
        claim = await queue.claim_and_pick(conn)
    return claim.message_id if claim is not None else None


async def _status(env, message_id: int) -> str:
    (row,) = await env.rows("SELECT status FROM messages WHERE id = %s", (message_id,))
    return row["status"]


@pytest.mark.parametrize("event", ["compacted", "session_cleared"])
async def test_claim_never_sends_request_into_reset_context(env, event):
    # C1: the request is queued, the context is reset, and the message drain runs before any
    # handoff pass: the claim itself must refuse the request
    c, a = await env.solo()
    req = await env.to_requested(c["id"], a["id"])
    await insert_event(env.pool, c["id"], a["id"], type=event, data={})
    assert await _claim(env) is None
    assert await _status(env, req["id"]) == "cancelled"


@pytest.mark.parametrize("event", ["compacted", "session_cleared"])
async def test_claim_never_sends_notice_into_reset_context(env, event):
    c, a = await env.solo()
    await env.step(c["id"])
    (notice,) = await env.messages(a["id"], "handoff_notice")
    await insert_event(env.pool, c["id"], a["id"], type=event, data={})
    assert await _claim(env) is None
    assert await _status(env, notice["id"]) == "cancelled"


async def test_claim_cancels_handoff_messages_of_a_moved_row(env):
    # an orphan request (its row is not requested) and a notice of a closed row never go out
    c, a = await env.solo()
    await env.step(c["id"])
    orphan = await insert_message(
        env.pool,
        c["id"],
        text="orphan",
        target_agent_id=a["id"],
        lane="regular",
        notice="handoff_request",
    )
    (notice,) = await env.messages(a["id"], "handoff_notice")
    await env.run(
        "UPDATE agent_handoffs SET phase = 'cancelled', close_reason = 'test', closed_at = now()"
        " WHERE agent_id = %s",
        (a["id"],),
    )
    assert await _claim(env) is None
    assert await _status(env, orphan["id"]) == "cancelled"
    assert await _status(env, notice["id"]) == "cancelled"


async def test_moving_to_requested_cancels_the_queued_notice(env):
    # M4: a notice still queued at the deadline is not delivered right before the request
    c, a = await env.solo()
    await env.step(c["id"])
    await env.past_deadline(a["id"])
    assert await env.step(c["id"]) == [f"requested:{a['id']}"]
    (notice,) = await env.messages(a["id"], "handoff_notice")
    assert notice["status"] == "cancelled"


async def test_solo_holds_other_messages_from_request_until_return_sent(env):
    # I1: work queued behind the request waits for the clear and the return
    c, a = await env.solo()
    await env.step(c["id"])
    (notice,) = await env.messages(a["id"], "handoff_notice")
    await env.turn_ends(notice, a["id"], "ok", ago=1)
    await env.past_deadline(a["id"])
    await env.step(c["id"])
    (req,) = await env.messages(a["id"], "handoff_request")
    work = await insert_message(env.pool, c["id"], text="user work", lane="regular")
    targeted = await insert_message(
        env.pool, c["id"], text="peer work", target_agent_id=a["id"], lane="regular"
    )
    assert await _claim(env) == req["id"]
    assert await _claim(env) is None  # requested: held
    await env.turn_ends(req, a["id"], "handoff text")
    assert await env.step(c["id"]) == [f"handed:{a['id']}", f"clearing:{a['id']}"]
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
    c, (a0, a1, _) = await env.hive()
    await env.to_requested(c["id"], a0["id"])
    member = await insert_message(
        env.pool, c["id"], text="member", target_agent_id=a1["id"], lane="regular"
    )
    claimed = {await _claim(env), await _claim(env)}
    assert member["id"] in claimed


async def _clearing(env) -> tuple[dict, dict, dict]:
    c, a = await env.solo()
    req = await env.to_requested(c["id"], a["id"])
    await env.turn_ends(req, a["id"], "text")
    await env.step(c["id"])
    (cmd,) = await env.commands(a["id"])
    return c, a, cmd


async def test_clearing_without_session_cleared_times_out_and_retries(env):
    # I2: the wrapper accepted /clear and then restarted: no session_cleared, no command_failed
    c, a, cmd = await _clearing(env)
    await env.finish_command(cmd["id"], "done", {"response": {"id": "c1"}})
    await env.idle_for(a["id"], 600)
    assert await env.step(c["id"]) == []  # accepted just now: not yet
    await env.run(
        "UPDATE commands SET finished_at = now() - make_interval(secs => %s) WHERE id = %s",
        (env.settings.handover_clear_timeout_seconds + 1, cmd["id"]),
    )
    await env.run(
        "UPDATE agent_status SET state = 'busy', current_turn_id = 't' WHERE agent_id = %s",
        (a["id"],),
    )
    assert await env.step(c["id"]) == []  # a running turn may still be ahead of the clear
    await env.idle_for(a["id"], env.settings.handover_clear_timeout_seconds + 1)
    assert await env.step(c["id"]) == [f"clear_retry:{a['id']}"]
    row = await env.row(a["id"])
    assert row.clear_attempts == 2
    # the last attempt times out too: closed clear_failed, the solo agent still gets its return
    await env.run("UPDATE agent_handoffs SET clear_attempts = 5 WHERE agent_id = %s", (a["id"],))
    last_cmd = (await env.commands(a["id"]))[-1]
    await env.run(
        "UPDATE commands SET status = 'done', finished_at = now() - make_interval(secs => %s)"
        " WHERE id = %s",
        (env.settings.handover_clear_timeout_seconds + 1, last_cmd["id"]),
    )
    assert await env.step(c["id"]) == [f"cancelled:{a['id']}:clear_failed"]
    assert len(await env.messages(a["id"], "handoff_return")) == 1


async def test_external_clear_during_countdown_cancels_cleared(env):
    # I3: a /clear by the user or the agent while the notice counts down
    c, a = await env.solo()
    await env.step(c["id"])
    await env.cleared(c["id"], a["id"])
    assert await env.step(c["id"]) == [f"cancelled:{a['id']}:cleared"]
    (notice,) = await env.messages(a["id"], "handoff_notice")
    assert notice["status"] == "cancelled"
    last = await env.last_row(a["id"])
    assert (last["phase"], last["close_reason"]) == ("cancelled", "cleared")


async def test_compacting_alone_does_not_cancel(env):
    # M2: a PreCompact whose compaction then fails leaves the context as it was
    c, a = await env.solo()
    await env.step(c["id"])
    await insert_event(env.pool, c["id"], a["id"], type="compacting", data={})
    await insert_event(env.pool, c["id"], a["id"], type="compact_failed", data={})
    assert await env.step(c["id"]) == []
    assert (await env.row(a["id"])).phase == "notified"


async def test_clear_failed_block_expires(env):
    # concern 2: after clear_failed a later strong crossing starts a new cycle once the block ends
    c, a, cmd = await _clearing(env)
    await env.finish_command(cmd["id"], "failed", {"error": "agent has no session"})
    assert await env.step(c["id"]) == [f"cancelled:{a['id']}:clear_failed"]
    assert await env.step(c["id"]) == []
    await env.run(
        "UPDATE agent_handoffs SET closed_at = now() - make_interval(secs => %s)"
        " WHERE agent_id = %s",
        (env.settings.handover_clear_failed_block_seconds + 1, a["id"]),
    )
    assert await env.step(c["id"]) == [f"notified:{a['id']}"]


def test_clear_settings_defaults_and_bounds(dsn):
    s = Settings(database_url=dsn, master_key=KEY)
    assert (s.handover_clear_timeout_seconds, s.handover_clear_failed_block_seconds) == (
        180,
        1800,
    )
    with pytest.raises(ValueError):
        Settings(database_url=dsn, master_key=KEY, handover_clear_timeout_seconds=5)


async def test_clear_failed_return_text_says_not_cleared(env):
    # M8
    c, a, cmd = await _clearing(env)
    await env.finish_command(cmd["id"], "failed", {"error": "agent has no session"})
    await env.step(c["id"])
    (ret,) = await env.messages(a["id"], "handoff_return")
    assert "cleared just now" not in ret["text"]
    assert ret["text"].endswith("\n\ntext")


async def test_single_live_hive_notice_promises_no_successor(env):
    # concern 3: the wording is decided at send time from whether a successor exists
    c, (a0, a1) = await env.hive(2)
    await env.run("UPDATE agents SET ready = false WHERE id = %s", (a1["id"],))
    await env.step(c["id"])
    (notice,) = await env.messages(a0["id"], "handoff_notice")
    assert "successor" not in notice["text"]
    c2, (b0, _, _) = await env.hive()
    await env.step(c2["id"])
    (notice2,) = await env.messages(b0["id"], "handoff_notice")
    assert "leadership moves to a successor" in notice2["text"]


async def test_single_live_hive_note_promises_no_successor(env, fake):
    clients: list[WrapperClient] = []

    def factory(session_id, url) -> WrapperClient:
        client = WrapperClient(url, TOKEN)
        clients.append(client)
        return client

    creator = WrapperClient(fake.base_url, TOKEN)
    try:
        wsid = (await creator.create_session(f"h{uuid.uuid4().hex[:4]}", "/w")).wrapper_session_id
    finally:
        await creator.aclose()
    c, (a0, a1) = await env.hive(2)
    await env.run(
        "UPDATE agents SET wrapper_session_id = %s, url = %s WHERE id = %s",
        (wsid, fake.base_url, a0["id"]),
    )
    await env.run("UPDATE agents SET ready = false WHERE id = %s", (a1["id"],))
    await env.step(c["id"])
    state = hive.HiveState()
    try:
        await hive.push_once(env.pool, env.settings, factory, state, clock=lambda: 1000.0)
        notes = [n["text"] for n in fake.notes.get(wsid, []) if n["key"] == "handoff_notice"]
        assert len(notes) == 1
        assert "successor" not in notes[0]
    finally:
        for client in clients:
            await client.aclose()


def test_notice_text_without_extension_and_rounding():
    # M7: no extension offer when extensions are off; minutes round up like the countdown
    text = strong_notice(5, 0, hive=False)
    assert "Need more time" not in text
    assert text.endswith("update your task and todo.")


async def test_notice_extension_minutes_round_up(env, dsn):
    env.settings = Settings(database_url=dsn, master_key=KEY, handover_extension_seconds=150)
    env.policy = handoff.HandoffPolicy.from_settings(env.settings)
    c, a = await env.solo()
    await env.step(c["id"])
    (notice,) = await env.messages(a["id"], "handoff_notice")
    assert "once for 3 more minutes" in notice["text"]


async def test_failing_session_list_does_not_stop_message_drain(env, monkeypatch):
    # M6
    d = Dispatcher(env.pool, env.settings)

    async def boom(conn):
        raise RuntimeError("db hiccup")

    monkeypatch.setattr(handoff, "handoff_sessions", boom)
    try:
        await d._drain_leadership()  # logged, not raised
    finally:
        await d.aclose()


# ---------------------------------------------------------------- fix round 2


@pytest.mark.parametrize("status", ["queued", "running"])
async def test_clear_command_never_finished_times_out(env, status):
    # N1: the dispatcher died between claiming the clear and completing it (running), or never
    # claimed it (queued): the row must not hang in clearing, holding the solo agent's messages
    c, a, cmd = await _clearing(env)
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
    # the lost command is finished, so it can never run after the retry (or after the return)
    # failed with the retryable clear_timeout in both cases (F1: a cancelled one reads as the
    # non-retryable `cancelled` on a later pass)
    assert first["status"] == "failed"
    assert first["result"] == {"error": "clear_timeout"}
    assert retry["status"] == "queued"
    # the last attempt is lost too: closed clear_failed, the solo agent still gets its return
    await env.run("UPDATE agent_handoffs SET clear_attempts = 5 WHERE agent_id = %s", (a["id"],))
    await env.run(
        "UPDATE commands SET status = %s, created_at = now() - make_interval(secs => %s),"
        " started_at = CASE WHEN %s = 'running' THEN now() - make_interval(secs => %s) END"
        " WHERE id = %s",
        (status, stale, status, stale, retry["id"]),
    )
    assert await env.step(c["id"]) == [f"cancelled:{a['id']}:clear_failed"]
    assert len(await env.messages(a["id"], "handoff_return")) == 1


async def _hive_note(env, fake, *, successor_used: int) -> tuple[str, str]:
    """The strong notice message and keyed note of a two-agent hive whose member is at
    `successor_used` tokens."""
    clients: list[WrapperClient] = []

    def factory(session_id, url) -> WrapperClient:
        client = WrapperClient(url, TOKEN)
        clients.append(client)
        return client

    creator = WrapperClient(fake.base_url, TOKEN)
    try:
        wsid = (await creator.create_session(f"h{uuid.uuid4().hex[:4]}", "/w")).wrapper_session_id
    finally:
        await creator.aclose()
    c, (a0, a1) = await env.hive(2)
    await env.run(
        "UPDATE agents SET wrapper_session_id = %s, url = %s WHERE id = %s",
        (wsid, fake.base_url, a0["id"]),
    )
    await env.gauge(a1["id"], successor_used)
    await env.step(c["id"])
    (message,) = await env.messages(a0["id"], "handoff_notice")
    try:
        await hive.push_once(env.pool, env.settings, factory, hive.HiveState(), clock=lambda: 1.0)
        (note,) = [n["text"] for n in fake.notes.get(wsid, []) if n["key"] == "handoff_notice"]
    finally:
        for client in clients:
            await client.aclose()
    return message["text"], note


async def test_hive_note_with_successor_keeps_the_clause(env, fake):
    # N3: the positive case of the keyed note
    message, note = await _hive_note(env, fake, successor_used=LOW)
    assert "successor" in message
    assert "successor" in note


async def test_hive_note_and_message_agree_on_successor_without_headroom(env, fake):
    # N3: a ready member without headroom is still the successor `handed()` picks, so the note
    # promises it as the message does
    message, note = await _hive_note(env, fake, successor_used=STRONG)
    assert "successor" in message
    assert "successor" in note

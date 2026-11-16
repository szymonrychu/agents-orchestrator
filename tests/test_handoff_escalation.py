# ruff: noqa: F811  (the `env` fixture imported from test_handoff_pass is a parameter name)
"""L3 escalation of an agent that does not hand off (decision 717, spec 5.9): once per cycle to
every live superleader session (a session message) and to the user (a taskboard comment on the
session's claimed task, or a notice task), never a clear, a move or an interrupt.

The handoff pass runs against Postgres and the fake wrapper (`test_handoff_pass.Env`); the user
side (`handoff.notify_escalations`) against `fakes/fake_taskboard.py`.
"""

from __future__ import annotations

import httpx
import pytest
from factories import insert_agent, insert_event, insert_session
from fakes.fake_taskboard import FakeTaskboard
from test_handoff_pass import (  # noqa: F401 (fixtures)
    CRITICAL,
    KEY,
    LOW,
    STRONG,
    TASK,
    Env,
    env,
    fake,
)

from agents_orchestrator import handoff_texts
from agents_orchestrator.db import handoffs
from agents_orchestrator.dispatcher import handoff
from agents_orchestrator.settings import Settings

TB_TOKEN = "tb-test-token"


@pytest.fixture
def tb() -> FakeTaskboard:
    return FakeTaskboard(token=TB_TOKEN)


@pytest.fixture
def tenv(env: Env, dsn):
    env.settings = Settings(
        database_url=dsn,
        master_key=KEY,
        taskboard_url="http://taskboard",
        taskboard_token=TB_TOKEN,
    )
    return env


async def superleader(env: Env, slug: str = "boss") -> dict:
    s = await insert_session(env.pool, spec={"session_messaging": True}, slug=slug)
    await insert_agent(env.pool, s["id"], ordinal=0, ready=True)
    return s


async def notify(env: Env, tb: FakeTaskboard) -> list[str]:
    async with env.pool.connection() as conn:
        return await handoff.notify_escalations(
            conn, env.settings, transport=httpx.MockTransport(tb.handler)
        )


async def escalations(env: Env, session_id) -> list[dict]:
    return await env.rows(
        "SELECT * FROM messages WHERE session_id = %s AND notice = 'handoff_escalation'"
        " ORDER BY id",
        (session_id,),
    )


async def nothing_forced(env: Env, session_id) -> None:
    """No clear, compact or interrupt command and no leadership request for the session."""
    assert (
        await env.rows(
            "SELECT k.id FROM commands k JOIN agents a ON a.id = k.agent_id"
            " WHERE a.session_id = %s",
            (session_id,),
        )
        == []
    )
    assert await env.requests(session_id) == []


async def test_l3_pressuring_escalates_once_per_cycle(tenv, tb):
    boss = await superleader(tenv)
    c, a = await tenv.solo(used=CRITICAL)
    await tenv.busy(a["id"])
    tb.assignees["T-1"] = str(c["id"])  # the session's claimed task
    for _ in range(2):
        await tenv.step(c["id"])
        await notify(tenv, tb)
    (msg,) = await escalations(tenv, boss["id"])
    assert (msg["lane"], msg["status"], msg["target_agent_id"]) == ("regular", "queued", None)
    assert msg["sender_session_id"] is None  # an orchestrator notice, not a leader's message
    assert c["slug"] in msg["text"] and a["pod_name"] in msg["text"]
    assert "context level critical" in msg["text"] and "85%" in msg["text"]
    assert "nothing was cleared or moved" in msg["text"]
    (comment,) = tb.comments
    assert comment["task_key"] == "T-1" and comment["session"] == str(c["id"])
    assert comment["body"] == msg["text"]
    assert tb.created == []
    row = await tenv.row(a["id"])
    assert row.phase == "pressuring" and row.escalated_at is not None
    assert row.escalation_notified_at is not None
    assert row.escalation_message_ids == [msg["id"]]
    await nothing_forced(tenv, c["id"])


async def test_three_rejects_at_l3_escalate(tenv, tb):
    boss = await superleader(tenv)
    c, a = await tenv.solo(used=STRONG)
    await tenv.busy(a["id"])  # never idle: an accepted submit waits for its clear
    assert await tenv.step(c["id"]) == [f"pressuring:{a['id']}"]
    assert await tenv.submit(a)
    await tenv.gauge(a["id"], CRITICAL, state="busy", current_turn_id="t-busy")
    await tenv.step(c["id"])
    # an accepted, gate-clear submit at L3 is cooperation: nothing to escalate
    assert (await tenv.row(a["id"])).escalated_at is None
    assert await escalations(tenv, boss["id"]) == []
    async with tenv.pool.connection() as conn:
        row = await handoffs.open_row(conn, a["id"])
        for _ in range(tenv.settings.handoff_max_rejects):
            await handoffs.record_reject(conn, row.id, ["too_short", "missing:next"])
    await tenv.step(c["id"])
    await tenv.step(c["id"])
    (msg,) = await escalations(tenv, boss["id"])
    assert "missing: too_short, missing:next." in msg["text"]
    row = await tenv.row(a["id"])
    assert row.phase == "submitted" and row.escalated_at is not None
    await nothing_forced(tenv, c["id"])


async def test_blocked_gate_at_l3_escalates_with_inflight_list(tenv, tb):
    boss = await superleader(tenv)
    c, a = await tenv.solo(used=STRONG)
    assert await tenv.step(c["id"]) == [f"pressuring:{a['id']}"]
    tenv.inflight(a, background_tasks=[TASK])
    assert not await tenv.submit(a)  # stored, blocked on the CI watch
    await tenv.step(c["id"])
    assert await escalations(tenv, boss["id"]) == []  # L2: no escalation yet
    await tenv.gauge(a["id"], CRITICAL)
    await tenv.step(c["id"])
    await tenv.step(c["id"])
    (msg,) = await escalations(tenv, boss["id"])
    assert "It is waiting on: task:bf3a (CI watch)." in msg["text"]
    assert await notify(tenv, tb) == [f"notified:{a['id']}"]
    (task,) = tb.created  # no claimed task: a notice task
    assert "task:bf3a (CI watch)" in task["body"]
    await nothing_forced(tenv, c["id"])


async def test_escalation_never_clears_or_moves(tenv, tb):
    boss = await superleader(tenv)
    c, agents = await tenv.hive(3, used=CRITICAL)
    leader = agents[0]
    await tenv.busy(leader["id"])
    for _ in range(4):
        await tenv.lpass(c["id"])
        await tenv.step(c["id"])
        await notify(tenv, tb)
    assert len(await escalations(tenv, boss["id"])) == 1
    await nothing_forced(tenv, c["id"])
    row = await tenv.row(leader["id"])
    assert row.phase == "pressuring" and row.escalated_at is not None
    got = await tenv.rows("SELECT agent_id FROM session_leaders WHERE session_id = %s", (c["id"],))
    assert got[0]["agent_id"] == leader["id"]  # still leads
    # nothing reached the agent itself because of the escalation
    assert await tenv.messages(leader["id"]) == []
    # the text names facts only: no request to clear and no time words
    text = (await escalations(tenv, boss["id"]))[0]["text"]
    assert "/clear" not in text and "minute" not in text.lower()


async def test_new_cycle_after_compaction_can_escalate_again(tenv, tb):
    boss = await superleader(tenv)
    c, a = await tenv.solo(used=CRITICAL)
    await tenv.busy(a["id"])
    tb.assignees["T-1"] = str(c["id"])
    await tenv.step(c["id"])
    first = await tenv.row(a["id"])
    assert first.escalated_at is not None
    # the cycle ends in a compaction before the superleader's leader read it: its escalation
    # is cancelled with the row; the user notice of a closed cycle is not posted
    await insert_event(tenv.pool, c["id"], a["id"], type="compacted", data={})
    await tenv.gauge(a["id"], LOW, state="busy", current_turn_id="t-busy")
    assert await tenv.step(c["id"]) == [f"done:{a['id']}:compacted"]
    assert await notify(tenv, tb) == []
    (old,) = await escalations(tenv, boss["id"])
    assert old["status"] == "cancelled"
    # the next cycle reaches L3 again: escalated again, once
    await tenv.gauge(a["id"], CRITICAL, state="busy", current_turn_id="t-busy")
    await tenv.step(c["id"])
    await tenv.step(c["id"])
    second = await tenv.row(a["id"])
    assert second.id != first.id and second.escalated_at is not None
    msgs = await escalations(tenv, boss["id"])
    assert [m["status"] for m in msgs] == ["cancelled", "queued"]
    assert await notify(tenv, tb) == [f"notified:{a['id']}"]
    assert len(tb.comments) == 1


async def test_escalation_without_superleader_still_notifies_user(tenv, tb):
    c, a = await tenv.solo(used=CRITICAL)
    await tenv.busy(a["id"])
    await tenv.step(c["id"])
    row = await tenv.row(a["id"])
    assert row.escalated_at is not None and row.escalation_message_ids == []
    assert await tenv.rows("SELECT id FROM messages WHERE notice = 'handoff_escalation'") == []
    tb.down = True  # an outage: not marked, retried later
    assert await notify(tenv, tb) == []
    assert (await tenv.row(a["id"])).escalation_notified_at is None
    tb.down = False
    assert await notify(tenv, tb) == [f"notified:{a['id']}"]
    (task,) = tb.created
    assert task["labels"] == [handoff.ESCALATION_LABEL]
    assert task["client_key"] == f"handoff-escalation-{row.id}"
    assert task["title"] == handoff_texts.escalation_title(c["slug"], a["pod_name"])
    assert task["session"] == str(c["id"])
    # a restarted dispatcher posts nothing again: the mark is in the row
    assert await notify(tenv, tb) == []
    assert len(tb.created) == 1


async def test_escalation_reaches_every_other_live_superleader(tenv, tb):
    boss = await superleader(tenv, "boss")
    other = await superleader(tenv, "boss2")
    gone = await insert_session(tenv.pool, spec={"session_messaging": True}, slug="gone")
    await insert_agent(tenv.pool, gone["id"], ordinal=0, gone_at=None, ready=False)
    await tenv.run("UPDATE agents SET gone_at = now() WHERE session_id = %s", (gone["id"],))
    c, a = await tenv.solo(used=CRITICAL)
    await tenv.busy(a["id"])
    await tenv.step(c["id"])
    assert len(await escalations(tenv, boss["id"])) == 1
    assert len(await escalations(tenv, other["id"])) == 1
    assert await escalations(tenv, gone["id"]) == []  # no live agent: not a live superleader
    row = await tenv.row(a["id"])
    assert len(row.escalation_message_ids) == 2


async def test_l2_and_shutdown_rows_never_escalate(tenv, tb):
    boss = await superleader(tenv)
    c, a = await tenv.solo(used=STRONG)
    await tenv.busy(a["id"])
    for _ in range(3):
        await tenv.step(c["id"])
    assert (await tenv.row(a["id"])).escalated_at is None
    assert await escalations(tenv, boss["id"]) == []


async def test_l2_pressuring_row_rising_to_l3_escalates(tenv, tb):
    boss = await superleader(tenv)
    c, a = await tenv.solo(used=STRONG)
    await tenv.busy(a["id"])
    await tenv.step(c["id"])
    row = await tenv.row(a["id"])
    assert (row.level, row.escalated_at) == (2, None)
    assert await escalations(tenv, boss["id"]) == []  # L2 reports nothing
    await tenv.gauge(a["id"], CRITICAL, state="busy", current_turn_id="t-busy")
    assert await tenv.step(c["id"]) == [f"level:{a['id']}:3"]
    row = await tenv.row(a["id"])
    assert (row.level, row.phase) == (3, "pressuring")
    assert row.escalated_at is not None
    (msg,) = await escalations(tenv, boss["id"])
    assert "context level critical" in msg["text"]
    await nothing_forced(tenv, c["id"])

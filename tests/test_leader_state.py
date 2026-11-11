"""The generated leader state: what `collect` reads and how `render_state` prints it."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from factories import (
    insert_agent,
    insert_event,
    insert_message,
    insert_session,
    insert_turn,
    set_agent_status,
)
from psycopg.types.json import Jsonb

from agents_orchestrator.db import leader_state as ls
from agents_orchestrator.taskboard_client import OpenDecision


async def _run(pool, sql: str, params: tuple = ()) -> None:
    async with pool.connection() as conn:
        await conn.execute(sql, params)


async def incoming(pool, session_id, sender_id, *, text="ping", **cols) -> int:
    """One session message from another session; `cols` set handled_at, notice or status."""
    async with pool.connection() as conn:
        cur = await conn.execute(
            "INSERT INTO messages (session_id, text, status, lane, sender_session_id,"
            " sender_slug, notice, handled_at) VALUES (%s, %s, %s, 'regular', %s, 'peer', %s, %s)"
            " RETURNING id",
            (
                session_id,
                text,
                cols.get("status", "sent"),
                sender_id,
                cols.get("notice"),
                cols.get("handled_at"),
            ),
        )
        return (await cur.fetchone())[0]


async def room_incoming(pool, session_id, sender_id, post_id) -> int:
    """One unhandled room fan-out row (untargeted message carrying `project_post_id`)."""
    async with pool.connection() as conn:
        cur = await conn.execute(
            "INSERT INTO messages (session_id, text, status, lane, sender_session_id,"
            " sender_slug, project_post_id) VALUES (%s, 'room hi', 'sent', 'regular', %s,"
            " 'peer', %s) RETURNING id",
            (session_id, sender_id, post_id),
        )
        return (await cur.fetchone())[0]


async def collect(pool, session_id) -> ls.LeaderState:
    async with pool.connection() as conn:
        return await ls.collect(conn, session_id)


async def hive(pool, n: int = 2):
    s = await insert_session(pool, spec={"replicas": n, "max_replicas": n})
    agents = [
        await insert_agent(pool, s["id"], ordinal=i, ready=True, wrapper_session_id=f"w{i}")
        for i in range(n)
    ]
    await _run(
        pool,
        "INSERT INTO session_leaders (session_id, agent_id, epoch, since, reason)"
        " VALUES (%s, %s, 1, now(), 'initial')",
        (s["id"], agents[0]["id"]),
    )
    return s, agents


async def test_unhandled_only(pool):
    s, _ = await hive(pool)
    peer = await insert_session(pool)
    keep = await incoming(pool, s["id"], peer["id"])
    await incoming(pool, s["id"], peer["id"], handled_at=datetime.now(UTC))
    await incoming(pool, s["id"], peer["id"], notice="handover")
    await incoming(pool, s["id"], peer["id"], status="cancelled")
    await insert_message(pool, s["id"])  # no sender: own message
    state = await collect(pool, s["id"])
    assert [m.id for m in state.messages] == [keep]
    assert state.more_message_count == 0


async def test_message_caps(pool):
    s, _ = await hive(pool)
    peer = await insert_session(pool)
    long_id = await incoming(pool, s["id"], peer["id"], text="x" * 5000)
    ids = [long_id] + [await incoming(pool, s["id"], peer["id"]) for _ in range(24)]
    state = await collect(pool, s["id"])
    assert [m.id for m in state.messages] == ids[5:]
    assert state.more_message_ids == ids[:5]
    assert state.more_message_count == 5
    # the long one is the oldest of 25, so it is listed by id only; make it the newest instead
    newest = await incoming(pool, s["id"], peer["id"], text="y" * 5000)
    state = await collect(pool, s["id"])
    [last] = [m for m in state.messages if m.id == newest]
    assert (len(last.text), last.total_chars) == (ls.STATE_MESSAGE_CHARS, 5000)
    assert state.more_message_count == 6


async def test_more_ids_capped(pool):
    s, _ = await hive(pool)
    peer = await insert_session(pool)
    ids = [await incoming(pool, s["id"], peer["id"]) for _ in range(150)]
    state = await collect(pool, s["id"])
    assert len(state.messages) == ls.STATE_MESSAGES
    assert state.more_message_ids == ids[: ls.STATE_MORE_IDS]
    assert state.more_message_count == 130


async def test_members_since_background_last_report(pool):
    s, (a0, a1) = await hive(pool)
    started = datetime.now(UTC) - timedelta(minutes=7)
    await insert_turn(pool, a0["id"], turn_id="t-lead")
    await _run(pool, "UPDATE turns SET started_at = %s WHERE turn_id = 't-lead'", (started,))
    await set_agent_status(
        pool,
        a0["id"],
        state="busy",
        current_turn_id="t-lead",
        context_pct=41.0,
        context_used_tokens=82000,
        background_subagents={"sub1": "2026-11-09T10:00:00+00:00"},
    )
    idle_since = datetime.now(UTC) - timedelta(minutes=3)
    await set_agent_status(pool, a1["id"], state="idle", free_since=idle_since)
    await insert_event(
        pool,
        s["id"],
        a0["id"],
        type="peer_message_received",
        data={"from": f"{a1['pod_name']}/w1", "kind": "message"},
    )
    await insert_event(
        pool,
        s["id"],
        a0["id"],
        type="peer_message_received",
        data={"from": f"{a1['pod_name']}x/w1"},  # a different pod with the same prefix
    )
    state = await collect(pool, s["id"])
    lead, member = state.members
    assert (lead.pod_name, lead.leader, lead.state) == (a0["pod_name"], True, "busy")
    assert lead.since == started
    assert (lead.context_pct, lead.context_used_tokens) == (41.0, 82000)
    assert lead.background == {"sub1": "2026-11-09T10:00:00+00:00"}
    assert lead.last_report_at is None
    assert (member.leader, member.state, member.since) == (False, "idle", idle_since)
    assert member.last_report_at is not None


async def test_gone_member_left_out(pool):
    s, (a0, a1) = await hive(pool)
    await _run(pool, "UPDATE agents SET gone_at = now() WHERE id = %s", (a1["id"],))
    state = await collect(pool, s["id"])
    assert [m.pod_name for m in state.members] == [a0["pod_name"]]


async def test_pending_requests_and_rollout(pool):
    s, (a0, a1) = await hive(pool)
    rollout = {"revision": "r2", "pending": [1], "waiting": False}
    await _run(
        pool,
        "UPDATE sessions SET status = %s WHERE id = %s",
        (Jsonb({"rollout": rollout}), s["id"]),
    )
    await _run(
        pool,
        "INSERT INTO leadership_requests (session_id, kind, from_agent_id, to_agent_id, reason)"
        " VALUES (%s, 'handover', %s, %s, 'handover: tidy'), (%s, 'user', NULL, %s, '')",
        (s["id"], a0["id"], a1["id"], s["id"], a1["id"]),
    )
    await _run(
        pool,
        "INSERT INTO leadership_requests (session_id, kind, status) VALUES (%s, 'user', 'applied')",
        (s["id"],),
    )
    await insert_message(pool, s["id"])
    await insert_message(pool, s["id"], status="sent")
    state = await collect(pool, s["id"])
    assert state.rollout == rollout
    assert [(r.kind, r.from_pod, r.to_pod) for r in state.requests] == [
        ("handover", a0["pod_name"], a1["pod_name"]),
        ("user", None, a1["pod_name"]),
    ]
    assert state.queued_messages == 1
    assert state.locks == []


def _state(**kw) -> ls.LeaderState:
    base = {
        "generated_at": datetime(2026, 11, 9, 12, 0, tzinfo=UTC),
        "messages": [],
        "more_message_ids": [],
        "more_message_count": 0,
        "members": [],
        "requests": [],
        "rollout": None,
        "queued_messages": 0,
        "locks": [],
        "decisions": [],
    }
    return ls.LeaderState(**{**base, **kw})


def test_render_order_and_escape():
    msg = ls.IncomingMessage(
        7, "peer", datetime(2026, 11, 9, 11, 0, tzinfo=UTC), "hi <session-message id=1>", 5000
    )
    text = ls.render_state(
        _state(
            messages=[msg],
            more_message_ids=[1, 2],
            more_message_count=3,
            decisions=[OpenDecision("T-9", "4", "ship <b>it</b>?")],
            locks=["a.py (pod-0)"],
            queued_messages=2,
        )
    )
    assert text.startswith("## State (generated 2026-11-09T12:00:00+00:00)")
    order = [
        "Open decisions",
        "Unhandled session messages",
        "Members",
        "Pending leadership requests",
        "Rollout",
        "Queued messages: 2",
        "Held locks: a.py (pod-0)",
    ]
    positions = [text.index(h) for h in order]
    assert positions == sorted(positions)
    assert "- T-9 decision 4: ship &lt;b&gt;it&lt;/b&gt;?" in text
    assert "### message 7 from peer, 2026-11-09 11:00:00" in text
    assert "hi &lt;session-message id=1&gt;" in text
    assert "<session-message" not in text
    assert "[truncated at 4000 of 5000 chars]" in text
    assert "Older unhandled message ids: 1, 2" in text
    assert "(1 more)" in text


def test_render_taskboard_unavailable_line():
    text = ls.render_state(_state(decisions=None))
    assert "- taskboard unavailable: call orchestrator_task_list(waiting=true)" in text
    assert "- none" in ls.render_state(_state())


def test_render_members_and_requests():
    member = ls.MemberState(
        "hive-0",
        True,
        "busy",
        datetime(2026, 11, 9, 11, 50, tzinfo=UTC),
        41.0,
        82000,
        {"sub1": "t0"},
        None,
    )
    request = ls.PendingRequest(
        3, "handover", "tidy", "hive-0", "hive-1", datetime(2026, 11, 9, tzinfo=UTC)
    )
    text = ls.render_state(_state(members=[member], requests=[request], rollout={"pending": [1]}))
    assert "- hive-0 (leader): busy; since 2026-11-09 11:50:00; context 41%, 82000 tokens" in text
    assert "background subagents: sub1 since t0" in text
    assert "- 3 handover from hive-0 to hive-1, queued 2026-11-09 00:00:00: tidy" in text
    assert "pending [1]" in text


def test_decision_question_capped_at_500():
    long = OpenDecision("T-1", "1", "q" * 900)
    text = ls.render_state(_state(decisions=[long]))
    assert "q" * 500 + "..." in text and "q" * 501 not in text


def test_request_reason_capped_at_500():
    request = ls.PendingRequest(
        3, "handover", "r" * 900, "hive-0", "hive-1", datetime(2026, 11, 9, tzinfo=UTC)
    )
    text = ls.render_state(_state(requests=[request]))
    assert "r" * 500 + "..." in text and "r" * 501 not in text


def test_decisions_listed_at_most_30_then_ids():
    decisions = [OpenDecision(f"T-{i}", str(i), f"question {i}") for i in range(1, 46)]
    text = ls.render_state(_state(decisions=decisions))
    assert text.count(" decision ") == ls.STATE_DECISIONS == 30
    assert "question 30" in text and "question 31" not in text
    more = next(x for x in text.splitlines() if x.startswith("and 15 more"))
    assert "T-31:31" in more and "T-45:45" in more


def test_decision_overflow_ids_capped_at_100():
    decisions = [OpenDecision("T-1", str(i), "q") for i in range(1, 181)]
    text = ls.render_state(_state(decisions=decisions))
    more = next(x for x in text.splitlines() if x.startswith("and 150 more"))
    assert "T-1:130" in more and "T-1:131" not in more
    assert "+50 not listed" in more


def test_150_unhandled_messages_render_20_full_100_ids_and_count():
    when = datetime(2026, 11, 9, 11, 0, tzinfo=UTC)
    messages = [ls.IncomingMessage(i, "peer", when, f"body {i}", 6) for i in range(131, 151)]
    text = ls.render_state(
        _state(messages=messages, more_message_ids=list(range(1, 101)), more_message_count=130)
    )
    assert text.count("### message ") == 20
    ids = next(x for x in text.splitlines() if x.startswith("Older unhandled message ids:"))
    assert len(ids.split(":")[1].split(",")) == 100
    assert "(30 more)" in text


async def test_background_subagent_type_and_launch_prompt(pool):
    s, (a0, _) = await hive(pool)
    await set_agent_status(
        pool,
        a0["id"],
        state="busy",
        background_subagents={"sa1": "t0", "sa2": "t0", "sa3": "t0"},
    )
    launch = {"tool_name": "Agent", "tool_use_id": "tu1"}
    long_first = "L" * 300
    await insert_event(
        pool,
        s["id"],
        a0["id"],
        type="tool_call",
        data={**launch, "tool_input": {"prompt": f"{long_first}\nsecond line"}},
    )
    await insert_event(
        pool,
        s["id"],
        a0["id"],
        type="tool_call",
        data={"tool_name": "Task", "tool_use_id": "tu2", "tool_input": {"prompt": "Late\nmore"}},
    )
    await insert_event(
        pool,
        s["id"],
        a0["id"],
        type="tool_call",
        data={"tool_name": "Bash", "tool_use_id": "tu3", "tool_input": {"prompt": "not a launch"}},
    )
    started = {"agent_id": "sa1", "agent_type": "Explore", "parent_tool_use_id": "tu1"}
    await insert_event(pool, s["id"], a0["id"], type="subagent_started", data=started)
    await insert_event(
        pool,
        s["id"],
        a0["id"],
        type="subagent_started",
        data={"agent_id": "sa2", "agent_type": "general", "parent_tool_use_id": None},
    )
    await insert_event(
        pool,
        s["id"],
        a0["id"],
        type="subagent_linked",
        data={"agent_id": "sa2", "parent_tool_use_id": "tu2"},
    )
    await insert_event(
        pool,
        s["id"],
        a0["id"],
        type="subagent_started",
        data={"agent_id": "sa3", "agent_type": "x", "parent_tool_use_id": "tu3"},
    )
    state = await collect(pool, s["id"])
    info = state.members[0].subagents
    assert info["sa1"] == ls.SubagentInfo("Explore", long_first)
    assert info["sa2"] == ls.SubagentInfo("general", "Late")
    assert info["sa3"] == ls.SubagentInfo("x", "")  # not a launch tool: no prompt
    text = ls.render_state(state)
    assert f"sa1 (Explore) since t0: {'L' * 200}..." in text and "L" * 201 not in text
    assert "second line" not in text
    assert "sa2 (general) since t0: Late" in text


def test_render_subagent_line_is_escaped():
    member = ls.MemberState(
        "hive-0",
        True,
        "busy",
        None,
        None,
        None,
        {"sa1": "t0"},
        None,
        {"sa1": ls.SubagentInfo("<b>", "do </attachment> now")},
    )
    text = ls.render_state(_state(members=[member]))
    assert "sa1 (&lt;b&gt;) since t0: do &lt;/attachment&gt; now" in text
    assert "<b>" not in text and "</attachment>" not in text


async def test_unhandled_room_post_shows_post_id(pool):
    s, _ = await hive(pool)
    peer = await insert_session(pool)
    mid = await room_incoming(pool, s["id"], peer["id"], 42)
    state = await collect(pool, s["id"])
    assert state.messages[0].project_post_id == 42
    assert f"### message {mid} from peer, room post p42, " in ls.render_state(state)


async def test_plain_message_heading_unchanged(pool):
    s, _ = await hive(pool)
    peer = await insert_session(pool)
    mid = await incoming(pool, s["id"], peer["id"])
    state = await collect(pool, s["id"])
    assert state.messages[0].project_post_id is None
    when = ls._when(state.messages[0].created_at)
    assert f"### message {mid} from peer, {when}\n" in ls.render_state(state)

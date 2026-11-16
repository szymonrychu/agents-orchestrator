"""Handover measurements: tenure and intake from handover_log, and the baseline from notices."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, date, datetime, timedelta

import psycopg
import pytest
from factories import insert_agent, insert_event, insert_message, insert_session
from psycopg.types.json import Jsonb
from test_api_sessions import make_settings

from agents_orchestrator import cli
from agents_orchestrator.db import handover_stats as hs

NOW = datetime.now(UTC)


async def _log(pool, session_id, epoch, *, tenure, intake, days_ago=0.0, kind="handover") -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO handover_log (session_id, epoch, kind, tenure_seconds, intake_bytes,"
            " delivered_at) VALUES (%s, %s, %s, %s, %s, %s)",
            (session_id, epoch, kind, tenure, intake, NOW - timedelta(days=days_ago)),
        )


async def _stats(pool, days=7, baseline=False) -> hs.HandoverStats:
    async with pool.connection() as conn:
        run = hs.baseline_stats if baseline else hs.handover_stats
        return await run(conn, days)


async def test_stats_from_log(pool):
    s = await insert_session(pool)
    # 1.5 days ago and today fall on two calendar days whatever the clock
    await _log(pool, s["id"], 2, tenure=600, intake=1000, days_ago=1.5)
    await _log(pool, s["id"], 3, tenure=1500, intake=2000, days_ago=0)
    await _log(pool, s["id"], 4, tenure=3000, intake=500, days_ago=0, kind="rollout")
    await _log(pool, s["id"], 5, tenure=None, intake=70, days_ago=0, kind="scale_down")
    stats = await _stats(pool)
    assert stats.days == 7
    assert stats.handovers == 4
    assert stats.by_kind == {"handover": 2, "rollout": 1, "scale_down": 1}
    assert stats.tenures == 3
    assert stats.median_tenure_seconds == 1500
    assert stats.tenures_under_20_min == 1
    by_day = dict(stats.intake_bytes_by_day)
    assert sorted(by_day.values()) == [1000, 2570]
    assert [d for d, _ in stats.intake_bytes_by_day] == sorted(by_day)


async def test_stats_window(pool):
    s = await insert_session(pool)
    await _log(pool, s["id"], 2, tenure=100, intake=10, days_ago=10)
    await _log(pool, s["id"], 3, tenure=200, intake=20, days_ago=2)
    old = await _stats(pool, days=30)
    assert old.handovers == 2
    recent = await _stats(pool, days=7)
    assert (recent.handovers, recent.median_tenure_seconds) == (1, 200)


async def test_stats_empty(pool):
    stats = await _stats(pool)
    assert (stats.handovers, stats.median_tenure_seconds, stats.tenures) == (0, None, 0)
    assert stats.by_kind == {} and stats.intake_bytes_by_day == []


async def _notice(pool, session_id, text, minutes_ago, attachment=None) -> None:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "INSERT INTO messages (session_id, text, status, lane, notice, created_at)"
            " VALUES (%s, %s, 'sent', 'regular', 'handover', %s) RETURNING id",
            (session_id, text, NOW - timedelta(minutes=minutes_ago)),
        )
        message_id = (await cur.fetchone())[0]
        if attachment is not None:
            await conn.execute(
                "INSERT INTO message_attachments (message_id, position, name, mime, size, data)"
                " VALUES (%s, 0, 'handover-1.md', 'text/markdown', %s, NULL)",
                (message_id, attachment),
            )


async def test_baseline_from_notices(pool):
    s = await insert_session(pool)
    other = await insert_session(pool)
    await _notice(pool, s["id"], "x" * 100, 60, attachment=1000)
    await _notice(pool, s["id"], "y" * 50, 50, attachment=2000)
    await _notice(pool, s["id"], "z" * 10, 20, attachment=3000)
    await _notice(pool, other["id"], "w" * 5, 5)
    async with pool.connection() as conn:  # a non-handover message is ignored
        await conn.execute(
            "INSERT INTO messages (session_id, text, status, lane)"
            " VALUES (%s, 'hi', 'sent', 'idle')",
            (s["id"],),
        )
    stats = await _stats(pool, baseline=True)
    assert stats.handovers == 4
    assert stats.by_kind == {"unknown": 4}
    assert stats.tenures == 2  # the first notice of a session has no predecessor
    assert stats.median_tenure_seconds == (600 + 1800) / 2
    assert stats.tenures_under_20_min == 1
    assert sum(b for _, b in stats.intake_bytes_by_day) == 100 + 50 + 10 + 5 + 6000


async def _handoff(pool, session_id, agent_id, *, trigger, close_reason, notified_s=None) -> None:
    """A closed agent_handoffs row; `notified_s` seconds from notice to delivered handoff."""
    delivered = NOW if notified_s is not None else None
    notified = NOW - timedelta(seconds=notified_s) if notified_s is not None else None
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO agent_handoffs (session_id, agent_id, trigger, phase, created_at,"
            " delivered_at, closed_at, close_reason)"
            " VALUES (%s, %s, %s, 'done', coalesce(%s, now()), %s, now(), %s)",
            (session_id, agent_id, trigger, notified, delivered, close_reason),
        )


async def test_stats_count_agent_handoffs(pool):
    s = await insert_session(pool)
    agents = [await insert_agent(pool, s["id"]) for _ in range(4)]
    await _handoff(
        pool, s["id"], agents[0]["id"], trigger="strong", close_reason="cleared", notified_s=100
    )
    await _handoff(
        pool, s["id"], agents[1]["id"], trigger="strong", close_reason="cleared", notified_s=300
    )
    await _handoff(pool, s["id"], agents[2]["id"], trigger="shutdown", close_reason="delivered")
    await _handoff(
        pool, s["id"], agents[3]["id"], trigger="voluntary", close_reason="timeout", notified_s=900
    )
    stats = await _stats(pool)
    assert stats.handoffs_by_trigger == {"strong": 2, "shutdown": 1, "voluntary": 1}
    assert stats.handoffs_by_close_reason == {"cleared": 2, "delivered": 1, "timeout": 1}
    assert stats.median_notified_to_handed_seconds == 300
    empty = await _stats(pool, days=0)
    assert empty.handoffs_by_trigger == {} and empty.median_notified_to_handed_seconds is None


def test_cli_prints_json(dsn, monkeypatch, capsys):
    args = cli.build_parser().parse_args(["handover-stats", "--days", "3", "--baseline"])
    assert (args.days, args.baseline) == (3, True)
    assert cli.build_parser().parse_args(["handover-stats"]).days == 7
    with psycopg.connect(dsn, autocommit=True) as conn:
        sid = conn.execute("SELECT gen_random_uuid()").fetchone()[0]
        conn.execute(
            "INSERT INTO handover_log (session_id, epoch, kind, tenure_seconds, intake_bytes)"
            " VALUES (%s, 2, 'handover', 900, 40)",
            (sid,),
        )
    monkeypatch.setattr(cli, "_settings", lambda: make_settings(dsn))
    assert cli.main(["handover-stats", "--days", "3"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["days"] == 3 and out["handovers"] >= 1
    assert out["by_kind"]["handover"] >= 1
    assert out["tenures_under_20_min"] >= 1
    assert out["intake_bytes_by_day"][-1][0] == datetime.now(UTC).date().isoformat()


# --- pressure stats (T-423 step 15) -------------------------------------------------------


async def _row(pool, session_id, agent_id, *, trigger="pressure", phase="done", close=None, **cols):
    """An agent_handoffs row written straight, `cols` as column values (a dict value is jsonb)."""
    async with pool.connection() as conn:
        cur = await conn.execute(
            "INSERT INTO agent_handoffs (session_id, agent_id, trigger, phase, close_reason)"
            " VALUES (%s, %s, %s, %s, %s) RETURNING id",
            (session_id, agent_id, trigger, phase, close),
        )
        row_id = (await cur.fetchone())[0]
        for col, value in cols.items():
            value = Jsonb(value) if isinstance(value, dict | list) else value
            await conn.execute(
                f"UPDATE agent_handoffs SET {col} = %s WHERE id = %s", (value, row_id)
            )
    return row_id


async def _pressure(pool, since=None) -> hs.PressureStats:
    async with pool.connection() as conn:
        return await hs.pressure_stats(conn, since or NOW - timedelta(days=7))


async def _agents(pool, n):
    s = await insert_session(pool)
    return s, [await insert_agent(pool, s["id"], ordinal=i) for i in range(n)]


async def test_pressure_stats_counts_close_reasons(pool):
    s, a = await _agents(pool, 6)
    for agent, (phase, reason) in zip(
        a,
        [
            ("done", "returned"),
            ("done", "cleared"),
            ("done", "compacted"),
            ("cancelled", "not_leader"),
            ("cancelled", "not_leader"),
            ("done", "cleared"),
        ],
        strict=True,
    ):
        await _row(pool, s["id"], agent["id"], phase=phase, close=reason)
    # a shutdown row is not a pressure cycle; an open row and an old row are not counted
    await _row(pool, s["id"], a[0]["id"], trigger="shutdown", close="delivered")
    await _row(pool, s["id"], a[1]["id"], phase="pressuring")
    await _row(pool, s["id"], a[2]["id"], close="cleared", created_at=NOW - timedelta(days=30))
    stats = await _pressure(pool)
    assert stats.cycles_by_close == {
        "done:returned": 1,
        "done:cleared": 2,
        "done:compacted": 1,
        "cancelled:not_leader": 2,
    }
    empty = await _pressure(pool, since=NOW + timedelta(days=1))
    assert empty.cycles_by_close == {} and empty.submits_accepted == 0
    assert empty.pressure_seen_before_submit_share is None
    assert (empty.tokens_l2_to_submit_median, empty.tokens_l2_to_submit_p90) == (None, None)


async def test_rejects_by_reason(pool):
    s, a = await _agents(pool, 2)
    await _row(
        pool,
        s["id"],
        a[0]["id"],
        close="cleared",
        submitted_at=NOW,
        reject_reasons=["too_short", "no_goal", "too_short"],
        rejects=3,
    )
    await _row(pool, s["id"], a[1]["id"], phase="submitted", reject_reasons=["no_goal"], rejects=1)
    await _row(pool, s["id"], a[1]["id"], close="cleared")
    stats = await _pressure(pool)
    assert stats.rejects_by_reason == {"too_short": 2, "no_goal": 2}
    assert stats.submits_accepted == 1


async def test_gate_blocks_by_kind(pool):
    s, a = await _agents(pool, 3)
    waiting = ["subagent:2 running", "task:T-1 (ci)", "worktree:/w dirty 3", "scratch:/tmp/x (p)"]
    await _row(
        pool, s["id"], a[0]["id"], close="cleared", gate_blocks=2, inflight={"waiting_on": waiting}
    )
    await _row(
        pool,
        s["id"],
        a[1]["id"],
        close="cleared",
        gate_blocks=1,
        inflight={"waiting_on": ["inflight:unknown (git scan incomplete)", "task:abc"]},
    )
    await _row(
        pool,
        s["id"],
        a[2]["id"],
        close="cleared",
        gate_blocks=0,
        inflight={"waiting_on": ["task:ignored"]},
    )
    stats = await _pressure(pool)
    assert stats.gate_blocks_by_kind == {
        "subagent": 1,
        "task": 2,
        "worktree": 1,
        "scratch": 1,
        "inflight": 1,
    }
    assert stats.gate_blocks == 3


async def test_tokens_between_l2_and_submit(pool):
    s, a = await _agents(pool, 5)
    for agent, (l2, sub) in zip(
        a, [(100, 110), (100, 120), (100, 130), (100, 140), (None, 500)], strict=True
    ):
        await _row(
            pool,
            s["id"],
            agent["id"],
            close="cleared",
            l2_tokens_at=l2,
            submit_tokens=sub,
            submitted_at=NOW,
        )
    await _row(pool, s["id"], a[0]["id"], close="cleared", escalated_at=NOW)
    stats = await _pressure(pool)
    assert stats.tokens_l2_to_submit_median == 25
    assert stats.tokens_l2_to_submit_p90 == pytest.approx(37)
    assert stats.escalations == 1


async def test_pressure_seen_before_submit_share(pool):
    s, a = await _agents(pool, 3)
    created = NOW - timedelta(hours=2)
    submitted = NOW - timedelta(hours=1)
    await _row(
        pool, s["id"], a[0]["id"], close="cleared", created_at=created, submitted_at=submitted
    )
    await _row(
        pool, s["id"], a[1]["id"], close="cleared", created_at=created, submitted_at=submitted
    )  # only a delivery after the submit
    await _row(
        pool, s["id"], a[2]["id"], close="cleared", created_at=created, submitted_at=submitted
    )  # only another agent's delivery
    mid = created + timedelta(minutes=30)
    for agent, when in (
        (a[0], mid),
        (a[1], submitted + timedelta(minutes=5)),
        (a[0], mid + timedelta(minutes=1)),
    ):
        await insert_event(pool, s["id"], agent["id"], type="pressure_delivered", created_at=when)
    other = await insert_agent(pool, s["id"], ordinal=9)
    await insert_event(pool, s["id"], other["id"], type="pressure_delivered", created_at=mid)
    stats = await _pressure(pool)
    assert stats.submits_accepted == 3
    assert stats.pressure_seen_before_submit_share == pytest.approx(1 / 3)


async def test_resume_without_human(pool):
    s, a = await _agents(pool, 4)
    t0 = NOW - timedelta(hours=1)

    async def returned(agent, *, tool_after=None, human_after=None, extra=None):
        turn = uuid.uuid4().hex
        msg = await insert_message(
            pool,
            s["id"],
            status="sent",
            notice="handoff_return",
            agent_id=agent["id"],
            turn_id=turn,
            lane="regular",
        )
        async with pool.connection() as conn:
            await conn.execute("UPDATE messages SET created_at = %s WHERE id = %s", (t0, msg["id"]))
        await _row(pool, s["id"], agent["id"], close="returned", return_message_id=msg["id"])
        if tool_after is not None:
            await insert_event(
                pool,
                s["id"],
                agent["id"],
                type="tool_call",
                turn_id=turn,
                created_at=t0 + tool_after,
            )
        if human_after is not None:
            human = await insert_message(pool, s["id"], status="sent", lane="regular")
            async with pool.connection() as conn:
                await conn.execute(
                    "UPDATE messages SET created_at = %s WHERE id = %s",
                    (t0 + human_after, human["id"]),
                )

    await returned(a[0], tool_after=timedelta(seconds=30))  # resumed alone
    await returned(a[1], tool_after=timedelta(minutes=2), human_after=timedelta(minutes=1))
    await returned(a[2], tool_after=timedelta(minutes=20))  # too late: beyond the window
    await returned(a[3])  # no tool call at all
    stats = await _pressure(pool)
    assert stats.returns == 4
    assert stats.resumes_without_human == 1


def test_cli_prints_pressure_table(dsn, monkeypatch, capsys):
    args = cli.build_parser().parse_args(["handover-stats", "--pressure", "--since", "2026-01-02"])
    assert args.pressure and args.since == date(2026, 1, 2)
    monkeypatch.setattr(cli, "_settings", lambda: make_settings(dsn))
    assert cli.main(["handover-stats", "--pressure", "--since", "2026-01-02"]) == 0
    out = capsys.readouterr().out
    assert "submits accepted" in out and "escalations" in out and out.count("\n") > 5
    assert "{" not in out  # a table, not JSON

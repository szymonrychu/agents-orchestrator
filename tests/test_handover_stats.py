"""Handover measurements: tenure and intake from handover_log, and the baseline from notices."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import psycopg
from factories import insert_session
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

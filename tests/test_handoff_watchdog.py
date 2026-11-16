"""The stuck-row watchdog (`handoff.watchdog`, spec 5.11): an open handoff row without progress
is reported once to the log and to the user's taskboard, and nothing is cleared, moved or closed;
and the chat_activity pruning that runs with it."""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from factories import insert_agent, insert_session
from fakes.fake_taskboard import FakeTaskboard
from test_handoff_pass import KEY

from agents_orchestrator import handoff_texts
from agents_orchestrator.db import chat_activity, handoffs
from agents_orchestrator.dispatcher import handoff
from agents_orchestrator.settings import Settings

TB_TOKEN = "tb-test-token"
STUCK = 1800
OLD = timedelta(seconds=STUCK + 60)
FRESH = timedelta(seconds=60)


@pytest.fixture
def tb() -> FakeTaskboard:
    return FakeTaskboard(token=TB_TOKEN)


@pytest.fixture
def settings(dsn) -> Settings:
    return Settings(
        database_url=dsn, master_key=KEY, taskboard_url="http://taskboard", taskboard_token=TB_TOKEN
    )


async def _open(pool, phase="submitted", *, progress=OLD, discuss=None):
    session = await insert_session(pool)
    agent = await insert_agent(pool, session["id"])
    now = datetime.now(UTC)
    async with pool.connection() as conn:
        row = await handoffs.insert(conn, session["id"], agent["id"], "pressure", phase)
        await conn.execute(
            "UPDATE agent_handoffs SET progress_at = %s, discuss_since = %s WHERE id = %s",
            (now - progress, None if discuss is None else now - discuss, row.id),
        )
    return session, agent, row


async def _run(pool, settings, tb) -> list[str]:
    async with pool.connection() as conn:
        return await handoff.watchdog(conn, settings, transport=httpx.MockTransport(tb.handler))


async def _row(pool, agent_id) -> handoffs.Handoff:
    async with pool.connection() as conn:
        row = await handoffs.open_row(conn, agent_id)
    assert row is not None
    return row


async def test_stuck_row_reported_once(pool, settings, tb, caplog):
    session, agent, row = await _open(pool, "moving")
    before = await _row(pool, agent["id"])
    with caplog.at_level(logging.WARNING, logger="agents_orchestrator.dispatcher.handoff"):
        assert await _run(pool, settings, tb) == [f"stuck:{row.id}"]
        assert await _run(pool, settings, tb) == []
    lines = [r for r in caplog.records if r.getMessage().startswith("handoff_stuck")]
    assert len(lines) == 1 and lines[0].levelno == logging.WARNING
    text = lines[0].getMessage()
    assert f"row={row.id} session={session['slug']} phase=moving since=" in text
    (task,) = tb.created
    assert task["client_key"] == f"handoff-stuck-{row.id}" and session["slug"] in task["body"]
    after = await _row(pool, agent["id"])
    assert after.stuck_reported_at is not None
    # reporting only: the phase and the progress stamp stay, nothing is cleared or queued
    assert (after.phase, after.progress_at) == ("moving", before.progress_at)
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT count(*) FROM commands WHERE agent_id = %s", (agent["id"],)
        )
        assert (await cur.fetchone())[0] == 0
        cur = await conn.execute("SELECT count(*) FROM leadership_requests")
        assert (await cur.fetchone())[0] == 0


async def test_progressing_row_not_reported(pool, settings, tb):
    _, agent, _ = await _open(pool, "pressuring", progress=FRESH)
    assert await _run(pool, settings, tb) == []
    assert tb.created == [] and tb.comments == []
    assert (await _row(pool, agent["id"])).stuck_reported_at is None


async def test_closed_row_ignored(pool, settings, tb):
    _, agent, row = await _open(pool, "submitted")
    async with pool.connection() as conn:
        assert await handoffs.close(conn, row.id, "cancelled", "compacted")
    assert await _run(pool, settings, tb) == []
    assert tb.created == []


async def test_discussing_row_keys_on_discuss_since(pool, settings, tb):
    # progress is bumped by every inventory change, so a churning ex-leader has a fresh
    # progress_at but an old discuss_since: reported
    _, churning, row = await _open(pool, "discussing", progress=FRESH, discuss=OLD)
    # a long idle progress_at but a recent move is not stuck
    _, recent, _ = await _open(pool, "discussing", progress=OLD, discuss=FRESH)
    assert await _run(pool, settings, tb) == [f"stuck:{row.id}"]
    assert (await _row(pool, churning["id"])).phase == "discussing"
    assert (await _row(pool, recent["id"])).stuck_reported_at is None


async def test_report_retried_while_taskboard_down(pool, settings, tb, caplog):
    _, agent, row = await _open(pool, "clearing")
    tb.down = True
    assert await _run(pool, settings, tb) == []
    assert (await _row(pool, agent["id"])).stuck_reported_at is None
    tb.down = False
    assert await _run(pool, settings, tb) == [f"stuck:{row.id}"]


async def test_stuck_without_taskboard_still_logged_and_marked(pool, dsn, tb, caplog):
    bare = Settings(database_url=dsn, master_key=KEY)
    _, agent, row = await _open(pool, "submitted")
    with caplog.at_level(logging.WARNING, logger="agents_orchestrator.dispatcher.handoff"):
        assert await _run(pool, bare, tb) == [f"stuck:{row.id}"]
    assert any(r.getMessage().startswith("handoff_stuck") for r in caplog.records)
    assert (await _row(pool, agent["id"])).stuck_reported_at is not None


def test_stuck_text_has_no_time_words():
    text = handoff_texts.stuck_text("s", "agent-s-0", "moving", "2026-11-16T10:00:00+00:00")
    assert "agent-s-0" in text and "nothing was cleared or moved" in text.lower()
    assert not re.search(r"minute|deadline|second", text, re.I)
    assert text.isascii() and handoff_texts.stuck_title("s", "agent-s-0").isascii()


async def test_prune_chat_activity(pool):
    session = await insert_session(pool)
    agent = await insert_agent(pool, session["id"])
    old = datetime.now(UTC) - timedelta(days=chat_activity.RETENTION_DAYS + 5)
    async with pool.connection() as conn:
        for direction in ("in", "out", "out", "in"):
            await chat_activity.record(conn, session["id"], agent["id"], direction)
        await conn.execute(
            "UPDATE chat_activity SET created_at = %s WHERE id IN"
            " (SELECT id FROM chat_activity ORDER BY id LIMIT 3)",
            (old,),
        )
        # the two old rows go; the newest outbound post survives however old (`last_out`)
        assert await chat_activity.prune(conn) == 2
        cur = await conn.execute("SELECT direction FROM chat_activity ORDER BY id")
        assert [d for (d,) in await cur.fetchall()] == ["out", "in"]
        assert await chat_activity.last_out(conn, session["id"]) == old
        assert await chat_activity.prune(conn) == 0

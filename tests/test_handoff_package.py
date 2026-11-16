"""handoff_package.build_package: the successor package (spec 5.7) built from an accepted
handoff row, the taskboard's claimed tasks, the chat room notes and the inbound chat messages
nobody answered. Postgres for the row and the chat messages; the taskboard and the chat are
injected callables."""

from __future__ import annotations

import re

import pytest
from factories import insert_agent, insert_session
from psycopg.types.json import Jsonb

from agents_orchestrator import handoff_package as hp
from agents_orchestrator.db import chat_activity, handoffs
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.settings import Settings
from agents_orchestrator.taskboard_client import TaskboardUnavailable

KEY = "k" * 32
HANDOFF = {
    "doing": "wiring the successor package into the dispatcher",
    "done": ["commit abc123 on feat/forced-handoff"],
    "in_flight": ["CI watcher on PR 12 (task bf3a)"],
    "next": ["open the draft PR", "watch CI"],
    "open_requests": ["Anna asked for the release notes (chat)"],
    "results": ["tests in tests/test_handoff_package.py"],
    "gotchas": ["reinstall the wrapper after mise run lint"],
    "abandoned": [],
}
INFLIGHT = {
    "subagents": 0,
    "background_tasks": [],
    "crons": [{"id": "c1", "schedule": "*/5 * * * *", "prompt": "check CI of PR 12"}],
    "worktrees": [{"path": "code/orch", "branch": "feat/x", "dirty": 2, "ahead": 1}],
    "scratch": [],
    "unknown": False,
    "waiting_on": [],
    "abandoned": [],
}
TASKS = [
    {
        "id": "T-436",
        "title": "successor package",
        "status": "active",
        "todo": [{"text": "write tests", "status": "done"}, {"text": "wire it", "status": "todo"}],
    }
]


@pytest.fixture
def settings(dsn) -> Settings:
    return Settings(database_url=dsn, master_key=KEY)


async def taskboard_ok(session):
    return TASKS


async def taskboard_down(session):
    raise TaskboardUnavailable("status 503")


async def chat_ok(session):
    return "The room prefers short answers.\nAnna owns releases."


async def chat_down(session):
    raise hp.ChatUnavailable("status 503")


async def make_row(pool, *, handoff=None, inflight=None, profile="work", trigger="pressure"):
    s = await insert_session(pool, spec={})
    a = await insert_agent(pool, s["id"], ordinal=0, ready=True)
    async with pool.connection() as conn:
        row = await handoffs.open_pressure(conn, s["id"], a["id"], 2, profile)
        await handoffs.submit(
            conn, row.id, handoff if handoff is not None else HANDOFF, inflight or INFLIGHT, None
        )
        row = await handoffs.open_row(conn, a["id"])
        session = await sessions_repo.get(conn, s["id"])
    return s, a, row, session


async def build(pool, row, session, settings, *, taskboard=taskboard_ok, chat=chat_ok):
    async with pool.connection() as conn:
        return await hp.build_package(conn, row, session, settings, taskboard=taskboard, chat=chat)


async def test_package_has_resume_instructions_first(pool, settings):
    _, _, row, session = await make_row(pool)
    pkg = await build(pool, row, session, settings)
    assert pkg.text.startswith("Resume now: 1. call orchestrator_me")
    first = pkg.text.split("\n", 1)[0]
    assert "orchestrator_task_get: T-436" in first
    assert "recreate the session crons" in first
    assert "continue with: open the draft PR" in first
    assert pkg.task_ids == ["T-436"]
    # the instruction list comes before any data
    assert pkg.text.index("Resume now") < pkg.text.index("Doing:")
    assert "Doing: wiring the successor package" in pkg.text
    assert "Next, in order:\n- open the draft PR\n- watch CI" in pkg.text
    for title in ("Done:", "In flight:", "Open requests:", "Results:", "Gotchas:"):
        assert title in pkg.text


async def test_package_lists_crons_verbatim(pool, settings):
    _, _, row, session = await make_row(pool)
    pkg = await build(pool, row, session, settings)
    assert "recreate these" in pkg.text.lower()
    assert "*/5 * * * *" in pkg.text and "check CI of PR 12" in pkg.text
    assert "*/5 * * * *" in pkg.attachment


async def test_package_lists_claimed_tasks_and_todo(pool, settings):
    _, _, row, session = await make_row(pool)
    pkg = await build(pool, row, session, settings)
    assert "T-436" in pkg.text and "successor package" in pkg.text and "active" in pkg.text
    assert "done: write tests" in pkg.text and "todo: wire it" in pkg.text
    assert "code/orch" in pkg.text and "feat/x" in pkg.text and "dirty 2" in pkg.text


async def test_chat_package_has_room_notes_and_unanswered_messages(pool, settings):
    s, a, row, session = await make_row(pool, profile="chat")
    async with pool.connection() as conn:
        await chat_activity.record(conn, s["id"], a["id"], "out")
        for i in range(12):
            await conn.execute(
                "INSERT INTO messages (session_id, text, status, origin_service, lane, created_at)"
                " VALUES (%s, %s, 'sent', 'agents-matrix-connector', 'idle',"
                " clock_timestamp() + interval '1 second')",
                (s["id"], f"question number {i}"),
            )
        await conn.execute(
            "INSERT INTO messages (session_id, text, status, origin_service, lane, created_at)"
            " VALUES (%s, 'from another service', 'sent', 'agents-webhook', 'idle',"
            " clock_timestamp() + interval '1 second')",
            (s["id"],),
        )
    pkg = await build(pool, row, session, settings)
    assert "room notes and recent chat" in pkg.text.split("\n", 1)[0]
    assert "The room prefers short answers." in pkg.text
    assert "question number 11" in pkg.text and "question number 2" in pkg.text
    assert "question number 1\n" not in pkg.text and "question number 0" not in pkg.text
    assert "from another service" not in pkg.text


async def test_work_package_has_no_room_section_and_never_asks_chat(pool, settings):
    called = []

    async def chat(session):
        called.append(session)
        return "notes"

    _, _, row, session = await make_row(pool, profile="work")
    pkg = await build(pool, row, session, settings, chat=chat)
    assert called == []
    assert "Room notes" not in pkg.text and "chat" not in pkg.text.split("\n", 1)[0]


async def test_chat_room_tail_setting_caps_messages(pool, dsn):
    s, a, row, session = await make_row(pool, profile="chat")
    small = Settings(database_url=dsn, master_key=KEY, handoff_room_tail=2)
    async with pool.connection() as conn:
        for i in range(5):
            await conn.execute(
                "INSERT INTO messages (session_id, text, status, origin_service, lane, created_at)"
                " VALUES (%s, %s, 'sent', 'agents-matrix-connector', 'idle',"
                " clock_timestamp() + interval '1 second')",
                (s["id"], f"msg-{i}"),
            )
    pkg = await build(pool, row, session, small)
    assert "msg-4" in pkg.text and "msg-3" in pkg.text and "msg-2" not in pkg.text


async def test_package_lists_abandoned_items_with_reasons(pool, settings):
    handoff = {
        **HANDOFF,
        "abandoned": [{"item": "task:bf3a (CI watch)", "why": "the PR was closed upstream"}],
    }
    _, _, row, session = await make_row(pool, handoff=handoff)
    pkg = await build(pool, row, session, settings)
    assert "Abandoned:" in pkg.text
    assert "- task:bf3a (CI watch): the PR was closed upstream" in pkg.text


async def test_package_degrades_when_taskboard_or_chat_down(pool, settings):
    _, _, row, session = await make_row(pool, profile="chat")
    pkg = await build(pool, row, session, settings, taskboard=taskboard_down, chat=chat_down)
    assert "room notes unavailable" in pkg.text.lower()
    assert "claimed tasks unavailable" in pkg.text.lower()
    assert pkg.task_ids == []
    # the rest of the package is intact
    assert pkg.text.startswith("Resume now:")
    assert "Doing: wiring the successor package" in pkg.text
    assert "*/5 * * * *" in pkg.text


async def test_package_lists_scratch_paths_to_read(pool, settings):
    handoff = {
        **HANDOFF,
        "gotchas": ["notes in .scratchpad/forced-handoff/notes.md: the design decisions so far"],
        "results": ["benchmark data under .scratchpad/forced-handoff/bench/ (raw timings)"],
    }
    _, _, row, session = await make_row(pool, handoff=handoff)
    pkg = await build(pool, row, session, settings)
    assert pkg.scratch_paths == [  # in the order of the fields
        ".scratchpad/forced-handoff/bench/",
        ".scratchpad/forced-handoff/notes.md",
    ]
    head = pkg.text.split("\n", 1)[0]
    assert "read these first" in head
    assert ".scratchpad/forced-handoff/notes.md" in head
    assert "the design decisions so far" in pkg.text


async def test_package_lists_abandoned_scratch_with_reason(pool, settings):
    inflight = {
        **INFLIGHT,
        "abandoned": [
            {
                "item": "scratch:/tmp/probe.log (pod-local, not persisted)",
                "why": "throwaway probe output, reproducible",
            }
        ],
    }
    _, _, row, session = await make_row(pool, inflight=inflight)
    pkg = await build(pool, row, session, settings)
    assert "/tmp/probe.log" in pkg.text and "throwaway probe output, reproducible" in pkg.text
    assert "/tmp/probe.log" not in pkg.scratch_paths  # nothing to read: it was dropped


async def test_package_fits_transcript_budget(pool, dsn):
    budget = 16384
    small = Settings(database_url=dsn, master_key=KEY, handover_transcript_bytes=budget)
    big = {**HANDOFF, "done": [f"step {i}: " + "x" * 400 for i in range(60)]}
    inflight = {
        **INFLIGHT,
        "crons": [{"id": f"c{i}", "schedule": "* * * * *", "prompt": "p" * 300} for i in range(60)],
    }
    _, _, row, session = await make_row(pool, handoff=big, inflight=inflight)
    pkg = await build(pool, row, session, small)
    assert len(pkg.attachment.encode()) <= budget // 2
    assert "truncated" in pkg.attachment


async def test_shutdown_thumbs_up_text_rejected_package_has_inventory(pool, settings):
    s = await insert_session(pool, spec={})
    a = await insert_agent(pool, s["id"], ordinal=0, ready=True)
    async with pool.connection() as conn:
        row = await handoffs.insert(
            conn, s["id"], a["id"], "shutdown", "handed", handoff_text="[thumbs-up]"
        )
        session = await sessions_repo.get(conn, s["id"])
        await conn.execute(
            "UPDATE agent_handoffs SET inflight = %s WHERE id = %s", (Jsonb(INFLIGHT), row.id)
        )
        row = await handoffs.open_row(conn, a["id"])
    pkg = await build(pool, row, session, settings)
    assert re.search(r"the shutdown handoff text was rejected \(too_short", pkg.text, re.I)
    assert "[thumbs-up]" not in pkg.text
    assert "T-436" in pkg.text and "*/5 * * * *" in pkg.text
    assert pkg.text.startswith("Resume now:")


async def test_no_time_words_in_package(pool, settings):
    _, _, row, session = await make_row(pool, profile="chat")
    pkg = await build(pool, row, session, settings)
    head = pkg.text.split("\n", 1)[0]
    assert not re.search(r"minute|deadline|\bin \d+", head, re.I)
    assert pkg.text.isascii()

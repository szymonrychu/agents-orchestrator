"""What a discard keeps and how long messages have waited (deploy-safe-agent-sessions D7, D8)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from factories import insert_message, insert_session
from test_api_sessions import client_for, make_settings
from test_api_streams import open_sse, serve

from agents_orchestrator.api.app import create_app
from agents_orchestrator.cache.save_rule import transcripts_skip
from agents_orchestrator.db.pool import notify

T0 = datetime(2026, 10, 28, 9, 0, tzinfo=UTC)
CACHED = {"backup": [{"root": "workspace", "path": ".", "mode": "both", "name": "k1"}]}
RESTORED = {"state": "restored", "finished_at": T0.isoformat(), "bytes": 10, "reason": None}


def _status(entry_restore=None, tx_restore=None, tx_save=None, **extra) -> dict:
    """A session status: the workspace entry's restore record and the transcripts runs."""
    transcripts = {k: v for k, v in (("restore", tx_restore), ("save", tx_save)) if v is not None}
    backup: dict = {"transcripts": transcripts}
    if entry_restore is not None:
        backup["entries"] = [{"index": 0, "restore": entry_restore, "save": None}]
    return {"backup": backup, **extra}


SAVED = {
    "state": "saved",
    "started_at": T0.isoformat(),
    "finished_at": (T0 + timedelta(seconds=7)).isoformat(),
    "bytes": 4096,
    "reason": None,
    "job": None,
}


def _at(value: str | None) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value)


async def _set_created(pool, message_id: int, at: datetime) -> None:
    async with pool.connection() as conn:
        await conn.execute("UPDATE messages SET created_at = %s WHERE id = %s", (at, message_id))


async def _get(pool, dsn, session_id, **settings) -> dict:
    app = create_app(make_settings(dsn, **settings), pool)
    async with client_for(app) as http:
        r = await http.get(f"/api/v1/sessions/{session_id}")
    assert r.status_code == 200, r.text
    return r.json()


async def _backup(pool, dsn, spec: dict, status: dict, *, cache: bool = True) -> dict:
    session = await insert_session(pool, spec=spec, status=status)
    view = await _get(pool, dsn, session["id"], **({"cache_s3_bucket": "b"} if cache else {}))
    return view["backup"]


async def test_summary_oldest_queued_at(pool, dsn):
    session = await insert_session(pool)
    assert (await _get(pool, dsn, session["id"]))["summary"]["oldest_queued_at"] is None

    failed = await insert_message(pool, session["id"], status="failed")
    sent = await insert_message(pool, session["id"], status="sent")
    newer = await insert_message(pool, session["id"])
    older = await insert_message(pool, session["id"])
    await _set_created(pool, failed["id"], T0 - timedelta(hours=2))
    await _set_created(pool, sent["id"], T0 - timedelta(hours=1))
    await _set_created(pool, newer["id"], T0 + timedelta(minutes=5))
    await _set_created(pool, older["id"], T0)
    other = await insert_session(pool)
    elsewhere = await insert_message(pool, other["id"])
    await _set_created(pool, elsewhere["id"], T0 - timedelta(days=1))

    summary = (await _get(pool, dsn, session["id"]))["summary"]
    assert summary["queued"] == 2
    assert _at(summary["oldest_queued_at"]) == T0

    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE messages SET status = 'cancelled' WHERE session_id = %s", (session["id"],)
        )
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as http:
        listed = {s["id"]: s for s in (await http.get("/api/v1/sessions")).json()}
    assert listed[str(session["id"])]["summary"]["oldest_queued_at"] is None
    assert _at(listed[str(other["id"])]["summary"]["oldest_queued_at"]) == T0 - timedelta(days=1)


@pytest_asyncio.fixture
async def srv(pool, dsn):
    app = create_app(make_settings(dsn, allowed_cidrs="127.0.0.0/8"), pool, stream_poll_seconds=0.3)
    async with serve(app) as s:
        yield s


async def _next_message_status(sse) -> dict:
    for _ in range(10):
        frame = await sse.next()
        if frame["event"] == "message_status":
            return frame["data"]
    raise AssertionError("no message_status frame")


async def test_message_status_frame_carries_oldest_queued_at(srv, pool):
    session = await insert_session(pool)
    async with open_sse(srv, "/api/v1/stream") as sse:
        assert (await sse.next())["event"] == "snapshot"

        msg = await insert_message(pool, session["id"])
        await _set_created(pool, msg["id"], T0)
        async with pool.connection() as conn:
            await notify(conn, "status", {"session_id": str(session["id"])})
        frame = await _next_message_status(sse)
        assert frame["queued"] == 1
        assert _at(frame["oldest_queued_at"]) == T0

        async with pool.connection() as conn:
            await conn.execute("UPDATE messages SET status = 'sent' WHERE id = %s", (msg["id"],))
            await notify(conn, "status", {"session_id": str(session["id"])})
        frame = await _next_message_status(sse)
        assert frame["queued"] == 0
        assert frame["oldest_queued_at"] is None


async def test_backup_view_when_cache_configured(pool, dsn):
    status = _status({"state": "restored"}, {**RESTORED}, replicas=1)
    backup = await _backup(pool, dsn, CACHED, status)
    assert backup == {
        "entries": [{"index": 0, "name_template": "k1", "action": "save", "reason": None}],
        "workdir": True,
        "transcripts": True,
        "reason": None,
        "last_transcripts_backup": None,
    }
    for state in ("missing", "unreadable"):
        status = _status({"state": state}, {"state": "miss"}, replicas=1)
        backup = await _backup(pool, dsn, CACHED, status)
        assert (backup["workdir"], backup["transcripts"], backup["reason"]) == (True, True, None)


async def test_backup_view_without_cache(pool, dsn):
    status = _status({"state": "restored"}, RESTORED, replicas=1)
    backup = await _backup(pool, dsn, {}, status)
    assert (backup["workdir"], backup["transcripts"]) == (False, False)
    assert backup["reason"] == "no_session_cache"
    assert backup["entries"] == [
        {"index": -1, "name_template": "", "action": "discard", "reason": "workspace_not_listed"}
    ]

    backup = await _backup(pool, dsn, CACHED, status, cache=False)
    assert (backup["workdir"], backup["transcripts"]) == (False, False)
    assert backup["reason"] == "cache_not_configured"


async def test_backup_view_lists_each_entry(pool, dsn):
    spec = {
        "backup": [
            {"root": "claude", "path": "skills", "mode": "backup", "name": "skills/{{ project }}"},
            {"root": "workspace", "path": "src", "mode": "restore", "name": "src"},
        ]
    }
    backup = await _backup(pool, dsn, spec, _status(None, RESTORED, replicas=1))
    assert backup["entries"] == [
        {"index": 0, "name_template": "skills/{{ project }}", "action": "save", "reason": None},
        {"index": 1, "name_template": "src", "action": "discard", "reason": "restore-only"},
        {"index": -1, "name_template": "", "action": "discard", "reason": "workspace_not_listed"},
    ]
    assert backup["workdir"] is False  # no workspace entry is saved


@pytest.mark.parametrize(
    ("restore", "sessions_restore", "workdir", "transcripts", "reason"),
    [
        (None, RESTORED, False, True, "restore_pending"),
        ({"state": "running"}, RESTORED, False, True, "restore_pending"),
        ({"state": "failed", "reason": "HTTP 403"}, RESTORED, True, True, None),
        ({"state": "restored"}, {"state": "running"}, True, False, "restore_running"),
        ({"state": "restored"}, {"state": "failed", "reason": "timeout"}, True, False, "timeout"),
        (
            {"state": "restored"},
            {"state": "skipped", "reason": "not_configured"},
            True,
            False,
            "restore_skipped",
        ),
        ({"state": "restored"}, {"state": "skipped", "reason": "pods_exist"}, True, True, None),
        ({"state": "restored"}, None, True, True, None),
        (
            {"state": "running"},
            {"state": "failed", "reason": "timeout"},
            False,
            False,
            "restore_pending",
        ),
    ],
)
async def test_backup_view_reports_skipped_save(
    pool, dsn, restore, sessions_restore, workdir, transcripts, reason
):
    status = _status(restore, sessions_restore, replicas=1)  # converged: its pods ran
    backup = await _backup(pool, dsn, CACHED, status)
    assert (backup["workdir"], backup["transcripts"]) == (workdir, transcripts)
    assert backup["reason"] == reason
    assert transcripts == (transcripts_skip(True, status) is None)


async def test_backup_view_never_converged_session_has_no_saves(pool, dsn):
    """No converge yet: the reconciler skips every save (no PVC to read)."""
    status = _status({"state": "restored"}, RESTORED)
    backup = await _backup(pool, dsn, CACHED, status)
    assert (backup["workdir"], backup["transcripts"]) == (False, True)
    assert backup["reason"] == "never_converged"
    assert backup["entries"][0]["action"] == "skip"
    status = _status({"state": "restored"})
    assert transcripts_skip(True, status) == "never_converged"
    converged = await _backup(pool, dsn, CACHED, {**status, "replicas": 1})
    assert (converged["workdir"], converged["transcripts"], converged["reason"]) == (
        True,
        True,
        None,
    )


async def test_backup_view_last_transcripts_backup(pool, dsn):
    status = _status({"state": "restored"}, RESTORED, SAVED)
    backup = await _backup(pool, dsn, CACHED, status)
    last = backup["last_transcripts_backup"]
    assert set(last) == {"finished_at", "bytes"}
    assert _at(last["finished_at"]) == T0 + timedelta(seconds=7)
    assert last["bytes"] == 4096

    for unfinished in (
        {**SAVED, "state": "running", "finished_at": None, "bytes": None},
        {**SAVED, "state": "skipped", "reason": "restore_running", "bytes": None},
        {**SAVED, "state": "failed", "reason": "too_large", "bytes": None},
    ):
        status = _status(None, RESTORED, unfinished)
        assert (await _backup(pool, dsn, CACHED, status))["last_transcripts_backup"] is None

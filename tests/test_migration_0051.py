import shutil
import uuid

import psycopg
import pytest

from agents_orchestrator.db.migrate import MIGRATIONS_DIR, run_migrations

LEGACY_OPEN = ("notified", "extended", "requested", "handed", "discussing", "clearing", "returning")


def _upto_0050(dsn, tmp_path):
    for path in MIGRATIONS_DIR.glob("*.sql"):
        if path.name < "0051":
            shutil.copy(path, tmp_path / path.name)
    run_migrations(dsn, tmp_path)


def _agent(c, n=0):
    session_id, agent_id = uuid.uuid4(), uuid.uuid4()
    c.execute(
        "INSERT INTO sessions (id, slug, spec) VALUES (%s, %s, '{}')",
        (session_id, f"s-{uuid.uuid4().hex[:8]}"),
    )
    c.execute(
        "INSERT INTO agents (id, session_id, ordinal, pod_name) VALUES (%s, %s, %s, 'p')",
        (agent_id, session_id, n),
    )
    return session_id, agent_id


def _row(c, session_id, agent_id, trigger, phase):
    return c.execute(
        "INSERT INTO agent_handoffs (session_id, agent_id, trigger, phase, handoff_text)"
        " VALUES (%s, %s, %s, %s, 'kept text') RETURNING id",
        (session_id, agent_id, trigger, phase),
    ).fetchone()[0]


def _msg(c, session_id, agent_id, notice, status="queued"):
    return c.execute(
        "INSERT INTO messages (session_id, target_agent_id, text, status, notice)"
        " VALUES (%s, %s, 't', %s, %s) RETURNING id",
        (session_id, agent_id, status, notice),
    ).fetchone()[0]


def _state(c, row_id):
    return c.execute(
        "SELECT phase, close_reason, closed_at IS NOT NULL, handoff_text"
        " FROM agent_handoffs WHERE id = %s",
        (row_id,),
    ).fetchone()


@pytest.mark.parametrize("trigger", ["strong", "voluntary"])
def test_legacy_open_strong_and_voluntary_rows_closed_superseded(empty_dsn, tmp_path, trigger):
    _upto_0050(empty_dsn, tmp_path)
    ids = []
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        for n, phase in enumerate(LEGACY_OPEN):
            s, a = _agent(c, n)
            ids.append(_row(c, s, a, trigger, phase))
    assert run_migrations(empty_dsn) == ["0051", "0052", "0053", "0054"]
    with psycopg.connect(empty_dsn) as c:
        for row_id in ids:
            assert _state(c, row_id) == ("cancelled", "superseded", True, "kept text")


def test_row_116_class_handed_with_queued_limit_request_closed_and_request_rejected(
    empty_dsn, tmp_path
):
    _upto_0050(empty_dsn, tmp_path)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        s, a = _agent(c)
        row = _row(c, s, a, "strong", "handed")
        limit = c.execute(
            "INSERT INTO leadership_requests (session_id, kind, from_agent_id, reason)"
            " VALUES (%s, 'handover', %s, 'limit') RETURNING id",
            (s, a),
        ).fetchone()[0]
        voluntary = c.execute(
            "INSERT INTO leadership_requests (session_id, kind, from_agent_id, reason)"
            " VALUES (%s, 'handover', %s, 'my own summary') RETURNING id",
            (s, a),
        ).fetchone()[0]
    run_migrations(empty_dsn)
    with psycopg.connect(empty_dsn) as c:
        assert _state(c, row)[:2] == ("cancelled", "superseded")
        q = "SELECT status, reject_reason, decided_at IS NOT NULL FROM leadership_requests"
        assert c.execute(q + " WHERE id = %s", (limit,)).fetchone() == (
            "rejected",
            "superseded",
            True,
        )
        assert c.execute(q + " WHERE id = %s", (voluntary,)).fetchone()[0] == "queued"


def test_shutdown_rows_untouched(empty_dsn, tmp_path):
    _upto_0050(empty_dsn, tmp_path)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        s, a = _agent(c)
        row = _row(c, s, a, "shutdown", "handed")
        queued = _msg(c, s, a, "handoff_notice")
    run_migrations(empty_dsn)
    with psycopg.connect(empty_dsn) as c:
        assert _state(c, row) == ("handed", None, False, "kept text")
        assert c.execute("SELECT status FROM messages WHERE id = %s", (queued,)).fetchone() == (
            "queued",
        )


def test_queued_handoff_messages_cancelled(empty_dsn, tmp_path):
    _upto_0050(empty_dsn, tmp_path)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        s, a = _agent(c)
        _row(c, s, a, "strong", "requested")
        notice, request, ret = (
            _msg(c, s, a, k) for k in ("handoff_notice", "handoff_request", "handoff_return")
        )
        other = _msg(c, s, a, "inactivity_reminder")
        plain = _msg(c, s, a, None)
        sent = _msg(c, s, a, "handoff_notice", status="sent")
    run_migrations(empty_dsn)
    with psycopg.connect(empty_dsn) as c:

        def status(i):
            return c.execute("SELECT status FROM messages WHERE id = %s", (i,)).fetchone()[0]

        assert [status(i) for i in (notice, request, ret)] == ["cancelled"] * 3
        assert [status(i) for i in (other, plain)] == ["queued", "queued"]
        assert status(sent) == "sent"


def test_closed_rows_keep_history(empty_dsn, tmp_path):
    _upto_0050(empty_dsn, tmp_path)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        s, a = _agent(c)
        row = _row(c, s, a, "strong", "notified")
        c.execute(
            "UPDATE agent_handoffs SET phase = 'done', close_reason = 'delivered',"
            " closed_at = now() WHERE id = %s",
            (row,),
        )
    run_migrations(empty_dsn)
    with psycopg.connect(empty_dsn) as c:
        assert _state(c, row) == ("done", "delivered", True, "kept text")
        cols = {
            r[0]
            for r in c.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = 'agent_handoffs'"
            )
        }
    assert not cols & {"deadline_at", "extended_at", "notified_at", "request_message_id"}
    assert {"level", "handoff", "inflight", "progress_at", "leadership_request_id"} <= cols


def test_new_phase_check_rejects_notified(empty_dsn, tmp_path):
    _upto_0050(empty_dsn, tmp_path)
    run_migrations(empty_dsn)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        s, a = _agent(c)
        for phase in ("notified", "extended", "requested"):
            with pytest.raises(psycopg.errors.CheckViolation):
                _row(c, s, a, "pressure", phase)
        with pytest.raises(psycopg.errors.CheckViolation):
            _row(c, s, a, "bogus", "pressuring")
        assert _row(c, s, a, "pressure", "pressuring")
        with pytest.raises(psycopg.errors.UniqueViolation):
            _row(c, s, a, "voluntary", "submitted")


def test_notice_check_accepts_nudge_and_escalation(empty_dsn, tmp_path):
    _upto_0050(empty_dsn, tmp_path)
    run_migrations(empty_dsn)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        s, a = _agent(c)
        for kind in ("handoff_nudge", "handoff_escalation", "handoff_notice", "runaway_parked"):
            assert _msg(c, s, a, kind)
        with pytest.raises(psycopg.errors.CheckViolation):
            _msg(c, s, a, "handoff_bogus")
        c.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS origin_service text")
        assert c.execute("SELECT to_regclass('chat_activity')").fetchone()[0] is not None

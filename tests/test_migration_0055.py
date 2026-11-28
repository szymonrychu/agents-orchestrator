import psycopg
import psycopg.errors
import pytest

from agents_orchestrator.db.migrate import run_migrations

pytestmark = pytest.mark.critical


def _seed(c):
    sid = c.execute(
        "INSERT INTO sessions (id, slug, spec) VALUES (gen_random_uuid(), 's', '{}') RETURNING id"
    ).fetchone()[0]
    return sid


def _row(c, sid, **kw):
    cols = {"session_id": sid, "kind": "send", **kw}
    names = ", ".join(cols)
    marks = ", ".join(["%s"] * len(cols))
    c.execute(f"INSERT INTO envelope_outbox ({names}) VALUES ({marks})", tuple(cols.values()))


def test_tables_and_defaults(empty_dsn):
    run_migrations(empty_dsn)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        sid = _seed(c)
        _row(c, sid, files=["a/b.png"], body="hi")
        row = c.execute("SELECT status, attempts, files FROM envelope_outbox").fetchone()
        assert row == ("queued", 0, ["a/b.png"])
        assert c.execute("SELECT count(*) FROM envelope_stop_checks").fetchone()[0] == 0


@pytest.mark.parametrize(
    ("column", "value"),
    [("kind", "bogus"), ("status", "bogus")],
)
def test_checks_reject_unknown_values(empty_dsn, column, value):
    run_migrations(empty_dsn)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        sid = _seed(c)
        with pytest.raises(psycopg.errors.CheckViolation):
            _row(c, sid, **{column: value})


@pytest.mark.parametrize("kind", ["send", "react", "notice", "session", "agent", "project_post"])
@pytest.mark.parametrize(
    "status", ["queued", "sending", "accepted", "refused", "failed", "invalid"]
)
def test_checks_accept_listed_values(empty_dsn, kind, status):
    run_migrations(empty_dsn)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        _row(c, _seed(c), kind=kind, status=status)


def test_unique_per_event_and_index_only_when_event_seq_set(empty_dsn):
    run_migrations(empty_dsn)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        sid = _seed(c)
        agent = c.execute(
            "INSERT INTO agents (id, session_id, ordinal, pod_name)"
            " VALUES (gen_random_uuid(), %s, 0, 'p') RETURNING id",
            (sid,),
        ).fetchone()[0]
        key = {"agent_id": agent, "wrapper_session_id": "w", "event_seq": 5, "envelope_index": 1}
        _row(c, sid, **key)
        with pytest.raises(psycopg.errors.UniqueViolation):
            _row(c, sid, **key)
        _row(c, sid, **{**key, "envelope_index": 2})
        _row(c, sid, **{**key, "event_seq": 6})
        # rows without an event (onStop notices) never collide
        for _ in range(2):
            _row(c, sid, kind="notice", agent_id=agent, event_seq=None, envelope_index=1)


def test_session_delete_cascades(empty_dsn):
    run_migrations(empty_dsn)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        sid = _seed(c)
        _row(c, sid)
        c.execute("DELETE FROM sessions WHERE id = %s", (sid,))
        assert c.execute("SELECT count(*) FROM envelope_outbox").fetchone()[0] == 0


def test_stop_checks_record_allowed_stops_without_a_block(empty_dsn):
    run_migrations(empty_dsn)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        sid = _seed(c)
        agent = c.execute(
            "INSERT INTO agents (id, session_id, ordinal, pod_name)"
            " VALUES (gen_random_uuid(), %s, 0, 'p') RETURNING id",
            (sid,),
        ).fetchone()[0]
        c.execute(
            "INSERT INTO envelope_stop_checks (agent_id, turn_id, allowed_at)"
            " VALUES (%s, 't1', now())",
            (agent,),
        )
        assert c.execute(
            "SELECT blocked_at IS NULL, allowed_at IS NOT NULL FROM envelope_stop_checks"
        ).fetchone() == (True, True)

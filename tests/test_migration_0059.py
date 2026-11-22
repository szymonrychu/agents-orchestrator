import psycopg
import psycopg.errors
import pytest

from agents_orchestrator.db.migrate import run_migrations


def _session(c):
    return c.execute(
        "INSERT INTO sessions (id, slug, spec) VALUES (gen_random_uuid(), 's', '{}') RETURNING id"
    ).fetchone()[0]


def test_message_defaults(empty_dsn):
    run_migrations(empty_dsn)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        sid = _session(c)
        c.execute("INSERT INTO messages (session_id, text) VALUES (%s, 'x')", (sid,))
        row = c.execute(
            "SELECT channel, sender_agent_id, notify, envelope_ref, notified_at FROM messages"
        ).fetchone()
        assert row == ("session", None, None, None, None)


@pytest.mark.parametrize(("column", "value"), [("channel", "bogus"), ("notify", "bogus")])
def test_message_checks_reject_unknown_values(empty_dsn, column, value):
    run_migrations(empty_dsn)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        sid = _session(c)
        with pytest.raises(psycopg.errors.CheckViolation):
            c.execute(
                f"INSERT INTO messages (session_id, text, {column}) VALUES (%s, 'x', %s)",
                (sid, value),
            )


def test_message_delete_nulls_outbox_message_id(empty_dsn):
    run_migrations(empty_dsn)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        sid = _session(c)
        mid = c.execute(
            "INSERT INTO messages (session_id, text) VALUES (%s, 'x') RETURNING id", (sid,)
        ).fetchone()[0]
        c.execute(
            "INSERT INTO envelope_outbox (session_id, kind, message_id) VALUES (%s, 'agent', %s)",
            (sid, mid),
        )
        c.execute("DELETE FROM messages WHERE id = %s", (mid,))
        assert c.execute("SELECT message_id FROM envelope_outbox").fetchone() == (None,)

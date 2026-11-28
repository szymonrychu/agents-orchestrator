import shutil

import psycopg
import pytest
from psycopg.types.json import Jsonb

from agents_orchestrator.db.migrate import MIGRATIONS_DIR, run_migrations

pytestmark = pytest.mark.critical


def _upto_0061(dsn, tmp_path):
    for path in MIGRATIONS_DIR.glob("*.sql"):
        if path.name < "0062":
            shutil.copy(path, tmp_path / path.name)
    run_migrations(dsn, tmp_path)


def _proposal(c, kind: str, paths: tuple[str, ...], status: str = "open") -> int:
    pid = c.execute(
        "INSERT INTO change_proposals (kind, target_id, target_name, proposer_session,"
        " proposer_slug, proposer_superleader, status, expires_at)"
        " VALUES (%s, 'p', 'p', gen_random_uuid(), 'x', false, %s, now() + interval '1 day')"
        " RETURNING id",
        (kind, status),
    ).fetchone()[0]
    c.execute(
        "INSERT INTO change_proposal_revisions (proposal_id, revision, reason, base, fingerprint)"
        " VALUES (%s, 1, 'r', %s, 'f')",
        (pid, Jsonb({})),
    )
    for path in paths:
        c.execute(
            "INSERT INTO change_proposal_items (proposal_id, revision, path, before, after,"
            ' security, "grant") VALUES (%s, 1, %s, %s, %s, false, false)',
            (pid, path, Jsonb(1), Jsonb(2)),
        )
    return pid


def test_migration_0062_withdraws_active_proposals_left_empty(empty_dsn, tmp_path):
    """An active preset or session proposal whose current revision has no item left (0060 took
    its `retrospective` item) is withdrawn; one with items, or already closed, is untouched."""
    _upto_0061(empty_dsn, tmp_path)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        ids = {
            "empty_preset": _proposal(c, "preset", ()),
            "empty_session": _proposal(c, "session", (), status="rework"),
            "with_items": _proposal(c, "preset", ("replicas",)),
            "closed": _proposal(c, "preset", (), status="denied"),
        }
    assert run_migrations(empty_dsn) == ["0062", "0063"]
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        rows = {
            pid: rest
            for pid, *rest in c.execute(
                "SELECT p.id, p.status, r.outcome, p.closed_at IS NOT NULL"
                " FROM change_proposals p JOIN change_proposal_revisions r"
                " ON r.proposal_id = p.id AND r.revision = p.revision"
            ).fetchall()
        }
    assert rows[ids["empty_preset"]] == ["withdrawn", "withdrawn", True]
    assert rows[ids["empty_session"]] == ["withdrawn", "withdrawn", True]
    assert rows[ids["with_items"]] == ["open", None, False]
    assert rows[ids["closed"]] == ["denied", None, False]

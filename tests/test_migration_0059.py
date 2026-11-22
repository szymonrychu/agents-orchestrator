import shutil

import psycopg
import pytest
from psycopg.types.json import Jsonb

from agents_orchestrator.db.migrate import MIGRATIONS_DIR, run_migrations
from agents_orchestrator.spec import load_stored_spec


def _upto_0058(dsn, tmp_path):
    for path in MIGRATIONS_DIR.glob("*.sql"):
        if path.name < "0059":
            shutil.copy(path, tmp_path / path.name)
    run_migrations(dsn, tmp_path)


STORED = {"replicas": 1, "retrospective": True}
CLEAN = {"replicas": 1}


def _proposal(c):
    pid = c.execute(
        "INSERT INTO change_proposals (kind, target_id, target_name, proposer_session,"
        " proposer_slug, proposer_superleader, expires_at)"
        " VALUES ('preset', 'p', 'p', gen_random_uuid(), 'x', false, now() + interval '1 day')"
        " RETURNING id"
    ).fetchone()[0]
    c.execute(
        "INSERT INTO change_proposal_revisions (proposal_id, revision, reason, base, fingerprint)"
        " VALUES (%s, 1, 'r', %s, 'f')",
        (pid, Jsonb({**STORED, "spec": dict(STORED)})),
    )
    for path, before, after in (
        ("retrospective", False, True),
        ("spec", {"spec": dict(STORED)}, {**STORED, "spec": dict(STORED)}),
    ):
        c.execute(
            "INSERT INTO change_proposal_items (proposal_id, revision, path, before, after,"
            ' security, "grant") VALUES (%s, 1, %s, %s, %s, true, true)',
            (pid, path, Jsonb(before), Jsonb(after)),
        )
    return pid


@pytest.mark.parametrize("table", ["sessions", "presets", "proposals"])
def test_migration_0059_strips_retrospective(empty_dsn, tmp_path, table):
    _upto_0058(empty_dsn, tmp_path)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        if table == "sessions":
            c.execute(
                "INSERT INTO sessions (id, slug, spec) VALUES (gen_random_uuid(), 's', %s)",
                (Jsonb(STORED),),
            )
        elif table == "presets":
            c.execute(
                "INSERT INTO presets (id, name, spec) VALUES (gen_random_uuid(), 'p', %s)",
                (Jsonb(STORED),),
            )
        else:
            pid = _proposal(c)
    assert run_migrations(empty_dsn) == ["0059"]
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        if table == "proposals":
            base = c.execute("SELECT base FROM change_proposal_revisions").fetchone()[0]
            assert base == {**CLEAN, "spec": CLEAN}
            items = c.execute(
                "SELECT path, before, after FROM change_proposal_items WHERE proposal_id = %s",
                (pid,),
            ).fetchall()
            assert items == [("spec", {"spec": CLEAN}, {**CLEAN, "spec": CLEAN})]
        else:
            raw = c.execute(f"SELECT spec FROM {table}").fetchone()[0]
            assert raw == CLEAN
            assert load_stored_spec(raw, None).replicas == 1

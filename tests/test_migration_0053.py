import json
import shutil

import psycopg

from agents_orchestrator.db.migrate import MIGRATIONS_DIR, run_migrations


def _upto_0052(dsn, tmp_path):
    for path in MIGRATIONS_DIR.glob("*.sql"):
        if path.name < "0053":
            shutil.copy(path, tmp_path / path.name)
    run_migrations(dsn, tmp_path)


def _cap(spec, value):
    if value == "absent":
        return {**spec, "runaway": {"max_turns_per_task": 50}}
    return {**spec, "runaway": {"max_stop_continuations": value}}


CASES = {
    "low": 3,
    "edge4": 4,
    "one": 1,
    "five": 5,
    "high": 25,
    "off": 0,
    "null": None,
    "absent": "absent",
}
EXPECTED = {
    "low": 5,
    "edge4": 5,
    "one": 5,
    "five": 5,
    "high": 25,
    "off": 0,
    "null": None,
    "absent": "absent",
}


def _seed(c):
    for name, v in CASES.items():
        spec = json.dumps(_cap({"replicas": 1}, v))
        c.execute(
            "INSERT INTO sessions (id, slug, spec) VALUES (gen_random_uuid(), %s, %s)",
            (f"s-{name}", spec),
        )
        c.execute(
            "INSERT INTO presets (id, name, spec) VALUES (gen_random_uuid(), %s, %s)",
            (f"p-{name}", spec),
        )
    c.execute("INSERT INTO sessions (id, slug, spec) VALUES (gen_random_uuid(), 's-none', '{}')")
    c.execute(
        "INSERT INTO sessions (id, slug, spec) VALUES (gen_random_uuid(), 's-str', %s)",
        ('{"runaway": "x"}',),
    )


def _read(c, table, key, name):
    return c.execute(f"SELECT spec FROM {table} WHERE {key} = %s", (name,)).fetchone()[0]


def _val(spec):
    r = spec["runaway"]
    return r.get("max_stop_continuations", "absent")


def test_stored_caps_1_to_4_raised_to_5_others_untouched(empty_dsn, tmp_path):
    _upto_0052(empty_dsn, tmp_path)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        _seed(c)
    assert run_migrations(empty_dsn) == [
        "0053", "0054", "0055", "0056", "0057", "0058", "0059", "0060",
    ]  # fmt: skip
    with psycopg.connect(empty_dsn) as c:
        for name, want in EXPECTED.items():
            assert _val(_read(c, "sessions", "slug", f"s-{name}")) == want, name
            assert _val(_read(c, "presets", "name", f"p-{name}")) == want, name
        assert _read(c, "sessions", "slug", "s-none") == {}
        assert _read(c, "sessions", "slug", "s-str") == {"runaway": "x"}
        assert _read(c, "sessions", "slug", "s-low")["replicas"] == 1
    assert run_migrations(empty_dsn) == []


def test_proposal_base_and_items_clamped(empty_dsn, tmp_path):
    _upto_0052(empty_dsn, tmp_path)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        pid = c.execute(
            "INSERT INTO change_proposals (kind, target_id, target_name, proposer_session,"
            " proposer_slug, proposer_superleader, expires_at)"
            " VALUES ('preset', 'x', 'x', gen_random_uuid(), 's', false, now() + interval '1 day')"
            " RETURNING id"
        ).fetchone()[0]
        c.execute(
            "INSERT INTO change_proposal_revisions"
            " (proposal_id, revision, reason, base, fingerprint)"
            " VALUES (%s, 1, 'r', %s, 'f')",
            (pid, json.dumps({"runaway": {"max_stop_continuations": 2}})),
        )
        c.execute(
            "INSERT INTO change_proposal_items (proposal_id, revision, path, before, after,"
            " security, \"grant\") VALUES (%s, 1, 'runaway', %s, %s, false, false)",
            (
                pid,
                json.dumps({"max_stop_continuations": 2}),
                json.dumps({"max_stop_continuations": 30}),
            ),
        )
    run_migrations(empty_dsn)
    with psycopg.connect(empty_dsn) as c:
        base = c.execute("SELECT base FROM change_proposal_revisions").fetchone()[0]
        before, after = c.execute("SELECT before, after FROM change_proposal_items").fetchone()
    assert base == {"runaway": {"max_stop_continuations": 5}}
    assert before == {"max_stop_continuations": 5}
    assert after == {"max_stop_continuations": 30}

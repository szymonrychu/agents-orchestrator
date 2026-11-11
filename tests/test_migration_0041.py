import json
import shutil
import uuid

import psycopg
import pytest

from agents_orchestrator.db.migrate import MIGRATIONS_DIR, run_migrations

FILE = MIGRATIONS_DIR / "0041_vault_refs.sql"


def _upto_0040(dsn, tmp_path):
    for path in MIGRATIONS_DIR.glob("*.sql"):
        if path.name < "0041":
            shutil.copy(path, tmp_path / path.name)
    run_migrations(dsn, tmp_path)


def _seed(dsn, name, spec):
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute(
            "INSERT INTO presets (id, name, spec) VALUES (gen_random_uuid(), %s, %s)",
            (name, json.dumps(spec)),
        )


def _preset(dsn, name):
    with psycopg.connect(dsn) as c:
        return c.execute(
            "SELECT spec, vault_reattach FROM presets WHERE name = %s", (name,)
        ).fetchone()


def test_reattach_lists_protected_names_and_strips_them_from_the_spec(empty_dsn, tmp_path):
    _upto_0040(empty_dsn, tmp_path)
    spec = {
        "env": {
            "A": {"value": "x", "protected": False, "expand": False},
            "Z": {"protected": True},
            "B": {"protected": True},
            "PLAIN": "text",
        }
    }
    _seed(empty_dsn, "p1", spec)
    _seed(empty_dsn, "p2", {"env": {"A": {"value": "x", "protected": False, "expand": False}}})
    _seed(empty_dsn, "p3", {"model": "opus"})
    assert run_migrations(empty_dsn) == ["0041", "0042", "0043", "0044"]
    assert _preset(empty_dsn, "p1") == (
        {"env": {"A": {"value": "x", "expand": False}, "PLAIN": "text"}},
        ["B", "Z"],
    )
    assert _preset(empty_dsn, "p2")[1] == []
    assert _preset(empty_dsn, "p3")[1] == []


def test_non_object_env_does_not_fail_the_migration(empty_dsn, tmp_path):
    _upto_0040(empty_dsn, tmp_path)
    _seed(empty_dsn, "ok", {"env": {"B": {"protected": True}}})
    _seed(empty_dsn, "noenv", {"model": "opus"})
    _seed(empty_dsn, "strenv", {"env": "oops"})
    _seed(empty_dsn, "arrenv", {"env": [1, 2]})
    _seed(empty_dsn, "nullenv", {"env": None})
    assert run_migrations(empty_dsn) == ["0041", "0042", "0043", "0044"]
    assert _preset(empty_dsn, "ok")[1] == ["B"]
    for name in ("noenv", "strenv", "arrenv", "nullenv"):
        assert _preset(empty_dsn, name)[1] == []


def test_rerun_is_a_noop_and_keeps_existing_names(empty_dsn, tmp_path):
    _upto_0040(empty_dsn, tmp_path)
    _seed(empty_dsn, "p1", {"env": {"B": {"protected": True}}})
    run_migrations(empty_dsn)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        c.execute(
            "UPDATE presets SET vault_reattach = vault_reattach || ARRAY['C'] WHERE name='p1'"
        )
        before = _preset(empty_dsn, "p1")
        c.execute(FILE.read_text())
        c.execute(FILE.read_text())
    assert _preset(empty_dsn, "p1") == (before[0], ["B", "C"])


async def test_set_vault_reattach_round_trips(pool):
    from agents_orchestrator.db import presets

    async with pool.connection() as conn:
        p = await presets.create(conn, "re", {})
        assert p.vault_reattach == []
        got = await presets.set_vault_reattach(conn, p.id, ["B", "A", "B"])
        assert got.vault_reattach == ["A", "B"]
        assert (await presets.get(conn, p.id)).vault_reattach == ["A", "B"]


# ---------------------------------------------- O10: the destructive part, on pre-apply data

SID_OLD = "aaaaaaaa-0000-4000-8000-000000000001"
SID_NONE = "aaaaaaaa-0000-4000-8000-000000000002"
LEGACY = "ps-1234abcd-abcdef-v3"
PROPOSER = "bbbbbbbb-0000-4000-8000-000000000001"
PLAIN_A = {"value": "x", "protected": False, "expand": False}
SECRET_B = {"protected": True, "expand": False}
# a stored session that names its protected variable in an MCP server env
WRAPPER_B = {"mcp_servers": {"s": {"type": "stdio", "command": "c", "env": {"K": "${B}"}}}}


def _proposal(c, pid, kind, status, revision, base, items):
    """A proposal at `revision` (every revision gets `items`: (path, before, after, secret))."""
    closed = status in ("applied", "denied", "withdrawn", "expired", "target_gone")
    c.execute(
        "INSERT INTO change_proposals (id, kind, target_id, target_name, proposer_session,"
        " proposer_slug, proposer_superleader, status, revision, expires_at, closed_at)"
        " VALUES (%s, %s, 't', 't', %s, 'boss', true, %s, %s, now() + interval '1 day',"
        " CASE WHEN %s THEN now() END)",
        (pid, kind, PROPOSER, status, revision, closed),
    )
    for rev in range(1, revision + 1):
        outcome = "revised" if rev < revision else ("applied" if status == "applied" else None)
        c.execute(
            "INSERT INTO change_proposal_revisions"
            " (proposal_id, revision, reason, base, fingerprint, outcome)"
            " VALUES (%s, %s, 'r', %s, 'f', %s)",
            (pid, rev, json.dumps(base), outcome),
        )
        for path, before, after, secret in items(rev):
            c.execute(
                "INSERT INTO change_proposal_items (proposal_id, revision, path, before, after,"
                ' security, "grant", secret) VALUES (%s, %s, %s, %s, %s, true, false, %s)',
                (pid, rev, path, json.dumps(before), json.dumps(after), secret),
            )


def _seed_pre_apply(dsn):
    """What a 6.x database holds: protected entries, legacy pins, the secret tables, proposals
    in every state with and without secret items, odd env shapes."""
    with psycopg.connect(dsn, autocommit=True) as c:
        for name, spec in (
            ("mixed", {"env": {"A": PLAIN_A, "B": SECRET_B, "S": "str", "L": [1]}}),
            ("noenv", {"model": "opus"}),
            ("strenv", {"env": "oops"}),
            ("nullenv", {"env": None}),
            ("arrspec", [1, 2]),
        ):
            c.execute(
                "INSERT INTO presets (id, name, spec) VALUES (gen_random_uuid(), %s, %s)",
                (name, json.dumps(spec)),
            )
        c.execute(
            "INSERT INTO sessions (id, slug, spec, secret_versions) VALUES (%s, 'old', %s, %s)",
            (
                SID_OLD,
                json.dumps({"env": {"A": PLAIN_A, "B": SECRET_B}, "wrapper": WRAPPER_B}),
                json.dumps({"B": LEGACY}),
            ),
        )
        c.execute(
            "INSERT INTO sessions (id, slug, spec) VALUES (%s, 'none', %s)",
            (SID_NONE, json.dumps({"env": "oops"})),
        )
        c.execute(
            "INSERT INTO preset_secrets (preset_id, name, version)"
            " SELECT id, 'B', 3 FROM presets WHERE name = 'mixed'"
        )
        c.execute(
            "INSERT INTO change_proposal_secrets"
            " (preset_id, name, version, proposal_id, revision, state)"
            " SELECT id, 'B', 4, 1, 1, 'staged' FROM presets WHERE name = 'mixed'"
        )
        base = {"env": {"A": PLAIN_A, "B": {**SECRET_B, "set": True, "version": 3}}}
        after_a = {"value": "y", "protected": False, "expand": False}
        _proposal(c, 1, "preset", "open", 1, base, lambda r: [("env.B", None, None, "set")])
        _proposal(c, 2, "preset", "rework", 1, base, lambda r: [("env.C", None, SECRET_B, None)])
        _proposal(c, 3, "preset", "open", 1, base, lambda r: [("env.A", PLAIN_A, after_a, None)])
        _proposal(c, 4, "preset", "applied", 1, base, lambda r: [("env.B", None, None, "set")])
        _proposal(c, 5, "session", "stale", 1, base, lambda r: [("env.B", SECRET_B, None, None)])
        _proposal(
            c, 6, "schedule", "open", 1, {"name": "n", "cron": "* * * * *"},
            lambda r: [("cron", "* * * * *", "0 * * * *", None)],
        )  # fmt: skip
        _proposal(
            c, 7, "preset", "open", 2, base,
            lambda r: [("env.B", None, None, "changed")] if r == 1 else [("model", "a", "b", None)],
        )  # fmt: skip


def _rows(dsn, sql):
    with psycopg.connect(dsn) as c:
        return c.execute(sql).fetchall()


def _proposals(dsn):
    rows = _rows(
        dsn,
        "SELECT p.id, p.status, r.outcome, p.closed_at IS NOT NULL"
        " FROM change_proposals p JOIN change_proposal_revisions r"
        " ON r.proposal_id = p.id AND r.revision = p.revision",
    )
    return {i: (status, outcome, closed) for i, status, outcome, closed in rows}


def _dump(dsn):
    """Every table the migration touches, as plain rows."""
    return {
        t: _rows(dsn, f"SELECT to_jsonb(x) FROM {t} x ORDER BY to_jsonb(x)::text")
        for t in (
            "presets",
            "sessions",
            "change_proposals",
            "change_proposal_revisions",
            "change_proposal_items",
        )
    }


def test_protected_entries_leave_presets_and_sessions_legacy_pins_stay(empty_dsn, tmp_path):
    _upto_0040(empty_dsn, tmp_path)
    _seed_pre_apply(empty_dsn)
    assert run_migrations(empty_dsn) == ["0041", "0042", "0043", "0044"]
    specs = dict(_rows(empty_dsn, "SELECT name, spec FROM presets"))
    assert specs["mixed"] == {"env": {"A": {"value": "x", "expand": False}, "S": "str", "L": [1]}}
    assert specs["noenv"] == {"model": "opus"}
    assert specs["strenv"] == {"env": "oops"}
    assert specs["nullenv"] == {"env": None}
    assert specs["arrspec"] == [1, 2]
    assert _rows(empty_dsn, "SELECT vault_reattach FROM presets WHERE name = 'mixed'") == [(["B"],)]
    sessions = {
        str(i): (spec, pins)
        for i, spec, pins in _rows(empty_dsn, "SELECT id, spec, secret_versions FROM sessions")
    }
    assert sessions[SID_OLD] == (
        {"env": {"A": {"value": "x", "expand": False}}, "wrapper": WRAPPER_B},
        {"B": LEGACY},
    )
    assert sessions[SID_NONE] == ({"env": "oops"}, None)


def test_secret_tables_and_the_item_column_are_dropped(empty_dsn, tmp_path):
    _upto_0040(empty_dsn, tmp_path)
    _seed_pre_apply(empty_dsn)
    run_migrations(empty_dsn)
    tables = {r[0] for r in _rows(empty_dsn, "SELECT tablename FROM pg_tables")}
    assert not {"preset_secrets", "change_proposal_secrets"} & tables
    columns = {
        r[0]
        for r in _rows(
            empty_dsn,
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = 'change_proposal_items'",
        )
    }
    assert "secret" not in columns


def test_active_proposals_with_secret_items_are_withdrawn(empty_dsn, tmp_path):
    _upto_0040(empty_dsn, tmp_path)
    _seed_pre_apply(empty_dsn)
    run_migrations(empty_dsn)
    assert _proposals(empty_dsn) == {
        1: ("withdrawn", "withdrawn", True),  # a secret item
        2: ("withdrawn", "withdrawn", True),  # a protected declaration added
        3: ("open", None, False),  # plain items only
        4: ("applied", "applied", True),  # closed: left as it was
        5: ("withdrawn", "withdrawn", True),  # session, a protected declaration removed
        6: ("open", None, False),  # schedule
        7: ("open", None, False),  # the secret item is in an older revision
    }


def test_open_proposals_lose_the_protected_key_and_still_apply(empty_dsn, tmp_path):
    from agents_orchestrator.spec import SessionSpec

    _upto_0040(empty_dsn, tmp_path)
    _seed_pre_apply(empty_dsn)
    run_migrations(empty_dsn)
    (before, after), = _rows(
        empty_dsn, "SELECT before, after FROM change_proposal_items WHERE proposal_id = 3"
    )  # fmt: skip
    assert before == {"value": "x", "expand": False}
    assert after == {"value": "y", "expand": False}
    SessionSpec.model_validate({"env": {"A": after}})
    (base,) = _rows(empty_dsn, "SELECT base FROM change_proposal_revisions WHERE proposal_id = 3")[
        0
    ]
    # the base is history: a protected entry stays as it was, the others lose the key
    assert base["env"] == {
        "A": {"value": "x", "expand": False},
        "B": {**SECRET_B, "set": True, "version": 3},
    }
    (schedule,) = _rows(
        empty_dsn, "SELECT base FROM change_proposal_revisions WHERE proposal_id = 6"
    )[0]
    assert schedule == {"name": "n", "cron": "* * * * *"}


async def test_migrated_rows_parse_and_the_old_session_renders_its_legacy_pin(empty_dsn, tmp_path):
    from test_render import TOKENS, make_settings

    from agents_orchestrator.db import presets as presets_repo
    from agents_orchestrator.db import sessions as sessions_repo
    from agents_orchestrator.db.pool import open_pool
    from agents_orchestrator.k8s import render

    _upto_0040(empty_dsn, tmp_path)
    _seed_pre_apply(empty_dsn)
    with psycopg.connect(empty_dsn, autocommit=True) as c:  # the odd shapes go first
        c.execute("DELETE FROM presets WHERE name <> 'noenv'")
        c.execute("DELETE FROM sessions WHERE slug <> 'old'")
        c.execute(
            "INSERT INTO presets (id, name, spec) VALUES (gen_random_uuid(), 'clean', %s)",
            (json.dumps({"env": {"A": PLAIN_A, "B": SECRET_B}}),),
        )
    run_migrations(empty_dsn)
    pool = await open_pool(empty_dsn)
    try:
        async with pool.connection() as conn:
            assert {p.name for p in await presets_repo.list_presets(conn)} == {"noenv", "clean"}
            old = await sessions_repo.get(conn, uuid.UUID(SID_OLD))
    finally:
        await pool.close()
    assert render.env_entries(old) == [
        {"name": "A", "value": "x"},
        {"name": "B", "valueFrom": {"secretKeyRef": {"name": LEGACY, "key": "value"}}},
    ]
    assert render.secret_env_names(old) == ["B"]
    # the `${B}` reference to the legacy pin is still valid and still rendered unescaped
    objs = render.render_session(old, make_settings(), TOKENS)
    config = next(o for o in objs if o["kind"] == "ConfigMap")["data"]["config.yaml"]
    assert "K: ${B}" in config


def test_a_second_run_changes_nothing(empty_dsn, tmp_path):
    _upto_0040(empty_dsn, tmp_path)
    _seed_pre_apply(empty_dsn)
    run_migrations(empty_dsn)
    once = _dump(empty_dsn)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        c.execute(FILE.read_text())
        c.execute(FILE.read_text())
    assert _dump(empty_dsn) == once


@pytest.mark.parametrize("table", ["presets", "sessions"])
def test_a_non_boolean_protected_flag_refuses_the_whole_migration(empty_dsn, tmp_path, table):
    _upto_0040(empty_dsn, tmp_path)
    _seed_pre_apply(empty_dsn)
    odd = json.dumps({"env": {"Q": {"value": "v", "protected": "yes"}}})
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        if table == "presets":
            c.execute(
                "INSERT INTO presets (id, name, spec) VALUES (gen_random_uuid(), 'odd', %s)",
                (odd,),
            )
        else:
            c.execute(
                "INSERT INTO sessions (id, slug, spec) VALUES (gen_random_uuid(), 'odd', %s)",
                (odd,),
            )
    before = _dump(empty_dsn)
    with pytest.raises(psycopg.errors.RaiseException, match="non-boolean protected flag"):
        run_migrations(empty_dsn)
    assert _dump(empty_dsn) == before
    tables = {r[0] for r in _rows(empty_dsn, "SELECT tablename FROM pg_tables")}
    assert "preset_secrets" in tables


async def test_a_protected_variable_removed_before_the_apply_does_not_come_back(
    empty_dsn, tmp_path
):
    """A pre-0041 PATCH that dropped or unprotected a protected entry left its str pin behind
    (replace_spec merged pins and never pruned them); the old render ignored it. 0041 drops every
    str pin whose name is not a protected entry of that session's spec, so the render matches."""
    from agents_orchestrator.db import sessions as sessions_repo
    from agents_orchestrator.db.pool import open_pool
    from agents_orchestrator.k8s import render

    sid = "aaaaaaaa-0000-4000-8000-000000000003"
    kept, dropped, unprotected = LEGACY, "ps-1234abcd-bcdef0-v1", "ps-1234abcd-cdef01-v2"
    vault_pin = {"value": "v", "entry": "e", "entry_env": "E", "version": 1}
    _upto_0040(empty_dsn, tmp_path)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        c.execute(
            "INSERT INTO sessions (id, slug, spec, secret_versions) VALUES (%s, 'gone', %s, %s)",
            (
                sid,
                json.dumps({"env": {"A": PLAIN_A, "C": SECRET_B, "D": PLAIN_A}}),
                json.dumps({"B": dropped, "C": kept, "D": unprotected, "V": vault_pin}),
            ),
        )
        c.execute(
            "INSERT INTO sessions (id, slug, spec, secret_versions) VALUES"
            " (gen_random_uuid(), 'nullpins', '{}', NULL),"
            " (gen_random_uuid(), 'strpins', %s, '\"x\"'),"
            " (gen_random_uuid(), 'strenv', %s, %s)",
            (json.dumps({"env": {}}), json.dumps({"env": "oops"}), json.dumps({"B": dropped})),
        )
    assert run_migrations(empty_dsn) == ["0041", "0042", "0043", "0044"]
    pins = dict(_rows(empty_dsn, "SELECT slug, secret_versions FROM sessions"))
    assert pins == {
        "gone": {"C": kept, "V": vault_pin},
        "nullpins": None,
        "strpins": "x",
        "strenv": {},
    }
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        c.execute(FILE.read_text())
    assert dict(_rows(empty_dsn, "SELECT slug, secret_versions FROM sessions")) == pins
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        c.execute("DELETE FROM sessions WHERE slug <> 'gone'")
        c.execute("UPDATE sessions SET secret_versions = secret_versions - 'V' WHERE slug = 'gone'")
    pool = await open_pool(empty_dsn)
    try:
        async with pool.connection() as conn:
            gone = await sessions_repo.get(conn, uuid.UUID(sid))
    finally:
        await pool.close()
    assert render.env_entries(gone) == [
        {"name": "A", "value": "x"},
        {"name": "C", "valueFrom": {"secretKeyRef": {"name": kept, "key": "value"}}},
        {"name": "D", "value": "x"},
    ]
    assert render.secret_env_names(gone) == ["C"]

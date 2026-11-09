import json
import shutil

import psycopg
import pytest

from agents_orchestrator.db.migrate import MIGRATIONS_DIR, run_migrations

pytestmark = pytest.mark.integration

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


def test_reattach_lists_protected_names_and_keeps_the_spec(empty_dsn, tmp_path):
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
    assert run_migrations(empty_dsn) == ["0041"]
    assert _preset(empty_dsn, "p1") == (spec, ["B", "Z"])
    assert _preset(empty_dsn, "p2")[1] == []
    assert _preset(empty_dsn, "p3")[1] == []


def test_non_object_env_does_not_fail_the_migration(empty_dsn, tmp_path):
    _upto_0040(empty_dsn, tmp_path)
    _seed(empty_dsn, "ok", {"env": {"B": {"protected": True}}})
    _seed(empty_dsn, "noenv", {"model": "opus"})
    _seed(empty_dsn, "strenv", {"env": "oops"})
    _seed(empty_dsn, "arrenv", {"env": [1, 2]})
    _seed(empty_dsn, "nullenv", {"env": None})
    assert run_migrations(empty_dsn) == ["0041"]
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

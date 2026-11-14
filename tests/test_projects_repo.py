import json
import shutil
import uuid

import psycopg
import psycopg.errors
import pytest
from factories import insert_session

from agents_orchestrator.db import presets, projects, sessions
from agents_orchestrator.db.migrate import MIGRATIONS_DIR, run_migrations
from agents_orchestrator.db.projects import (
    DefaultProject,
    ProjectArchived,
    ProjectExists,
    ProjectNotFound,
    UnknownProject,
)
from agents_orchestrator.spec import SessionSpec

MIGRATION = MIGRATIONS_DIR / "0037_projects.sql"


async def test_migration_seeds_default(pool):
    async with pool.connection() as conn:
        default = await projects.get_project(conn, "default")
        assert (default.slug, default.name, default.description) == ("default", "Default", "")
        assert default.archived_at is None
        await conn.execute(MIGRATION.read_text())
        assert await projects.list_projects(conn, archived=None) == [default]


def test_existing_presets_and_sessions_land_in_default(empty_dsn, tmp_path):
    for f in MIGRATIONS_DIR.glob("*.sql"):
        if f.name < "0037":
            shutil.copy(f, tmp_path / f.name)
    assert run_migrations(empty_dsn, tmp_path)[-1] == "0036"
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        c.execute("INSERT INTO presets (id, name, spec) VALUES (gen_random_uuid(), 'old', '{}')")
        c.execute(
            "INSERT INTO sessions (id, slug, spec) VALUES (gen_random_uuid(), 'old', %s)",
            (json.dumps({"replicas": 1}),),
        )
    assert run_migrations(empty_dsn) == [
        "0037", "0038", "0039", "0040", "0041", "0042", "0043", "0044", "0045", "0046", "0047",
    ]  # fmt: skip
    with psycopg.connect(empty_dsn) as c:
        assert {r[0] for r in c.execute("SELECT project FROM presets")} == {"default"}
        assert c.execute("SELECT project FROM sessions").fetchall() == [("default",)]


async def test_slug_check_refuses_bad_slugs(pool):
    async with pool.connection() as conn:
        for bad in ("A", "-x", "x" * 41):
            with pytest.raises(ValueError, match="slug"):
                await projects.create_project(conn, bad, "Name")
            with pytest.raises(psycopg.errors.CheckViolation):
                async with conn.transaction():
                    await conn.execute(
                        "INSERT INTO projects (slug, name) VALUES (%s, 'Name')", (bad,)
                    )
        for bad_name in ("", "x" * 81):
            with pytest.raises(ValueError, match="name"):
                await projects.create_project(conn, "ok", bad_name)
        with pytest.raises(ValueError, match="description"):
            await projects.create_project(conn, "ok", "Ok", "d" * 4001)
        edge = await projects.create_project(conn, "a" + "-" * 39, "x" * 80, "d" * 4000)
        assert edge.slug == "a" + "-" * 39
        assert await projects.get_project(conn, "ok") is None


async def test_create_duplicate_raises_project_exists(pool):
    async with pool.connection() as conn:
        made = await projects.create_project(conn, "alpha", "Alpha", "first")
        assert (made.slug, made.name, made.description, made.archived_at) == (
            "alpha",
            "Alpha",
            "first",
            None,
        )
        assert made.created_at == made.updated_at
        assert await projects.get_project(conn, "alpha") == made
        with pytest.raises(ProjectExists):
            await projects.create_project(conn, "alpha", "Other")
        with pytest.raises(ProjectExists):
            await projects.create_project(conn, "default", "Default again")


async def test_archive_default_raises(pool):
    async with pool.connection() as conn:
        with pytest.raises(DefaultProject):
            await projects.update_project(conn, "default", archived=True)
        renamed = await projects.update_project(conn, "default", name="Main", description="d")
        assert (renamed.name, renamed.description, renamed.archived_at) == ("Main", "d", None)
        with pytest.raises(ProjectNotFound):
            await projects.update_project(conn, "nope", name="X")
        with pytest.raises(ValueError, match="name"):
            await projects.update_project(conn, "default", name="")


async def test_archive_then_require_active_raises_project_archived(pool):
    async with pool.connection() as conn:
        made = await projects.create_project(conn, "beta", "Beta")
        assert await projects.require_active(conn, "beta") == made
        archived = await projects.update_project(conn, "beta", archived=True)
        assert archived.archived_at is not None and archived.updated_at >= made.updated_at
        again = await projects.update_project(conn, "beta", archived=True)
        assert again.archived_at == archived.archived_at
        for lock in (None, "share", "update"):
            async with conn.transaction():
                with pytest.raises(ProjectArchived):
                    await projects.require_active(conn, "beta", lock=lock)
        restored = await projects.update_project(conn, "beta", archived=False)
        assert restored.archived_at is None
        async with conn.transaction():
            assert (await projects.require_active(conn, "beta", lock="update")).slug == "beta"


async def test_require_active_unknown_raises(pool):
    async with pool.connection() as conn:
        for lock in (None, "share", "update"):
            with pytest.raises(UnknownProject):
                await projects.require_active(conn, "nope", lock=lock)
        with pytest.raises(ValueError, match="lock"):
            await projects.require_active(conn, "default", lock="bogus")


async def test_list_hides_archived_by_default(pool):
    async with pool.connection() as conn:
        await projects.create_project(conn, "zeta", "Zeta")
        await projects.create_project(conn, "alpha", "Alpha")
        await projects.update_project(conn, "zeta", archived=True)
        assert [p.slug for p in await projects.list_projects(conn)] == ["alpha", "default"]
        assert [p.slug for p in await projects.list_projects(conn, archived=True)] == ["zeta"]
        everything = await projects.list_projects(conn, archived=None)
        assert [p.slug for p in everything] == ["alpha", "default", "zeta"]
        assert all(p.presets is None and p.sessions is None for p in everything)


async def test_counts_presets_and_active_sessions(pool):
    async with pool.connection() as conn:
        await projects.create_project(conn, "alpha", "Alpha")
        await projects.create_project(conn, "empty", "Empty")
        moved = await presets.create(conn, "Moved", {})
        await conn.execute("UPDATE presets SET project = 'alpha' WHERE id = %s", (moved.id,))
    live = await insert_session(pool, slug="live")
    await insert_session(pool, slug="gone", desired="deleting")
    other = await insert_session(pool, slug="other")
    async with pool.connection() as conn:
        await conn.execute("UPDATE sessions SET project = 'alpha' WHERE id = %s", (live["id"],))
        await conn.execute("UPDATE sessions SET project = 'alpha' WHERE slug = 'gone'")
        assert await projects.project_counts(conn, "alpha") == {"presets": 1, "sessions": 1}
        assert await projects.project_counts(conn, "default") == {"presets": 1, "sessions": 1}
        assert await projects.project_counts(conn, "empty") == {"presets": 0, "sessions": 0}
        counted = await projects.list_projects(conn, counts=True)
        assert {p.slug: (p.presets, p.sessions) for p in counted} == {
            "alpha": (1, 1),
            "default": (1, 1),
            "empty": (0, 0),
        }
        assert other["project"] == "default"


async def test_set_project_moves_session(pool):
    async with pool.connection() as conn:
        await projects.create_project(conn, "alpha", "Alpha")
        s = await sessions.create(conn, "mover", SessionSpec())
        assert s.project == "default"
        moved = await sessions.set_project(conn, s.id, "alpha")
        assert moved.project == "alpha" and moved.generation == s.generation
        assert (await sessions.get(conn, s.id)).project == "alpha"
        with pytest.raises(UnknownProject):
            await sessions.set_project(conn, s.id, "nope")
        assert (await sessions.get(conn, s.id)).project == "alpha"
        with pytest.raises(sessions.SessionNotFound):
            await sessions.set_project(conn, uuid.uuid4(), "alpha")
        await sessions.request_delete(conn, s.id)
        with pytest.raises(sessions.SessionBeingDeleted):
            await sessions.set_project(conn, s.id, "default")


async def test_preset_project_fk_refuses_unknown_slug(pool):
    async with pool.connection() as conn:
        made = await presets.create(conn, "Fk", {})
        assert made.project == "default"
        await sessions.create(conn, "fk", SessionSpec())
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            async with conn.transaction():
                await conn.execute("UPDATE presets SET project = 'nope' WHERE id = %s", (made.id,))
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            async with conn.transaction():
                await conn.execute("UPDATE sessions SET project = 'nope'")

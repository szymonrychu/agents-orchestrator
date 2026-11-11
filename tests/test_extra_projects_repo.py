"""Extra projects of sessions and presets (T-120 slice 1): stored, read live, never rendered."""

from __future__ import annotations

import psycopg
import pytest
from factories import insert_session

from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import sessions as sessions_repo


async def _projects(pool, *slugs: str) -> None:
    async with pool.connection() as conn:
        for slug in slugs:
            await conn.execute(
                "INSERT INTO projects (slug, name) VALUES (%s, %s)", (slug, slug.upper())
            )


async def test_new_rows_have_no_extra_projects(pool):
    row = await insert_session(pool, slug="xp-new")
    async with pool.connection() as conn:
        session = await sessions_repo.get(conn, row["id"])
        preset = await presets_repo.create(conn, "xp-preset", {})
    assert session is not None
    assert session.extra_projects == ()
    assert session.projects == ("default",)
    assert preset.extra_projects == ()


async def test_set_extra_projects_is_live_and_bumps_nothing(pool):
    await _projects(pool, "a", "b", "c")
    row = await insert_session(pool, slug="xp-set", project="b")
    async with pool.connection() as conn:
        session = await sessions_repo.set_extra_projects(conn, row["id"], ["c", "a"])
        again = await sessions_repo.get(conn, row["id"])
    assert session.extra_projects == ("a", "c")
    assert again is not None and again.projects == ("b", "a", "c")
    assert again.generation == row["generation"]


async def test_extra_projects_are_capped_at_nine(pool):
    slugs = [f"p{n}" for n in range(10)]
    await _projects(pool, *slugs)
    row = await insert_session(pool, slug="xp-cap")
    async with pool.connection() as conn:
        await sessions_repo.set_extra_projects(conn, row["id"], slugs[:9])
        with pytest.raises(psycopg.errors.CheckViolation):
            await sessions_repo.set_extra_projects(conn, row["id"], slugs)


async def test_moving_home_into_an_extra_project_drops_it(pool):
    await _projects(pool, "a", "b")
    row = await insert_session(pool, slug="xp-move", project="a")
    async with pool.connection() as conn:
        await sessions_repo.set_extra_projects(conn, row["id"], ["b"])
        moved = await sessions_repo.set_project(conn, row["id"], "b")
    assert moved.project == "b"
    assert moved.extra_projects == ()


async def test_preset_extra_projects(pool):
    await _projects(pool, "a", "b")
    async with pool.connection() as conn:
        preset = await presets_repo.create(conn, "xp-p", {}, project="a")
        preset = await presets_repo.set_extra_projects(conn, preset.id, ["b"])
        assert preset.extra_projects == ("b",)
        moved = await presets_repo.update(conn, preset.id, preset.name, {}, project="b")
    assert moved.project == "b"
    assert moved.extra_projects == ()

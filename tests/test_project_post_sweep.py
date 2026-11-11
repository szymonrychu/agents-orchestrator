"""Project room post retention sweep (T-117 O6): real Postgres."""

from __future__ import annotations

import asyncio

from factories import insert_session
from test_api_agent import make_settings

from agents_orchestrator.api import project_post_sweep
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import project_posts


async def _aged_post(pool, project: str, sender, days: int) -> int:
    async with pool.connection() as conn:
        post_id = await project_posts.insert_post(
            conn,
            project=project,
            sender_kind="leader",
            sender_session_id=sender["id"],
            sender_slug=sender["slug"],
            text="hi",
            reply_to=None,
            addressed=[],
        )
        await conn.execute(
            "UPDATE project_posts SET created_at = now() - make_interval(days => %s) WHERE id = %s",
            (days, post_id),
        )
    return post_id


async def _ids(pool) -> list[int]:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT id FROM project_posts ORDER BY id")
        return [r[0] for r in await cur.fetchall()]


async def test_sweep_once_deletes_rows_older_than_retention(pool, dsn):
    async with pool.connection() as conn:
        await conn.execute("INSERT INTO projects (slug, name) VALUES ('a', 'A')")
    s = await insert_session(pool, project="a")
    old = await _aged_post(pool, "a", s, 31)
    new = await _aged_post(pool, "a", s, 29)
    settings = make_settings(dsn).model_copy(update={"project_chat_retention_days": 30})
    assert await project_post_sweep.sweep_once(pool, settings) == 1
    assert old not in await _ids(pool)
    assert await _ids(pool) == [new]


async def test_sweep_once_loops_batches(pool, dsn, monkeypatch):
    async with pool.connection() as conn:
        await conn.execute("INSERT INTO projects (slug, name) VALUES ('a', 'A')")
    s = await insert_session(pool, project="a")
    for _ in range(5):
        await _aged_post(pool, "a", s, 40)
    keep = await _aged_post(pool, "a", s, 1)
    monkeypatch.setattr(project_post_sweep, "BATCH", 2)
    settings = make_settings(dsn).model_copy(update={"project_chat_retention_days": 30})
    assert await project_post_sweep.sweep_once(pool, settings) == 5
    assert await _ids(pool) == [keep]


async def test_sweep_task_started_and_cancelled(dsn):
    app = create_app(make_settings(dsn))
    async with app.router.lifespan_context(app):
        task = app.state.project_post_sweep_task
        assert task.get_name() == "project_post_sweep"
        assert not task.done()
        await asyncio.sleep(0)
    assert task.cancelled() or task.done()

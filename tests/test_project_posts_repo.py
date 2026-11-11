"""Project room posts repository (migration 0044)."""

from __future__ import annotations

import pytest
from factories import insert_message, insert_session

from agents_orchestrator.db import project_posts, queue
from agents_orchestrator.db.project_posts import ProjectMark


async def _projects(pool, *slugs: str) -> None:
    async with pool.connection() as conn:
        for slug in slugs:
            await conn.execute(
                "INSERT INTO projects (slug, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (slug, slug.upper()),
            )


async def _post(pool, project: str, sender, text: str = "hi", **kw) -> int:
    async with pool.connection() as conn:
        return await project_posts.insert_post(
            conn,
            project=project,
            sender_kind=kw.pop("sender_kind", "leader"),
            sender_session_id=sender["id"],
            sender_slug=sender["slug"],
            text=text,
            reply_to=kw.pop("reply_to", None),
            addressed=kw.pop("addressed", []),
        )


async def test_insert_and_page_newest_first(pool):
    await _projects(pool, "a")
    s = await insert_session(pool, project="a")
    p1, p2, p3 = [await _post(pool, "a", s, f"t{n}") for n in range(3)]
    async with pool.connection() as conn:
        assert [
            p.id
            for p in await project_posts.page(conn, "a", before_id=None, after_id=None, limit=2)
        ] == [p3, p2]
        assert [
            p.id for p in await project_posts.page(conn, "a", before_id=p2, after_id=None, limit=10)
        ] == [p1]
        assert [
            p.id for p in await project_posts.page(conn, "a", before_id=None, after_id=p1, limit=10)
        ] == [p2, p3]
        newest = (await project_posts.page(conn, "a", before_id=None, after_id=None, limit=1))[0]
        assert await project_posts.last_post_id(conn, "a") == p3
        assert await project_posts.last_post_id(conn, "empty") is None
    assert (newest.project, newest.sender_kind, newest.sender_session_id) == (
        "a",
        "leader",
        s["id"],
    )
    assert (newest.sender_slug, newest.text, newest.reply_to) == (s["slug"], "t2", None)
    assert (newest.addressed, newest.recipients) == ([], [])


async def test_recipients_exclude_sender_superleader_inactive_other_project(pool):
    await _projects(pool, "a", "b")
    s1 = await insert_session(pool, project="a")
    s2 = await insert_session(pool, project="a", spec={"session_messaging": False})
    await insert_session(pool, project="a", spec={"session_messaging": True})
    await insert_session(pool, project="a", desired="deleting")
    await insert_session(pool, project="b")
    async with pool.connection() as conn:
        got = await project_posts.recipients(conn, "a", s1["id"])
    assert [(r.id, r.slug) for r in got] == [(s2["id"], s2["slug"])]


async def test_recipients_ordered_by_slug_and_set_recipients(pool):
    await _projects(pool, "a")
    s1 = await insert_session(pool, project="a", slug="zz-sender")
    await insert_session(pool, project="a", slug="mm")
    await insert_session(pool, project="a", slug="aa")
    post_id = await _post(pool, "a", s1)
    async with pool.connection() as conn:
        got = await project_posts.recipients(conn, "a", s1["id"])
        assert [r.slug for r in got] == ["aa", "mm"]
        await project_posts.set_recipients(conn, post_id, ["aa", "mm"])
        post = (await project_posts.page(conn, "a", before_id=None, after_id=None, limit=1))[0]
    assert post.recipients == ["aa", "mm"]


async def test_enqueue_fanout_sets_post_id_and_is_untargeted(pool):
    await _projects(pool, "a")
    s1 = await insert_session(pool, project="a")
    s2 = await insert_session(pool, project="a")
    post_id = await _post(pool, "a", s1)
    async with pool.connection() as conn:
        mid = await project_posts.enqueue_fanout(
            conn,
            s2["id"],
            "x",
            sender_session_id=s1["id"],
            sender_slug=s1["slug"],
            hop=2,
            post_id=post_id,
        )
        row = await queue._one(conn, "SELECT * FROM messages WHERE id = %s", (mid,))
    assert row["project_post_id"] == post_id
    assert row["target_agent_id"] is None
    assert (row["sender_slug"], row["hop"], row["lane"]) == (s1["slug"], 2, "regular")
    assert row["sender_session_id"] == s1["id"]


async def test_enqueue_fanout_refuses_deleting_session(pool):
    await _projects(pool, "a")
    s1 = await insert_session(pool, project="a")
    s2 = await insert_session(pool, project="a", desired="deleting")
    post_id = await _post(pool, "a", s1)
    async with pool.connection() as conn:
        with pytest.raises(queue.SessionBeingDeleted):
            await project_posts.enqueue_fanout(
                conn,
                s2["id"],
                "x",
                sender_session_id=s1["id"],
                sender_slug=s1["slug"],
                hop=0,
                post_id=post_id,
            )
        assert await queue._one(conn, "SELECT id FROM messages") is None


async def test_project_mark(pool):
    await _projects(pool, "a")
    s1 = await insert_session(pool, project="a")
    s2 = await insert_session(pool, project="a")
    p1 = await _post(pool, "a", s1)
    p2 = await _post(pool, "a", s1, reply_to=p1)
    async with pool.connection() as conn:
        m1 = await project_posts.enqueue_fanout(
            conn,
            s2["id"],
            "x",
            sender_session_id=s1["id"],
            sender_slug=s1["slug"],
            hop=0,
            post_id=p1,
        )
        m2 = await project_posts.enqueue_fanout(
            conn,
            s2["id"],
            "y",
            sender_session_id=s1["id"],
            sender_slug=s1["slug"],
            hop=0,
            post_id=p2,
        )
        plain = await queue.enqueue_message(conn, s2["id"], "z", sender_session_id=s1["id"])
        assert await project_posts.project_mark(conn, m1) == ProjectMark("a", p1, None)
        assert await project_posts.project_mark(conn, m2) == ProjectMark("a", p2, p1)
        assert await project_posts.project_mark(conn, plain) is None
        assert await project_posts.project_mark(conn, 999999) is None


async def test_reply_to_other_project_refused(pool):
    await _projects(pool, "a", "b")
    sa = await insert_session(pool, project="a")
    sb = await insert_session(pool, project="b")
    pa = await _post(pool, "a", sa)
    with pytest.raises(ValueError, match="reply_to"):
        await _post(pool, "b", sb, reply_to=pa)
    with pytest.raises(ValueError, match="reply_to"):
        await _post(pool, "b", sb, reply_to=999999)


async def test_counts(pool):
    await _projects(pool, "a", "b")
    s1 = await insert_session(pool, project="a")
    s2 = await insert_session(pool, project="a")
    sb = await insert_session(pool, project="b")
    p = await _post(pool, "a", s1)
    await _post(pool, "a", s1)
    await _post(pool, "a", s2)
    await _post(pool, "b", sb)
    async with pool.connection() as conn:
        for target in (s2, await insert_session(pool, project="a")):
            await project_posts.enqueue_fanout(
                conn,
                target["id"],
                "x",
                sender_session_id=s1["id"],
                sender_slug=s1["slug"],
                hop=0,
                post_id=p,
            )
        assert await project_posts.count_posts_by(conn, s1["id"], 3600) == 2
        assert await project_posts.count_posts_by(conn, sb["id"], 3600) == 1
        assert await project_posts.count_project_posts(conn, "a", 3600) == 3
        assert await project_posts.count_project_posts(conn, "b", 3600) == 1
        await conn.execute("UPDATE project_posts SET created_at = now() - interval '2 hours'")
        assert await project_posts.count_project_posts(conn, "a", 3600) == 0


async def test_sweep_older_deletes_only_old_rows(pool):
    await _projects(pool, "a")
    s = await insert_session(pool, project="a")
    old = await _post(pool, "a", s, "old")
    new = await _post(pool, "a", s, "new", reply_to=old)
    keep = await _post(pool, "a", s, "keep")
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE project_posts SET created_at = now() - interval '31 days' WHERE id = %s",
            (old,),
        )
        assert await project_posts.sweep_older(conn, 30) == 1
        assert await project_posts.sweep_older(conn, 30) == 0
        left = await project_posts.page(conn, "a", before_id=None, after_id=None, limit=10)
    assert [p.id for p in left] == [keep, new]
    assert left[1].reply_to is None


async def test_sweep_older_deletes_in_batches(pool):
    await _projects(pool, "a")
    s = await insert_session(pool, project="a")
    for _ in range(3):
        await _post(pool, "a", s)
    async with pool.connection() as conn:
        await conn.execute("UPDATE project_posts SET created_at = now() - interval '40 days'")
        assert await project_posts.sweep_older(conn, 30, batch=2) == 2
        assert await project_posts.sweep_older(conn, 30, batch=2) == 1


async def test_plain_message_unaffected_by_column(pool):
    s = await insert_session(pool)
    row = await insert_message(pool, s["id"])
    assert row["project_post_id"] is None

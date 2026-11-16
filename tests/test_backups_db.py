"""db.backups: versioned backups with a current pointer (Postgres)."""

import asyncio
import uuid

from factories import insert_session

from agents_orchestrator.db import backups


async def _save(conn, name="n", digest="d1", key=None, orphan=False, **kw):
    return await backups.record_save(
        conn,
        name=name,
        object_key=key or f"b/{name}/{uuid.uuid4()}.tar.zst",
        size=10,
        digest=digest,
        manifest=kw.get("manifest"),
        base_version=kw.get("base_version"),
        origin=kw.get("origin", "save"),
        session_slug="s",
        preset_id=None,
        project="p",
        created_by="actor",
        orphan=orphan,
    )


async def _states(pool, name):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT state FROM backup_versions WHERE name = %s ORDER BY created_at, state", (name,)
        )
        return sorted(r[0] for r in await cur.fetchall())


async def test_first_save_becomes_current(pool):
    async with pool.connection() as conn:
        result, v = await _save(conn)
        cur = await backups.current(conn, "n")
    assert result == "advanced"
    assert v.state == "current"
    assert cur == v


async def test_second_save_supersedes_first(pool):
    async with pool.connection() as conn:
        _, first = await _save(conn, digest="a")
        result, second = await _save(conn, digest="b")
        cur = await backups.current(conn, "n")
        page = await backups.versions(conn, "n", limit=10, cursor=None)
    assert result == "advanced"
    assert cur.id == second.id
    assert [(v.id, v.state) for v in page.items] == [
        (second.id, "current"),
        (first.id, "superseded"),
    ]


async def test_unchanged_digest_is_noop(pool):
    async with pool.connection() as conn:
        _, first = await _save(conn, digest="a")
        result, v = await _save(conn, digest="a")
        page = await backups.versions(conn, "n", limit=10, cursor=None)
    assert (result, v) == ("unchanged", None)
    assert [x.id for x in page.items] == [first.id]


async def test_orphan_does_not_move_pointer(pool):
    async with pool.connection() as conn:
        _, first = await _save(conn, digest="a")
        result, orphan = await _save(conn, digest="b", orphan=True)
        cur = await backups.current(conn, "n")
    assert result == "orphan"
    assert orphan.state == "orphan"
    assert cur.id == first.id


async def test_concurrent_saves_one_current(pool):
    async def one(digest):
        async with pool.connection() as conn, conn.transaction():
            return await _save(conn, digest=digest)

    results = await asyncio.gather(one("a"), one("b"))
    assert [r[0] for r in results] == ["advanced", "advanced"]
    assert await _states(pool, "n") == ["current", "superseded"]
    async with pool.connection() as conn:
        cur = await backups.current(conn, "n")
    assert cur is not None
    assert [r for r in await _states(pool, "n")].count("current") == 1
    async with pool.connection() as conn:
        rows = await conn.execute(
            "SELECT id FROM backup_versions WHERE name='n' AND state='current'"
        )
        assert [r[0] for r in await rows.fetchall()] == [cur.id]


async def test_delete_current_refused_while_others_exist(pool):
    async with pool.connection() as conn:
        _, first = await _save(conn, digest="a")
        _, second = await _save(conn, digest="b")
        assert not await backups.delete_version(conn, "n", second.id)
        assert await backups.delete_version(conn, "n", first.id)
        assert await backups.get_version(conn, "n", first.id) is None
        assert await backups.delete_version(conn, "n", second.id)
        assert await backups.current(conn, "n") is None
        page = await backups.list_names(conn, q=None, limit=10, cursor=None)
    assert page.items == []


async def test_delete_name_returns_object_keys(pool):
    async with pool.connection() as conn:
        _, a = await _save(conn, digest="a", key="k/a")
        _, b = await _save(conn, digest="b", key="k/b")
        await _save(conn, name="other", digest="c", key="k/c")
        keys = await backups.delete_name(conn, "n")
        assert sorted(keys) == ["k/a", "k/b"]
        assert await backups.delete_name(conn, "n") == []
        assert await backups.current(conn, "other") is not None


async def test_list_names_filters_and_pages(pool):
    async with pool.connection() as conn:
        for n in ("alpha", "beta", "alp_x"):
            await _save(conn, name=n, digest=n)
        await _save(conn, name="alpha", digest="again")
        first = await backups.list_names(conn, q=None, limit=2, cursor=None)
        second = await backups.list_names(conn, q=None, limit=2, cursor=first.next_cursor)
        filtered = await backups.list_names(conn, q="alp_", limit=10, cursor=None)
    assert [r.name for r in first.items] == ["alp_x", "alpha"]
    assert first.items[1].version_count == 2
    assert [r.name for r in second.items] == ["beta"]
    assert second.next_cursor is None
    assert [r.name for r in filtered.items] == ["alp_x"]


async def test_versions_paging_and_touch(pool):
    async with pool.connection() as conn:
        for d in "abc":
            await _save(conn, digest=d)
        p1 = await backups.versions(conn, "n", limit=2, cursor=None)
        p2 = await backups.versions(conn, "n", limit=2, cursor=p1.next_cursor)
        await backups.touch_used(conn, "n")
        row = (await backups.list_names(conn, q=None, limit=5, cursor=None)).items[0]
    assert len(p1.items) == 2
    assert len(p2.items) == 1
    assert p2.next_cursor is None
    assert row.last_used_at is not None


async def test_referenced_versions_reads_session_status(pool):
    async with pool.connection() as conn:
        _, v = await _save(conn)
    base = uuid.uuid4()
    session = await insert_session(pool)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET status = status || jsonb_build_object('backup',"
            " jsonb_build_array(jsonb_build_object('version', %s::text, 'base_version', %s::text)))"
            " WHERE id = %s",
            (str(v.id), str(base), session["id"]),
        )
        refs = await backups.referenced_versions(conn)
    assert refs == {v.id, base}

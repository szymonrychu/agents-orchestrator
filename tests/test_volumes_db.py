"""db.volumes: named shared volumes, claims and audit events (Postgres)."""

import uuid

import pytest

from agents_orchestrator.db import volumes


async def _create(conn, name="data", projects=("p",), size="10Gi", **kw):
    return await volumes.create(
        conn,
        name=name,
        projects=list(projects),
        size=size,
        description="",
        labels=[],
        storage_class="rook-ceph-rwx",
        created_by="alice",
        actor_kind="user",
        actor="alice",
        **kw,
    )


async def _kinds(conn, vid):
    page = await volumes.events(conn, vid, limit=50)
    return [e.kind for e in reversed(page.items)]


async def _grow(conn, vid, size, **kw):
    return await volumes.update(conn, vid, size=size, actor_kind="user", actor="alice", **kw)


async def test_create_round_trips_and_writes_created_event(pool):
    async with pool.connection() as conn:
        v = await _create(conn)
        assert (v.pvc_name, v.state) == ("vol-data", "provisioning")
        assert await volumes.get(conn, v.id) == v
        assert await volumes.by_name(conn, "data") == v
        assert await _kinds(conn, v.id) == ["created"]


async def test_duplicate_live_name_refused_and_reusable_after_delete(pool):
    async with pool.connection() as conn:
        v = await _create(conn)
        with pytest.raises(volumes.VolumeExists):
            await _create(conn)
        await volumes.mark_deleting(conn, v.id)
        await volumes.finish_delete(conn, v.id)
        assert await volumes.get(conn, v.id) is None
        again = await _create(conn)
        assert again.id != v.id
        assert await _kinds(conn, v.id) == ["created", "deleted"]


async def test_grow_sets_resizing_and_writes_event(pool):
    async with pool.connection() as conn:
        v = await _create(conn)
        grown = await _grow(conn, v.id, "20Gi")
        assert (grown.size, grown.state) == ("20Gi", "resizing")
        assert await _kinds(conn, v.id) == ["created", "resized"]


async def test_shrink_refused(pool):
    async with pool.connection() as conn:
        v = await _create(conn)
        with pytest.raises(volumes.ShrinkRefused):
            await _grow(conn, v.id, "5Gi")


@pytest.mark.parametrize("same", ["10Gi", "10240Mi"])
async def test_equal_size_is_noop(pool, same):
    async with pool.connection() as conn:
        v = await _create(conn)
        after = await _grow(conn, v.id, same)
        assert (after.size, after.state) == ("10Gi", "provisioning")
        assert await _kinds(conn, v.id) == ["created"]


async def test_over_cap_refused(pool):
    async with pool.connection() as conn:
        v = await _create(conn)
        with pytest.raises(volumes.OverCap):
            await _grow(conn, v.id, "201Gi")


async def test_list_by_project_matches_any_of_several(pool):
    async with pool.connection() as conn:
        a = await _create(conn, "a", projects=("p1", "p2"))
        await _create(conn, "b", projects=("p3",))
        assert [v.id for v in await volumes.list(conn, project="p2")] == [a.id]
        assert len(await volumes.list(conn, project=None)) == 2


async def test_release_claims_sets_released_at_and_writes_event(pool):
    async with pool.connection() as conn:
        v = await _create(conn)
        session = uuid.uuid4()
        await volumes.claim(
            conn, v.id, session_id=session, preset_id=None, mode="rw", backup_restore=True
        )
        assert len(await volumes.unreleased_claims(conn, v.id)) == 1
        released = await volumes.release_claims(
            conn, session, actor_kind="system", actor="reconciler"
        )
        assert [c.volume_id for c in released] == [v.id]
        assert released[0].released_at is not None
        assert await volumes.unreleased_claims(conn, v.id) == []
        assert (await _kinds(conn, v.id))[-1] == "released"

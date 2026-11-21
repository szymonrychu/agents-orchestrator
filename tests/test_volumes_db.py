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
        await volumes.set_state(conn, v.id, "ready")
        grown = await _grow(conn, v.id, "20Gi")
        assert (grown.size, grown.state) == ("20Gi", "resizing")
        assert await _kinds(conn, v.id) == ["created", "resized"]


async def test_shrink_refused(pool):
    async with pool.connection() as conn:
        v = await _create(conn)
        await volumes.set_state(conn, v.id, "ready")
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
        await volumes.set_state(conn, v.id, "ready")
        session = uuid.uuid4()
        await volumes.claim(
            conn,
            volume_id=v.id,
            session_id=session,
            preset_id=None,
            mode="rw",
            backup_restore=True,
            session_slug="s",
            current_version=None,
        )
        assert len(await volumes.unreleased_claims(conn, v.id)) == 1
        released = await volumes.release_claims(
            conn, session, actor_kind="system", actor="reconciler"
        )
        assert [c.volume_id for c in released] == [v.id]
        assert released[0].released_at is not None
        assert await volumes.unreleased_claims(conn, v.id) == []
        assert (await _kinds(conn, v.id))[-1] == "released"


async def test_exclude_round_trips_and_update_writes_event(pool):
    async with pool.connection() as conn:
        v = await _create(conn, exclude=["tmp", "cache/x"])
        assert (await volumes.get(conn, v.id)).exclude == ["tmp", "cache/x"]
        upd = await volumes.update(conn, v.id, exclude=["logs"], actor_kind="user", actor="alice")
        assert upd.exclude == ["logs"]
        assert await _kinds(conn, v.id) == ["created", "exclude_updated"]


@pytest.mark.parametrize("bad", ["/abs", "a/../b", ""])
async def test_invalid_exclude_refused(pool, bad):
    async with pool.connection() as conn:
        with pytest.raises(ValueError):
            await _create(conn, exclude=[bad])
        v = await _create(conn)
        with pytest.raises(ValueError):
            await volumes.update(conn, v.id, exclude=[bad], actor_kind="user", actor="alice")


@pytest.mark.parametrize("state", ["provisioning", "resizing", "restoring", "saving", "deleting"])
async def test_grow_refused_unless_ready_and_keeps_state(pool, state):
    async with pool.connection() as conn:
        v = await _create(conn)
        await volumes.set_state(conn, v.id, state)
        with pytest.raises(volumes.VolumeBusy):
            await _grow(conn, v.id, "20Gi")
        got = await volumes.get(conn, v.id)
        assert (got.size, got.state) == ("10Gi", state)


async def test_deleting_only_leaves_through_finish_delete(pool):
    async with pool.connection() as conn:
        v = await _create(conn)
        await volumes.mark_deleting(conn, v.id)
        with pytest.raises(volumes.VolumeBusy):
            await volumes.set_state(conn, v.id, "ready")
        await volumes.finish_delete(conn, v.id)
        assert await volumes.get(conn, v.id) is None


@pytest.mark.parametrize("bad", ["data\n", "Data", "-a", ""])
async def test_invalid_name_refused(pool, bad):
    async with pool.connection() as conn:
        with pytest.raises(ValueError):
            await _create(conn, bad)


async def test_equal_size_allowed_even_if_cap_lowered(pool):
    async with pool.connection() as conn:
        v = await _create(conn, size="10Gi")
        after = await _grow(conn, v.id, "10Gi", max_size="5Gi")
        assert after.size == "10Gi"


@pytest.mark.parametrize("bad", ["1e999999999", "1" + "0" * 40, "1e19"])
async def test_hostile_size_refused(pool, bad):
    async with pool.connection() as conn:
        with pytest.raises(ValueError):
            await _create(conn, size=bad)


async def test_claim_event_only_when_created_or_reopened_and_reclaim_clears_release(pool):
    async with pool.connection() as conn:
        v = await _create(conn)
        await volumes.set_state(conn, v.id, "ready")
        s = uuid.uuid4()
        kw = dict(
            volume_id=v.id,
            session_id=s,
            preset_id=None,
            mode="rw",
            backup_restore=False,
            session_slug="s",
            current_version=None,
        )
        await volumes.claim(conn, **kw)
        await volumes.claim(conn, **kw)
        assert await _kinds(conn, v.id) == ["created", "mounted"]
        await volumes.release_claims(conn, s, actor_kind="system", actor="reconciler")
        again = await volumes.claim(conn, **kw)
        assert again.claim.released_at is None
        assert len(await volumes.unreleased_claims(conn, v.id)) == 1
        assert await _kinds(conn, v.id) == ["created", "mounted", "released", "mounted"]


async def test_projects_trimmed_deduped_and_empty_refused(pool):
    async with pool.connection() as conn:
        v = await _create(conn, projects=(" a ", "b", "a"))
        assert v.projects == ["a", "b"]
        for bad in ([], [""], ["  "]):
            with pytest.raises(ValueError):
                await volumes.update(conn, v.id, projects=bad, actor_kind="user", actor="alice")
            with pytest.raises(ValueError):
                await _create(conn, "other", projects=bad)


async def test_labels_normalised_and_invalid_refused(pool):
    async with pool.connection() as conn:
        v = await _create(conn, labels=[" Foo ", "foo", "bar"])
        assert v.labels == ["foo", "bar"]
        for bad in (["has space"], ["project:x"]):
            with pytest.raises(ValueError):
                await volumes.update(conn, v.id, labels=bad, actor_kind="user", actor="alice")

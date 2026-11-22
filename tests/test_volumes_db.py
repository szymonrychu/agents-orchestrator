"""db.volumes: named shared volumes, claims and audit events (Postgres)."""

import asyncio
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


async def test_two_on_demand_saves_one_starts_and_one_is_busy(pool):
    async with pool.connection() as conn:
        v = await _create(conn)
        await volumes.set_state(conn, v.id, "ready")

    async def attempt(trigger):
        async with pool.connection() as conn:
            try:
                return await volumes.request_save(
                    conn, v.id, trigger=trigger, session_slug=None, actor="alice"
                )
            except volumes.VolumeBusy:
                return "busy"

    results = await asyncio.gather(attempt("user"), attempt("agent"))
    assert sorted(r if r == "busy" else "started" for r in results) == ["busy", "started"]
    async with pool.connection() as conn:
        assert (await _kinds(conn, v.id)).count("save_started") == 1
        assert (
            await volumes.request_save(
                conn, v.id, trigger="release", session_slug="s", actor="system"
            )
            is None
        )


async def _claim_as(conn, vid, session_id, mode="rw", backup_restore=True):
    return await volumes.claim(
        conn,
        volume_id=vid,
        session_id=session_id,
        preset_id=None,
        mode=mode,
        backup_restore=backup_restore,
        session_slug="s",
        current_version=None,
    )


async def _release(conn, session_id):
    await volumes.release_claims(conn, session_id, actor_kind="system", actor="t")


async def _save(conn, vid, kind="saved"):
    await volumes.request_save(conn, vid, trigger="user", session_slug=None, actor="alice")
    await volumes.finish_save(conn, vid, kind=kind)


@pytest.mark.parametrize(
    ("kind", "writer_released", "dirty"),
    [
        ("saved", "before", False),
        ("save_unchanged", "before", False),
        ("save_failed", "before", True),
        ("saved", "during", True),
        ("saved", "after", True),
    ],
)
async def test_a_save_cleans_only_the_backup_writes_made_before_it_started(
    pool, kind, writer_released, dirty
):
    async with pool.connection() as conn:
        v = await _create(conn)
        await volumes.set_state(conn, v.id, "ready")
        writer = uuid.uuid4()
        await _claim_as(conn, v.id, writer)
        if writer_released == "before":
            await _release(conn, writer)
        await volumes.request_save(conn, v.id, trigger="user", session_slug=None, actor="alice")
        if writer_released == "during":
            await _release(conn, writer)
        await volumes.finish_save(conn, v.id, kind=kind)
        if writer_released == "after":
            await _release(conn, writer)
        assert (await volumes.get(conn, v.id)).backup_dirty is dirty


@pytest.mark.parametrize(("pending_kind", "dirty"), [("saved", False), ("save_failed", True)])
async def test_the_pending_save_after_a_writer_released_mid_save_decides_the_dirty_flag(
    pool, pending_kind, dirty
):
    async with pool.connection() as conn:
        v = await _create(conn)
        await volumes.set_state(conn, v.id, "ready")
        writer = uuid.uuid4()
        await _claim_as(conn, v.id, writer)
        await volumes.request_save(conn, v.id, trigger="user", session_slug=None, actor="alice")
        await _release(conn, writer)
        await volumes.request_save(
            conn, v.id, trigger="release", session_slug="s", actor="reconciler"
        )
        await volumes.finish_save(conn, v.id, kind="saved")
        assert (await volumes.get(conn, v.id)).backup_dirty
        assert await volumes.start_pending_save(conn, v.id) is not None
        await volumes.finish_save(conn, v.id, kind=pending_kind)
        assert (await volumes.get(conn, v.id)).backup_dirty is dirty


@pytest.mark.parametrize(
    ("before", "after", "dirty"),
    [
        (("ro", False), ("rw", True), True),
        (("rw", True), ("ro", False), True),
        (("ro", False), ("rw", False), False),
    ],
)
async def test_a_changed_attachment_updates_the_active_claim_and_its_backup_writes_count(
    pool, before, after, dirty
):
    async with pool.connection() as conn:
        v = await _create(conn)
        await volumes.set_state(conn, v.id, "ready")
        s = uuid.uuid4()
        await _claim_as(conn, v.id, s, *before)
        await _save(conn, v.id)
        await _claim_as(conn, v.id, s, *after)
        (c,) = await volumes.unreleased_claims(conn, v.id)
        assert (c.mode, c.backup_restore) == after
        await _release(conn, s)
        assert (await volumes.get(conn, v.id)).backup_dirty is dirty

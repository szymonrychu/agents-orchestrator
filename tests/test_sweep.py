"""The backup cleanup planner, the references from the database and the executor."""

from __future__ import annotations

import logging
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from test_api_backups import PRE, add_version, settings_for

from agents_orchestrator.cache import sweep
from agents_orchestrator.cache.policy import SweepPolicy
from agents_orchestrator.cache.presign import ArchiveObject, DeleteFailure
from agents_orchestrator.cache.sweep import References, plan_sweep, references, run_sweep
from agents_orchestrator.db import backups, sessions, volumes
from agents_orchestrator.spec import SessionSpec

NOW = datetime(2026, 10, 31, 12, tzinfo=UTC)
DAY = timedelta(days=1)


def ver(name, age_days, *, state="superseded", vid=None, size=100):
    vid = vid or uuid.uuid4()
    return backups.Version(
        id=vid,
        name=name,
        object_key=f"{PRE}b/{name}/{vid.hex[:8]}.tar.zst",
        size=size,
        digest=vid.hex,
        manifest=None,
        base_version=None,
        origin="save",
        session_slug=None,
        preset_id=None,
        project=None,
        created_by="a",
        created_at=NOW - timedelta(days=age_days),
        state=state,
    )


def plan(versions, *, objects=(), refs=None, activity=None, days=30, keep=3, grace=600):
    activity = activity if activity is not None else {}
    return plan_sweep(
        versions=versions,
        activity=activity,
        objects=objects,
        refs=refs or References(),
        now=NOW,
        retention_days=days,
        keep_newest=keep,
        grace_seconds=grace,
        prefix=PRE,
    )


def history(name, ages):
    """Versions of `name`, `ages` newest first; the first is current."""
    return [
        ver(name, age, state="current" if i == 0 else "superseded") for i, age in enumerate(ages)
    ]


# ------------------------------------------------------------------ planner


def test_sweep_keeps_current_and_referenced():
    vs = history("proj/a", [1, 40, 50, 60, 70])
    refs = References(versions={vs[3].id: ["alpha"]})
    p = plan(vs, keep=1, activity={"proj/a": NOW - DAY}, refs=refs)
    assert set(p.keys) == {vs[2].object_key, vs[4].object_key}
    assert p.reason(vs[0].object_key) == "kept: the current version"
    assert p.reason(vs[3].object_key).startswith("protected")
    assert p.reason(vs[1].object_key).startswith("kept: among the newest")


def test_ancient_current_is_kept_while_the_name_is_active():
    vs = history("proj/a", [400])
    assert plan(vs, activity={"proj/a": NOW - DAY}).keys == []


def test_sweep_deletes_stale_name():
    vs = history("proj/a", [40, 50, 60]) + history("proj/b", [2])
    p = plan(vs, activity={"proj/a": NOW - 45 * DAY, "proj/b": NOW - DAY})
    assert set(p.keys) == {v.object_key for v in vs[:3]}
    assert all(d.whole_name for d in p.deletes)
    assert p.deletes[0].reason.startswith("name not restored or saved")


def test_a_stale_name_with_a_referenced_version_is_kept():
    vs = history("proj/a", [40, 50])
    act = {"proj/a": NOW - 45 * DAY}
    assert plan(vs, activity=act, refs=References(versions={vs[1].id: ["x"]}), keep=5).keys == []


def test_a_stale_name_with_a_young_version_is_kept():
    vs = history("proj/a", [0.0001, 50])
    assert plan(vs, activity={"proj/a": NOW - 45 * DAY}, keep=5).keys == []


def test_count_alone_and_age_alone_do_not_delete():
    vs = history("proj/a", [1, 2, 3, 4, 5])
    assert plan(vs, keep=1, activity={"proj/a": NOW}).keys == []  # all young
    old = history("proj/b", [100, 110, 120])
    assert plan(old, keep=5, activity={"proj/b": NOW}).keys == []  # within the newest K


def test_grace_period_keeps_a_version_that_may_be_mid_save():
    vs = history("proj/a", [0, 0.001, 50])
    vs[1] = replace(vs[1], created_at=NOW - timedelta(seconds=30))
    p = plan(vs, keep=0, activity={"proj/a": NOW})
    assert p.reason(vs[1].object_key) == "kept: younger than the grace period"
    assert p.keys == [vs[2].object_key]


def test_orphan_objects_are_deleted_by_age_only():
    owned = ver("proj/a", 1, state="current")
    objs = [
        ArchiveObject(owned.object_key, 1, NOW - 100 * DAY),
        ArchiveObject(f"{PRE}b/proj/a/orphan-old.tar.zst", 5, NOW - 40 * DAY),
        ArchiveObject(f"{PRE}b/proj/a/orphan-fresh.tar.zst", 5, NOW - 5 * DAY),
        ArchiveObject(f"{PRE}b/proj/a/orphan-grace.tar.zst", 5, NOW - timedelta(seconds=5)),
        ArchiveObject(f"{PRE}b/proj/a/readme.txt", 5, NOW - 400 * DAY),
    ]
    p = plan([owned], objects=objs, activity={"proj/a": NOW})
    assert p.keys == [f"{PRE}b/proj/a/orphan-old.tar.zst"]


def test_an_object_in_a_run_plan_is_protected_and_without_it_is_planned():
    obj = ArchiveObject(f"{PRE}b/proj/a/inflight.tar.zst", 5, NOW - 400 * DAY)
    assert plan([], objects=[obj]).keys == [obj.key]
    assert plan([], objects=[obj], refs=References(planned_keys={obj.key})).keys == []


def test_legacy_flat_archives_are_never_planned_for_deletion():
    legacy = [
        ArchiveObject(f"{PRE}proj-a.tar.zst", 5, NOW - 400 * DAY),
        ArchiveObject(f"{PRE}proj/a.tar.zst", 5, NOW - 400 * DAY),
        ArchiveObject(f"{PRE}preset-0123456789abcdef.tar.zst", 5, NOW - 400 * DAY),
    ]
    assert plan([], objects=legacy).keys == []


def test_transcripts_are_deleted_by_age_only_and_respect_protection():
    key = f"{PRE}sessions/old.tar.zst"
    kept = f"{PRE}sessions/live.tar.zst"
    objs = [ArchiveObject(key, 1, NOW - 90 * DAY), ArchiveObject(kept, 1, NOW - 90 * DAY)]
    p = plan([], objects=objs, refs=References(transcripts={kept}))
    assert p.keys == [key]
    assert p.reason(kept).startswith("protected")


@pytest.mark.parametrize("seed", range(20))
def test_invariants_on_random_listings(seed):
    import random

    rng = random.Random(seed)
    vs: list[backups.Version] = []
    for n in range(4):
        ages = sorted(rng.uniform(0, 120) for _ in range(rng.randint(1, 6)))
        vs += history(f"n/{n}", ages)
    refs = References(versions={v.id: ["s"] for v in rng.sample(vs, 3)})
    act = {f"n/{n}": NOW - rng.uniform(0, 100) * DAY for n in range(4)}
    p = plan(vs, refs=refs, activity=act, keep=rng.randint(0, 3))
    deleted = {d.version_id for d in p.deletes}
    assert not deleted & set(refs.versions)
    for name in {v.name for v in vs}:
        group = [v for v in vs if v.name == name]
        gone = {v.id for v in group} & deleted
        current = next(v for v in group if v.state == "current")
        if current.id in deleted:  # only a whole-name delete takes the current version
            assert gone == {v.id for v in group}
    assert [d.key for d in p.decisions] == sorted(d.key for d in p.decisions)


# ------------------------------------------------------------------ references


TRANSCRIPTS = SessionSpec(
    volumes=[
        {
            "volume": "11111111-1111-4111-8111-111111111111",
            "mount_path": "/workdir/workdir",
            "mode": "rw",
            "backup_restore": True,
        }
    ]
)


async def test_references_cover_versions_and_transcripts(pool, dsn):
    v = await add_version(pool, "proj/a")
    async with pool.connection() as conn:
        s = await sessions.create(conn, "alpha", TRANSCRIPTS)
        await conn.execute(
            "UPDATE sessions SET status = jsonb_build_object('backup', jsonb_build_object("
            "'entries', jsonb_build_array(jsonb_build_object("
            "'version', %s::text, 'base_version', %s::text)))) WHERE id = %s",
            (str(v.id), str(uuid.uuid4()), s.id),
        )
        await sessions.create(conn, "plain", SessionSpec())
        refs = await references(conn, settings_for(dsn))
    assert refs.versions == {v.id: ["alpha"]}  # the unknown base_version is not a reference
    assert refs.transcripts == {f"{PRE}sessions/alpha.tar.zst"}


async def test_a_deleted_session_references_nothing(pool, dsn):
    async with pool.connection() as conn:
        gone = await sessions.create(conn, "gone", TRANSCRIPTS)
        await sessions.request_delete(conn, gone.id)
        await sessions.purge(conn, gone.id)
        refs = await references(conn, settings_for(dsn))
    assert refs.versions == {} and refs.transcripts == set()


async def test_content_version_and_current_of_volume_names_survive_a_sweep(pool, dsn):
    loose = await add_version(pool, "volume-data", digest="c" * 64, age=NOW - 350 * DAY)
    old = await add_version(pool, "volume-data", digest="a" * 64, age=NOW - 400 * DAY)
    newer = await add_version(pool, "volume-data", digest="b" * 64, age=NOW - 300 * DAY)
    async with pool.connection() as conn, conn.transaction():
        v = await volumes.create(
            conn,
            name="data",
            projects=["p"],
            size="1Gi",
            description="",
            storage_class="c",
            created_by="a",
            actor_kind="user",
            actor="a",
        )
        await conn.execute("UPDATE volumes SET content_version = %s WHERE id = %s", (old.id, v.id))
        refs = await references(conn, settings_for(dsn))
        assert await backups.referenced_versions(conn) >= {old.id, newer.id}
        found = await backups.all_versions(conn)
    assert set(refs.versions) == {old.id, newer.id}
    p = plan_sweep(
        versions=found,
        activity={},
        objects=[],
        refs=refs,
        now=NOW + 1000 * DAY,
        retention_days=30,
        keep_newest=0,
        grace_seconds=600,
        prefix=PRE,
    )
    deleted = {d.key for d in p.decisions if d.delete}
    assert deleted == {loose.object_key}


# ------------------------------------------------------------------ executor


class Fake:
    def __init__(self, objects=()):
        self.objects = list(objects)
        self.deleted: list[list[str]] = []
        self.fail: dict[str, DeleteFailure] = {}

    def list(self, settings, prefix, **kw):
        assert prefix == PRE
        return self.objects

    def delete(self, settings, keys, **kw):
        self.deleted.append(list(keys))
        return [self.fail[k] for k in keys if k in self.fail]


def wire(monkeypatch, fake):
    monkeypatch.setattr(sweep, "list_archives", fake.list)
    monkeypatch.setattr(sweep, "delete_keys", fake.delete)


def policy_for(settings, *, dry_run, **kw):
    return replace(SweepPolicy.from_settings(settings), dry_run=dry_run, grace_seconds=0, **kw)


async def stale_name(pool):
    """proj/a: current (1 day) plus three old versions; returns them newest first."""
    out = []
    for age in (60, 50, 40, 1):
        out.append(await add_version(pool, "proj/a", age=datetime.now(UTC) - timedelta(days=age)))
    return out[::-1]


async def rows(pool, name="proj/a"):
    async with pool.connection() as conn:
        return {v.id for v in await backups.name_versions(conn, name)}


async def test_dry_run_plans_but_never_deletes(pool, dsn, monkeypatch):
    vs = await stale_name(pool)
    fake = Fake()
    wire(monkeypatch, fake)
    s = settings_for(dsn)
    res = await run_sweep(s, pool, policy=policy_for(s, dry_run=True, keep_newest=1))
    assert fake.deleted == [] and await rows(pool) == {v.id for v in vs}
    assert (res.dry_run, res.planned, res.deleted, res.error) == (True, 2, 0, None)
    assert res.planned_bytes == 200


async def test_real_run_deletes_rows_then_objects_and_keeps_current(pool, dsn, monkeypatch):
    vs = await stale_name(pool)
    fake = Fake()
    wire(monkeypatch, fake)
    s = settings_for(dsn)
    res = await run_sweep(s, pool, policy=policy_for(s, dry_run=False, keep_newest=1))
    assert await rows(pool) == {vs[0].id, vs[1].id}  # current and the newest non-current
    assert sorted(k for b in fake.deleted for k in b) == sorted(
        [vs[2].object_key, vs[3].object_key]
    )
    assert (res.deleted, res.deleted_bytes, len(res.failures)) == (2, 200, 0)
    assert res.to_dict()["finished_at"]


async def test_a_stale_name_goes_whole_and_the_name_row_too(pool, dsn, monkeypatch):
    vs = await stale_name(pool)
    async with pool.connection() as conn:
        await conn.execute("UPDATE backups SET updated_at = now() - interval '45 days'")
    fake = Fake()
    wire(monkeypatch, fake)
    s = settings_for(dsn)
    res = await run_sweep(s, pool, policy=policy_for(s, dry_run=False))
    assert res.deleted == 4 and await rows(pool) == set()
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT count(*) FROM backups")
        assert (await cur.fetchone())[0] == 0
    assert {k for b in fake.deleted for k in b} == {v.object_key for v in vs}


async def test_a_reference_made_after_the_plan_stops_the_delete(pool, dsn, monkeypatch):
    vs = await stale_name(pool)
    fake = Fake()
    wire(monkeypatch, fake)
    real = sweep.references
    calls = []

    async def late(conn, settings):
        refs = await real(conn, settings)
        calls.append(1)
        if len(calls) > 1:  # the re-read right before deleting
            refs.versions[vs[2].id] = ["racer"]
        return refs

    monkeypatch.setattr(sweep, "references", late)
    s = settings_for(dsn)
    res = await run_sweep(s, pool, policy=policy_for(s, dry_run=False, keep_newest=1))
    assert vs[2].id in await rows(pool) and vs[3].id not in await rows(pool)
    assert (res.deleted, res.skipped_protected) == (1, 1)


async def test_orphans_are_swept_and_a_recorded_object_is_not(pool, dsn, monkeypatch):
    vs = await stale_name(pool)
    orphan = ArchiveObject(f"{PRE}b/proj/a/lost.tar.zst", 7, NOW - 400 * DAY)
    real = ArchiveObject(vs[0].object_key, 100, NOW - 400 * DAY)
    fake = Fake([orphan, real])
    wire(monkeypatch, fake)
    s = settings_for(dsn)
    res = await run_sweep(s, pool, policy=policy_for(s, dry_run=False, keep_newest=9))
    assert [k for b in fake.deleted for k in b] == [orphan.key]
    assert res.deleted == 1 and vs[0].id in await rows(pool)


async def test_an_object_in_a_run_plan_survives_a_real_sweep(pool, dsn, monkeypatch):
    planned = ArchiveObject(f"{PRE}b/proj/a/inflight.tar.zst", 7, NOW - 400 * DAY)
    bare = ArchiveObject(f"{PRE}b/proj/a/lost.tar.zst", 7, NOW - 400 * DAY)
    async with pool.connection() as conn:
        s = await sessions.create(conn, "saver", SessionSpec())
        await conn.execute(
            "UPDATE sessions SET status = jsonb_build_object('backup', jsonb_build_object("
            "'runs', jsonb_build_object('save', jsonb_build_object('plan', jsonb_build_array("
            "jsonb_build_object('name', 'proj/a', 'key', %s::text)))))) WHERE id = %s",
            (planned.key, s.id),
        )
    fake = Fake([planned, bare])
    wire(monkeypatch, fake)
    st = settings_for(dsn)
    await run_sweep(st, pool, policy=policy_for(st, dry_run=False))
    assert [k for b in fake.deleted for k in b] == [bare.key]


async def test_a_legacy_flat_archive_survives_a_real_sweep(pool, dsn, monkeypatch):
    legacy = ArchiveObject(f"{PRE}proj-a.tar.zst", 7, NOW - 400 * DAY)
    orphan = ArchiveObject(f"{PRE}b/proj/a/lost.tar.zst", 7, NOW - 400 * DAY)
    fake = Fake([legacy, orphan])
    wire(monkeypatch, fake)
    s = settings_for(dsn)
    await run_sweep(s, pool, policy=policy_for(s, dry_run=False))
    assert [k for b in fake.deleted for k in b] == [orphan.key]


async def test_per_key_errors_are_logged_and_counted(pool, dsn, monkeypatch, caplog):
    vs = await stale_name(pool)
    fake = Fake()
    fake.fail[vs[3].object_key] = DeleteFailure(vs[3].object_key, "AccessDenied", "no")
    wire(monkeypatch, fake)
    s = settings_for(dsn)
    with caplog.at_level(logging.WARNING):
        res = await run_sweep(s, pool, policy=policy_for(s, dry_run=False, keep_newest=1))
    assert (res.deleted, len(res.failures)) == (1, 1)
    assert any("AccessDenied" in m for m in caplog.messages)
    assert vs[3].id not in await rows(pool)  # the row is gone: the object is a later orphan


async def test_a_listing_failure_is_recorded_not_raised(pool, dsn, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("s3 down")

    monkeypatch.setattr(sweep, "list_archives", boom)
    s = settings_for(dsn)
    res = await run_sweep(s, pool, policy=policy_for(s, dry_run=False))
    assert res.error and "s3 down" in res.error and res.finished_at


async def test_nothing_planned_calls_no_delete(pool, dsn, monkeypatch):
    await add_version(pool, "proj/a", age=datetime.now(UTC))
    fake = Fake()
    wire(monkeypatch, fake)
    s = settings_for(dsn)
    res = await run_sweep(s, pool, policy=policy_for(s, dry_run=False))
    assert fake.deleted == [] and res.planned == 0

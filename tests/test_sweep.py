"""The cleanup planner, the protected set from the database and the executor."""

from __future__ import annotations

import logging
import random
from datetime import UTC, datetime, timedelta

import pytest

from agents_orchestrator.cache import sweep
from agents_orchestrator.cache.presign import ArchiveObject, DeleteFailure
from agents_orchestrator.cache.sweep import plan_sweep, protected_keys, referenced_keys, run_sweep
from agents_orchestrator.db import sessions
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec

PRE = "workspace-cache/"
NOW = datetime(2026, 10, 31, 12, tzinfo=UTC)
DAY = timedelta(days=1)


def obj(name: str, age_days: float, size: int = 100) -> ArchiveObject:
    return ArchiveObject(f"{PRE}{name}.tar.zst", size, NOW - timedelta(days=age_days))


def plan(objects, protected=(), *, days=30, keep=3, grace=600):
    return plan_sweep(
        objects,
        protected,
        NOW,
        retention_days=days,
        keep_newest=keep,
        grace_seconds=grace,
        prefix=PRE,
    )


def keys_of(p):
    return set(p.keys)


def versions(group: str, ages: list[float]) -> list[ArchiveObject]:
    out = []
    for age in ages:
        stamp = (NOW - timedelta(days=age)).strftime("%Y%m%dt%H%M%Sz")
        out.append(obj(f"{group}/{stamp}", age))
    return out


# ------------------------------------------------------------------ planner


def test_old_versions_beyond_the_newest_k_are_deleted():
    objs = versions("proj", [1, 40, 50, 60, 70])
    p = plan(objs, keep=3)
    assert keys_of(p) == {objs[3].key, objs[4].key}  # the 4th and 5th newest, both older than 30d


def test_age_alone_does_not_delete_the_newest_k():
    objs = versions("proj", [100, 200, 300])
    assert plan(objs, keep=3).keys == []


def test_count_alone_does_not_delete_young_objects():
    objs = versions("proj", [1, 2, 3, 4, 5])
    assert plan(objs, keep=3).keys == []


def test_grace_period_keeps_objects_that_may_be_mid_save():
    objs = [obj("proj/a", 2), obj("proj/b", 3), obj("proj/c", 4)]
    p = plan_sweep(
        objs, [], NOW, retention_days=1, keep_newest=1, grace_seconds=10 * 86400, prefix=PRE
    )
    assert p.keys == []
    assert "grace" in p.reason(objs[1].key)
    late = plan_sweep(objs, [], NOW, retention_days=1, keep_newest=1, grace_seconds=60, prefix=PRE)
    assert keys_of(late) == {objs[1].key}  # c is the greatest key, a the most recent


def test_never_deletes_a_protected_key_and_says_why():
    objs = versions("proj", [1, 40, 50, 60, 70])
    p = plan(objs, [objs[4].key], keep=1)
    assert objs[4].key not in keys_of(p)
    assert "protected" in p.reason(objs[4].key)
    assert keys_of(p) == {objs[1].key, objs[2].key, objs[3].key}


def test_never_deletes_the_newest_of_a_group_even_when_ancient():
    only = [obj("lonely", 400)]
    assert plan(only, keep=1).keys == []
    flat = [obj("a", 400), obj("b", 500)]
    assert plan(flat).keys == []  # flat names group alone, each is its own newest


def test_greatest_key_is_kept_even_when_its_mtime_is_old():
    # names that are not timestamps: the greatest key is what a prefix restore would pick
    objs = [obj("proj/zzz", 400), obj("proj/aaa", 1), obj("proj/bbb", 40), obj("proj/ccc", 50)]
    p = plan(objs, keep=1)
    assert objs[0].key not in keys_of(p)
    assert keys_of(p) == {objs[2].key, objs[3].key}


def test_groups_are_independent_and_nested_groups_are_separate():
    a = versions("a", [1, 40, 50])
    b = versions("a/b", [1, 40, 50])
    p = plan(a + b, keep=1)
    assert keys_of(p) == {a[1].key, a[2].key, b[1].key, b[2].key}


def test_prefix_similar_groups_do_not_mix():
    foo = versions("foo", [1, 2])
    bar = versions("foo-bar", [100, 200])
    p = plan(foo + bar, keep=1)
    assert keys_of(p) == {bar[1].key}  # foo-bar keeps its own newest; foo untouched


def test_transcripts_are_deleted_by_age_only_and_respect_protection():
    old = obj("sessions/old", 90)
    young = obj("sessions/young", 2)
    live = obj("sessions/live", 90)
    p = plan([old, young, live], [live.key], keep=1)
    assert keys_of(p) == {old.key}
    assert "transcripts" in p.reason(old.key)


def test_foreign_objects_are_ignored():
    stray = ArchiveObject(f"{PRE}proj/readme.txt", 1, NOW - 500 * DAY)
    assert plan([stray]).decisions == ()


def test_reasons_cover_every_archive_and_the_plan_is_deterministic():
    objs = versions("a", [1, 40, 50, 60]) + versions("b", [3, 90]) + [obj("sessions/x", 99)]
    first = plan(objs, keep=2)
    assert {d.key for d in first.decisions} == {o.key for o in objs}
    assert all(d.reason for d in first.decisions)
    for seed in range(5):
        shuffled = random.Random(seed).sample(objs, len(objs))
        assert plan(shuffled, keep=2) == first
    assert list(first.keys) == sorted(first.keys)
    assert first.bytes == 100 * len(first.deletes)


@pytest.mark.parametrize("seed", range(20))
def test_invariants_on_random_listings(seed):
    rng = random.Random(seed)
    objs = [
        obj(f"g{rng.randrange(4)}/{n:04d}", rng.uniform(0, 120))
        for n in range(rng.randrange(1, 40))
    ] + [obj(f"flat{n}", rng.uniform(0, 120)) for n in range(rng.randrange(0, 5))]
    protected = {o.key for o in rng.sample(objs, k=min(len(objs), 3))}
    keep = rng.randrange(1, 5)
    p = plan(objs, protected, keep=keep)
    deleted = keys_of(p)
    assert not deleted & protected
    by_group: dict[str, list[ArchiveObject]] = {}
    for o in objs:
        by_group.setdefault(
            o.key.rsplit("/", 1)[0] if "/" in o.key[len(PRE) :] else o.key, []
        ).append(o)
    for members in by_group.values():
        survivors = [o for o in members if o.key not in deleted]
        assert survivors, "a group is never emptied"
        assert max(o.key for o in members) not in deleted
        assert len(survivors) >= min(keep, len(members))


# ------------------------------------------------------------------ protected set (database)


def settings_for(dsn=None, **kw):
    return Settings(
        database_url=dsn or "postgresql://x/y",
        agent_image="img:1",
        cache_s3_bucket="b",
        cache_s3_access_key_id="a",
        cache_s3_secret_access_key="s",
        cache_image="c:1",
        **kw,
    )


async def make(conn, slug, restore="", save_key="", **cache_status):
    spec = SessionSpec(cache={"restore_key": restore, "save_key": save_key})
    c = await sessions.create(conn, slug, spec)
    if cache_status:
        await sessions.set_status(conn, c.id, {"cache": cache_status}, c.observed_generation)
    return c


async def test_protected_set_covers_resolved_keys_and_transcripts(pool):
    async with pool.connection() as conn:
        await make(
            conn,
            "alpha",
            restore_resolved=f"{PRE}proj/20260101t000000z.tar.zst",
            save_resolved=f"{PRE}proj/20261031t100000z.tar.zst",
        )
        got = await protected_keys(conn, settings_for())
    assert got == {
        f"{PRE}proj/20260101t000000z.tar.zst",
        f"{PRE}proj/20261031t100000z.tar.zst",
        f"{PRE}sessions/alpha.tar.zst",
    }


async def test_protected_set_includes_spec_names_before_the_reconciler_resolves_them(pool):
    async with pool.connection() as conn:
        await make(conn, "beta", restore="shared/{{ session_slug }}", save_key="fixed")
        refs = await referenced_keys(conn, settings_for())
    assert refs[f"{PRE}shared/beta.tar.zst"] == ["beta"]
    assert refs[f"{PRE}fixed.tar.zst"] == ["beta"]


async def test_prefix_form_restore_adds_no_spec_object(pool):
    async with pool.connection() as conn:
        await make(conn, "gamma", restore="proj/", save_key="")
        got = await protected_keys(conn, settings_for())
    assert got == {f"{PRE}sessions/gamma.tar.zst"}


async def test_deleting_session_stays_protected_and_a_deleted_one_is_not(pool):
    async with pool.connection() as conn:
        live = await make(conn, "live", restore_resolved=f"{PRE}a.tar.zst")
        gone = await make(conn, "gone", restore_resolved=f"{PRE}b.tar.zst")
        dying = await make(conn, "dying", save_resolved=f"{PRE}c.tar.zst")
        await sessions.request_delete(conn, dying.id)
        await sessions.request_delete(conn, gone.id)
        await sessions.purge(conn, gone.id)
        got = await protected_keys(conn, settings_for())
        refs = await referenced_keys(conn, settings_for())
    assert f"{PRE}a.tar.zst" in got and f"{PRE}c.tar.zst" in got
    assert f"{PRE}b.tar.zst" not in got and f"{PRE}sessions/gone.tar.zst" not in got
    assert f"{PRE}sessions/dying.tar.zst" in got
    assert refs[f"{PRE}a.tar.zst"] == [live.slug]


async def test_in_flight_cache_run_keys_are_protected(pool):
    async with pool.connection() as conn:
        await make(
            conn,
            "flight",
            save_key="v/{{ now }}",
            save_resolved=f"{PRE}v/20261031t115900z.tar.zst",
            save={"state": "running", "job": "agent-flight-cache-save"},
        )
        got = await protected_keys(conn, settings_for())
    assert f"{PRE}v/20261031t115900z.tar.zst" in got


async def test_a_session_without_a_cache_protects_nothing(pool):
    async with pool.connection() as conn:
        await sessions.create(conn, "plain", SessionSpec())
        assert await protected_keys(conn, settings_for()) == set()


# ------------------------------------------------------------------ executor


class Fake:
    def __init__(self, objects, protected_calls):
        self.objects = objects
        self.deleted: list[list[str]] = []
        self.fail: dict[str, DeleteFailure] = {}
        self.protected_calls = list(protected_calls)

    def list(self, settings, prefix, **kw):
        assert prefix == PRE
        return self.objects

    def delete(self, settings, keys, **kw):
        self.deleted.append(list(keys))
        return [self.fail[k] for k in keys if k in self.fail]

    async def protected(self):
        return set(
            self.protected_calls.pop(0)
            if len(self.protected_calls) > 1
            else self.protected_calls[0]
        )

    async def now(self):
        return NOW


def wire(monkeypatch, fake):
    monkeypatch.setattr(sweep, "list_archives", fake.list)
    monkeypatch.setattr(sweep, "delete_keys", fake.delete)


def stale() -> list[ArchiveObject]:
    return versions("proj", [1, 40, 50, 60])


async def test_dry_run_plans_but_never_deletes(monkeypatch):
    fake = Fake(stale(), [set()])
    wire(monkeypatch, fake)
    res = await run_sweep(settings_for(cache_keep_newest=1), fake.protected, fake.now, dry_run=True)
    assert fake.deleted == []
    assert (res.dry_run, res.listed, res.planned, res.deleted) == (True, 4, 3, 0)
    assert res.planned_bytes == 300 and res.error is None


async def test_real_run_deletes_the_plan_and_summarises(monkeypatch):
    fake = Fake(stale(), [set()])
    wire(monkeypatch, fake)
    res = await run_sweep(
        settings_for(cache_keep_newest=1), fake.protected, fake.now, dry_run=False
    )
    assert sorted(k for batch in fake.deleted for k in batch) == sorted(o.key for o in stale()[1:])
    assert (res.deleted, res.deleted_bytes, len(res.failures)) == (3, 300, 0)
    assert res.to_dict()["deleted"] == 3 and res.to_dict()["finished_at"]


async def test_protection_is_rechecked_right_before_deleting(monkeypatch):
    objs = stale()
    fake = Fake(objs, [set(), {objs[1].key}])  # the second read sees a new reference
    wire(monkeypatch, fake)
    res = await run_sweep(
        settings_for(cache_keep_newest=1), fake.protected, fake.now, dry_run=False
    )
    assert objs[1].key not in {k for batch in fake.deleted for k in batch}
    assert res.skipped_protected == 1 and res.deleted == 2


async def test_per_key_errors_are_logged_and_counted(monkeypatch, caplog):
    objs = stale()
    fake = Fake(objs, [set()])
    fake.fail[objs[2].key] = DeleteFailure(objs[2].key, "AccessDenied", "nope")
    wire(monkeypatch, fake)
    with caplog.at_level(logging.WARNING):
        res = await run_sweep(
            settings_for(cache_keep_newest=1), fake.protected, fake.now, dry_run=False
        )
    assert res.deleted == 2 and [f.key for f in res.failures] == [objs[2].key]
    assert res.to_dict()["failed"] == 1
    assert "AccessDenied" in caplog.text


async def test_a_listing_failure_is_recorded_not_raised(monkeypatch):
    fake = Fake([], [set()])

    def boom(*a, **kw):
        raise RuntimeError("s3 down")

    monkeypatch.setattr(sweep, "list_archives", boom)
    res = await run_sweep(settings_for(), fake.protected, fake.now, dry_run=False)
    assert res.error and "s3 down" in res.error and res.finished_at is not None


async def test_nothing_planned_calls_no_delete(monkeypatch):
    fake = Fake(versions("proj", [1, 2]), [set()])
    wire(monkeypatch, fake)
    res = await run_sweep(settings_for(), fake.protected, fake.now, dry_run=False)
    assert fake.deleted == [] and res.planned == 0

"""Restore resolution (exact, latest under a prefix, fresh start) and the rendered save name."""

from __future__ import annotations

import re
from urllib.parse import urlsplit

import pytest
import pytest_asyncio
from test_cache_restore import line
from test_cache_save import SAVE, SaveEnv

from agents_orchestrator.cache.presign import ArchiveObject
from agents_orchestrator.db import sessions
from agents_orchestrator.reconciler import cache as cache_mod
from agents_orchestrator.reconciler.cache import restore_job_name
from agents_orchestrator.spec import SessionSpec

pytestmark = pytest.mark.asyncio

RESTORE = restore_job_name("alpha")
PRE = "workspace-cache/"
# the transcripts save is pinned elsewhere: its restore record skips it
SKIP = {"state": "skipped", "reason": "not_configured"}


@pytest_asyncio.fixture
async def env(pool, dsn) -> SaveEnv:
    return SaveEnv(pool, dsn)


async def make(env, restore_key, save_key, slug="alpha", **spec):
    cache = {"restore_key": restore_key, "save_key": save_key}
    async with env.pool.connection() as conn:
        c = await sessions.create(conn, slug, SessionSpec(cache=cache, **spec))
        status = {**c.status, "cache": {"sessions_restore": SKIP}}
        return await sessions.set_status(conn, c.id, status, c.observed_generation)


def job_path(env, name):
    job = env.kube.get("Job", name)
    vars_ = {v["name"]: v["value"] for v in job["spec"]["template"]["spec"]["containers"][0]["env"]}
    return urlsplit(vars_["CACHE_URL"]).path


async def finish_restore(env, c, outcome="miss"):
    await env.tick()
    env.kube.finish_job(RESTORE, exit_code=0, log=line(outcome))
    await env.tick(2)


def stub_latest(monkeypatch, found):
    calls = []

    def fake(settings, prefix, **kw):
        calls.append(prefix)
        return found

    monkeypatch.setattr(cache_mod, "latest_under", fake)
    return calls


async def test_exact_name_restore_presigns_the_rendered_object(env):
    c = await make(env, "proj/{{ session_slug }}", "proj/{{ session_slug }}")
    await env.tick()
    cache = await env.cache(c.id)
    assert cache["restore"]["state"] == "running"
    assert cache["restore_resolved"] == f"{PRE}proj/alpha.tar.zst"
    assert job_path(env, RESTORE).endswith(f"/{PRE}proj/alpha.tar.zst")


async def test_prefix_restore_resolves_the_newest_object(env, monkeypatch):
    newest = ArchiveObject(f"{PRE}proj/20261031t120000z.tar.zst", 5, None)  # type: ignore[arg-type]
    calls = stub_latest(monkeypatch, newest)
    c = await make(env, "proj/", "proj/{{ now }}")
    await env.tick()
    cache = await env.cache(c.id)
    assert calls == [f"{PRE}proj/"]
    assert cache["restore_resolved"] == newest.key
    assert cache["restore"]["state"] == "running"
    assert job_path(env, RESTORE).endswith(f"/{newest.key}")


async def test_prefix_restore_without_an_object_is_a_miss_and_no_job(env, monkeypatch):
    stub_latest(monkeypatch, None)
    c = await make(env, "proj/", "proj/{{ now }}", init_scripts=["echo a"])
    await env.tick()
    cache = await env.cache(c.id)
    assert cache["restore"]["state"] == "miss"
    assert cache["restore_resolved"] is None
    assert env.creates(RESTORE) == 0
    await env.tick()
    assert env.kube.has("Job", "agent-alpha-init")  # the gate is open: init follows


async def test_prefix_listing_failure_fails_the_restore(env, monkeypatch):
    def boom(settings, prefix, **kw):
        raise RuntimeError("s3 down")

    monkeypatch.setattr(cache_mod, "latest_under", boom)
    c = await make(env, "proj/", "proj/{{ now }}")
    await env.tick()
    restore = (await env.cache(c.id))["restore"]
    assert restore["state"] == "failed" and restore["reason"].startswith("list_failed")
    assert env.creates(RESTORE) == 0


async def test_empty_restore_key_is_a_fresh_start_that_still_saves(env):
    c = await make(env, "", "proj/{{ now }}")
    await env.tick()
    cache = await env.cache(c.id)
    assert cache["restore"]["state"] == "miss" and cache["restore"]["reason"] == "fresh_start"
    assert cache["restore_resolved"] is None
    assert env.creates(RESTORE) == 0
    await env.delete(c)
    await env.tick()
    assert env.kube.has("Job", SAVE)  # a fresh start is a miss: the save is allowed


async def test_bad_restore_template_fails_the_restore(env):
    c = await make(env, "{{ nope }}", "x")
    await env.tick()
    restore = (await env.cache(c.id))["restore"]
    assert restore["state"] == "failed" and restore["reason"].startswith("bad_cache_key")
    assert env.creates(RESTORE) == 0


async def test_empty_save_key_is_restore_only_and_discards_without_a_job(env):
    c = await make(env, "proj", "")
    await finish_restore(env, c, "restored")
    await env.delete(c)
    await env.tick()
    assert env.saves[-1] == {**env.saves[-1], "state": "skipped", "reason": "restore-only"}
    assert env.saves_created() == 0
    assert await env.gone(c)


async def test_save_name_is_rendered_at_save_time_and_stored_before_the_job(env):
    c = await make(env, "proj", "proj/{{ now }}")
    await finish_restore(env, c)
    seen = {}
    real = env.kube.create_job

    async def spy(job):
        if job["metadata"]["name"] == SAVE:
            seen["resolved"] = (await env.cache(c.id)).get("save_resolved")
        return await real(job)

    env.kube.create_job = spy
    await env.delete(c)
    await env.tick()
    resolved = (await env.cache(c.id))["save_resolved"]
    assert re.fullmatch(rf"{PRE}proj/\d{{8}}t\d{{6}}z\.tar\.zst", resolved)
    assert seen["resolved"] == resolved  # recorded before the Job existed
    assert job_path(env, SAVE).endswith(f"/{resolved}")


async def test_a_stored_save_name_is_reused_by_a_retry(env):
    c = await make(env, "proj", "proj/{{ now }}")
    await finish_restore(env, c)
    fixed = f"{PRE}proj/20200101t000000z.tar.zst"
    async with env.pool.connection() as conn:
        cur = await sessions.get(conn, c.id)
        status = {**cur.status, "cache": {**cur.status["cache"], "save_resolved": fixed}}
        await sessions.set_status(conn, c.id, status, cur.observed_generation)
    await env.delete(c)
    await env.tick()
    assert (await env.cache(c.id))["save_resolved"] == fixed
    assert job_path(env, SAVE).endswith(f"/{fixed}")


async def test_two_saves_of_a_versioned_name_differ_in_time_not_in_template(env):
    c = await make(env, "proj", "proj/{{ now }}")
    await finish_restore(env, c)
    await env.delete(c)
    await env.tick()
    assert "{{" not in (await env.cache(c.id))["save_resolved"]


async def test_bad_save_template_fails_the_save_but_not_the_discard(env):
    c = await make(env, "proj", "{{ nope }}")
    await finish_restore(env, c)
    await env.delete(c)
    await env.tick()
    assert env.saves[-1]["state"] == "failed"
    assert env.saves[-1]["reason"].startswith("bad_cache_key")
    assert env.saves_created() == 0
    assert await env.gone(c)


async def test_save_name_rendering_into_the_reserved_prefix_fails_the_save(env):
    c = await make(env, "proj", "{{ 'preset' ~ '-' }}{{ session_slug }}")
    await finish_restore(env, c)
    await env.delete(c)
    await env.tick()
    assert env.saves[-1]["state"] == "failed"
    assert env.saves[-1]["reason"].startswith("bad_cache_key")
    assert env.saves_created() == 0

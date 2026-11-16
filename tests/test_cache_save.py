"""The backup entries are saved at session end, before the PVC goes (D3, D4, D11; S-68)."""

from __future__ import annotations

import logging
from datetime import timedelta

import pytest
import pytest_asyncio
from test_cache_restore import CACHE_KEY, RESTORE, WS_ENTRY, Env, line, res

from agents_orchestrator.cache.save_rule import transcripts_skip
from agents_orchestrator.db import backups as backups_repo
from agents_orchestrator.db import sessions
from agents_orchestrator.k8s.labels import object_names
from agents_orchestrator.reconciler import cache as cache_mod
from agents_orchestrator.reconciler.cache import (
    RESTORE_PRE,
    TX_SAVE,
    job_name,
)
from agents_orchestrator.reconciler.cache import (
    SAVE as SAVE_KEY,
)
from agents_orchestrator.reconciler.checks import DEADLINE_GRACE_SECONDS

pytestmark = pytest.mark.asyncio

SAVE = job_name("alpha", SAVE_KEY)
DIGEST = "e" * 64


def uploaded(size: int = 10, digest: str = DIGEST, index: int = 0) -> str:
    return line(res("uploaded", index, size=size, digest=digest, manifest=None))


class SaveEnv(Env):
    def __init__(self, pool, dsn, **settings) -> None:
        super().__init__(
            pool,
            dsn,
            **{"discard_drain_seconds": 0, "discard_quiesce_timeout_seconds": 60, **settings},
        )
        self.saves: list[dict] = []
        self.sessions_saves: list[dict] = []
        inner = self.rec.cache.write_status

        async def spy(session, fields, generation, **kw):
            backup = fields.get("backup") or {}
            save = (backup.get("runs") or {}).get(SAVE_KEY)
            if save is not None:
                self.saves.append(dict(save))
            tx = (backup.get("transcripts") or {}).get("save")
            if tx is not None:
                self.sessions_saves.append(dict(tx))
            return await inner(session, fields, generation, **kw)

        self.rec.cache.write_status = spy

    async def session(self, slug: str = "alpha", cache: bool = True, **spec):
        """A session whose sessions restore record skips the transcripts save.

        The discard then follows the entry save alone; the transcripts save is pinned by
        tests/test_sessions_backup.py.
        """
        c = await super().session(slug, cache, **spec)
        if not cache:
            return c
        skip = {"state": "skipped", "reason": "not_configured"}
        status = {**c.status, "backup": {"transcripts": {"restore": skip}}}
        async with self.pool.connection() as conn:
            return await sessions.set_status(conn, c.id, status, c.observed_generation)

    async def ready(self, slug: str = "alpha", restore: str = "missing"):
        """A session whose entry restore finished with `restore`."""
        c = await self.session(slug)
        await self.tick()
        self.kube.finish_job(
            job_name(slug, RESTORE_PRE),
            exit_code=0,
            log=line(restore, size=5 if restore == "restored" else None),
        )
        await self.tick(2)
        assert (await self.entry(c.id))["restore"]["state"] == restore
        return c

    async def delete(self, c) -> None:
        async with self.pool.connection() as conn:
            await sessions.request_delete(conn, c.id)

    async def gone(self, c) -> bool:
        async with self.pool.connection() as conn:
            return await sessions.get(conn, c.id) is None

    def saves_created(self, name: str = SAVE) -> int:
        return self.creates(name)

    async def versions(self, name: str = CACHE_KEY):
        async with self.pool.connection() as conn:
            return (await backups_repo.versions(conn, name, limit=50, cursor=None)).items


@pytest_asyncio.fixture
async def env(pool, dsn) -> SaveEnv:
    return SaveEnv(pool, dsn)


async def test_save_runs_after_quiesce_before_prune(env):
    c = await env.ready()
    sts = object_names("alpha")["statefulset"]
    assert any(p for p in env.kube.names("Pod") if p.startswith(sts))
    await env.delete(c)
    await env.tick()
    assert env.kube.get("StatefulSet", sts)["spec"]["replicas"] == 0
    assert not [p for p in env.kube.names("Pod") if p.startswith(sts + "-") and p[-1].isdigit()]
    assert env.kube.has("Job", SAVE)
    assert env.kube.has("PersistentVolumeClaim", object_names("alpha")["pvc"])
    status = await env.status(c.id)
    assert status["phase"] == "deleting" and "saving the backup entries" in status["message"]
    await env.tick(2)  # still running: nothing is pruned
    assert env.kube.has("PersistentVolumeClaim", object_names("alpha")["pvc"])
    assert env.saves_created() == 1


async def test_saved_then_purged(env):
    c = await env.ready()
    await env.delete(c)
    await env.tick()
    env.kube.finish_job(SAVE, exit_code=0, log=uploaded(4242))
    await env.tick()
    assert env.saves[-1]["state"] == "done"
    assert await env.gone(c)
    assert not env.kube.has("Job", SAVE)
    assert not env.kube.has("PersistentVolumeClaim", object_names("alpha")["pvc"])
    (current, *_) = await env.versions()
    assert current.digest == DIGEST and current.session_slug == "alpha" and current.size == 4242


@pytest.mark.parametrize(
    ("finish", "reason"),
    [
        ({"exit_code": 1, "log": line("failed", reason="too_large")}, "too_large"),
        (
            {"exit_code": 1, "log": line("failed", reason="HTTP 500")},
            "HTTP 500",
        ),
        ({"deadline_exceeded": True}, "timeout"),
        ({"exit_code": 0, "log": "not json"}, "bad_output"),
    ],
)
async def test_failed_save_does_not_block_discard(env, caplog, finish, reason):
    c = await env.ready()
    await env.delete(c)
    await env.tick()
    env.kube.finish_job(SAVE, **finish)
    with caplog.at_level(logging.WARNING):
        await env.tick()
    assert env.saves[-1]["state"] == "failed" and env.saves[-1]["reason"] == reason
    warned = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("alpha" in m and reason in m for m in warned)
    assert await env.gone(c)


async def test_save_job_vanishing_does_not_block_discard(env):
    c = await env.ready()
    await env.delete(c)
    await env.tick()
    await env.kube.delete("Job", "agents", SAVE)
    await env.tick()
    assert env.saves[-1]["reason"] == "job_missing"
    assert await env.gone(c)


async def test_save_timeout_without_job_status(env):
    c = await env.ready()
    await env.delete(c)
    await env.tick()
    async with env.pool.connection() as conn:
        now = (await (await conn.execute("SELECT now()")).fetchone())[0]
    limit = env.settings.cache_timeout_seconds + DEADLINE_GRACE_SECONDS + 5
    await env.patch_run(c.id, SAVE_KEY, started_at=(now - timedelta(seconds=limit)).isoformat())
    await env.tick()
    assert env.saves[-1]["reason"] == "timeout"
    assert await env.gone(c)


async def test_create_failure_does_not_block_discard(env):
    c = await env.ready()
    env.kube.fail("Job", SAVE, verbs=["create"])
    await env.delete(c)
    await env.tick()
    assert env.saves[-1]["state"] == "failed"
    assert env.saves[-1]["reason"].startswith("job_create_failed")
    assert await env.gone(c)


async def test_save_skipped_while_the_restore_did_not_finish(env):
    c = await env.session("alpha")
    await env.tick()  # the restore Job runs
    await env.delete(c)
    await env.tick()
    assert env.saves[-1]["state"] == "skipped" and env.saves[-1]["reason"] == "restore_pending"
    assert env.saves_created() == 0
    assert await env.gone(c)


async def test_save_skipped_when_cache_unavailable_now(pool, dsn):
    e = SaveEnv(pool, dsn, cache_s3_bucket="")
    async with pool.connection() as conn:
        from agents_orchestrator.spec import SessionSpec

        c = await sessions.create(conn, "alpha", SessionSpec(backup=[WS_ENTRY]))
    await e.tick()
    await e.delete(c)
    await e.tick()
    assert e.saves[-1]["state"] == "skipped" and e.saves[-1]["reason"] == "cache_not_configured"
    assert e.saves_created() == 0
    assert await e.gone(c)


async def test_sessions_save_skipped_when_never_converged(env):
    """Discarded before its first converge: no PVC, so no sessions save Job to wait on."""
    c = await env.create("alpha")
    await env.delete(c)
    await env.tick(2)
    assert env.kube.has("PersistentVolumeClaim", object_names("alpha")["pvc"]) is False
    assert env.sessions_saves == [{"state": "skipped", "reason": "never_converged"}]
    assert env.saves[-1]["reason"] == "never_converged"
    assert env.creates(job_name("alpha", TX_SAVE)) == 0
    assert await env.gone(c)


async def test_sessions_save_runs_for_session_predating_restore(env):
    """No sessions restore record but a converged session (its PVC exists): saved as before."""
    c = await env.ready()
    async with env.pool.connection() as conn:
        current = await sessions.get(conn, c.id)
        backup = {**current.status["backup"], "transcripts": {}}
        await sessions.set_status(
            conn, c.id, {**current.status, "backup": backup}, current.observed_generation
        )
    assert env.kube.has("PersistentVolumeClaim", object_names("alpha")["pvc"])
    await env.delete(c)
    await env.tick()
    env.kube.finish_job(SAVE, exit_code=0, log=uploaded())
    await env.tick()
    assert env.creates(job_name("alpha", TX_SAVE)) == 1
    assert (await env.run(c.id, TX_SAVE))["state"] == "running"
    assert not await env.gone(c)


@pytest.mark.parametrize(
    ("restore", "converged", "skip"),
    [
        (None, False, "never_converged"),
        (None, True, None),
        ({"state": "skipped", "reason": "pods_exist"}, False, None),
        ({"state": "running"}, False, "restore_running"),
        ({"state": "miss"}, False, None),
        ({"state": "unreadable"}, False, None),
    ],
)
async def test_sessions_save_rule_separates_never_converged(restore, converged, skip):
    status: dict = {"backup": {"transcripts": {} if restore is None else {"restore": restore}}}
    if converged:
        status["replicas"] = 1  # the converge's closing status write
    assert transcripts_skip(True, status) == skip


async def test_no_cache_session_unchanged(env):
    c = await env.session("alpha", cache=False)
    await env.tick()
    await env.delete(c)
    await env.tick()
    assert env.saves == [] and env.saves_created() == 0
    assert await env.gone(c)


async def test_failed_restore_saves_an_orphan_version(env):
    c = await env.session("alpha")
    await env.tick()
    env.kube.finish_job(RESTORE, exit_code=1, log=line("failed", reason="http 403"))
    await env.tick(2)
    await env.delete(c)
    await env.tick()
    env.kube.finish_job(SAVE, exit_code=0, log=uploaded(digest="f" * 64))
    await env.tick()
    states = sorted(v.state for v in await env.versions())
    assert states == ["current", "orphan"]  # the seed stays current


@pytest.mark.parametrize("restore", ["restored", "missing", "unreadable"])
async def test_save_runs_after_success_states(env, restore):
    c = await env.ready(restore=restore)
    await env.delete(c)
    await env.tick()
    assert env.kube.has("Job", SAVE)
    assert env.saves[-1]["state"] == "running"


async def test_flipped_session_saves(env):
    """A session flipped to deleting by the idle fallback (autoclose or inactivity) saves like
    any discard."""
    c = await env.ready()
    async with env.pool.connection() as conn:
        await sessions.request_delete(conn, c.id)
    await env.tick()
    assert env.kube.has("Job", SAVE)
    env.kube.finish_job(SAVE, exit_code=0, log=uploaded())
    await env.tick()
    assert env.saves[-1]["state"] == "done"
    assert await env.gone(c)


async def test_quiesce_timeout_saves_anyway(pool, dsn, caplog):
    e = SaveEnv(pool, dsn, discard_quiesce_timeout_seconds=0)
    c = await e.ready()
    e.kube.controller_paused = True
    await e.delete(c)
    with caplog.at_level(logging.WARNING):
        await e.tick()
    sts = object_names("alpha")["statefulset"]
    assert any(p.startswith(sts + "-0") for p in e.kube.names("Pod"))  # pods still running
    assert e.kube.has("Job", SAVE)


async def test_pods_running_hold_the_save(env):
    c = await env.ready()
    env.kube.controller_paused = True
    await env.delete(c)
    await env.tick(2)
    assert not env.kube.has("Job", SAVE)
    assert "waiting for agent pods" in (await env.status(c.id))["message"]


async def test_save_crash_safety(env):
    c = await env.ready()
    await env.delete(c)
    await env.tick()
    assert env.saves_created() == 1
    # crash between the running write and the create
    await env.kube.delete("Job", "agents", SAVE)
    await env.patch_run(c.id, SAVE_KEY, job=None)
    await env.tick(3)
    assert env.saves_created() == 2 and env.kube.has("Job", SAVE)
    # crash between the create and the job write: the Job is adopted, not created again
    await env.patch_run(c.id, SAVE_KEY, job=None)
    await env.tick(3)
    assert env.saves_created() == 2
    assert (await env.run(c.id, SAVE_KEY))["job"] == SAVE
    # crash between the outcome and the Job delete: it is finished next tick
    env.kube.finish_job(SAVE, exit_code=0, log=uploaded())
    await env.tick()
    assert await env.gone(c)
    assert not env.kube.has("Job", SAVE)


async def test_recorded_outcome_is_not_saved_again(env):
    c = await env.ready()
    await env.delete(c)
    await env.tick()
    await env.patch_run(c.id, SAVE_KEY, state="done", job=SAVE)
    await env.tick()
    assert env.saves_created() == 1
    assert await env.gone(c)


async def test_each_save_is_a_fresh_version_object(env, monkeypatch):
    calls: list[tuple[str, str]] = []
    real = cache_mod.presign_object

    def spy(settings, method, key, *a, **kw):
        calls.append((method, key))
        return real(settings, method, key, *a, **kw)

    monkeypatch.setattr(cache_mod, "presign_object", spy)
    c = await env.ready()
    await env.delete(c)
    await env.tick()
    put = next(key for method, key in calls if method == "PUT")
    assert put.startswith(f"workspace-cache/b/{CACHE_KEY}/") and put.endswith(".tar.zst")
    run = await env.run(c.id, SAVE_KEY)
    assert run["plan"][0]["key"] == put  # recorded before the Job existed
    assert RESTORE  # the restore Job name is the pre phase

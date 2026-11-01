"""The workspace cache is saved at session end, before the PVC goes (D3, D4, D11)."""

from __future__ import annotations

import logging
from urllib.parse import urlsplit

import pytest
import pytest_asyncio
from test_cache_restore import CACHE_KEY, RESTORE, Env, line

from agents_orchestrator.cache.save_rule import save_skip
from agents_orchestrator.db import sessions
from agents_orchestrator.k8s.labels import object_names
from agents_orchestrator.reconciler import cache as cache_mod
from agents_orchestrator.reconciler.cache import job_name, save_job_name

pytestmark = pytest.mark.asyncio

SAVE = save_job_name("alpha")


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
            save = (fields.get("cache") or {}).get("save")
            if save is not None:
                self.saves.append(dict(save))
            sessions_save = (fields.get("cache") or {}).get("sessions_save")
            if sessions_save is not None:
                self.sessions_saves.append(dict(sessions_save))
            return await inner(session, fields, generation, **kw)

        self.rec.cache.write_status = spy

    async def session(self, slug: str = "alpha", cache: bool = True, **spec):
        """A session whose sessions restore record skips the transcripts save.

        The discard then follows the workdir save alone; the transcripts save is pinned by
        tests/test_sessions_backup.py.
        """
        c = await super().session(slug, cache, **spec)
        if not cache:
            return c
        skip = {"state": "skipped", "reason": "not_configured"}
        status = {**c.status, "cache": {"sessions_restore": skip}}
        async with self.pool.connection() as conn:
            return await sessions.set_status(conn, c.id, status, c.observed_generation)

    async def ready(self, slug: str = "alpha", restore: str = "miss"):
        """A cached session whose restore finished with `restore`."""
        c = await self.session(slug)
        await self.tick()
        self.kube.finish_job(
            cache_mod.restore_job_name(slug),
            exit_code=0,
            log=line(restore, 5 if restore == "restored" else None),
        )
        await self.tick(2)
        assert (await self.cache(c.id))["restore"]["state"] == restore
        return c

    async def delete(self, c) -> None:
        async with self.pool.connection() as conn:
            await sessions.request_delete(conn, c.id)

    async def patch_save(self, session_id, **fields) -> None:
        async with self.pool.connection() as conn:
            c = await sessions.get(conn, session_id)
            status = dict(c.status)
            cache = dict(status["cache"])
            cache["save"] = {**cache["save"], **fields}
            status["cache"] = cache
            await sessions.set_status(conn, session_id, status, c.observed_generation)

    async def gone(self, c) -> bool:
        async with self.pool.connection() as conn:
            return await sessions.get(conn, c.id) is None

    def saves_created(self, name: str = SAVE) -> int:
        return self.creates(name)


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
    assert status["phase"] == "deleting" and "saving the workspace cache" in status["message"]
    await env.tick(2)  # still running: nothing is pruned
    assert env.kube.has("PersistentVolumeClaim", object_names("alpha")["pvc"])
    assert env.saves_created() == 1


async def test_saved_then_purged(env):
    c = await env.ready()
    await env.delete(c)
    await env.tick()
    env.kube.finish_job(SAVE, exit_code=0, log=line("saved", 4242))
    await env.tick()
    assert env.saves[-1]["state"] == "saved" and env.saves[-1]["bytes"] == 4242
    assert await env.gone(c)
    assert not env.kube.has("Job", SAVE)
    assert not env.kube.has("PersistentVolumeClaim", object_names("alpha")["pvc"])


@pytest.mark.parametrize(
    ("finish", "reason"),
    [
        ({"exit_code": 1, "log": line("failed", reason="too_large")}, "too_large"),
        (
            {"exit_code": 1, "log": line("failed", reason="upload_failed: HTTP 500")},
            "upload_failed: HTTP 500",
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
    assert any("alpha" in m and CACHE_KEY in m and reason in m for m in warned)
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
    from datetime import timedelta

    from agents_orchestrator.reconciler.checks import DEADLINE_GRACE_SECONDS

    c = await env.ready()
    await env.delete(c)
    await env.tick()
    async with env.pool.connection() as conn:
        now = (await (await conn.execute("SELECT now()")).fetchone())[0]
    await env.patch_save(
        c.id,
        started_at=(
            now - timedelta(seconds=env.settings.cache_timeout_seconds + DEADLINE_GRACE_SECONDS + 5)
        ).isoformat(),
    )
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


@pytest.mark.parametrize(
    ("restore", "reason"),
    [
        ({"state": "running"}, "restore_running"),
        ({"state": "failed", "reason": "download_failed: HTTP 403"}, "download_failed"),
        ({"state": "failed", "reason": "timeout"}, "timeout"),
        ({"state": "failed", "reason": "job_missing"}, "job_missing"),
        ({"state": "failed", "reason": "foreign_object"}, "foreign_object"),
        ({"state": "failed", "reason": "job_create_failed: x"}, "job_create_failed"),
        ({"state": "failed", "reason": "not_configured"}, "not_configured"),
    ],
)
async def test_save_skipped_when_restore_did_not_finish(env, restore, reason):
    c = await env.ready()
    await env.patch_restore(c.id, **restore)
    await env.delete(c)
    await env.tick()
    assert env.saves[-1] == {"state": "skipped", "reason": reason} or (
        env.saves[-1]["state"] == "skipped" and env.saves[-1]["reason"] == reason
    )
    assert env.saves_created() == 0
    assert await env.gone(c)


async def test_save_skipped_when_cache_unavailable_now(pool, dsn):
    e = SaveEnv(pool, dsn, cache_s3_bucket="")
    async with pool.connection() as conn:
        from agents_orchestrator.spec import SessionSpec

        c = await sessions.create(
            conn, "alpha", SessionSpec(cache={"restore_key": CACHE_KEY, "save_key": CACHE_KEY})
        )
    await e.tick()
    await e.patch_restore(c.id, state="restored", reason=None, job=None)
    await e.delete(c)
    await e.tick()
    assert e.saves[-1]["state"] == "skipped" and e.saves[-1]["reason"] == "not_configured"
    assert e.saves_created() == 0
    assert await e.gone(c)


async def test_save_without_restore_record_is_skipped(env):
    c = await env.session("alpha")
    await env.delete(c)
    await env.tick()
    assert env.saves_created() == 0
    assert await env.gone(c)


async def test_sessions_save_skipped_when_never_converged(env):
    """Discarded before its first converge: no PVC, so no sessions save Job to wait on."""
    c = await env.create("alpha")
    await env.delete(c)
    await env.tick(2)
    assert env.kube.has("PersistentVolumeClaim", object_names("alpha")["pvc"]) is False
    assert env.sessions_saves == [{"state": "skipped", "reason": "never_converged"}]
    assert env.creates(job_name("alpha", "save", "sessions")) == 0
    assert await env.gone(c)


async def test_sessions_save_runs_for_session_predating_restore(env):
    """No sessions restore record but a converged session (its PVC exists): saved as before."""
    c = await env.ready()
    async with env.pool.connection() as conn:
        current = await sessions.get(conn, c.id)
        cache = {k: v for k, v in current.status["cache"].items() if k != "sessions_restore"}
        await sessions.set_status(
            conn, c.id, {**current.status, "cache": cache}, current.observed_generation
        )
    assert env.kube.has("PersistentVolumeClaim", object_names("alpha")["pvc"])
    await env.delete(c)
    await env.tick()
    env.kube.finish_job(SAVE, exit_code=0, log=line("saved", 10))
    await env.tick()
    sessions_save = job_name("alpha", "save", "sessions")
    assert env.creates(sessions_save) == 1
    assert (await env.cache(c.id))["sessions_save"]["state"] == "running"
    assert not await env.gone(c)


@pytest.mark.parametrize(
    ("restore", "converged", "skip"),
    [
        (None, False, "never_converged"),
        (None, True, None),
        ({"state": "skipped", "reason": "pods_exist"}, False, None),
        ({"state": "running"}, False, "restore_running"),
        ({"state": "miss"}, False, None),
    ],
)
async def test_sessions_save_rule_separates_never_converged(restore, converged, skip):
    status: dict = {"cache": {} if restore is None else {"sessions_restore": restore}}
    if converged:
        status["replicas"] = 1  # the converge's closing status write
    assert save_skip(True, status, "sessions") == skip


async def test_no_cache_session_unchanged(env):
    c = await env.session("alpha", cache=False)
    await env.tick()
    await env.delete(c)
    await env.tick()
    assert env.saves == [] and env.saves_created() == 0
    assert await env.gone(c)


@pytest.mark.parametrize(
    ("restore", "reason"),
    [
        ({"state": "failed", "reason": "extract_failed: bad archive"}, "extract_failed"),
        ({"state": "failed", "reason": "exit 137"}, "exit"),
        ({"state": "failed", "reason": "bad_output"}, "bad_output"),
        ({"state": "failed", "reason": "no_cache_key"}, "no_cache_key"),
        ({"state": "failed", "reason": "a_future_reason"}, "a_future_reason"),
        ({"state": "failed", "reason": None}, "restore_failed"),
        ({"state": "skipped"}, "restore_skipped"),
        ({"state": "saved"}, "restore_saved"),
    ],
)
async def test_save_only_after_successful_restore(env, restore, reason):
    c = await env.ready()
    await env.patch_restore(c.id, **restore)
    await env.delete(c)
    await env.tick()
    assert env.saves[-1]["state"] == "skipped" and env.saves[-1]["reason"] == reason
    assert env.saves_created() == 0
    assert await env.gone(c)


@pytest.mark.parametrize("restore", ["restored", "miss", "reset"])
async def test_save_runs_after_success_states(env, restore):
    c = await env.ready(restore=restore)
    await env.delete(c)
    await env.tick()
    assert env.kube.has("Job", SAVE)
    assert env.saves[-1]["state"] == "running"


async def test_cache_miss_then_save(env):
    c = await env.ready(restore="miss")
    await env.delete(c)
    await env.tick()
    assert env.saves_created() == 1
    env.kube.finish_job(SAVE, exit_code=0, log=line("saved", 7))
    await env.tick(2)
    assert env.saves[-1]["state"] == "saved"


async def test_autoclose_saves(env):
    c = await env.ready()
    async with env.pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET spec = spec || '{\"autoclose_after_seconds\": 1}'::jsonb,"
            " created_at = now() - interval '1 hour' WHERE id = %s",
            (c.id,),
        )
        assert await sessions.close_expired(conn) == [c.id]
    await env.tick()
    assert env.kube.has("Job", SAVE)
    env.kube.finish_job(SAVE, exit_code=0, log=line("saved", 7))
    await env.tick()
    assert env.saves[-1]["state"] == "saved"
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
    await env.patch_save(c.id, job=None)
    await env.tick(3)
    assert env.saves_created() == 2 and env.kube.has("Job", SAVE)
    # crash between the create and the job write: the Job is adopted, not created again
    await env.patch_save(c.id, job=None)
    await env.tick(3)
    assert env.saves_created() == 2
    assert (await env.cache(c.id))["save"]["job"] == SAVE
    # crash between the outcome and the Job delete: it is finished next tick
    env.kube.finish_job(SAVE, exit_code=0, log=line("saved", 1))
    await env.tick()
    assert await env.gone(c)
    assert not env.kube.has("Job", SAVE)


async def test_recorded_outcome_is_not_saved_again(env):
    c = await env.ready()
    await env.delete(c)
    await env.tick()
    await env.patch_save(c.id, state="saved", bytes=3, job=SAVE)
    await env.tick()
    assert env.saves_created() == 1
    assert await env.gone(c)


async def test_same_object_name(env, monkeypatch):
    calls: list[tuple[str, str]] = []
    real = cache_mod.presign

    def spy(settings, method, key, *a, **kw):
        calls.append((method, key))
        return real(settings, method, key, *a, **kw)

    monkeypatch.setattr(cache_mod, "presign", spy)
    c = await env.ready()
    await env.delete(c)
    await env.tick()
    assert calls[0] == ("GET", CACHE_KEY) and calls[-1] == ("PUT", CACHE_KEY)
    job = env.kube.get("Job", SAVE)
    vars_ = {v["name"]: v["value"] for v in job["spec"]["template"]["spec"]["containers"][0]["env"]}
    obj = (await env.cache(c.id))["object"]
    assert urlsplit(vars_["CACHE_URL"]).path == "/" + obj
    assert obj == f"workspace-cache/{CACHE_KEY}.tar.zst"
    assert RESTORE  # the restore used the same key

"""The workspace cache restore runs before init and the Claude sessions (D1, D3, D11)."""

from __future__ import annotations

import json
from datetime import timedelta

import pytest
import pytest_asyncio
from factories import insert_command
from fakes.fake_kube import FakeKube
from fakes.fake_wrapper import FakeWrapper
from psycopg.rows import dict_row

from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import sessions
from agents_orchestrator.k8s.kube import KubeError
from agents_orchestrator.reconciler import Reconciler
from agents_orchestrator.reconciler.cache import CacheGate, restore_job_name
from agents_orchestrator.reconciler.checks import DEADLINE_GRACE_SECONDS
from agents_orchestrator.reconciler.init import init_job_name
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec
from agents_orchestrator.tokens import tokens_for
from agents_orchestrator.wrapper.client import WrapperClient

pytestmark = pytest.mark.asyncio

KEY = "k" * 32
CACHE_KEY = "preset-0123456789abcdef"
RESTORE = restore_job_name("alpha")
INIT = init_job_name("alpha")
SESSIONS_MISS = {
    "state": "miss",
    "started_at": "2026-10-28T00:00:00+00:00",
    "finished_at": "2026-10-28T00:00:01+00:00",
    "bytes": None,
    "reason": None,
    "job": None,
}


def line(outcome: str, nbytes: int | None = None, reason: str | None = None) -> str:
    return "noise\n" + json.dumps({"outcome": outcome, "bytes": nbytes, "reason": reason})


class Env:
    def __init__(self, pool, dsn: str, fake: FakeWrapper | None = None, **settings) -> None:
        self.pool, self.fake = pool, fake
        self.kube = FakeKube(namespace="agents")
        base = {
            "cache_s3_endpoint": "http://rgw.example:80",
            "cache_s3_bucket": "bucket",
            "cache_s3_access_key_id": "TESTIDEXAMPLE",
            "cache_s3_secret_access_key": "s3cr3t",
            "cache_image": "harbor.example/orch:1",
        }
        self.settings = Settings(
            database_url=dsn,
            master_key=KEY,
            agent_image="harbor.example/wrapper:1",
            **{**base, **settings},
        )
        factory = self._factory if fake is not None else None
        self.rec = Reconciler(pool, self.kube, self.settings, wrapper_factory=factory)

    def _factory(self, url: str, token: str):
        return WrapperClient(self.fake.base_url, token)

    async def create(self, slug: str = "alpha", cache: bool = True, **spec):
        """A new session row, with the workspace cache unless `cache` is False."""
        if cache:
            spec["cache"] = {"restore_key": CACHE_KEY, "save_key": CACHE_KEY}
        async with self.pool.connection() as conn:
            c = await sessions.create(conn, slug, SessionSpec(**spec))
        if self.fake is not None:
            self.fake.token = tokens_for(self.settings, c.id).auth
        return c

    async def session(self, slug: str = "alpha", cache: bool = True, **spec):
        """A session whose Claude transcripts restore already found no object (`miss`).

        Its pods then wait for the workdir restore alone, which this suite tests; the
        transcripts restore is pinned by tests/test_sessions_restore.py.
        """
        c = await self.create(slug, cache, **spec)
        if not cache:
            return c
        status = {**c.status, "cache": {"sessions_restore": SESSIONS_MISS}}
        async with self.pool.connection() as conn:
            return await sessions.set_status(conn, c.id, status, c.observed_generation)

    async def tick(self, times: int = 1) -> None:
        for _ in range(times):
            await self.rec.reconcile_once()

    async def status(self, session_id) -> dict:
        async with self.pool.connection() as conn:
            return (await sessions.get(conn, session_id)).status

    async def cache(self, session_id) -> dict | None:
        return (await self.status(session_id)).get("cache")

    async def patch_restore(self, session_id, **fields) -> None:
        async with self.pool.connection() as conn:
            c = await sessions.get(conn, session_id)
            status = dict(c.status)
            cache = dict(status["cache"])
            cache["restore"] = {**cache["restore"], **fields}
            status["cache"] = cache
            await sessions.set_status(conn, session_id, status, c.observed_generation)

    async def agents(self, session_id) -> list[agents_repo.Agent]:
        async with self.pool.connection() as conn:
            return await agents_repo.list_agents(conn, session_id)

    def creates(self, name: str) -> int:
        return sum(
            1 for c in self.kube.calls if (c.verb, c.kind, c.name) == ("create", "Job", name)
        )


@pytest_asyncio.fixture
async def env(pool, dsn) -> Env:
    return Env(pool, dsn)


@pytest_asyncio.fixture
async def fake():
    async with FakeWrapper() as fw:
        yield fw


@pytest_asyncio.fixture
async def wenv(pool, dsn, fake) -> Env:
    e = Env(pool, dsn, fake)
    e.kube.pods_ready = True
    return e


async def test_no_cache_no_job(env):
    c = await env.session("alpha", cache=False)
    await env.tick(2)
    assert await env.cache(c.id) is None
    assert not env.kube.has("Job", RESTORE)
    assert env.creates(RESTORE) == 0


async def test_restore_job_created_before_init(env):
    c = await env.session("alpha", init_scripts=["echo a"])
    await env.tick()
    cache = await env.cache(c.id)
    assert cache["restore_resolved"] == f"workspace-cache/{CACHE_KEY}.tar.zst"
    restore = cache["restore"]
    assert restore["state"] == "running" and restore["job"] == RESTORE
    assert restore["started_at"] and restore["finished_at"] is None
    assert restore["bytes"] is None and restore["reason"] is None
    job = env.kube.get("Job", RESTORE)
    env_vars = {
        e["name"]: e["value"] for e in job["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert env_vars["CACHE_URL"].startswith("http://bucket.rgw.example:80/workspace-cache/")
    assert "X-Amz-Signature" in env_vars["CACHE_URL"]
    assert "X-Amz-Algorithm" in env_vars["CACHE_URL"]
    await env.tick(2)  # still running: no init Job
    assert not env.kube.has("Job", INIT)
    assert env.creates(INIT) == 0
    assert (await env.status(c.id))["phase"] == "pending"
    assert "cache restore running" in (await env.status(c.id))["message"]


async def test_restored_then_init_runs(env):
    c = await env.session("alpha", init_scripts=["echo a"])
    await env.tick()
    env.kube.finish_job(RESTORE, exit_code=0, log=line("restored", 4096))
    await env.tick()
    restore = (await env.cache(c.id))["restore"]
    assert restore["state"] == "restored" and restore["bytes"] == 4096
    assert restore["finished_at"] and restore["reason"] is None
    assert not env.kube.has("Job", RESTORE)
    await env.tick()
    assert env.kube.has("Job", INIT)


async def test_miss_is_not_degraded(wenv, fake):
    c = await wenv.session("alpha")
    await wenv.tick()
    wenv.kube.finish_job(RESTORE, exit_code=0, log=line("miss"))
    await wenv.tick(2)
    restore = (await wenv.cache(c.id))["restore"]
    assert restore["state"] == "miss" and restore["bytes"] is None
    assert len(fake.sessions) == 1
    status = await wenv.status(c.id)
    assert status["phase"] == "ready" and "cache" not in status["message"]


async def test_reset_is_not_degraded(wenv, fake):
    c = await wenv.session("alpha")
    await wenv.tick()
    wenv.kube.finish_job(
        RESTORE, exit_code=0, log=line("reset", reason="extract_failed: ExtractError: bad")
    )
    await wenv.tick(2)
    restore = (await wenv.cache(c.id))["restore"]
    assert restore["state"] == "reset" and restore["reason"].startswith("extract_failed: ")
    assert len(fake.sessions) == 1
    status = await wenv.status(c.id)
    assert status["phase"] == "ready" and "cache" not in status["message"]


async def test_failed_restore_degrades(wenv, fake):
    c = await wenv.session("alpha")
    await wenv.tick()
    wenv.kube.finish_job(RESTORE, exit_code=1, log=line("failed", reason="http 403"))
    await wenv.tick(2)
    restore = (await wenv.cache(c.id))["restore"]
    assert (restore["state"], restore["reason"]) == ("failed", "http 403")
    assert not wenv.kube.has("Job", RESTORE)
    assert len(fake.sessions) == 1  # the sessions start anyway
    status = await wenv.status(c.id)
    assert status["phase"] == "degraded"
    assert "cache restore failed: http 403" in status["message"]

    # no JSON line at all: the exit code
    d = await wenv.session("beta")
    await wenv.tick()
    wenv.kube.finish_job(restore_job_name("beta"), exit_code=137, log="killed")
    await wenv.tick()
    assert (await wenv.cache(d.id))["restore"]["reason"] == "exit 137"


async def test_unparseable_outcome_is_never_success(env):
    c = await env.session("alpha")
    await env.tick()
    env.kube.finish_job(RESTORE, exit_code=0, log="all done\nnot json")
    await env.tick()
    restore = (await env.cache(c.id))["restore"]
    assert (restore["state"], restore["reason"]) == ("failed", "bad_output")
    d = await env.session("beta")
    await env.tick()
    env.kube.finish_job(restore_job_name("beta"), exit_code=0, log="")
    await env.tick()
    assert (await env.cache(d.id))["restore"]["state"] == "failed"
    e = await env.session("gamma")
    await env.tick()
    env.kube.finish_job(restore_job_name("gamma"), exit_code=0, log='{"outcome": "weird"}')
    await env.tick()
    assert (await env.cache(e.id))["restore"]["state"] == "failed"


async def test_restore_timeout_missing_foreign(env):
    # the Job's own deadline fired
    c = await env.session("alpha")
    await env.tick()
    env.kube.finish_job(RESTORE, deadline_exceeded=True)
    await env.tick()
    restore = (await env.cache(c.id))["restore"]
    assert (restore["state"], restore["reason"]) == ("failed", "timeout")
    assert not env.kube.has("Job", RESTORE)

    # older than the cache timeout plus the grace
    b = await env.session("beta")
    await env.tick()
    old = (
        await _now(env)
        - timedelta(seconds=env.settings.cache_timeout_seconds + DEADLINE_GRACE_SECONDS + 5)
    ).isoformat()
    await env.patch_restore(b.id, started_at=old)
    await env.tick()
    assert (await env.cache(b.id))["restore"]["reason"] == "timeout"

    # the Job vanished after its create
    g = await env.session("gamma")
    await env.tick()
    await env.kube.delete("Job", "agents", restore_job_name("gamma"))
    await env.tick()
    restore = (await env.cache(g.id))["restore"]
    assert (restore["state"], restore["reason"]) == ("failed", "job_missing")

    # a foreign Job under the name is never touched
    env.kube.seed(
        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": restore_job_name("delta"), "labels": {"app": "other"}},
        }
    )
    d = await env.session("delta")
    await env.tick()
    restore = (await env.cache(d.id))["restore"]
    assert (restore["state"], restore["reason"]) == ("failed", "foreign_object")
    assert env.kube.has("Job", restore_job_name("delta"))

    # a create error
    env.kube.fail("Job", restore_job_name("eps"), KubeError("quota"), verbs=["create"])
    e = await env.session("eps")
    await env.tick()
    restore = (await env.cache(e.id))["restore"]
    assert restore["state"] == "failed" and restore["reason"].startswith("job_create_failed: quota")


async def _now(env):
    async with env.pool.connection() as conn:
        return (await (await conn.execute("SELECT now()")).fetchone())[0]


async def test_not_configured(pool, dsn):
    e = Env(pool, dsn, cache_s3_bucket="")
    async with pool.connection() as conn:
        c = await sessions.create(
            conn, "alpha", SessionSpec(cache={"restore_key": CACHE_KEY, "save_key": CACHE_KEY})
        )
    await e.tick()
    restore = (await e.cache(c.id))["restore"]
    assert (restore["state"], restore["reason"]) == ("failed", "not_configured")
    assert e.creates(RESTORE) == 0 and not e.kube.has("Job", RESTORE)
    status = await e.status(c.id)
    assert status["phase"] != "error"
    assert "cache restore failed: not_configured" in status["message"]


async def test_crash_safety(env):
    c = await env.session("alpha")
    await env.tick()
    # crash between the running write and the create: no Job, job null
    await env.kube.delete("Job", "agents", RESTORE)
    await env.patch_restore(c.id, job=None)
    await env.tick()
    assert (await env.cache(c.id))["restore"]["state"] == "running"
    assert env.kube.has("Job", RESTORE) and env.creates(RESTORE) == 2
    await env.tick(3)
    assert env.creates(RESTORE) == 2  # exactly once more, then adopted
    assert (await env.cache(c.id))["restore"]["job"] == RESTORE

    # crash between the create and the job write: the existing Job is adopted
    d = await env.session("beta")
    await env.tick()
    await env.patch_restore(d.id, job=None)
    await env.tick()
    assert env.creates(restore_job_name("beta")) == 1
    assert (await env.cache(d.id))["restore"]["job"] == restore_job_name("beta")

    # crash between the outcome and the delete: a later tick deletes it
    env.kube.finish_job(restore_job_name("beta"), exit_code=0, log=line("miss"))
    await env.tick()
    assert not env.kube.has("Job", restore_job_name("beta"))
    env.kube.seed(env_job(restore_job_name("beta")))
    await env.tick()
    assert not env.kube.has("Job", restore_job_name("beta"))


def env_job(name: str) -> dict:
    from agents_orchestrator.reconciler.cache import cache_labels

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": name, "labels": cache_labels("beta", "restore")},
    }


async def test_rebootstrap_held_during_restore(wenv, fake):
    c = await wenv.session("alpha")
    await wenv.tick()
    (agent,) = await wenv.agents(c.id)
    assert agent.ready and not fake.sessions
    cmd = await insert_command(wenv.pool, agent.id, kind="rebootstrap")
    await wenv.rec._run_rebootstraps()
    assert not fake.sessions
    async with wenv.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT status, result FROM commands WHERE id = %s", (cmd["id"],))
        row = await cur.fetchone()
    assert row["status"] == "done" and row["result"]["bootstrapped"] is False
    assert "cache restore running" in row["result"]["message"]


async def test_reconciler_requires_cache_credentials(pool, dsn):
    kube = FakeKube(namespace="agents")
    bad = Settings(
        database_url=dsn,
        master_key=KEY,
        agent_image="x:1",
        cache_s3_bucket="bucket",
        cache_image="img:1",
    )
    with pytest.raises(Exception, match="cache_s3|CACHE_S3"):
        Reconciler(pool, kube, bad)
    Reconciler(pool, kube, Settings(database_url=dsn, master_key=KEY, agent_image="x:1"))


async def test_cache_gate_shapes():
    assert CacheGate.of(None, False) == CacheGate()
    assert CacheGate.of(None, True).hold
    assert CacheGate.of({"restore": {"state": "running"}}, True).hold
    assert CacheGate.of({"restore": {"state": "restored"}}, True) == CacheGate()
    assert CacheGate.of({"restore": {"state": "miss"}}, True) == CacheGate()
    assert CacheGate.of({"restore": {"state": "reset"}}, True) == CacheGate()
    failed = CacheGate.of({"restore": {"state": "failed", "reason": "timeout"}}, True)
    assert (failed.hold, failed.degraded, failed.message) == (
        False,
        True,
        "cache restore failed: timeout",
    )

"""The backup entry restore runs before init and the Claude sessions (D1, D3, D11; S-68)."""

from __future__ import annotations

import json
from datetime import timedelta

import pytest
import pytest_asyncio
from fakes.fake_kube import FakeKube
from fakes.fake_wrapper import FakeWrapper

from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import sessions, volumes
from agents_orchestrator.k8s.kube import KubeError
from agents_orchestrator.reconciler import Reconciler
from agents_orchestrator.reconciler.cache import TX_RESTORE, CacheGate, job_name
from agents_orchestrator.reconciler.checks import DEADLINE_GRACE_SECONDS
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec
from agents_orchestrator.tokens import tokens_for
from agents_orchestrator.wrapper.client import WrapperClient

pytestmark = pytest.mark.asyncio

KEY = "k" * 32
TX_JOB = job_name("alpha", TX_RESTORE)
SESSIONS_MISS = {
    "state": "miss",
    "started_at": "2026-10-28T00:00:00+00:00",
    "finished_at": "2026-10-28T00:00:01+00:00",
    "bytes": None,
    "reason": None,
    "job": None,
}


def res(state: str, index: int = 0, **fields) -> dict:
    return {"index": index, "state": state, **fields}


def line(*results: dict | str, **fields) -> str:
    """The Job's last log line: `line("restored", size=5)` or `line(res(...), res(...))`."""
    if len(results) == 1 and isinstance(results[0], str):
        results = (res(results[0], **fields),)
    return "noise\n" + json.dumps({"entries": list(results)})


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
        """A new session row, with the transcripts cache (a backup_restore volume) unless `cache`
        is False."""
        if cache:
            spec["volumes"] = [await self._volume(slug)]
        async with self.pool.connection() as conn:
            c = await sessions.create(conn, slug, SessionSpec(**spec))
        if self.fake is not None:
            self.fake.token = tokens_for(self.settings, c.id).auth
        return c

    async def _volume(self, slug: str) -> dict:
        """A ready volume of its own (a fresh one: nothing to restore) and its attachment."""
        async with self.pool.connection() as conn, conn.transaction():
            vol = await volumes.create(
                conn,
                name=f"v-{slug}",
                projects=["p"],
                size="10Gi",
                storage_class="rook-ceph-rwx",
                created_by="alice",
                actor_kind="user",
                actor="alice",
            )
            await volumes.set_state(conn, vol.id, "ready")
        return {
            "volume": str(vol.id),
            "mount_path": "/workdir/workdir",
            "mode": "rw",
            "backup_restore": True,
        }

    async def session(self, slug: str = "alpha", cache: bool = True, **spec):
        """A session whose Claude transcripts restore already found no object (`miss`).

        Its pods then wait for the workdir restore alone, which this suite tests; the
        transcripts restore is pinned by tests/test_sessions_restore.py.
        """
        c = await self.create(slug, cache, **spec)
        if not cache:
            return c
        status = {**c.status, "backup": {"transcripts": {"restore": SESSIONS_MISS}}}
        async with self.pool.connection() as conn:
            return await sessions.set_status(conn, c.id, status, c.observed_generation)

    async def tick(self, times: int = 1) -> None:
        for _ in range(times):
            await self.rec.reconcile_once()

    async def status(self, session_id) -> dict:
        async with self.pool.connection() as conn:
            return (await sessions.get(conn, session_id)).status

    async def backup(self, session_id) -> dict | None:
        return (await self.status(session_id)).get("backup")

    async def run(self, session_id, key: str = TX_RESTORE) -> dict | None:
        """The run record of `key` (a transcripts run for the two transcripts keys)."""
        from agents_orchestrator.reconciler.cache import get_run

        return get_run(await self.status(session_id), key)

    async def patch_run(self, session_id, key: str = TX_RESTORE, **fields) -> None:
        from agents_orchestrator.reconciler.cache import _with_run, get_run

        async with self.pool.connection() as conn:
            c = await sessions.get(conn, session_id)
            run = {**get_run(c.status, key), **fields}
            backup = _with_run(c.status.get("backup") or {}, key, run)
            await sessions.set_status(
                conn, session_id, {**c.status, "backup": backup}, c.observed_generation
            )

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


async def test_no_volume_no_transcripts_job(env):
    c = await env.session("alpha", cache=False)
    await env.tick(2)
    assert await env.backup(c.id) is None
    assert not env.kube.has("Job", TX_JOB)
    assert env.creates(TX_JOB) == 0


async def test_unparseable_outcome_is_never_success(env):
    c = await env.create("alpha")
    await env.tick()
    env.kube.finish_job(TX_JOB, exit_code=0, log="all done\nnot json")
    await env.tick()
    run = await env.run(c.id, TX_RESTORE)
    assert (run["state"], run["reason"]) == ("failed", "bad_output")
    d = await env.create("beta")
    await env.tick()
    env.kube.finish_job(job_name("beta", TX_RESTORE), exit_code=0, log="")
    await env.tick()
    assert (await env.run(d.id, TX_RESTORE))["state"] == "failed"
    e = await env.create("gamma")
    await env.tick()
    env.kube.finish_job(job_name("gamma", TX_RESTORE), exit_code=0, log='{"outcome": "weird"}')
    await env.tick()
    assert (await env.run(e.id, TX_RESTORE))["state"] == "failed"


def _big_line(manifest_bytes: int) -> str:
    manifest = {f"dir/file-{i}.txt": "a" * 40 for i in range(manifest_bytes // 60)}
    return "noise\n" + json.dumps({"entries": [res("restored", manifest=manifest)]})


async def test_outcome_line_over_the_16k_tail_still_parses(env):
    log = _big_line(200_000)
    assert len(log.splitlines()[-1]) > 150_000
    c = await env.create("alpha")
    await env.tick()
    env.kube.finish_job(TX_JOB, exit_code=0, log=log)
    await env.tick()
    run = await env.run(c.id, TX_RESTORE)
    assert run["state"] == "restored" and run["reason"] != "bad_output"


async def test_outcome_line_over_the_cap_is_bad_output(env, monkeypatch):
    monkeypatch.setattr("agents_orchestrator.reconciler.cache.OUTCOME_MAX_BYTES", 50_000)
    c = await env.create("alpha")
    await env.tick()
    env.kube.finish_job(TX_JOB, exit_code=0, log=_big_line(200_000))
    await env.tick()
    run = await env.run(c.id, TX_RESTORE)
    assert (run["state"], run["reason"]) == ("failed", "bad_output")


async def test_restore_timeout_missing_foreign(env):
    # the Job's own deadline fired
    c = await env.create("alpha")
    await env.tick()
    env.kube.finish_job(TX_JOB, deadline_exceeded=True)
    await env.tick()
    run = await env.run(c.id, TX_RESTORE)
    assert (run["state"], run["reason"]) == ("failed", "timeout")
    assert not env.kube.has("Job", TX_JOB)

    # older than the cache timeout plus the grace
    b = await env.create("beta")
    await env.tick()
    old = (
        await _now(env)
        - timedelta(seconds=env.settings.cache_timeout_seconds + DEADLINE_GRACE_SECONDS + 5)
    ).isoformat()
    await env.patch_run(b.id, TX_RESTORE, started_at=old)
    await env.tick()
    assert (await env.run(b.id, TX_RESTORE))["reason"] == "timeout"

    # the Job vanished after its create
    g = await env.create("gamma")
    await env.tick()
    await env.kube.delete("Job", "agents", job_name("gamma", TX_RESTORE))
    await env.tick()
    run = await env.run(g.id, TX_RESTORE)
    assert (run["state"], run["reason"]) == ("failed", "job_missing")

    # a foreign Job under the name is never touched
    env.kube.seed(
        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": job_name("delta", TX_RESTORE), "labels": {"app": "other"}},
        }
    )
    d = await env.create("delta")
    await env.tick()
    run = await env.run(d.id, TX_RESTORE)
    assert (run["state"], run["reason"]) == ("failed", "foreign_object")
    assert env.kube.has("Job", job_name("delta", TX_RESTORE))

    # a create error
    env.kube.fail("Job", job_name("eps", TX_RESTORE), KubeError("quota"), verbs=["create"])
    e = await env.create("eps")
    await env.tick()
    run = await env.run(e.id, TX_RESTORE)
    assert run["state"] == "failed" and run["reason"].startswith("job_create_failed")


async def _now(env):
    async with env.pool.connection() as conn:
        return (await (await conn.execute("SELECT now()")).fetchone())[0]


async def test_not_configured(pool, dsn):
    e = Env(pool, dsn, cache_s3_bucket="")
    c = await e.create("alpha")
    await e.tick()
    run = await e.run(c.id, TX_RESTORE)
    assert (run["state"], run["reason"]) == ("skipped", "not_configured")
    assert e.creates(TX_JOB) == 0 and not e.kube.has("Job", TX_JOB)
    assert (await e.status(c.id))["phase"] != "error"


async def test_crash_safety(env):
    c = await env.create("alpha")
    await env.tick()
    # crash between the running write and the create: no Job, job null
    await env.kube.delete("Job", "agents", TX_JOB)
    await env.patch_run(c.id, TX_RESTORE, job=None)
    await env.tick()
    assert (await env.run(c.id, TX_RESTORE))["state"] == "running"
    assert env.kube.has("Job", TX_JOB) and env.creates(TX_JOB) == 2
    await env.tick(3)
    assert env.creates(TX_JOB) == 2  # exactly once more, then adopted
    assert (await env.run(c.id, TX_RESTORE))["job"] == TX_JOB

    # crash between the create and the job write: the existing Job is adopted
    d = await env.create("beta")
    await env.tick()
    await env.patch_run(d.id, TX_RESTORE, job=None)
    await env.tick()
    beta = job_name("beta", TX_RESTORE)
    assert env.creates(beta) == 1
    assert (await env.run(d.id, TX_RESTORE))["job"] == beta

    # crash between the outcome and the delete: a later tick deletes it
    env.kube.finish_job(beta, exit_code=0, log=line("missing"))
    await env.tick()
    assert not env.kube.has("Job", beta)
    env.kube.seed(env_job(beta))
    await env.tick()
    assert not env.kube.has("Job", beta)


def env_job(name: str) -> dict:
    from agents_orchestrator.reconciler.cache import cache_labels

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": name, "labels": cache_labels("beta", TX_RESTORE)},
    }


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
    assert CacheGate.of_run(None, "x").hold
    assert CacheGate.of_run({"state": "running"}, "x").hold
    for state in ("done", "skipped"):
        assert CacheGate.of_run({"state": state}, "x") == CacheGate()
    failed = CacheGate.of_run({"state": "failed", "reason": "timeout"}, "backup restore")
    assert (failed.hold, failed.degraded, failed.message) == (
        False,
        True,
        "backup restore failed: timeout",
    )

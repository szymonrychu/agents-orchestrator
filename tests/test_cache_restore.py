"""The backup entry restore runs before init and the Claude sessions (D1, D3, D11; S-68)."""

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
from agents_orchestrator.db import backups as backups_repo
from agents_orchestrator.db import sessions
from agents_orchestrator.k8s.kube import KubeError
from agents_orchestrator.reconciler import Reconciler
from agents_orchestrator.reconciler.cache import (
    RESTORE_POST,
    RESTORE_PRE,
    CacheGate,
    job_name,
)
from agents_orchestrator.reconciler.checks import DEADLINE_GRACE_SECONDS
from agents_orchestrator.reconciler.init import init_job_name
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec
from agents_orchestrator.tokens import tokens_for
from agents_orchestrator.wrapper.client import WrapperClient

pytestmark = pytest.mark.asyncio

KEY = "k" * 32
CACHE_KEY = "proj/workspace"
WS_ENTRY = {"root": "workspace", "path": ".", "mode": "both", "name": CACHE_KEY}
RESTORE = job_name("alpha", RESTORE_PRE)
RESTORE_AFTER = job_name("alpha", RESTORE_POST)
INIT = init_job_name("alpha")
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


async def seed_backup(pool, name: str = CACHE_KEY) -> str:
    """A current version of `name`; returns its object key."""
    async with pool.connection() as conn:
        existing = await backups_repo.current(conn, name)
        if existing is not None:
            return existing.object_key
        _, version = await backups_repo.record_save(
            conn,
            name=name,
            object_key=f"workspace-cache/b/{name}/20260101t000000z-seed-aaaa.tar.zst",
            size=1,
            digest="d" * 64,
            manifest=None,
            base_version=None,
            origin="save",
            session_slug=None,
            preset_id=None,
            project=None,
            created_by="seed",
            orphan=False,
        )
    return version.object_key


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
            spec["backup"] = [WS_ENTRY]
            await seed_backup(self.pool)
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

    async def run(self, session_id, key: str = RESTORE_PRE) -> dict | None:
        """The run record of `key` (a transcripts run for the two transcripts keys)."""
        from agents_orchestrator.reconciler.cache import get_run

        return get_run(await self.status(session_id), key)

    async def entry(self, session_id, index: int = 0) -> dict:
        return ((await self.backup(session_id)) or {})["entries"][index]

    async def patch_run(self, session_id, key: str = RESTORE_PRE, **fields) -> None:
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


async def test_no_cache_no_job(env):
    c = await env.session("alpha", cache=False)
    await env.tick(2)
    assert await env.backup(c.id) is None
    assert not env.kube.has("Job", RESTORE)
    assert env.creates(RESTORE) == 0


async def test_restore_job_created_before_init(env):
    c = await env.session("alpha", init_scripts=["echo a"])
    await env.tick()
    run = await env.run(c.id)
    assert run["state"] == "running" and run["job"] == RESTORE
    assert run["started_at"] and run["finished_at"] is None and run["reason"] is None
    assert [p["name"] for p in run["plan"]] == [CACHE_KEY]
    entry = await env.entry(c.id)
    assert entry["restore"]["state"] == "running" and entry["version"] == run["plan"][0]["version"]
    job = env.kube.get("Job", RESTORE)
    env_vars = {
        e["name"]: e["value"] for e in job["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    (item,) = json.loads(env_vars["CACHE_ENTRIES"])
    assert item["url"].startswith("http://bucket.rgw.example:80/workspace-cache/b/proj/workspace/")
    assert "X-Amz-Signature" in item["url"] and item["version"] == run["plan"][0]["version"]
    await env.tick(2)  # still running: no init Job
    assert not env.kube.has("Job", INIT)
    assert env.creates(INIT) == 0
    assert (await env.status(c.id))["phase"] == "pending"
    assert "backup restore pre running" in (await env.status(c.id))["message"]


async def test_restored_then_init_runs(env):
    c = await env.session("alpha", init_scripts=["echo a"])
    await env.tick()
    env.kube.finish_job(RESTORE, exit_code=0, log=line("restored", size=4096))
    await env.tick()
    run = await env.run(c.id)
    assert run["state"] == "done" and run["finished_at"] and run["reason"] is None
    assert (await env.entry(c.id))["restore"]["state"] == "restored"
    assert not env.kube.has("Job", RESTORE)
    await env.tick()
    assert env.kube.has("Job", INIT)


async def test_missing_and_unreadable_are_not_degraded(wenv, fake):
    c = await wenv.session("alpha")
    await wenv.tick()
    wenv.kube.finish_job(RESTORE, exit_code=0, log=line("unreadable", reason="ExtractError: bad"))
    await wenv.tick(2)
    assert (await wenv.entry(c.id))["restore"]["state"] == "unreadable"
    assert len(fake.sessions) == 1
    status = await wenv.status(c.id)
    assert status["phase"] == "ready" and "backup" not in status["message"]


async def test_failed_restore_degrades(wenv, fake):
    c = await wenv.session("alpha")
    await wenv.tick()
    wenv.kube.finish_job(RESTORE, exit_code=1, log=line("failed", reason="http 403"))
    await wenv.tick(2)
    run = await wenv.run(c.id)
    assert (run["state"], run["reason"]) == ("failed", "http 403")
    assert (await wenv.entry(c.id))["restore"]["state"] == "failed"
    assert not wenv.kube.has("Job", RESTORE)
    assert len(fake.sessions) == 1  # the sessions start anyway
    status = await wenv.status(c.id)
    assert status["phase"] == "degraded"
    assert "backup restore pre failed: http 403" in status["message"]

    # no JSON line at all: the exit code
    d = await wenv.session("beta")
    await wenv.tick()
    wenv.kube.finish_job(job_name("beta", RESTORE_PRE), exit_code=137, log="killed")
    await wenv.tick()
    assert (await wenv.run(d.id))["reason"] == "exit 137"


async def test_unparseable_outcome_is_never_success(env):
    c = await env.session("alpha")
    await env.tick()
    env.kube.finish_job(RESTORE, exit_code=0, log="all done\nnot json")
    await env.tick()
    run = await env.run(c.id)
    assert (run["state"], run["reason"]) == ("failed", "bad_output")
    d = await env.session("beta")
    await env.tick()
    env.kube.finish_job(job_name("beta", RESTORE_PRE), exit_code=0, log="")
    await env.tick()
    assert (await env.run(d.id))["state"] == "failed"
    e = await env.session("gamma")
    await env.tick()
    env.kube.finish_job(job_name("gamma", RESTORE_PRE), exit_code=0, log='{"outcome": "weird"}')
    await env.tick()
    assert (await env.run(e.id))["state"] == "failed"


async def test_restore_timeout_missing_foreign(env):
    # the Job's own deadline fired
    c = await env.session("alpha")
    await env.tick()
    env.kube.finish_job(RESTORE, deadline_exceeded=True)
    await env.tick()
    run = await env.run(c.id)
    assert (run["state"], run["reason"]) == ("failed", "timeout")
    assert not env.kube.has("Job", RESTORE)

    # older than the cache timeout plus the grace
    b = await env.session("beta")
    await env.tick()
    old = (
        await _now(env)
        - timedelta(seconds=env.settings.cache_timeout_seconds + DEADLINE_GRACE_SECONDS + 5)
    ).isoformat()
    await env.patch_run(b.id, started_at=old)
    await env.tick()
    assert (await env.run(b.id))["reason"] == "timeout"

    # the Job vanished after its create
    g = await env.session("gamma")
    await env.tick()
    await env.kube.delete("Job", "agents", job_name("gamma", RESTORE_PRE))
    await env.tick()
    run = await env.run(g.id)
    assert (run["state"], run["reason"]) == ("failed", "job_missing")

    # a foreign Job under the name is never touched
    env.kube.seed(
        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": job_name("delta", RESTORE_PRE), "labels": {"app": "other"}},
        }
    )
    d = await env.session("delta")
    await env.tick()
    run = await env.run(d.id)
    assert (run["state"], run["reason"]) == ("failed", "foreign_object")
    assert env.kube.has("Job", job_name("delta", RESTORE_PRE))

    # a create error
    env.kube.fail("Job", job_name("eps", RESTORE_PRE), KubeError("quota"), verbs=["create"])
    e = await env.session("eps")
    await env.tick()
    run = await env.run(e.id)
    assert run["state"] == "failed" and run["reason"].startswith("job_create_failed")


async def _now(env):
    async with env.pool.connection() as conn:
        return (await (await conn.execute("SELECT now()")).fetchone())[0]


async def test_not_configured(pool, dsn):
    e = Env(pool, dsn, cache_s3_bucket="")
    async with pool.connection() as conn:
        c = await sessions.create(conn, "alpha", SessionSpec(backup=[WS_ENTRY]))
    await e.tick()
    run = await e.run(c.id)
    assert (run["state"], run["reason"]) == ("failed", "not_configured")
    assert e.creates(RESTORE) == 0 and not e.kube.has("Job", RESTORE)
    status = await e.status(c.id)
    assert status["phase"] != "error"
    assert "backup restore pre failed: not_configured" in status["message"]


async def test_crash_safety(env):
    c = await env.session("alpha")
    await env.tick()
    # crash between the running write and the create: no Job, job null
    await env.kube.delete("Job", "agents", RESTORE)
    await env.patch_run(c.id, job=None)
    await env.tick()
    assert (await env.run(c.id))["state"] == "running"
    assert env.kube.has("Job", RESTORE) and env.creates(RESTORE) == 2
    await env.tick(3)
    assert env.creates(RESTORE) == 2  # exactly once more, then adopted
    assert (await env.run(c.id))["job"] == RESTORE

    # crash between the create and the job write: the existing Job is adopted
    d = await env.session("beta")
    await env.tick()
    await env.patch_run(d.id, job=None)
    await env.tick()
    beta = job_name("beta", RESTORE_PRE)
    assert env.creates(beta) == 1
    assert (await env.run(d.id))["job"] == beta

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
        "metadata": {"name": name, "labels": cache_labels("beta", RESTORE_PRE)},
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
    assert "backup restore pre running" in row["result"]["message"]


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

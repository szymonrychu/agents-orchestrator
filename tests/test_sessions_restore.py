"""Claude transcripts (/sessions) are restored from S3 before new session pods start (D7, G6)."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlsplit

import pytest
import pytest_asyncio
from fakes.fake_wrapper import FakeWrapper
from test_cache_jobs_render import container_of, pod_of, walk
from test_cache_restore import Env, line
from test_cache_save import uploaded

from agents_orchestrator.db import sessions
from agents_orchestrator.k8s import labels, render
from agents_orchestrator.k8s.labels import object_names, session_labels
from agents_orchestrator.reconciler import cache as cache_mod
from agents_orchestrator.reconciler.cache import (
    CACHE_LABEL,
    TX_RESTORE,
    TX_SAVE,
    job_name,
    render_cache_job,
)
from agents_orchestrator.spec import SESSIONS_EXCLUDE

pytestmark = pytest.mark.asyncio

SESSIONS_RESTORE = job_name("alpha", TX_RESTORE)
SESSIONS_SAVE = job_name("alpha", TX_SAVE)
STS = object_names("alpha")["statefulset"]


class RestoreEnv(Env):
    """Cached sessions start without a sessions restore record, as every new session does."""

    def __init__(self, pool, dsn, fake: FakeWrapper | None = None, **settings) -> None:
        super().__init__(
            pool,
            dsn,
            fake,
            **{"discard_drain_seconds": 0, "discard_quiesce_timeout_seconds": 60, **settings},
        )
        self.kube.pods_ready = True
        self.sessions_saves: list[dict] = []
        inner = self.rec.cache.write_status

        async def spy(session, fields, generation, **kw):
            run = ((fields.get("backup") or {}).get("transcripts") or {}).get("save")
            if run is not None:
                self.sessions_saves.append(dict(run))
            return await inner(session, fields, generation, **kw)

        self.rec.cache.write_status = spy

    async def session(self, slug: str = "alpha", cache: bool = True, **spec):
        return await self.create(slug, cache, **spec)

    def agent_pods(self, slug: str = "alpha") -> list[str]:
        base = object_names(slug)["statefulset"] + "-"
        return [
            p for p in self.kube.names("Pod") if p.startswith(base) and p[len(base) :].isdigit()
        ]

    def sts_replicas(self, slug: str = "alpha") -> list[int]:
        """The replicas of every StatefulSet apply of `slug`, in order."""
        name = object_names(slug)["statefulset"]
        return [
            a.obj["spec"]["replicas"] for a in self.kube.applied("StatefulSet") if a.name == name
        ]

    async def delete(self, c) -> None:
        async with self.pool.connection() as conn:
            await sessions.request_delete(conn, c.id)

    async def gone(self, c) -> bool:
        async with self.pool.connection() as conn:
            return await sessions.get(conn, c.id) is None


@pytest_asyncio.fixture
async def fake():
    async with FakeWrapper() as fw:
        yield fw


@pytest_asyncio.fixture
async def env(pool, dsn, fake) -> RestoreEnv:
    return RestoreEnv(pool, dsn, fake)


def _env(job: dict[str, Any]) -> dict[str, str]:
    return {e["name"]: e["value"] for e in container_of(job)["env"]}


async def test_restore_runs_before_agent_pods_start(env, fake):
    c = await env.session("alpha")
    await env.tick()
    run = await env.run(c.id, TX_RESTORE)
    assert run["state"] == "running" and run["job"] == SESSIONS_RESTORE
    assert env.kube.has("Job", SESSIONS_RESTORE)
    assert env.kube.has("StatefulSet", STS) and env.sts_replicas() == [0]
    assert env.agent_pods() == [] and await env.agents(c.id) == []
    status = await env.status(c.id)
    assert status["phase"] == "pending" and "sessions restore running" in status["message"]

    await env.tick(2)
    assert env.agent_pods() == [] and fake.sessions == {}
    assert set(env.sts_replicas()) == {0}
    assert (await env.status(c.id))["phase"] == "pending"

    # the outcome starts them in the same tick, and the wrapper sessions follow
    env.kube.finish_job(SESSIONS_RESTORE, exit_code=0, log=line("restored", size=2048))
    await env.tick()
    assert env.sts_replicas()[-1] == 1
    assert env.agent_pods() == [f"{STS}-0"]
    await env.tick()
    assert len(fake.sessions) == 1
    status = await env.status(c.id)
    assert status["phase"] == "ready" and "sessions restore" not in status["message"]
    # every apply before the outcome held the pods at zero, and the Job came first
    first_pods = next(i for i, r in enumerate(env.sts_replicas()) if r > 0)
    assert set(env.sts_replicas()[:first_pods]) == {0}
    creates = [
        (call.verb, call.kind, call.name)
        for call in env.kube.calls
        if call.verb == "create" and call.kind == "Job"
    ]
    assert ("create", "Job", SESSIONS_RESTORE) in creates
    assert env.creates(SESSIONS_RESTORE) == 1


async def test_sessions_restore_job_render(env, monkeypatch):
    signed: list[tuple[str, str]] = []
    real = cache_mod.presign_object

    def spy(settings, method, key):
        signed.append((method, key))
        return real(settings, method, key)

    monkeypatch.setattr(cache_mod, "presign_object", spy)
    c = await env.session("alpha")
    await env.tick()
    assert ("GET", "workspace-cache/sessions/alpha.tar.zst") in signed

    job = env.kube.get("Job", SESSIONS_RESTORE)
    assert job["metadata"]["name"] == "agent-alpha-sessions-restore"
    want = {**labels.session_labels("alpha"), CACHE_LABEL: "sessions-restore"}
    assert job["metadata"]["labels"] == want
    assert job["spec"]["template"]["metadata"]["labels"] == want
    container = container_of(job)
    assert container["command"] == ["orch", "cache", "restore"]
    # only the PVC subPath `sessions`, writable, plus the HOME emptyDir; no scratch for a restore
    assert container["volumeMounts"] == [
        {"name": render.WORKSPACE_VOLUME, "mountPath": "/sessions", "subPath": "sessions"},
        {"name": "tmp", "mountPath": "/tmp"},
    ]
    pvc = object_names("alpha")["pvc"]
    assert [v for k, v in walk(job) if k == "persistentVolumeClaim"] == [{"claimName": pvc}]
    assert [v["name"] for v in pod_of(job)["volumes"]] == [render.WORKSPACE_VOLUME, "tmp"]

    env_vars = _env(job)
    (entry,) = json.loads(env_vars["CACHE_ENTRIES"])
    assert (entry["root"], entry["root_dir"], entry["path"]) == ("sessions", "/sessions", ".")
    assert entry["exclude"] == list(SESSIONS_EXCLUDE)
    parts = urlsplit(entry["url"])
    assert parts.path == "/workspace-cache/sessions/alpha.tar.zst"
    assert "X-Amz-Signature" in parts.query
    # the reconciler's Job is the render of the transcripts run with that entry
    session = await _get(env, c.id)
    assert job["spec"] == render_cache_job(session, env.settings, TX_RESTORE, [entry])["spec"]
    for forbidden in ("envFrom", "secretKeyRef", "secretName"):
        assert forbidden not in json.dumps(job)
    assert pod_of(job)["automountServiceAccountToken"] is False


async def _get(env, session_id):
    async with env.pool.connection() as conn:
        return await sessions.get(conn, session_id)


async def test_sessions_restore_miss_starts_pods(env, fake):
    c = await env.session("alpha")
    await env.tick()
    env.kube.finish_job(SESSIONS_RESTORE, exit_code=0, log=line("missing"))
    await env.tick(2)
    run = await env.run(c.id, TX_RESTORE)
    assert run["state"] == "miss" and run["bytes"] is None and run["reason"] is None
    assert not env.kube.has("Job", SESSIONS_RESTORE)
    assert env.agent_pods() == [f"{STS}-0"] and len(fake.sessions) == 1
    status = await env.status(c.id)
    assert status["phase"] == "ready" and "sessions restore" not in status["message"]


@pytest.mark.parametrize(
    ("finish", "reason", "skip"),
    [
        (
            {"exit_code": 1, "log": line("failed", reason="download_failed: HTTP 403")},
            "download_failed: HTTP 403",
            "download_failed",
        ),
        ({"exit_code": 137, "log": "killed"}, "exit 137", "exit"),
        ({"deadline_exceeded": True}, "timeout", "timeout"),
    ],
)
async def test_sessions_restore_failure_starts_pods_and_skips_save(env, fake, finish, reason, skip):
    c = await env.session("alpha")
    await env.tick()
    env.kube.finish_job(SESSIONS_RESTORE, **finish)
    await env.tick(2)
    run = await env.run(c.id, TX_RESTORE)
    assert (run["state"], run["reason"]) == ("failed", reason)
    assert not env.kube.has("Job", SESSIONS_RESTORE)
    assert env.agent_pods() == [f"{STS}-0"] and len(fake.sessions) == 1  # started anyway
    status = await env.status(c.id)
    assert status["phase"] == "degraded"
    assert f"sessions restore failed: {reason}" in status["message"]

    # the transcripts save after a failed restore is skipped
    await env.delete(c)
    await env.tick(2)
    assert await env.gone(c)
    assert env.creates(SESSIONS_SAVE) == 0
    assert env.sessions_saves[-1] == {"state": "skipped", "reason": skip}


async def test_existing_session_skips_restore_and_still_saves(env, fake):
    # the StatefulSet existed before the feature arrived
    env.kube.seed(
        {
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": {"name": STS, "labels": session_labels("alpha")},
            "spec": {"replicas": 1},
        }
    )
    c = await env.session("alpha")
    await env.tick()
    run = await env.run(c.id, TX_RESTORE)
    assert run == {"state": "skipped", "reason": "pods_exist"}
    assert env.creates(SESSIONS_RESTORE) == 0 and not env.kube.has("Job", SESSIONS_RESTORE)
    assert 0 not in env.sts_replicas()  # never held: the pods keep running
    await env.tick(3)
    assert env.creates(SESSIONS_RESTORE) == 0
    assert await env.run(c.id, TX_RESTORE) == run
    assert env.agent_pods() == [f"{STS}-0"]

    # it still saves at discard
    await env.delete(c)
    await env.tick(2)
    assert env.creates(SESSIONS_SAVE) == 1
    env.kube.finish_job(SESSIONS_SAVE, exit_code=0, log=uploaded(20))
    await env.tick()
    assert await env.gone(c)

    # an agent pod alone (no StatefulSet) also counts
    env.kube.seed(
        {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "agent-beta-0", "labels": session_labels("beta")},
            "status": {"phase": "Running"},
        }
    )
    d = await env.session("beta")
    await env.tick()
    assert await env.run(d.id, TX_RESTORE) == run
    assert env.creates(job_name("beta", TX_RESTORE)) == 0
    assert 0 not in env.sts_replicas("beta")


async def test_existing_job_pods_are_not_agent_pods(env):
    # a leftover cache Job pod of the session is no agent pod: the restore still runs
    env.kube.seed(
        {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "agent-alpha-cache-restore-x1",
                "labels": {**session_labels("alpha"), CACHE_LABEL: "sessions-restore"},
            },
            "status": {"phase": "Succeeded"},
        }
    )
    c = await env.session("alpha")
    await env.tick()
    assert (await env.run(c.id, TX_RESTORE))["state"] == "running"
    assert env.sts_replicas() == [0]


async def test_sessions_restore_status_recorded(env, fake):
    c = await env.session("alpha")
    await env.tick()
    await env.tick()
    env.kube.finish_job(SESSIONS_RESTORE, exit_code=0, log=line("restored", size=4096))
    await env.tick()
    run = await env.run(c.id, TX_RESTORE)
    assert run["state"] == "restored" and run["bytes"] == 4096
    assert run["started_at"] and run["finished_at"] and run["reason"] is None
    assert run["job"] == SESSIONS_RESTORE
    assert not env.kube.has("Job", SESSIONS_RESTORE)
    await env.tick(2)
    assert await env.run(c.id, TX_RESTORE) == run  # never rerun
    assert env.creates(SESSIONS_RESTORE) == 1


async def test_sessions_restore_crash_safety(env):
    # crash between the running write and the create: the next tick creates the Job once
    c = await env.session("alpha")
    await env.tick()
    await env.kube.delete("Job", "agents", SESSIONS_RESTORE)
    await env.patch_run(c.id, TX_RESTORE, job=None)
    await env.tick()
    assert (await env.run(c.id, TX_RESTORE))["job"] == SESSIONS_RESTORE
    assert env.creates(SESSIONS_RESTORE) == 2
    assert set(env.sts_replicas()) == {0}


async def test_no_sessions_restore_without_session_cache(pool, dsn, fake):
    e = RestoreEnv(pool, dsn, fake)
    c = await e.session("alpha", cache=False)
    await e.tick(2)
    assert await e.backup(c.id) is None
    assert e.creates(SESSIONS_RESTORE) == 0
    assert e.sts_replicas()[0] == 1 and e.agent_pods() == [f"{STS}-0"]
    assert len(fake.sessions) == 1

    # the cache unavailable: recorded as skipped, no Job, the pods start at once
    u = RestoreEnv(pool, dsn, cache_s3_bucket="")
    d = await u.session("beta")
    await u.tick()
    run = await u.run(d.id, TX_RESTORE)
    assert run == {"state": "skipped", "reason": "not_configured"}
    assert u.creates(job_name("beta", TX_RESTORE)) == 0
    assert u.sts_replicas("beta")[0] == 1
    assert u.agent_pods("beta") == ["agent-beta-0"]
    assert "sessions restore" not in (await u.status(d.id))["message"]

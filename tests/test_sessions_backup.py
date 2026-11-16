"""Claude transcripts (/sessions) are saved to S3 at discard, after the workdir (D7, G6)."""

from __future__ import annotations

import json
import logging
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
import pytest_asyncio
from test_cache_jobs_render import (
    ACCESS_KEY,
    CACHE_IMAGE,
    SECRET_KEY,
    container_of,
    make_session,
    make_settings,
    pod_of,
    walk,
)
from test_cache_restore import WS_ENTRY, Env, line
from test_cache_save import SAVE, SaveEnv, uploaded

from agents_orchestrator.cache.presign import presign_object, sessions_object_name
from agents_orchestrator.k8s import labels, render
from agents_orchestrator.k8s.labels import object_names
from agents_orchestrator.reconciler import cache as cache_mod
from agents_orchestrator.reconciler.cache import (
    CACHE_LABEL,
    TX_SAVE,
    job_name,
    render_cache_job,
)
from agents_orchestrator.reconciler.cache import (
    SAVE as SAVE_KEY,
)
from agents_orchestrator.spec import SESSIONS_EXCLUDE

SESSIONS_SAVE = job_name("alpha", TX_SAVE)


# ---------------------------------------------------------------------- the Job


def _env(job: dict[str, Any]) -> dict[str, str]:
    return {e["name"]: e["value"] for e in container_of(job)["env"]}


def _entry(settings, url=None):
    url = url or presign_object(settings, "PUT", sessions_object_name(settings, "alpha"))
    return {
        "index": 0,
        "root": "sessions",
        "root_dir": render.SESSIONS_ROOT,
        "path": ".",
        "exclude": list(SESSIONS_EXCLUDE),
        "url": url,
    }


def test_sessions_save_job_render():
    s, settings = make_session(), make_settings()
    entry = _entry(settings)
    job = render_cache_job(s, settings, TX_SAVE, [entry])
    workdir = render_cache_job(s, settings, SAVE_KEY, [entry])

    assert job["metadata"]["name"] == SESSIONS_SAVE == "agent-alpha-sessions-save"
    want = {**labels.session_labels("alpha"), CACHE_LABEL: "sessions-save"}
    assert job["metadata"]["labels"] == want
    assert job["spec"]["template"]["metadata"]["labels"] == want
    c = container_of(job)
    assert c["image"] == CACHE_IMAGE and c["command"] == ["orch", "cache", "save"]

    # Only the PVC subPath `sessions`, read-only, plus the two emptyDirs.
    pvc = object_names("alpha")["pvc"]
    assert c["volumeMounts"] == [
        {
            "name": render.WORKSPACE_VOLUME,
            "mountPath": render.SESSIONS_ROOT,
            "subPath": "sessions",
            "readOnly": True,
        },
        {"name": "tmp", "mountPath": "/tmp"},
        {"name": "scratch", "mountPath": "/scratch"},
    ]
    assert [v for k, v in walk(job) if k == "persistentVolumeClaim"] == [{"claimName": pvc}]
    assert pod_of(job)["volumes"] == pod_of(workdir)["volumes"]
    assert _env(job) == _env(workdir)

    # Exactly one URL: a presigned PUT of <prefix>sessions/<slug>.tar.zst.
    assert [v for v in json.loads(_env(job)["CACHE_ENTRIES"])] == [entry]
    parts = urlsplit(entry["url"])
    assert parts.path == "/workspace-cache/sessions/alpha.tar.zst"
    query = {k: v[0] for k, v in parse_qs(parts.query).items()}
    assert query["X-Amz-SignedHeaders"] == "content-type;host"  # PUT signs Content-Type
    assert "X-Amz-Signature" in query

    # No credential, no spec.env, no token. The access key id is in the signed URL's
    # X-Amz-Credential by design; nowhere else.
    dumped = json.dumps(job).replace(json.dumps(entry["url"])[1:-1], "")
    for leaked in (ACCESS_KEY, SECRET_KEY, "SPEC_VAR", "AWS_"):
        assert leaked not in dumped
    assert SECRET_KEY not in entry["url"]
    keys = [k for k, _ in walk(job)]
    assert "secretKeyRef" not in keys and "envFrom" not in keys and "secret" not in keys
    pod, wpod = pod_of(job), pod_of(workdir)
    assert pod["automountServiceAccountToken"] is False
    assert {k: v for k, v in pod.items() if k not in ("containers", "volumes")} == {
        k: v for k, v in wpod.items() if k not in ("containers", "volumes")
    }
    assert c["securityContext"] == container_of(workdir)["securityContext"]


def test_save_respects_max_bytes():
    s, settings = make_session(), make_settings(cache_max_bytes=12345)
    job = render_cache_job(s, settings, TX_SAVE, [_entry(settings, "https://u")])
    env = _env(job)
    assert env["CACHE_MAX_BYTES"] == "12345"
    assert env["CACHE_SCRATCH"] == "/scratch"
    scratch = [v for v in pod_of(job)["volumes"] if v["name"] == "scratch"]
    assert scratch == [{"name": "scratch", "emptyDir": {"sizeLimit": "12345"}}]
    assert {"name": "scratch", "mountPath": "/scratch"} in container_of(job)["volumeMounts"]


def test_sessions_object_name_is_outside_the_version_objects():
    s = make_settings()
    assert sessions_object_name(s, "alpha") == "workspace-cache/sessions/alpha.tar.zst"
    # version objects live under b/, so no backup name can collide with a transcripts object
    assert not sessions_object_name(s, "alpha").startswith(f"{s.cache_s3_prefix}b/")
    assert cache_mod.presign_object is presign_object


# ---------------------------------------------------------------------- the discard


class SessionsEnv(SaveEnv):
    """SaveEnv whose sessions carry a `miss` sessions restore record: the transcripts save runs."""

    def __init__(self, pool, dsn, **settings) -> None:
        super().__init__(pool, dsn, **settings)
        self.sessions_saves: list[dict] = []
        inner = self.rec.cache.write_status

        async def spy(session, fields, generation, **kw):
            run = ((fields.get("backup") or {}).get("transcripts") or {}).get("save")
            if run is not None:
                self.sessions_saves.append(dict(run))
            return await inner(session, fields, generation, **kw)

        self.rec.cache.write_status = spy

    async def session(self, slug: str = "alpha", cache: bool = True, **spec):
        return await Env.session(self, slug, cache, **spec)

    async def patch_tx(self, session_id, field: str, value: dict | None) -> None:
        """Set (or, with None, remove) the transcripts restore or save record."""
        from agents_orchestrator.db import sessions

        async with self.pool.connection() as conn:
            c = await sessions.get(conn, session_id)
            backup = dict(c.status["backup"])
            transcripts = {**backup.get("transcripts", {}), field: value}
            if value is None:  # no record at all: a session older than the transcripts backup
                del transcripts[field]
            backup["transcripts"] = transcripts
            await sessions.set_status(
                conn, session_id, {**c.status, "backup": backup}, c.observed_generation
            )

    async def workdir_saved(self, c) -> None:
        """Delete `c` and let its workdir save finish."""
        await self.delete(c)
        await self.tick()
        self.kube.finish_job(SAVE, exit_code=0, log=uploaded(10))
        await self.tick()


@pytest_asyncio.fixture
async def env(pool, dsn) -> SessionsEnv:
    return SessionsEnv(pool, dsn)


async def test_discard_saves_sessions_after_workdir(env):
    order: list[tuple[str, Any]] = []
    pre_discard, prune = env.rec.pre_discard, env.rec._prune

    async def pre_spy(session):
        tx = (session.status.get("backup") or {}).get("transcripts") or {}
        order.append(("pre_discard", (tx.get("save") or {}).get("state")))
        return await pre_discard(session)

    async def prune_spy(slug, desired, *a, **kw):
        order.append(("prune", None))
        return await prune(slug, desired, *a, **kw)

    env.rec.pre_discard, env.rec._prune = pre_spy, prune_spy
    c = await env.ready()
    order.clear()
    sts = object_names("alpha")["statefulset"]
    await env.delete(c)
    await env.tick(2)
    assert env.kube.has("Job", SAVE)
    assert env.creates(SESSIONS_SAVE) == 0  # the workdir save is still running
    assert order == []

    env.kube.finish_job(SAVE, exit_code=0, log=uploaded(10))
    await env.tick()
    assert not env.kube.has("Job", SAVE) and env.kube.has("Job", SESSIONS_SAVE)
    assert not [p for p in env.kube.names("Pod") if p.startswith(sts + "-") and p[-1].isdigit()]
    assert (await env.run(c.id, SAVE_KEY))["state"] == "done"
    tx = await env.run(c.id, TX_SAVE)
    assert tx["state"] == "running" and tx["job"] == SESSIONS_SAVE
    (item,) = json.loads(_env(env.kube.get("Job", SESSIONS_SAVE))["CACHE_ENTRIES"])
    url = item["url"]
    assert urlsplit(url).path == "/workspace-cache/sessions/alpha.tar.zst"
    status = await env.status(c.id)
    assert status["phase"] == "deleting" and "saving the Claude transcripts" in status["message"]

    await env.tick(2)  # still running: it holds the discard, nothing is pruned
    assert env.kube.has("PersistentVolumeClaim", object_names("alpha")["pvc"])
    assert order == [] and env.creates(SESSIONS_SAVE) == 1

    env.kube.finish_job(SESSIONS_SAVE, exit_code=0, log=uploaded(20))
    await env.tick()
    assert await env.gone(c)
    assert order[0] == ("pre_discard", "saved") and order[1][0] == "prune"
    # the sessions write kept the workdir outcome
    assert env.saves[-1]["state"] == "done"
    assert not env.kube.has("Job", SESSIONS_SAVE)


async def test_sessions_save_status_records_bytes(env):
    c = await env.ready()
    await env.workdir_saved(c)
    env.kube.finish_job(SESSIONS_SAVE, exit_code=0, log=uploaded(4242))
    await env.tick()
    run = env.sessions_saves[-1]
    assert run["state"] == "saved" and run["bytes"] == 4242
    assert run["finished_at"] and run["started_at"] and run["reason"] is None
    assert run["job"] == SESSIONS_SAVE
    assert await env.gone(c)


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
        ({"exit_code": 137, "log": "killed"}, "exit 137"),
    ],
)
async def test_sessions_save_failure_does_not_hold_discard(env, caplog, finish, reason):
    c = await env.ready()
    await env.workdir_saved(c)
    env.kube.finish_job(SESSIONS_SAVE, **finish)
    with caplog.at_level(logging.WARNING):
        await env.tick()
    assert env.sessions_saves[-1]["state"] == "failed"
    assert env.sessions_saves[-1]["reason"] == reason
    warned = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("sessions save" in m and "alpha" in m and reason in m for m in warned)
    assert await env.gone(c)


async def test_sessions_save_create_failure_or_timeout_does_not_hold_discard(env, pool, dsn):
    c = await env.ready()
    env.kube.fail("Job", SESSIONS_SAVE, verbs=["create"])
    await env.workdir_saved(c)
    assert env.sessions_saves[-1]["state"] == "failed"
    assert env.sessions_saves[-1]["reason"].startswith("job_create_failed")
    assert await env.gone(c)

    from datetime import timedelta

    from agents_orchestrator.reconciler.checks import DEADLINE_GRACE_SECONDS

    e = SessionsEnv(pool, dsn)
    d = await e.ready("beta")
    await e.delete(d)
    await e.tick()
    e.kube.finish_job(job_name("beta", SAVE_KEY), exit_code=0, log=uploaded(1))
    await e.tick()
    run = await e.run(d.id, TX_SAVE)
    async with pool.connection() as conn:
        now = (await (await conn.execute("SELECT now()")).fetchone())[0]
    old = now - timedelta(seconds=e.settings.cache_timeout_seconds + DEADLINE_GRACE_SECONDS + 5)
    await e.patch_tx(d.id, "save", {**run, "started_at": old.isoformat()})
    await e.tick()
    assert e.sessions_saves[-1]["state"] == "failed"
    assert e.sessions_saves[-1]["reason"] == "timeout"
    assert await e.gone(d)


async def test_no_sessions_save_without_session_cache(env, pool, dsn):
    c = await env.session("alpha", cache=False)
    await env.tick()
    await env.delete(c)
    await env.tick()
    assert await env.gone(c)
    assert env.sessions_saves == [] and env.creates(SESSIONS_SAVE) == 0

    e = SessionsEnv(pool, dsn, cache_s3_bucket="")
    from agents_orchestrator.db import sessions
    from agents_orchestrator.spec import SessionSpec

    async with pool.connection() as conn:
        d = await sessions.create(conn, "beta", SessionSpec(backup=[WS_ENTRY]))
    await e.tick()
    await e.delete(d)
    await e.tick()
    assert e.sessions_saves[-1] == {"state": "skipped", "reason": "not_configured"}
    assert e.creates(job_name("beta", TX_SAVE)) == 0
    assert await e.gone(d)


@pytest.mark.parametrize(
    ("restore", "skip"),
    [
        (None, None),
        ({"state": "restored", "bytes": 3}, None),
        ({"state": "miss"}, None),
        ({"state": "unreadable", "reason": "ExtractError: bad"}, None),
        ({"state": "skipped", "reason": "pods_exist"}, None),
        ({"state": "running"}, "restore_running"),
        ({"state": "failed", "reason": "download_failed: HTTP 403"}, "download_failed"),
        ({"state": "failed", "reason": "timeout"}, "timeout"),
        ({"state": "failed", "reason": None}, "restore_failed"),
        ({"state": "skipped", "reason": "something_else"}, "restore_skipped"),
        ({"state": "saved"}, "restore_saved"),
    ],
)
async def test_sessions_save_skip_rules(env, restore, skip):
    c = await env.ready()
    await env.patch_tx(c.id, "restore", restore)
    await env.workdir_saved(c)
    if skip is None:
        assert env.creates(SESSIONS_SAVE) == 1
        assert env.sessions_saves[-1]["state"] == "running"
        assert not await env.gone(c)
    else:
        assert env.sessions_saves[-1] == {"state": "skipped", "reason": skip}
        assert env.creates(SESSIONS_SAVE) == 0
        assert await env.gone(c)


async def test_sessions_save_waits_for_agent_pods(env):
    c = await env.ready()
    env.kube.controller_paused = True
    await env.delete(c)
    await env.tick(2)
    assert env.creates(SESSIONS_SAVE) == 0
    assert "waiting for agent pods" in (await env.status(c.id))["message"]

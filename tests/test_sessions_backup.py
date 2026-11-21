"""The transcripts (/sessions) save Job and its discard outcome, on the volume gate."""

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
from test_cache_restore import line
from test_cache_save import SaveEnv, uploaded

from agents_orchestrator.cache.presign import presign_object, sessions_object_name
from agents_orchestrator.k8s import labels, render
from agents_orchestrator.k8s.labels import object_names
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

SESSIONS_SAVE = job_name("alpha", TX_SAVE)


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


async def test_sessions_save_job_render():
    s, settings = make_session(), make_settings()
    entry = _entry(settings)
    job = render_cache_job(s, settings, TX_SAVE, [entry])
    restore = render_cache_job(s, settings, TX_RESTORE, [entry])

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
    assert _env(job) == _env(restore)

    # Exactly one URL: a presigned PUT of <prefix>sessions/<slug>.tar.zst.
    assert json.loads(_env(job)["CACHE_ENTRIES"]) == [entry]
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
    pod, rpod = pod_of(job), pod_of(restore)
    assert pod["automountServiceAccountToken"] is False
    assert {k: v for k, v in pod.items() if k not in ("containers", "volumes")} == {
        k: v for k, v in rpod.items() if k not in ("containers", "volumes")
    }
    assert c["securityContext"] == container_of(restore)["securityContext"]


async def test_save_respects_max_bytes():
    s, settings = make_session(), make_settings(cache_max_bytes=12345)
    job = render_cache_job(s, settings, TX_SAVE, [_entry(settings, "https://u")])
    env = _env(job)
    assert env["CACHE_MAX_BYTES"] == "12345"
    assert env["CACHE_SCRATCH"] == "/scratch"
    scratch = [v for v in pod_of(job)["volumes"] if v["name"] == "scratch"]
    assert scratch == [{"name": "scratch", "emptyDir": {"sizeLimit": "12345"}}]
    assert {"name": "scratch", "mountPath": "/scratch"} in container_of(job)["volumeMounts"]


async def test_sessions_object_name_is_outside_the_version_objects():
    s = make_settings()
    assert sessions_object_name(s, "alpha") == "workspace-cache/sessions/alpha.tar.zst"
    # version objects live under b/, so no backup name can collide with a transcripts object
    assert not sessions_object_name(s, "alpha").startswith(f"{s.cache_s3_prefix}b/")
    assert cache_mod.presign_object is presign_object


@pytest_asyncio.fixture
async def env(pool, dsn) -> SaveEnv:
    return SaveEnv(pool, dsn)


async def test_sessions_save_status_records_bytes(env):
    c = await env.ready()
    await env.delete(c)
    await env.tick()
    env.kube.finish_job(SESSIONS_SAVE, exit_code=0, log=uploaded(4242))
    await env.tick()
    run = env.saves[-1]
    assert run["state"] == "saved" and run["bytes"] == 4242
    assert run["finished_at"] and run["started_at"] and run["reason"] is None
    assert run["job"] == SESSIONS_SAVE
    assert await env.gone(c)


@pytest.mark.parametrize(
    ("finish", "reason"),
    [
        ({"exit_code": 1, "log": line("failed", reason="too_large")}, "too_large"),
        ({"exit_code": 1, "log": line("failed", reason="HTTP 500")}, "HTTP 500"),
        ({"deadline_exceeded": True}, "timeout"),
        ({"exit_code": 0, "log": "not json"}, "bad_output"),
        ({"exit_code": 137, "log": "killed"}, "exit 137"),
    ],
)
async def test_sessions_save_failure_does_not_hold_discard(env, caplog, finish, reason):
    c = await env.ready()
    await env.delete(c)
    await env.tick()
    env.kube.finish_job(SESSIONS_SAVE, **finish)
    with caplog.at_level(logging.WARNING):
        await env.tick()
    assert env.saves[-1]["state"] == "failed"
    assert env.saves[-1]["reason"] == reason
    warned = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("sessions save" in m and "alpha" in m and reason in m for m in warned)
    assert await env.gone(c)

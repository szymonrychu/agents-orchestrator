"""Cache restore and save Jobs (D1, D2, D6): hardened, one presigned URL, no credential."""

from __future__ import annotations

import fnmatch
import json
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from agents_orchestrator.db.sessions import Session
from agents_orchestrator.k8s import labels, render
from agents_orchestrator.reconciler.cache import (
    CACHE_LABEL,
    render_cache_job,
    restore_job_name,
    save_job_name,
)
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SESSIONS_EXCLUDE, SessionSpec
from agents_orchestrator.tokens import SessionTokens

AGENT_IMAGE = "harbor.example/containers/claude-code-wrapper:ca2ddf3"
SPEC_IMAGE = "harbor.example/containers/custom-agent:1"
CACHE_IMAGE = "harbor.example/containers/agents-orchestrator:abc1234"
ACCESS_KEY = "AKIAEXAMPLEACCESSKEY"
SECRET_KEY = "s3cr3t-example-secret-access-key"
URL = "https://s3.example/bucket/workspace-cache/k.tar.zst?X-Amz-Signature=abc"
TOKENS = SessionTokens(auth="a" * 32, peer="p" * 32, agent="g" * 32)


def make_settings(**over: Any) -> Settings:
    base: dict[str, Any] = {
        "database_url": "postgresql://u:p@db/orch",
        "master_key": "m" * 32,
        "agent_image": AGENT_IMAGE,
        "default_model": "sonnet",
        "models": ["sonnet"],
        "image_pull_secret": "harbor-pull",
        "cache_s3_endpoint": "http://rgw.example:80",
        "cache_s3_bucket": "agents-workspace-cache",
        "cache_s3_access_key_id": ACCESS_KEY,
        "cache_s3_secret_access_key": SECRET_KEY,
        "cache_image": CACHE_IMAGE,
        "cache_max_bytes": 1073741824,
        "cache_timeout_seconds": 900,
    }
    base.update(over)
    return Settings(**base)


def make_session(slug: str = "alpha", **spec: Any) -> Session:
    spec = {
        "image": SPEC_IMAGE,
        "workdir": "repo",
        "env": {"SPEC_VAR": "x", "AWS_PROFILE": "agent"},
        "cache": {
            "restore_key": "preset-0123456789abcdef",
            "save_key": "preset-0123456789abcdef",
            "exclude": ["node_modules", "*.log"],
        },
        **spec,
    }
    return Session(
        id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        slug=slug,
        spec=SessionSpec.model_validate(spec),
        generation=1,
        observed_generation=0,
        desired="running",
        status={},
        created_at=datetime(2026, 10, 25, tzinfo=UTC),
    )


def pod_of(job: dict[str, Any]) -> dict[str, Any]:
    return job["spec"]["template"]["spec"]


def container_of(job: dict[str, Any]) -> dict[str, Any]:
    (container,) = pod_of(job)["containers"]
    return container


def walk(obj: Any):
    """Every (key, value) pair anywhere in a nested object."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield k, v
            yield from walk(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from walk(v)


def test_restore_job_image_and_command():
    s, settings = make_session(), make_settings()
    job = render_cache_job(s, settings, "restore", URL)
    assert job["apiVersion"] == "batch/v1" and job["kind"] == "Job"
    assert job["metadata"]["name"] == restore_job_name("alpha") == "agent-alpha-cache-restore"
    assert job["metadata"]["namespace"] == settings.namespace
    want_labels = {**labels.session_labels("alpha"), CACHE_LABEL: "restore"}
    assert CACHE_LABEL == "agents.szymonrichert.pl/cache"
    assert job["metadata"]["labels"] == want_labels
    assert job["spec"]["template"]["metadata"]["labels"] == want_labels
    c = container_of(job)
    assert c["image"] == CACHE_IMAGE
    assert c["command"] == ["orch", "cache", "restore"]
    assert "args" not in c
    # Never the session's image, wherever it would appear.
    assert SPEC_IMAGE not in json.dumps(job) and AGENT_IMAGE not in json.dumps(job)

    save = render_cache_job(s, settings, "save", URL)
    assert save["metadata"]["name"] == save_job_name("alpha") == "agent-alpha-cache-save"
    assert save["metadata"]["labels"][CACHE_LABEL] == "save"
    assert container_of(save)["image"] == CACHE_IMAGE
    assert container_of(save)["command"] == ["orch", "cache", "save"]

    unknown: Any = "purge"
    with pytest.raises(ValueError):
        render_cache_job(s, settings, unknown, URL)
    with pytest.raises(ValueError):
        render_cache_job(make_session(cache=None), settings, "restore", URL)


@pytest.mark.parametrize("action", ["restore", "save"])
def test_sessions_save_excludes_credentials(action):
    job = render_cache_job(make_session(), make_settings(), action, URL, "sessions")
    env = {e["name"]: e["value"] for e in container_of(job)["env"]}
    exclude = json.loads(env["CACHE_EXCLUDE"])
    assert exclude == list(SESSIONS_EXCLUDE)
    # a top-level file and a nested one both match a pattern
    assert any(fnmatch.fnmatchcase(".credentials.json", p) for p in exclude)
    assert any(fnmatch.fnmatchcase("ses/config/.credentials.json", p) for p in exclude)
    assert not any(fnmatch.fnmatchcase("ses/config/projects/p/t.jsonl", p) for p in exclude)
    # the workdir target keeps the spec's own list
    workdir = render_cache_job(make_session(), make_settings(), action, URL)
    wenv = {e["name"]: e["value"] for e in container_of(workdir)["env"]}
    assert json.loads(wenv["CACHE_EXCLUDE"]) == ["node_modules", "*.log"]


@pytest.mark.parametrize("action", ["restore", "save"])
def test_job_env_has_no_credentials(action):
    s, settings = make_session(), make_settings()
    job = render_cache_job(s, settings, action, URL)
    c = container_of(job)
    assert c["env"] == [
        {"name": "CACHE_URL", "value": URL},
        {"name": "CACHE_DIR", "value": "/workdir/repo"},
        {"name": "CACHE_MAX_BYTES", "value": "1073741824"},
        {"name": "CACHE_EXCLUDE", "value": json.dumps(["node_modules", "*.log"])},
        {"name": "CACHE_SCRATCH", "value": "/scratch"},
        {"name": "CACHE_VERIFY_TLS", "value": "true"},
        {"name": "HOME", "value": "/tmp"},
    ]
    assert "envFrom" not in c
    keys = [k for k, _ in walk(job)]
    assert "secretKeyRef" not in keys and "envFrom" not in keys and "secret" not in keys
    dumped = json.dumps(job)
    for leaked in (ACCESS_KEY, SECRET_KEY, "SPEC_VAR", "AWS_"):
        assert leaked not in dumped
    # Exactly one URL: the one given.
    assert dumped.count("https://") == 1


@pytest.mark.parametrize(("verify", "value"), [(True, "true"), (False, "false")])
def test_job_env_carries_verify_tls(verify, value):
    job = render_cache_job(make_session(), make_settings(cache_s3_verify_tls=verify), "save", URL)
    env = {e["name"]: e["value"] for e in container_of(job)["env"]}
    assert env["CACHE_VERIFY_TLS"] == value


def test_save_job_volumes():
    s, settings = make_session(), make_settings()
    save = render_cache_job(s, settings, "save", URL)
    pvc = labels.object_names("alpha")["pvc"]
    assert container_of(save)["volumeMounts"] == [
        {**render.workdir_mount(s.spec), "readOnly": True},
        {"name": "tmp", "mountPath": "/tmp"},
        {"name": "scratch", "mountPath": "/scratch"},
    ]
    assert pod_of(save)["volumes"] == [
        {"name": render.WORKSPACE_VOLUME, "persistentVolumeClaim": {"claimName": pvc}},
        {"name": "tmp", "emptyDir": {}},
        {"name": "scratch", "emptyDir": {"sizeLimit": "1073741824"}},
    ]

    restore = render_cache_job(s, settings, "restore", URL)
    assert container_of(restore)["volumeMounts"] == [
        render.workdir_mount(s.spec),
        {"name": "tmp", "mountPath": "/tmp"},
    ]
    assert pod_of(restore)["volumes"] == [
        {"name": render.WORKSPACE_VOLUME, "persistentVolumeClaim": {"claimName": pvc}},
        {"name": "tmp", "emptyDir": {}},
    ]
    # The only PVC either Job mounts is the session's workspace; nothing else from the host.
    for job in (save, restore):
        claims = [v for k, v in walk(job) if k == "persistentVolumeClaim"]
        assert claims == [{"claimName": pvc}]
        for vol in pod_of(job)["volumes"]:
            assert set(vol) <= {"name", "persistentVolumeClaim", "emptyDir"}


@pytest.mark.parametrize("action", ["restore", "save"])
def test_job_pod_hardened(action):
    job = render_cache_job(make_session(), make_settings(), action, URL)
    pod = pod_of(job)
    assert pod["restartPolicy"] == "Never"
    assert pod["automountServiceAccountToken"] is False
    assert pod["serviceAccountName"] == "default" and "serviceAccount" not in pod
    assert pod["securityContext"] == {
        "runAsUser": 1000,
        "runAsNonRoot": True,
        "fsGroup": 1000,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    assert pod["imagePullSecrets"] == [{"name": "harbor-pull"}]
    assert "hostNetwork" not in pod and "hostPID" not in pod
    assert container_of(job)["securityContext"] == {
        "readOnlyRootFilesystem": True,
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
    }
    plain = render_cache_job(make_session(), make_settings(image_pull_secret=""), action, URL)
    assert "imagePullSecrets" not in pod_of(plain)


@pytest.mark.parametrize("action", ["restore", "save"])
def test_job_limits(action):
    job = render_cache_job(make_session(), make_settings(cache_timeout_seconds=1234), action, URL)
    assert job["spec"]["backoffLimit"] == 2
    assert job["spec"]["activeDeadlineSeconds"] == 1234
    assert "ttlSecondsAfterFinished" not in job["spec"]


def test_agent_pods_carry_no_cache_credentials():
    s, settings = make_session(env={"SPEC_VAR": "x"}), make_settings()
    objs = render.render_session(s, settings, TOKENS)
    names = labels.object_names("alpha")
    env_names = [e["name"] for k, v in walk(objs) if k == "env" for e in v]
    assert env_names, "the agent container has env"
    for name in env_names:
        assert not name.startswith(("AWS_", "CACHE_", "ORCH_CACHE_")), name
    refs = {v["name"] for k, v in walk(objs) if k == "secretKeyRef"}
    assert refs == {names["secret"], settings.claude_secret_name}
    keys = [k for k, _ in walk(objs)]
    assert "envFrom" not in keys
    dumped = json.dumps(objs)
    for leaked in (ACCESS_KEY, SECRET_KEY, CACHE_IMAGE, "agents-workspace-cache"):
        assert leaked not in dumped

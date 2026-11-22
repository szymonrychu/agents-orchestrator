"""Cache restore and save Jobs (D1, D2, D6): hardened, one presigned URL, no credential."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from agents_orchestrator.db.sessions import Session
from agents_orchestrator.db.volumes import Volume
from agents_orchestrator.k8s import labels, render
from agents_orchestrator.reconciler.cache import (
    CACHE_LABEL,
    TX_RESTORE,
    TX_SAVE,
    job_name,
    render_cache_job,
    render_volume_job,
    render_volume_owner_job,
)
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec
from agents_orchestrator.tokens import SessionTokens

AGENT_IMAGE = "harbor.example/containers/claude-code-wrapper:ca2ddf3"
SPEC_IMAGE = "harbor.example/containers/custom-agent:1"
CACHE_IMAGE = "harbor.example/containers/agents-orchestrator:abc1234"
ACCESS_KEY = "TESTIDEXAMPLEACCESSKEY"
SECRET_KEY = "s3cr3t-example-secret-access-key"
URL = "https://s3.example/bucket/workspace-cache/b/n/k.tar.zst?X-Amz-Signature=abc"


def entries() -> list[dict[str, Any]]:
    return [
        {
            "index": 0,
            "root": "sessions",
            "root_dir": "/sessions",
            "path": ".",
            "exclude": [".credentials.json"],
            "url": URL,
        }
    ]


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
        "env": {"SPEC_VAR": {"value": "x"}, "AWS_PROFILE": {"value": "agent"}},
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
    job = render_cache_job(s, settings, TX_RESTORE, entries())
    assert job["apiVersion"] == "batch/v1" and job["kind"] == "Job"
    assert (
        job["metadata"]["name"] == job_name("alpha", TX_RESTORE) == "agent-alpha-sessions-restore"
    )
    assert job["metadata"]["namespace"] == settings.namespace
    want_labels = {**labels.session_labels("alpha"), CACHE_LABEL: "sessions-restore"}
    assert CACHE_LABEL == "agents.szymonrichert.pl/cache"
    assert job["metadata"]["labels"] == want_labels
    assert job["spec"]["template"]["metadata"]["labels"] == want_labels
    c = container_of(job)
    assert c["image"] == CACHE_IMAGE
    assert c["command"] == ["orch", "cache", "restore"]
    assert "args" not in c
    # Never the session's image, wherever it would appear.
    assert SPEC_IMAGE not in json.dumps(job) and AGENT_IMAGE not in json.dumps(job)

    save = render_cache_job(s, settings, TX_SAVE, entries())
    assert save["metadata"]["name"] == job_name("alpha", TX_SAVE) == "agent-alpha-sessions-save"
    assert save["metadata"]["labels"][CACHE_LABEL] == "sessions-save"
    assert container_of(save)["image"] == CACHE_IMAGE
    assert container_of(save)["command"] == ["orch", "cache", "save"]


@pytest.mark.parametrize("key", [TX_RESTORE, TX_SAVE])
def test_job_env_has_no_credentials(key):
    s, settings = make_session(), make_settings()
    job = render_cache_job(s, settings, key, entries())
    c = container_of(job)
    assert c["env"] == [
        {"name": "CACHE_ENTRIES", "value": json.dumps(entries())},
        {"name": "CACHE_MAX_BYTES", "value": "1073741824"},
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
    # Exactly one URL: the entry's.
    assert dumped.count("https://") == 1


@pytest.mark.parametrize(("verify", "value"), [(True, "true"), (False, "false")])
def test_job_env_carries_verify_tls(verify, value):
    job = render_cache_job(
        make_session(), make_settings(cache_s3_verify_tls=verify), TX_SAVE, entries()
    )
    env = {e["name"]: e["value"] for e in container_of(job)["env"]}
    assert env["CACHE_VERIFY_TLS"] == value


def test_job_volumes_mount_the_sessions_tree_read_only_for_a_save():
    s, settings = make_session(), make_settings()
    pvc = labels.object_names("alpha")["pvc"]
    sessions = {"name": render.WORKSPACE_VOLUME, "mountPath": "/sessions", "subPath": "sessions"}
    save = render_cache_job(s, settings, TX_SAVE, entries())
    assert container_of(save)["volumeMounts"] == [
        {**sessions, "readOnly": True},
        {"name": "tmp", "mountPath": "/tmp"},
        {"name": "scratch", "mountPath": "/scratch"},
    ]
    assert pod_of(save)["volumes"] == [
        {"name": render.WORKSPACE_VOLUME, "persistentVolumeClaim": {"claimName": pvc}},
        {"name": "tmp", "emptyDir": {}},
        {"name": "scratch", "emptyDir": {"sizeLimit": "1073741824"}},
    ]

    restore = render_cache_job(s, settings, TX_RESTORE, entries())
    assert container_of(restore)["volumeMounts"] == [sessions, {"name": "tmp", "mountPath": "/tmp"}]
    assert pod_of(restore)["volumes"] == [
        {"name": render.WORKSPACE_VOLUME, "persistentVolumeClaim": {"claimName": pvc}},
        {"name": "tmp", "emptyDir": {}},
    ]
    # The only PVC any Job mounts is the session's workspace; nothing else from the host.
    for job in (save, restore):
        claims = [v for k, v in walk(job) if k == "persistentVolumeClaim"]
        assert claims == [{"claimName": pvc}]
        for vol in pod_of(job)["volumes"]:
            assert set(vol) <= {"name", "persistentVolumeClaim", "emptyDir"}


@pytest.mark.parametrize("key", [TX_RESTORE, TX_SAVE])
def test_job_pod_hardened(key):
    job = render_cache_job(make_session(), make_settings(), key, entries())
    pod = pod_of(job)
    assert pod["restartPolicy"] == "Never"
    assert pod["automountServiceAccountToken"] is False
    assert pod["serviceAccountName"] == "default" and "serviceAccount" not in pod
    assert pod["securityContext"] == {
        "runAsUser": 1000,
        "runAsNonRoot": True,
        "fsGroup": 1000,
        "fsGroupChangePolicy": "OnRootMismatch",
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    assert pod["imagePullSecrets"] == [{"name": "harbor-pull"}]
    assert "hostNetwork" not in pod and "hostPID" not in pod
    assert container_of(job)["securityContext"] == {
        "readOnlyRootFilesystem": True,
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
    }
    plain = render_cache_job(make_session(), make_settings(image_pull_secret=""), key, entries())
    assert "imagePullSecrets" not in pod_of(plain)


@pytest.mark.parametrize("key", [TX_RESTORE, TX_SAVE])
def test_job_limits(key):
    job = render_cache_job(
        make_session(), make_settings(cache_timeout_seconds=1234), key, entries()
    )
    assert job["spec"]["backoffLimit"] == 2
    assert job["spec"]["activeDeadlineSeconds"] == 1234
    assert "ttlSecondsAfterFinished" not in job["spec"]


def test_agent_pods_carry_no_cache_credentials():
    s, settings = make_session(env={"SPEC_VAR": {"value": "x"}}), make_settings()
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


def make_volume() -> Volume:
    now = datetime(2026, 11, 21, tzinfo=UTC)
    return Volume(
        id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
        name="data",
        projects=["p"],
        labels=[],
        description="",
        exclude=[],
        size="10Gi",
        storage_class="rook-ceph-rwx",
        pvc_name="vol-data",
        state="restoring",
        state_reason=None,
        content_version=None,
        created_by="alice",
        created_at=now,
        updated_at=now,
        deleted_at=None,
    )


@pytest.mark.parametrize(("action", "read_only"), [("restore", False), ("save", True)])
def test_volume_job_mounts_the_pvc_at_volume_and_is_not_a_session_job(action, read_only):
    vol, settings = make_volume(), make_settings()
    job = render_volume_job(vol, settings, action=action, version=None, n=7, url=URL)
    assert job["metadata"]["name"] == f"vol-data-{action}-7"
    assert labels.SESSION_LABEL not in job["metadata"]["labels"]
    assert job["metadata"]["labels"][labels.VOLUME_ID_LABEL] == str(vol.id)
    assert job["spec"]["activeDeadlineSeconds"] == settings.cache_timeout_seconds
    pod = pod_of(job)
    claim = next(v for v in pod["volumes"] if "persistentVolumeClaim" in v)
    assert claim["persistentVolumeClaim"].get("claimName") == "vol-data"
    assert claim["persistentVolumeClaim"].get("readOnly", False) is read_only
    mount = next(m for m in container_of(job)["volumeMounts"] if m["mountPath"] == "/volume")
    assert mount.get("readOnly", False) is read_only
    assert pod["securityContext"]["fsGroupChangePolicy"] == "OnRootMismatch"


def _owner_steps(job: dict[str, Any]) -> list[tuple]:
    pod = pod_of(job)
    steps = [("init", c) for c in pod.get("initContainers", [])]
    steps += [("main", c) for c in pod["containers"] if c["name"] == "volume-owner"]
    return [
        (
            where,
            c["image"],
            tuple(c["command"]),
            c["securityContext"]["runAsUser"],
            tuple(c["securityContext"]["capabilities"]["add"]),
            tuple(c["securityContext"]["capabilities"]["drop"]),
            tuple((m["name"], m["mountPath"], m.get("readOnly", False)) for m in c["volumeMounts"]),
            bool(c["resources"]["requests"] and c["resources"]["limits"]),
        )
        for where, c in steps
    ]


# image, command, uid, added and dropped capabilities, mounts, requests and limits set
OWNER = (
    CACHE_IMAGE,
    ("chown", "1000:1000", "/volume"),
    0,
    ("CHOWN",),
    ("ALL",),
    (("volume", "/volume", False),),
    True,
)


@pytest.mark.parametrize(
    ("kind", "want"),
    [
        ("owner", [("main", *OWNER)]),
        ("restore", [("init", *OWNER)]),
        ("save", []),
    ],
)
def test_volume_jobs_hand_the_volume_root_to_the_agent_user(kind, want):
    """The CSI driver creates the root as uid 0; the owner Job (new volume) and the restore Job
    chown `/volume` itself, as uid 0 with CAP_CHOWN only, before anything writes as uid 1000."""
    vol, settings = make_volume(), make_settings()
    if kind == "owner":
        job = render_volume_owner_job(vol, settings, n=3)
        assert job["metadata"]["name"] == "vol-data-owner-3"
    else:
        job = render_volume_job(vol, settings, action=kind, n=1, url=URL, version=None)
    assert _owner_steps(job) == want

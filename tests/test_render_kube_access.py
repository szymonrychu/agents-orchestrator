"""A kube_cluster_access session's agent pods run as their own ServiceAccount, bound to
cluster-admin by a per-session ClusterRoleBinding (T-55, plan 3.3); renders fail closed."""

from __future__ import annotations

import uuid
from typing import Any

import pytest
import yaml
from test_service_accounts import TOKENS, URL, WHEN, make_session, make_settings

from agents_orchestrator.db import queue
from agents_orchestrator.k8s import render
from agents_orchestrator.k8s.labels import SESSION_ID_LABEL, session_labels
from agents_orchestrator.reconciler.cache import render_cache_job
from agents_orchestrator.reconciler.checks import render_job
from agents_orchestrator.reconciler.init import render_init_job
from agents_orchestrator.settings import Settings

TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount"
KUBE_ENV = {"KUBERNETES_SERVICE_HOST", "KUBERNETES_SERVICE_PORT"}
KUBE_KINDS = {"ServiceAccount", "ClusterRoleBinding", "RoleBinding"}


def on(**over: Any) -> Settings:
    return make_settings(kube_cluster_access_enabled=True, **over)


def kinds(objs: list[dict[str, Any]]) -> set[str]:
    return {o["kind"] for o in objs}


def one(objs: list[dict[str, Any]], kind: str) -> dict[str, Any]:
    (obj,) = (o for o in objs if o["kind"] == kind)
    return obj


def session_sa(session: Any) -> str:
    return f"agent-{session.slug}-{session.id.hex[:8]}"


def pod_of(objs: list[dict[str, Any]]) -> dict[str, Any]:
    (sts,) = (o for o in objs if o["kind"] == "StatefulSet")
    return sts["spec"]["template"]["spec"]


def passthrough(objs: list[dict[str, Any]]) -> list[str]:
    (cm,) = (o for o in objs if o["kind"] == "ConfigMap")
    return yaml.safe_load(cm["data"]["config.yaml"])["claude"]["passthrough_env"]


def jobs_of(session: Any, settings: Any) -> list[dict[str, Any]]:
    check = queue.Check(
        id=7,
        session_id=session.id,
        command="pytest -q",
        status="queued",
        job_name=None,
        created_at=WHEN,
        started_at=WHEN,
    )
    return [
        render_init_job(session, settings),
        render_job(session, check, settings),
        render_cache_job(session, settings, "restore", URL),
        render_cache_job(session, settings, "save", URL),
    ]


def test_cluster_access_renders_session_sa_and_binding() -> None:
    s = make_session(kube_cluster_access=True)
    objs = render.render_session(s, make_settings(kube_cluster_access_enabled=True), TOKENS)
    sa = one(objs, "ServiceAccount")
    assert sa["apiVersion"] == "v1"
    assert sa["metadata"]["name"] == f"agent-{s.slug}-{s.id.hex[:8]}"
    assert sa["metadata"]["namespace"] == "agents" and sa["automountServiceAccountToken"] is False
    assert sa["metadata"]["labels"][SESSION_ID_LABEL] == str(s.id)
    assert sa["metadata"]["labels"] == {**session_labels(s.slug), SESSION_ID_LABEL: str(s.id)}
    crb = one(objs, "ClusterRoleBinding")
    assert crb["apiVersion"] == "rbac.authorization.k8s.io/v1"
    assert crb["metadata"]["name"] == f"{sa['metadata']['name']}-cluster-admin"
    assert "namespace" not in crb["metadata"]
    assert crb["metadata"]["labels"] == sa["metadata"]["labels"]
    assert crb["roleRef"] == {
        "apiGroup": "rbac.authorization.k8s.io",
        "kind": "ClusterRole",
        "name": "cluster-admin",
    }
    assert crb["subjects"] == [
        {"kind": "ServiceAccount", "name": sa["metadata"]["name"], "namespace": "agents"}
    ]
    order = [o["kind"] for o in objs]
    assert order == [
        "PersistentVolumeClaim",
        "Secret",
        "ConfigMap",
        "ServiceAccount",
        "ClusterRoleBinding",
        "Service",
        "StatefulSet",
    ]
    assert "RoleBinding" not in order


def test_cluster_access_pod_identity() -> None:
    pod = pod_of(render.render_session(s := make_session(kube_cluster_access=True), on(), TOKENS))
    assert pod["serviceAccountName"] == f"agent-{s.slug}-{s.id.hex[:8]}"
    assert pod["automountServiceAccountToken"] is True
    for container in pod["containers"]:
        assert all(m["mountPath"] != TOKEN_PATH for m in container.get("volumeMounts", []))
        assert all(e["name"] != "KUBECONFIG" for e in container.get("env", []))
    assert all("KUBECONFIG" not in str(v) for v in pod["volumes"])
    assert all(v["name"] not in {"kube", "token"} for v in pod["volumes"])


def test_sa_name_differs_per_session_id() -> None:
    a = make_session(kube_cluster_access=True)
    b = make_session(uuid.UUID("22222222-2222-2222-2222-222222222222"), kube_cluster_access=True)
    assert a.slug == b.slug
    sa_a = one(render.render_session(a, on(), TOKENS), "ServiceAccount")["metadata"]["name"]
    sa_b = one(render.render_session(b, on(), TOKENS), "ServiceAccount")["metadata"]["name"]
    assert sa_a == "agent-alpha-11111111" and sa_b == "agent-alpha-22222222"
    crb_b = one(render.render_session(b, on(), TOKENS), "ClusterRoleBinding")
    assert crb_b["metadata"]["name"] == "agent-alpha-22222222-cluster-admin"
    assert pod_of(render.render_session(b, on(), TOKENS))["serviceAccountName"] == sa_b


@pytest.mark.parametrize(
    "spec",
    [
        {},
        {"max_replicas": 3},
        {"static": True, "replicas": 2, "max_replicas": 2},
        {"session_messaging": True},
        {"kube_cluster_access": False},
        {
            "cache": {
                "restore_key": "preset-0123456789abcdef",
                "save_key": "preset-0123456789abcdef",
            }
        },
    ],
    ids=["alone", "hive", "static", "messaging-only", "kube-false", "cache"],
)
def test_only_cluster_access_yields_privileged_account(spec: dict[str, Any]) -> None:
    session = make_session(**spec)
    settings = on()
    objs = render.render_session(session, settings, TOKENS)
    assert not kinds(objs) & KUBE_KINDS
    pods = [pod_of(objs)]
    pods += [j["spec"]["template"]["spec"] for j in jobs_of(session, settings)]
    for pod in pods:
        assert pod["serviceAccountName"] == settings.agent_service_account
        assert pod["automountServiceAccountToken"] is False


def test_cluster_access_passthrough_env() -> None:
    settings = on()
    granted = passthrough(
        render.render_session(make_session(kube_cluster_access=True), settings, TOKENS)
    )
    off = passthrough(render.render_session(make_session(), settings, TOKENS))
    assert set(granted) >= KUBE_ENV
    assert granted == sorted(granted)
    assert not KUBE_ENV & set(off)


def test_cluster_access_leaves_on_start_unchanged() -> None:
    settings = on()

    def on_start(**spec: Any) -> str:
        objs = render.render_session(make_session(**spec), settings, TOKENS)
        (cm,) = (o for o in objs if o["kind"] == "ConfigMap")
        return cm["data"]["on-start.sh"]

    assert on_start(kube_cluster_access=True) == on_start()
    assert "kubectl" not in on_start(kube_cluster_access=True)


def test_cluster_access_fails_closed_without_setting() -> None:
    settings = make_settings()
    assert settings.kube_cluster_access_enabled is False
    objs = render.render_session(make_session(kube_cluster_access=True), settings, TOKENS)
    assert not kinds(objs) & KUBE_KINDS
    pod = pod_of(objs)
    assert pod["serviceAccountName"] == settings.agent_service_account
    assert pod["automountServiceAccountToken"] is False
    assert not KUBE_ENV & set(passthrough(objs))


def test_cluster_access_jobs_stay_unprivileged() -> None:
    settings = on()
    for job in jobs_of(make_session(kube_cluster_access=True), settings):
        pod = job["spec"]["template"]["spec"]
        assert pod["serviceAccountName"] == settings.agent_service_account
        assert pod["automountServiceAccountToken"] is False

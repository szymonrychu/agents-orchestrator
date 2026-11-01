"""A kube_access session's agent pods run as the privileged ServiceAccount (D3, D5)."""

from __future__ import annotations

from typing import Any

import pytest
import yaml
from test_service_accounts import TOKENS, URL, WHEN, make_session, make_settings

from agents_orchestrator.db import queue
from agents_orchestrator.k8s import render
from agents_orchestrator.reconciler.cache import render_cache_job
from agents_orchestrator.reconciler.checks import render_job
from agents_orchestrator.reconciler.init import render_init_job

PRIVILEGED = "orch-kube-access"
TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount"
KUBE_ENV = {"KUBERNETES_SERVICE_HOST", "KUBERNETES_SERVICE_PORT"}


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


def test_kube_access_pod_identity() -> None:
    objs = render.render_session(make_session(kube_access=True), make_settings(), TOKENS)
    pod = pod_of(objs)
    assert pod["serviceAccountName"] == PRIVILEGED
    assert pod["automountServiceAccountToken"] is True
    for container in pod["containers"]:
        assert all(m["mountPath"] != TOKEN_PATH for m in container.get("volumeMounts", []))
        assert all(e["name"] != "KUBECONFIG" for e in container.get("env", []))
    assert all("KUBECONFIG" not in str(v) for v in pod["volumes"])
    assert all(v["name"] not in {"kube", "token"} for v in pod["volumes"])


@pytest.mark.parametrize(
    "spec",
    [
        {},
        {"max_replicas": 3},
        {"static": True, "replicas": 2, "max_replicas": 2},
        {"session_messaging": True},
        {"kube_access": False},
        {
            "cache": {
                "restore_key": "preset-0123456789abcdef",
                "save_key": "preset-0123456789abcdef",
            }
        },
    ],
    ids=["alone", "hive", "static", "messaging-only", "kube-false", "cache"],
)
def test_only_kube_access_yields_privileged_account(spec: dict[str, Any]) -> None:
    session = make_session(**spec)
    settings = make_settings()
    pods = [pod_of(render.render_session(session, settings, TOKENS))]
    pods += [j["spec"]["template"]["spec"] for j in jobs_of(session, settings)]
    for pod in pods:
        assert pod["serviceAccountName"] != PRIVILEGED
        assert pod["automountServiceAccountToken"] is False


def test_kube_access_passthrough_env() -> None:
    settings = make_settings()
    on = passthrough(render.render_session(make_session(kube_access=True), settings, TOKENS))
    off = passthrough(render.render_session(make_session(), settings, TOKENS))
    assert set(on) >= KUBE_ENV
    assert on == sorted(on)
    assert not KUBE_ENV & set(off)


def test_kube_access_leaves_on_start_unchanged() -> None:
    settings = make_settings()

    def on_start(**spec: Any) -> str:
        objs = render.render_session(make_session(**spec), settings, TOKENS)
        (cm,) = (o for o in objs if o["kind"] == "ConfigMap")
        return cm["data"]["on-start.sh"]

    assert on_start(kube_access=True) == on_start()
    assert "kubectl" not in on_start(kube_access=True)


def test_kube_access_fails_closed_without_setting() -> None:
    settings = make_settings(kube_access_service_account="")
    objs = render.render_session(make_session(kube_access=True), settings, TOKENS)
    pod = pod_of(objs)
    assert pod["serviceAccountName"] == settings.agent_service_account
    assert pod["automountServiceAccountToken"] is False
    assert not KUBE_ENV & set(passthrough(objs))


def test_kube_access_jobs_stay_unprivileged() -> None:
    settings = make_settings()
    for job in jobs_of(make_session(kube_access=True), settings):
        pod = job["spec"]["template"]["spec"]
        assert pod["serviceAccountName"] == settings.agent_service_account
        assert pod["automountServiceAccountToken"] is False

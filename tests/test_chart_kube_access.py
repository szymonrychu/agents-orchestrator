"""Value-gated Kubernetes access: the privileged ServiceAccount, its cluster-admin binding and
the unprivileged agent ServiceAccount (design D2, D3, D4)."""

from __future__ import annotations

from typing import Any

import yaml
from helm_render import CHART, render

FULL = "agents-orchestrator"
NS = "agents"
BASE: dict[str, Any] = {
    "agentImage": "registry.example/wrapper:1",
    "masterKey": {"value": "mk"},
    "claude": {"token": "tok"},
    "postgres": {"url": "postgresql://u:p@db:5432/app"},
}
ON = {"kubeAccess": {"enabled": True}}


def _docs(values: dict[str, Any] | None = None) -> list[dict]:
    return render({**BASE, **(values or {})}, namespace=NS)


def _named(docs: list[dict], kind: str) -> dict[str, dict]:
    return {d["metadata"]["name"]: d for d in docs if d["kind"] == kind}


def _settings(docs: list[dict]) -> dict[str, str]:
    return _named(docs, "ConfigMap")[f"{FULL}-settings"]["data"]


def test_kube_access_off_by_default() -> None:
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    assert values["kubeAccess"] == {"enabled": False, "annotations": {}}
    docs = _docs()
    kinds = {d["kind"] for d in docs}
    assert not kinds & {"ClusterRole", "ClusterRoleBinding"}
    assert f"{FULL}-kube-access" not in _named(docs, "ServiceAccount")
    assert "ORCH_KUBE_ACCESS_SERVICE_ACCOUNT" not in _settings(docs)


def test_kube_access_enabled_renders_binding() -> None:
    docs = _docs(ON)
    sa = _named(docs, "ServiceAccount")[f"{FULL}-kube-access"]
    assert sa["automountServiceAccountToken"] is False
    assert sa["metadata"]["labels"]["app.kubernetes.io/component"] == "kube-access"
    assert sa["metadata"]["labels"]["app.kubernetes.io/managed-by"] == "Helm"
    (crb,) = [d for d in docs if d["kind"] == "ClusterRoleBinding"]
    assert crb["metadata"]["name"] == f"{FULL}-kube-access"
    assert "namespace" not in crb["metadata"]
    assert crb["roleRef"] == {
        "apiGroup": "rbac.authorization.k8s.io",
        "kind": "ClusterRole",
        "name": "cluster-admin",
    }
    assert crb["subjects"] == [
        {"kind": "ServiceAccount", "name": f"{FULL}-kube-access", "namespace": NS}
    ]
    assert not [d for d in docs if d["kind"] == "ClusterRole"]
    assert _settings(docs)["ORCH_KUBE_ACCESS_SERVICE_ACCOUNT"] == f"{FULL}-kube-access"
    # the privileged identity exists even when the chart creates no other ServiceAccount
    off = _docs({**ON, "serviceAccount": {"create": False}})
    assert set(_named(off, "ServiceAccount")) == {f"{FULL}-kube-access"}
    assert _named(off, "ServiceAccount")[f"{FULL}-kube-access"]["automountServiceAccountToken"] is (
        False
    )
    (crb,) = [d for d in off if d["kind"] == "ClusterRoleBinding"]
    assert crb["subjects"][0]["name"] == f"{FULL}-kube-access"
    assert _settings(off)["ORCH_KUBE_ACCESS_SERVICE_ACCOUNT"] == f"{FULL}-kube-access"


def test_kube_access_without_rbac_create() -> None:
    docs = _docs({**ON, "rbac": {"create": False}})
    kinds = {d["kind"] for d in docs}
    assert not kinds & {"ClusterRole", "ClusterRoleBinding", "Role", "RoleBinding"}
    sa = _named(docs, "ServiceAccount")[f"{FULL}-kube-access"]
    assert sa["automountServiceAccountToken"] is False
    assert _settings(docs)["ORCH_KUBE_ACCESS_SERVICE_ACCOUNT"] == f"{FULL}-kube-access"


def test_agent_service_account() -> None:
    for values in ({}, ON):
        docs = _docs(values)
        sa = _named(docs, "ServiceAccount")[f"{FULL}-agent"]
        assert sa["automountServiceAccountToken"] is False
        labels = sa["metadata"]["labels"]
        assert labels["app.kubernetes.io/component"] == "agent"
        assert labels["app.kubernetes.io/instance"] == FULL
        assert labels["app.kubernetes.io/managed-by"] == "Helm"
        for binding in (d for d in docs if d["kind"] in {"RoleBinding", "ClusterRoleBinding"}):
            assert all(s["name"] != f"{FULL}-agent" for s in binding["subjects"])
        assert _settings(docs)["ORCH_AGENT_SERVICE_ACCOUNT"] == f"{FULL}-agent"
    off = _docs({"serviceAccount": {"create": False}})
    assert f"{FULL}-agent" not in _named(off, "ServiceAccount")
    assert _settings(off)["ORCH_AGENT_SERVICE_ACCOUNT"] == "default"


def test_reconciler_role_unchanged_with_kube_access() -> None:
    def role_and_binding(values: dict[str, Any]) -> tuple[dict, dict]:
        docs = _docs(values)
        role = _named(docs, "Role")[f"{FULL}-reconciler"]
        rb = _named(docs, "RoleBinding")[f"{FULL}-reconciler"]
        return role, rb

    off_role, off_rb = role_and_binding({})
    on_role, on_rb = role_and_binding(ON)
    assert on_role["rules"] == off_role["rules"]
    assert on_rb["roleRef"] == off_rb["roleRef"]
    assert on_rb["subjects"] == off_rb["subjects"]
    assert len([d for d in _docs(ON) if d["kind"] == "Role"]) == 2
    assert len([d for d in _docs(ON) if d["kind"] == "RoleBinding"]) == 2
    for rule in on_role["rules"]:
        assert rule["apiGroups"] != ["rbac.authorization.k8s.io"]


def _annotations(docs: list[dict]) -> dict[str, dict | None]:
    return {n: d["metadata"].get("annotations") for n, d in _named(docs, "ServiceAccount").items()}


def test_agent_accounts_do_not_inherit_api_annotations() -> None:
    ann = {"eks.amazonaws.com/role-arn": "arn:aws:iam::1:role/api"}
    got = _annotations(_docs({**ON, "serviceAccount": {"create": True, "annotations": ann}}))
    assert got[FULL] == ann
    assert got[f"{FULL}-reconciler"] == ann
    assert got[f"{FULL}-agent"] is None
    assert got[f"{FULL}-kube-access"] is None


def test_agent_account_annotations_own_values() -> None:
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    assert values["agentServiceAccount"]["annotations"] == {}
    assert values["kubeAccess"]["annotations"] == {}
    got = _annotations(
        _docs(
            {
                "kubeAccess": {"enabled": True, "annotations": {"k": "kube"}},
                "agentServiceAccount": {"annotations": {"a": "agent"}},
            }
        )
    )
    assert got[f"{FULL}-agent"] == {"a": "agent"}
    assert got[f"{FULL}-kube-access"] == {"k": "kube"}
    assert got[FULL] is None
    assert got[f"{FULL}-reconciler"] is None

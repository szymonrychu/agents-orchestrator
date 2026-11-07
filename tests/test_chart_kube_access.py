"""Chart RBAC and env of per-session cluster and namespace access, the transition-only legacy
shared ServiceAccount and the unprivileged agent ServiceAccount."""

from __future__ import annotations

from typing import Any

import yaml
from helm_render import CHART, render, render_error

FULL = "agents-orchestrator"
NS = "agents"
BASE: dict[str, Any] = {
    "agentImage": "registry.example/wrapper:1",
    "masterKey": {"value": "mk"},
    "claude": {"token": "tok"},
    "postgres": {"url": "postgresql://u:p@db:5432/app"},
}
CLUSTER = {"kubeClusterAccess": {"enabled": True}}
NSV = {
    "kubeNamespaceAccess": {
        "namespaces": ["media", "games"],
        "tiers": ["read", "port-forward", "exec", "edit", "admin"],
    }
}
ON = CLUSTER


def _docs(values: dict[str, Any] | None = None) -> list[dict]:
    return render({**BASE, **(values or {})}, namespace=NS)


def _named(docs: list[dict], kind: str) -> dict[str, dict]:
    return {d["metadata"]["name"]: d for d in docs if d["kind"] == kind}


def _settings(docs: list[dict]) -> dict[str, str]:
    return _named(docs, "ConfigMap")[f"{FULL}-settings"]["data"]


def test_defaults_render_no_kube_objects() -> None:
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    assert values["kubeClusterAccess"] == {"enabled": False, "legacySharedServiceAccount": False}
    assert values["kubeNamespaceAccess"] == {
        "namespaces": [],
        "tiers": ["read", "port-forward", "exec", "edit", "admin"],
    }
    assert "kubeAccess" not in values
    docs = _docs()
    assert not [d for d in docs if d["kind"] in ("ClusterRole", "ClusterRoleBinding")]
    assert f"{FULL}-kube-access" not in _named(docs, "ServiceAccount")
    assert not any(k.startswith("ORCH_KUBE_") for k in _settings(docs))


def test_cluster_access_reconciler_role() -> None:
    docs = _docs(CLUSTER)
    cr = _named(docs, "ClusterRole")[f"{FULL}-kube-cluster-access"]
    assert {
        "apiGroups": ["rbac.authorization.k8s.io"],
        "resources": ["clusterroles"],
        "verbs": ["bind"],
        "resourceNames": ["cluster-admin"],
    } in cr["rules"]
    assert any(
        r["resources"] == ["clusterrolebindings"]
        and set(r["verbs"]) == {"get", "list", "watch", "create", "patch", "delete"}
        for r in cr["rules"]
    )
    crb = _named(docs, "ClusterRoleBinding")[f"{FULL}-kube-cluster-access"]
    assert crb["roleRef"]["name"] == f"{FULL}-kube-cluster-access"
    assert crb["subjects"] == [
        {"kind": "ServiceAccount", "name": f"{FULL}-reconciler", "namespace": NS}
    ]
    assert _settings(docs)["ORCH_KUBE_CLUSTER_ACCESS_ENABLED"] == "true"
    assert "ORCH_KUBE_ACCESS_SERVICE_ACCOUNT" not in _settings(docs)
    assert f"{FULL}-kube-access" not in _named(docs, "ServiceAccount")


def test_cluster_access_without_rbac_create() -> None:
    docs = _docs({**CLUSTER, "rbac": {"create": False}})
    assert not {d["kind"] for d in docs} & {"ClusterRole", "ClusterRoleBinding", "Role"}
    assert _settings(docs)["ORCH_KUBE_CLUSTER_ACCESS_ENABLED"] == "true"


def test_legacy_shared_sa_only_with_flag() -> None:
    docs = _docs({"kubeClusterAccess": {"enabled": True, "legacySharedServiceAccount": True}})
    crb = _named(docs, "ClusterRoleBinding")[f"{FULL}-kube-access"]
    assert crb["roleRef"]["name"] == "cluster-admin"
    assert crb["subjects"] == [
        {"kind": "ServiceAccount", "name": f"{FULL}-kube-access", "namespace": NS}
    ]
    sa = _named(docs, "ServiceAccount")[f"{FULL}-kube-access"]
    assert sa["automountServiceAccountToken"] is False
    assert f"{FULL}-kube-access" not in _named(_docs(CLUSTER), "ServiceAccount")
    nobind = _docs(
        {"kubeClusterAccess": {"legacySharedServiceAccount": True}, "rbac": {"create": False}}
    )
    assert f"{FULL}-kube-access" in _named(nobind, "ServiceAccount")
    assert f"{FULL}-kube-access" not in _named(nobind, "ClusterRoleBinding")


def test_namespace_access_rbac() -> None:
    docs = _docs(NSV)
    crs = _named(docs, "ClusterRole")
    assert set(crs) == {
        f"{FULL}-ns-port-forward",
        f"{FULL}-ns-exec",
        f"{FULL}-kube-namespace-access",
    }
    pf = crs[f"{FULL}-ns-port-forward"]["rules"]
    assert {"apiGroups": [""], "resources": ["pods/portforward"], "verbs": ["get", "create"]} in pf
    assert {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]} in pf
    ex = crs[f"{FULL}-ns-exec"]["rules"]
    assert {"apiGroups": [""], "resources": ["pods/exec"], "verbs": ["get", "create"]} in ex
    assert {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]} in ex
    rules = crs[f"{FULL}-kube-namespace-access"]["rules"]
    assert any(
        r["resources"] == ["rolebindings"]
        and set(r["verbs"]) == {"get", "list", "watch", "create", "patch", "delete"}
        for r in rules
    )
    (bind,) = [r for r in rules if r["verbs"] == ["bind"]]
    assert set(bind["resourceNames"]) == {
        "view",
        "edit",
        "admin",
        f"{FULL}-ns-port-forward",
        f"{FULL}-ns-exec",
    }
    rbs = [
        d
        for d in docs
        if d["kind"] == "RoleBinding" and d["metadata"]["name"] == f"{FULL}-kube-namespace-access"
    ]
    assert sorted(d["metadata"]["namespace"] for d in rbs) == ["games", "media"]
    for rb in rbs:
        assert rb["roleRef"]["kind"] == "ClusterRole"
        assert rb["roleRef"]["name"] == f"{FULL}-kube-namespace-access"
        assert rb["subjects"] == [
            {"kind": "ServiceAccount", "name": f"{FULL}-reconciler", "namespace": NS}
        ]
    settings = _settings(docs)
    assert settings["ORCH_KUBE_NAMESPACE_ACCESS_NAMESPACES"] == "media,games"
    assert settings["ORCH_KUBE_NAMESPACE_ACCESS_TIERS"] == "read,port-forward,exec,edit,admin"
    assert settings["ORCH_KUBE_NAMESPACE_ACCESS_ROLE_PREFIX"] == f"{FULL}-ns-"
    assert "ORCH_KUBE_CLUSTER_ACCESS_ENABLED" not in settings


def test_only_enabled_tiers_render_and_bind() -> None:
    docs = _docs({"kubeNamespaceAccess": {"namespaces": ["media"], "tiers": ["read"]}})
    crs = _named(docs, "ClusterRole")
    assert set(crs) == {f"{FULL}-kube-namespace-access"}
    (bind,) = [r for r in crs[f"{FULL}-kube-namespace-access"]["rules"] if r["verbs"] == ["bind"]]
    assert bind["resourceNames"] == ["view"]
    assert _settings(docs)["ORCH_KUBE_NAMESPACE_ACCESS_TIERS"] == "read"


def test_unknown_tier_fails_render() -> None:
    err = render_error(
        {**BASE, "kubeNamespaceAccess": {"namespaces": ["media"], "tiers": ["write"]}}
    )
    assert "write" in err


def test_namespace_access_without_rbac_create() -> None:
    docs = _docs({**NSV, "rbac": {"create": False}})
    assert not {d["kind"] for d in docs} & {"ClusterRole", "ClusterRoleBinding", "Role"}
    assert not [d for d in docs if d["kind"] == "RoleBinding"]
    assert _settings(docs)["ORCH_KUBE_NAMESPACE_ACCESS_NAMESPACES"] == "media,games"


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


def test_reconciler_role_serviceaccounts_verbs() -> None:
    def sa_rules(values: dict[str, Any]) -> list[dict]:
        role = _named(_docs(values), "Role")[f"{FULL}-reconciler"]
        return [r for r in role["rules"] if "serviceaccounts" in r["resources"]]

    for values in (CLUSTER, NSV):
        (rule,) = sa_rules(values)
        assert rule["apiGroups"] == [""]
        assert rule["resources"] == ["serviceaccounts"]
        assert set(rule["verbs"]) == {"get", "list", "watch", "create", "patch", "delete"}
    assert sa_rules({}) == []
    for rule in _named(_docs(NSV), "Role")[f"{FULL}-reconciler"]["rules"]:
        assert rule["apiGroups"] != ["rbac.authorization.k8s.io"]


def _annotations(docs: list[dict]) -> dict[str, dict | None]:
    return {n: d["metadata"].get("annotations") for n, d in _named(docs, "ServiceAccount").items()}


def test_agent_accounts_do_not_inherit_api_annotations() -> None:
    ann = {"eks.amazonaws.com/role-arn": "arn:aws:iam::1:role/api"}
    got = _annotations(_docs({"serviceAccount": {"create": True, "annotations": ann}}))
    assert got[FULL] == ann
    assert got[f"{FULL}-reconciler"] == ann
    assert got[f"{FULL}-agent"] is None


def test_agent_account_annotations_own_values() -> None:
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    assert values["agentServiceAccount"]["annotations"] == {}
    got = _annotations(
        _docs(
            {
                "agentServiceAccount": {"annotations": {"a": "agent"}},
            }
        )
    )
    assert got[f"{FULL}-agent"] == {"a": "agent"}
    assert got[FULL] is None
    assert got[f"{FULL}-reconciler"] is None

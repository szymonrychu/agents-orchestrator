"""Component Deployments, Service, ServiceAccounts and helm test of the chart (D28, D1, D9, D20)."""

from __future__ import annotations

from typing import Any

from helm_render import render, render_text

FULL = "agents-orchestrator"
COMPONENTS = ["api", "dispatcher", "ingester", "reconciler"]
BASE: dict[str, Any] = {
    "agentImage": "registry.example/wrapper:1",
    "masterKey": {"value": "mk"},
    "claude": {"token": "tok"},
    "postgres": {"url": "postgresql://u:p@db:5432/app"},
}


def _deployments(values: dict[str, Any] | None = None) -> dict[str, dict]:
    docs = render({**BASE, **(values or {})})
    return {d["metadata"]["name"]: d for d in docs if d["kind"] == "Deployment"}


def _container(dep: dict) -> dict:
    return dep["spec"]["template"]["spec"]["containers"][0]


def test_four_deployments_with_migrate_init() -> None:
    deps = _deployments()
    assert sorted(deps) == sorted(f"{FULL}-{c}" for c in COMPONENTS)
    for c in COMPONENTS:
        dep = deps[f"{FULL}-{c}"]
        spec = dep["spec"]["template"]["spec"]
        assert dep["metadata"]["labels"]["app.kubernetes.io/component"] == c
        assert dep["spec"]["selector"]["matchLabels"]["app.kubernetes.io/component"] == c
        (init,) = spec["initContainers"]
        assert init["name"] == "migrate"
        assert init["args"] == ["migrate"] or init["command"][-1] == "migrate"
        assert init["image"] == _container(dep)["image"]
        assert _container(dep)["env"][: len(init["env"])] == init["env"]
        assert init["envFrom"] == _container(dep)["envFrom"]
        names = [e["name"] for e in init["env"]]
        assert "ORCH_DATABASE_URL" in names and "ORCH_MASTER_KEY" in names
        assert spec["terminationGracePeriodSeconds"] == 30


def test_component_commands_and_values() -> None:
    values = {
        "components": {
            "api": {"replicas": 2, "resources": {"limits": {"memory": "1Gi"}}},
            "dispatcher": {"extraArgs": ["--verbose"]},
            "reconciler": {"replicas": 3},
        }
    }
    deps = _deployments(values)
    for c in COMPONENTS:
        cont = _container(deps[f"{FULL}-{c}"])
        assert cont["command"] == ["orch"]
        assert cont["args"][0] == c
    assert deps[f"{FULL}-api"]["spec"]["replicas"] == 2
    assert deps[f"{FULL}-reconciler"]["spec"]["replicas"] == 3
    assert deps[f"{FULL}-ingester"]["spec"]["replicas"] == 1
    assert _container(deps[f"{FULL}-api"])["resources"] == {"limits": {"memory": "1Gi"}}
    assert _container(deps[f"{FULL}-dispatcher"])["args"] == ["dispatcher", "--verbose"]


def test_only_reconciler_and_api_get_kube_token() -> None:
    deps = _deployments()
    for c in COMPONENTS:
        spec = deps[f"{FULL}-{c}"]["spec"]["template"]["spec"]
        if c == "reconciler":
            assert spec["serviceAccountName"] == f"{FULL}-reconciler"
            assert spec.get("automountServiceAccountToken") is not False
        elif c == "api":
            assert spec["serviceAccountName"] == f"{FULL}-api"
            assert spec.get("automountServiceAccountToken") is not False
        else:
            assert spec["automountServiceAccountToken"] is False
            assert spec["serviceAccountName"] not in (f"{FULL}-reconciler", f"{FULL}-api")
    sas = {
        d["metadata"]["name"]: d
        for d in render({**BASE}, show_only=["templates/serviceaccount.yaml"])
    }
    assert set(sas) == {FULL, f"{FULL}-reconciler", f"{FULL}-api", f"{FULL}-agent"}
    assert sas[f"{FULL}-api"]["automountServiceAccountToken"] is True
    assert sas[FULL]["automountServiceAccountToken"] is False
    assert sas[f"{FULL}-agent"]["automountServiceAccountToken"] is False
    off = render({**BASE, "serviceAccount": {"create": False}}, show_only=None)
    assert not [d for d in off if d["kind"] == "ServiceAccount"]


def test_api_service_and_probes() -> None:
    docs = render(BASE)
    (svc,) = [d for d in docs if d["kind"] == "Service" and d["metadata"]["name"] == FULL]
    assert svc["metadata"]["name"] == FULL
    assert svc["spec"]["selector"]["app.kubernetes.io/component"] == "api"
    (port,) = svc["spec"]["ports"]
    assert port["port"] == 80 and port["targetPort"] == "http"
    deps = _deployments()
    api = _container(deps[f"{FULL}-api"])
    assert api["ports"] == [
        {"name": "http", "containerPort": 8000, "protocol": "TCP"},
        {"name": "agents", "containerPort": 8001, "protocol": "TCP"},
        {"name": "services", "containerPort": 8002, "protocol": "TCP"},
    ]
    assert api["livenessProbe"]["httpGet"] == {"path": "/healthz", "port": "http"}
    assert api["readinessProbe"]["httpGet"] == {"path": "/readyz", "port": "http"}
    assert {"name": "ORCH_HTTP_PORT", "value": "8000"} in api["env"]
    for c in ("dispatcher", "ingester", "reconciler"):
        cont = _container(deps[f"{FULL}-{c}"])
        assert "livenessProbe" not in cont and "readinessProbe" not in cont
        assert "ports" not in cont
    custom = _deployments(
        {"components": {"ingester": {"livenessProbe": {"exec": {"command": ["true"]}}}}}
    )
    assert _container(custom[f"{FULL}-ingester"])["livenessProbe"] == {
        "exec": {"command": ["true"]}
    }


def test_image_tag_defaults_to_app_version() -> None:
    repo = "harbor.szymonrichert.pl/containers/agents-orchestrator"
    for dep in _deployments().values():
        assert _container(dep)["image"] == f"{repo}:dev"
    for dep in _deployments({"image": {"tag": "9.9.9"}}).values():
        assert _container(dep)["image"] == f"{repo}:9.9.9"
        assert dep["spec"]["template"]["spec"]["initContainers"][0]["image"] == f"{repo}:9.9.9"


def test_helm_test_targets_healthz() -> None:
    (pod,) = render(BASE, show_only=["templates/tests/test-connection.yaml"])
    assert pod["metadata"]["annotations"]["helm.sh/hook"] == "test"
    args = pod["spec"]["containers"][0]["args"]
    assert args == [f"{FULL}:80/healthz"]


def test_settings_checksum_rolls_components() -> None:
    before = _deployments()
    after = _deployments({"env": {"ORCH_LOG_LEVEL": "debug"}})
    for c in COMPONENTS:
        name = f"{FULL}-{c}"
        a = before[name]["spec"]["template"]["metadata"]["annotations"]["checksum/settings"]
        b = after[name]["spec"]["template"]["metadata"]["annotations"]["checksum/settings"]
        assert a and b and a != b


def test_component_pod_annotations_reach_only_that_component() -> None:
    # A sidecar-injecting webhook (oauth2-proxy admission) rejects port-less pods, so its
    # annotations must land on the api pod alone; global podAnnotations still reach all.
    deps = _deployments(
        {
            "podAnnotations": {"shared": "yes"},
            "components": {"api": {"podAnnotations": {"only-api": "true", "shared": "api"}}},
        }
    )
    for c in COMPONENTS:
        ann = deps[f"{FULL}-{c}"]["spec"]["template"]["metadata"]["annotations"]
        if c == "api":
            assert ann["only-api"] == "true"
            assert ann["shared"] == "api"
        else:
            assert "only-api" not in ann
            assert ann["shared"] == "yes"
        assert ann["checksum/settings"]


def _kind(kind: str, values: dict[str, Any] | None = None) -> list[dict]:
    return [d for d in render({**BASE, **(values or {})}) if d["kind"] == kind]


def test_agents_service() -> None:
    svcs = {d["metadata"]["name"]: d for d in _kind("Service")}
    svc = svcs[f"{FULL}-agents"]
    assert svc["spec"]["type"] == "ClusterIP"
    (port,) = svc["spec"]["ports"]
    assert port["port"] == 8001 and port["targetPort"] == "agents"
    assert svc["spec"]["selector"]["app.kubernetes.io/component"] == "api"
    ports = {
        p["name"]: p["containerPort"] for p in _container(_deployments()[f"{FULL}-api"])["ports"]
    }
    assert ports == {"http": 8000, "agents": 8001, "services": 8002}
    custom = {d["metadata"]["name"]: d for d in _kind("Service", {"agentApi": {"port": 9100}})}
    assert custom[f"{FULL}-agents"]["spec"]["ports"][0]["port"] == 9100
    assert custom[f"{FULL}-agents"]["spec"]["ports"][0]["targetPort"] == "agents"


def test_ingress_excludes_agents_service() -> None:
    (ing,) = _kind(
        "Ingress",
        {
            "ingress": {
                "enabled": True,
                "hosts": [{"host": "o.example", "paths": [{"path": "/", "pathType": "Prefix"}]}],
            }
        },
    )
    backends = [
        p["backend"]["service"]["name"] for r in ing["spec"]["rules"] for p in r["http"]["paths"]
    ]
    assert backends and all(b == FULL for b in backends)
    # neither the agents Service nor its "agents" port is reachable through the Ingress
    assert f"{FULL}-agents" not in str(ing)
    assert "'agents'" not in str(ing)


def test_agent_api_settings() -> None:
    (cm,) = [d for d in _kind("ConfigMap") if d["metadata"]["name"] == f"{FULL}-settings"]
    assert cm["data"]["ORCH_AGENT_PORT"] == "8001"
    assert cm["data"]["ORCH_AGENT_API_URL"] == f"http://{FULL}-agents.agents.svc:8001"
    (cm,) = [
        d
        for d in _kind("ConfigMap", {"agentApi": {"port": 9100}})
        if d["metadata"]["name"] == f"{FULL}-settings"
    ]
    assert cm["data"]["ORCH_AGENT_PORT"] == "9100"
    assert cm["data"]["ORCH_AGENT_API_URL"].endswith(":9100")
    (cm,) = [
        d
        for d in _kind("ConfigMap", {"env": {"ORCH_AGENT_API_URL": "http://x:1"}})
        if d["metadata"]["name"] == f"{FULL}-settings"
    ]
    assert cm["data"]["ORCH_AGENT_API_URL"] == "http://x:1"


def test_agent_api_url_rendered_once_when_overridden() -> None:
    text = render_text(
        {**BASE, "env": {"ORCH_AGENT_API_URL": "http://x:1"}},
        show_only=["templates/configmap.yaml"],
    )
    assert text.count("ORCH_AGENT_API_URL:") == 1
    assert 'ORCH_AGENT_API_URL: "http://x:1"' in text


def test_services_secret_checksum_rolls_api() -> None:
    h1, h2 = "a" * 64, "b" * 64

    def sums(hashes: dict[str, str]) -> dict[str, str | None]:
        deps = _deployments({"serviceApi": {"tokenHashes": hashes}})
        return {
            n: d["spec"]["template"]["metadata"]["annotations"].get("checksum/services-secret")
            for n, d in deps.items()
        }

    one, two = sums({"schedule": h1}), sums({"schedule": h1, "webhook": h2})
    assert sorted(one) == sorted(f"{FULL}-{c}" for c in COMPONENTS)
    assert all(one.values()) and all(two.values())
    assert one[f"{FULL}-api"] != two[f"{FULL}-api"]
    assert sums({"schedule": h1}) == one
    assert set(sums({}).values()) == {None}

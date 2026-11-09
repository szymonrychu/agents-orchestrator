"""Service port of the chart (D10): own Service, container port, token wiring."""

from __future__ import annotations

from typing import Any

from helm_render import render, render_error

FULL = "agents-orchestrator"
BASE: dict[str, Any] = {
    "agentImage": "registry.example/wrapper:1",
    "masterKey": {"value": "mk"},
    "claude": {"token": "tok"},
    "postgres": {"url": "postgresql://u:p@db:5432/app"},
}
H_A = "a" * 64
H_B = "b" * 64


def _docs(values: dict[str, Any] | None = None) -> list[dict]:
    return render({**BASE, **(values or {})})


def _api_container(docs: list[dict]) -> dict:
    (dep,) = [
        d for d in docs if d["metadata"]["name"] == f"{FULL}-api" and d["kind"] == "Deployment"
    ]
    return dep["spec"]["template"]["spec"]["containers"][0]


def _tokens_env(docs: list[dict]) -> list[dict]:
    return [e for e in _api_container(docs)["env"] if e["name"] == "ORCH_SERVICE_TOKENS"]


def test_services_service_rendered() -> None:
    docs = _docs()
    (svc,) = [
        d for d in docs if d["kind"] == "Service" and d["metadata"]["name"] == f"{FULL}-services"
    ]
    assert svc["spec"]["type"] == "ClusterIP"
    (port,) = svc["spec"]["ports"]
    assert port["port"] == 8002 and port["name"] == "services" and port["targetPort"] == "services"
    assert svc["spec"]["selector"]["app.kubernetes.io/component"] == "api"
    ports = {p["name"]: p["containerPort"] for p in _api_container(docs)["ports"]}
    assert ports == {"http": 8000, "agents": 8001, "services": 8002}
    cm = next(
        d for d in docs if d["kind"] == "ConfigMap" and d["metadata"]["name"] == f"{FULL}-settings"
    )
    assert cm["data"]["ORCH_SERVICE_PORT"] == "8002"
    custom = _docs({"serviceApi": {"port": 9200}})
    svc2 = next(
        d for d in custom if d["metadata"]["name"] == f"{FULL}-services" and d["kind"] == "Service"
    )
    assert svc2["spec"]["ports"][0]["port"] == 9200
    assert {p["name"]: p["containerPort"] for p in _api_container(custom)["ports"]}[
        "services"
    ] == 9200


def test_ingress_excludes_services_port() -> None:
    (ing,) = [
        d
        for d in _docs(
            {
                "ingress": {
                    "enabled": True,
                    "hosts": [
                        {"host": "o.example", "paths": [{"path": "/", "pathType": "Prefix"}]}
                    ],
                }
            }
        )
        if d["kind"] == "Ingress"
    ]
    backends = [p["backend"]["service"] for r in ing["spec"]["rules"] for p in r["http"]["paths"]]
    assert backends and all(b["name"] == FULL for b in backends)
    assert "-services" not in str(ing)


def test_service_tokens_from_secret() -> None:
    assert _tokens_env(_docs()) == []
    existing = _tokens_env(
        _docs(
            {
                "serviceApi": {
                    "existingSecret": "ext",
                    "existingSecretKey": "k",
                    "tokenHashes": {"x": H_A},
                }
            }
        )
    )
    assert existing == [
        {"name": "ORCH_SERVICE_TOKENS", "valueFrom": {"secretKeyRef": {"name": "ext", "key": "k"}}}
    ]
    default_key = _tokens_env(_docs({"serviceApi": {"existingSecret": "ext"}}))
    assert default_key[0]["valueFrom"]["secretKeyRef"] == {"name": "ext", "key": "service-tokens"}

    # two values files merged by helmfile: sorted CSV in the chart Secret
    docs = _docs({"serviceApi": {"tokenHashes": {"webhook": H_B, "trigger": H_A}}})
    (secret,) = [
        d for d in docs if d["kind"] == "Secret" and d["metadata"]["name"] == f"{FULL}-services"
    ]
    assert secret["stringData"]["service-tokens"] == f"trigger:{H_A},webhook:{H_B}"
    (env,) = _tokens_env(docs)
    assert env["valueFrom"]["secretKeyRef"] == {"name": f"{FULL}-services", "key": "service-tokens"}
    assert HEX_NOT_IN_CONFIGMAP(docs)


def HEX_NOT_IN_CONFIGMAP(docs: list[dict]) -> bool:
    for d in docs:
        if d["kind"] == "ConfigMap":
            text = str(d)
            if H_A in text or H_B in text or "ORCH_SERVICE_TOKENS" in text:
                return False
    return True


def test_malformed_hash_fails_render() -> None:
    for bad in ("short", "A" * 64, "g" * 64, "a" * 63):
        err = render_error({**BASE, "serviceApi": {"tokenHashes": {"svc": bad}}})
        assert "tokenHashes" in err


def test_service_scopes_env_rendered_only_when_set() -> None:
    def scopes(values: dict) -> list[dict]:
        return [
            e for e in _api_container(_docs(values))["env"] if e["name"] == "ORCH_SERVICE_SCOPES"
        ]

    assert scopes({}) == []
    got = scopes(
        {
            "serviceApi": {
                "tokenHashes": {"agents-vault": H_A},
                "scopes": {"agents-vault": ["GET /api/v1/presets", "GET /api/v1/sessions"]},
            }
        }
    )
    assert [e["value"] for e in got] == [
        '{"agents-vault":["GET /api/v1/presets","GET /api/v1/sessions"]}'
    ]

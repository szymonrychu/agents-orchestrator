"""C3: the usage gate upstream. ORCH_USAGE_URL and ORCH_USAGE_TOKEN are one pair, env of the api
main container only (Settings refuses a URL without a token); the ConfigMap carries only
ORCH_USAGE_QUOTA_DEFAULT."""

from __future__ import annotations

from typing import Any

import pytest
import yaml
from helm_render import CHART, render, render_error

FULL = "agents-orchestrator"
COMPONENTS = ["api", "dispatcher", "ingester", "reconciler"]
USAGE_URL = "http://agents-usage-api-service:8001"
BASE: dict[str, Any] = {
    "agentImage": "registry.example/wrapper:1",
    "masterKey": {"value": "mk"},
    "claude": {"token": "tok"},
    "postgres": {"url": "postgresql://u:p@db:5432/app"},
}
USAGE_VALUES = {
    "inline": {"url": USAGE_URL, "token": "t"},
    "existing": {"url": USAGE_URL, "existingSecret": "usage-s", "existingSecretKey": "k"},
}


def _docs(extra: dict[str, Any] | None = None) -> list[dict]:
    return render({**BASE, **(extra or {})})


def _by_kind(docs: list[dict], kind: str) -> dict[str, dict]:
    return {d["metadata"]["name"]: d for d in docs if d["kind"] == kind}


def _containers(docs: list[dict]) -> dict[str, list[dict]]:
    """Every container (init containers included) of each Deployment, by Deployment name."""
    out = {}
    for name, dep in _by_kind(docs, "Deployment").items():
        spec = dep["spec"]["template"]["spec"]
        out[name] = spec.get("initContainers", []) + spec["containers"]
    return out


def _env_names(container: dict) -> list[str]:
    return [e["name"] for e in container.get("env", [])]


def _is_api_main(dep: str, container: dict) -> bool:
    return dep == f"{FULL}-api" and container["name"] == "api"


def _settings(docs: list[dict]) -> dict[str, str]:
    return _by_kind(docs, "ConfigMap")[f"{FULL}-settings"]["data"]


def test_values_defaults() -> None:
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    assert values["usage"] == {
        "url": "",
        "token": "",
        "existingSecret": "",
        "existingSecretKey": "usage-token",
    }
    assert values["usageQuotaDefault"] == 100


def test_off_by_default_renders_no_url_or_token() -> None:
    docs = _docs()
    assert f"{FULL}-usage" not in _by_kind(docs, "Secret")
    for items in _containers(docs).values():
        for c in items:
            assert "ORCH_USAGE_URL" not in _env_names(c)
            assert "ORCH_USAGE_TOKEN" not in _env_names(c)


@pytest.mark.parametrize(
    "half",
    [{"url": USAGE_URL}, {"token": "t"}, {"existingSecret": "usage-s"}],
    ids=["url-only", "token-only", "existing-secret-only"],
)
def test_pair_or_fail(half: dict[str, str]) -> None:
    err = render_error({**BASE, "usage": half})
    assert "usage.url together with usage.token or usage.existingSecret, or none of them" in err


def test_quota_default_in_configmap() -> None:
    assert _settings(_docs())["ORCH_USAGE_QUOTA_DEFAULT"] == "100"
    assert _settings(_docs({"usageQuotaDefault": 80}))["ORCH_USAGE_QUOTA_DEFAULT"] == "80"


@pytest.mark.parametrize("usage", USAGE_VALUES.values(), ids=USAGE_VALUES.keys())
def test_configmap_has_no_usage_url_or_token(usage: dict[str, str]) -> None:
    data = _settings(_docs({"usage": usage}))
    assert "ORCH_USAGE_URL" not in data
    assert "ORCH_USAGE_TOKEN" not in data
    assert data["ORCH_USAGE_QUOTA_DEFAULT"] == "100"


@pytest.mark.parametrize("usage", USAGE_VALUES.values(), ids=USAGE_VALUES.keys())
def test_pair_only_in_api_main_container(usage: dict[str, str]) -> None:
    """Settings refuses a usage URL without its token: no container may get one alone, and the
    dispatcher, ingester, reconciler and every migrate init container get neither."""
    containers = _containers(_docs({"usage": usage}))
    assert sorted(containers) == sorted(f"{FULL}-{c}" for c in COMPONENTS)
    for dep, items in containers.items():
        for c in items:
            names = _env_names(c)
            assert ("ORCH_USAGE_URL" in names) == ("ORCH_USAGE_TOKEN" in names), (dep, c["name"])
            assert ("ORCH_USAGE_URL" in names) == _is_api_main(dep, c), (dep, c["name"])


@pytest.mark.parametrize("usage", USAGE_VALUES.values(), ids=USAGE_VALUES.keys())
def test_api_container_has_usage_pair(usage: dict[str, str]) -> None:
    api = next(c for c in _containers(_docs({"usage": usage}))[f"{FULL}-api"] if c["name"] == "api")
    env = {e["name"]: e for e in api["env"]}
    assert env["ORCH_USAGE_URL"] == {"name": "ORCH_USAGE_URL", "value": USAGE_URL}
    assert env["ORCH_USAGE_TOKEN"]["valueFrom"]["secretKeyRef"] == {
        "name": usage.get("existingSecret", f"{FULL}-usage"),
        "key": usage.get("existingSecretKey", "usage-token"),
    }


def test_chart_secret_and_checksum_on_api_only() -> None:
    tok = "".join(["marker", "usage", "value"])
    docs = _docs({"usage": {"url": USAGE_URL, "token": tok}})
    assert _by_kind(docs, "Secret")[f"{FULL}-usage"]["stringData"] == {"usage-token": tok}
    for cm in _by_kind(docs, "ConfigMap").values():
        assert tok not in yaml.safe_dump(cm)

    def sums(token: str) -> dict[str, str | None]:
        deps = _by_kind(_docs({"usage": {"url": USAGE_URL, "token": token}}), "Deployment")
        return {
            n: d["spec"]["template"]["metadata"]["annotations"].get("checksum/usage-secret")
            for n, d in deps.items()
        }

    one, two = sums("t1"), sums("t2")
    assert one[f"{FULL}-api"] and two[f"{FULL}-api"] and one[f"{FULL}-api"] != two[f"{FULL}-api"]
    assert {v for n, v in one.items() if n != f"{FULL}-api"} == {None}


def test_existing_secret_wins_over_inline_token() -> None:
    docs = _docs(
        {
            "usage": {
                "url": USAGE_URL,
                "token": "t",
                "existingSecret": "usage-s",
                "existingSecretKey": "k",
            }
        }
    )
    assert f"{FULL}-usage" not in _by_kind(docs, "Secret")
    api = next(c for c in _containers(docs)[f"{FULL}-api"] if c["name"] == "api")
    ref = next(e for e in api["env"] if e["name"] == "ORCH_USAGE_TOKEN")
    assert ref["valueFrom"]["secretKeyRef"] == {"name": "usage-s", "key": "k"}
    for d in _by_kind(docs, "Deployment").values():
        assert "checksum/usage-secret" not in d["spec"]["template"]["metadata"]["annotations"]

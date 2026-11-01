"""C6: agentUsage renders ORCH_AGENT_USAGE_URL and ORCH_AGENT_USAGE_SECRET into the settings
ConfigMap, both or neither; the chart never carries the ingest token itself."""

from __future__ import annotations

import json
from typing import Any

import yaml
from helm_render import CHART, render, render_error

FULL = "agents-orchestrator"
BASE: dict[str, Any] = {
    "agentImage": "registry.example/wrapper:1",
    "masterKey": {"value": "mk"},
    "claude": {"token": "tok"},
    "postgres": {"url": "postgresql://u:p@db:5432/app"},
}
URL = "http://agents-usage:8002"
SECRET = "agents-usage-ingest"
BOTH = {"agentUsage": {"url": URL, "secretName": SECRET}}


def _docs(extra: dict[str, Any] | None = None) -> list[dict]:
    return render({**BASE, **(extra or {})}, namespace="agents")


def _settings(docs: list[dict]) -> dict[str, str]:
    (cm,) = [
        d for d in docs if d["kind"] == "ConfigMap" and d["metadata"]["name"] == f"{FULL}-settings"
    ]
    return cm["data"]


def test_values_default_is_empty() -> None:
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    assert values["agentUsage"] == {"url": "", "secretName": ""}


def test_empty_renders_neither_variable() -> None:
    data = _settings(_docs())
    assert "ORCH_AGENT_USAGE_URL" not in data
    assert "ORCH_AGENT_USAGE_SECRET" not in data


def test_both_set_land_in_the_configmap() -> None:
    data = _settings(_docs(BOTH))
    assert data["ORCH_AGENT_USAGE_URL"] == URL
    assert data["ORCH_AGENT_USAGE_SECRET"] == SECRET


def test_only_one_set_fails_the_render() -> None:
    for half in ({"url": URL}, {"secretName": SECRET}):
        err = render_error({**BASE, "agentUsage": half})
        assert "agentUsage.url together with agentUsage.secretName, or none of them" in err, half


def test_no_token_value_in_the_chart_for_this_feature() -> None:
    docs = _docs(BOTH)
    off = _docs()
    # the only difference from the default render is the two ConfigMap entries
    on_data = _settings(docs)
    off_data = _settings(off)
    assert set(on_data) - set(off_data) == {"ORCH_AGENT_USAGE_URL", "ORCH_AGENT_USAGE_SECRET"}
    assert len(docs) == len(off)
    assert "CCW_USAGE_TOKEN" not in json.dumps(docs)
    assert "ORCH_AGENT_USAGE_TOKEN" not in json.dumps(docs)
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    assert set(values["agentUsage"]) == {"url", "secretName"}

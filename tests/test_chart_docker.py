"""Value-gated docker sidecar settings: ORCH_DOCKER_* reach the ConfigMap only with an image."""

from __future__ import annotations

from typing import Any

import pytest
import yaml
from helm_render import CHART, render, render_error

FULL = "agents-orchestrator"
BASE: dict[str, Any] = {
    "agentImage": "registry.example/wrapper:1",
    "masterKey": {"value": "mk"},
    "claude": {"token": "tok"},
    "postgres": {"url": "postgresql://u:p@db:5432/app"},
}
IMAGE = "docker:27.3.1-dind"


def _settings(values: dict[str, Any] | None = None) -> dict[str, str]:
    docs = render({**BASE, **(values or {})}, namespace="agents")
    (cm,) = (
        d for d in docs if d["kind"] == "ConfigMap" and d["metadata"]["name"] == f"{FULL}-settings"
    )
    return cm["data"]


def test_docker_off_by_default() -> None:
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    assert values["docker"]["image"] == ""
    assert not [k for k in _settings() if "DOCKER" in k]


def test_docker_image_set_emits_settings_and_caps() -> None:
    data = _settings({"docker": {"image": IMAGE}})
    assert data["ORCH_DOCKER_IMAGE"] == IMAGE
    assert data["ORCH_DOCKER_STORAGE"] == "30Gi"
    assert data["ORCH_DOCKER_CPU_REQUEST"] == "500m"
    assert data["ORCH_DOCKER_CPU_LIMIT"] == "4"
    assert data["ORCH_DOCKER_MEMORY_REQUEST"] == "1Gi"
    assert data["ORCH_DOCKER_MEMORY_LIMIT"] == "8Gi"
    assert data["ORCH_DOCKER_EPHEMERAL_REQUEST"] == "5Gi"
    assert data["ORCH_MAX_DOCKER_STORAGE"] == "100Gi"
    assert data["ORCH_MAX_DOCKER_CPU"] == "8"
    assert data["ORCH_MAX_DOCKER_MEMORY"] == "16Gi"


def test_docker_overrides_reach_the_configmap() -> None:
    data = _settings(
        {
            "docker": {
                "image": IMAGE,
                "storage": "50Gi",
                "cpu": {"request": "1", "limit": "2"},
                "memory": {"request": "2Gi", "limit": "4Gi"},
                "ephemeralRequest": "10Gi",
                "maxStorage": "60Gi",
                "maxCpu": "4",
                "maxMemory": "8Gi",
            }
        }
    )
    assert data["ORCH_DOCKER_STORAGE"] == "50Gi"
    assert data["ORCH_DOCKER_CPU_LIMIT"] == "2"
    assert data["ORCH_DOCKER_MEMORY_REQUEST"] == "2Gi"
    assert data["ORCH_DOCKER_EPHEMERAL_REQUEST"] == "10Gi"
    assert data["ORCH_MAX_DOCKER_STORAGE"] == "60Gi"
    assert data["ORCH_MAX_DOCKER_CPU"] == "4"
    assert data["ORCH_MAX_DOCKER_MEMORY"] == "8Gi"


@pytest.mark.parametrize(
    "image",
    [
        "docker:dind",
        "docker:latest",
        "docker:27-dind",
        "docker:27.3.1",
        "docker:27.3.1-dind-rootless",
        "docker",
    ],
)
def test_docker_image_without_semver_dind_tag_fails_render(image: str) -> None:
    err = render_error({**BASE, "docker": {"image": image}}, namespace="agents")
    assert "docker.image" in err

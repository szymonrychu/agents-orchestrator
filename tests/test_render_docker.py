"""A docker session's agent pod gets the privileged dind sidecar, its socket and the CLI copy."""

from __future__ import annotations

from typing import Any

import pytest
import yaml
from test_service_accounts import TOKENS, make_session, make_settings

from agents_orchestrator.k8s import render

IMAGE = "docker:27.3.1-dind"
SOCK = "unix:///var/run/docker/docker.sock"


def settings(**over: Any) -> Any:
    return make_settings(docker_image=IMAGE, **over)


def pod_of(objs: list[dict[str, Any]]) -> dict[str, Any]:
    (sts,) = (o for o in objs if o["kind"] == "StatefulSet")
    return sts["spec"]["template"]["spec"]


def docker_pod(session_kw: dict[str, Any] | None = None, **over: Any) -> dict[str, Any]:
    session = make_session(docker=True, **(session_kw or {}))
    return pod_of(render.render_session(session, settings(**over), TOKENS))


def by_name(items: list[dict[str, Any]], name: str) -> dict[str, Any]:
    (item,) = (i for i in items if i["name"] == name)
    return item


def mounts(container: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {m["name"]: m for m in container["volumeMounts"]}


@pytest.mark.parametrize("spec", [{}, {"docker": False}], ids=["unset", "false"])
@pytest.mark.parametrize("image", ["", IMAGE], ids=["setting-off", "setting-on"])
def test_no_docker_keys_without_the_flag(spec: dict[str, Any], image: str) -> None:
    session = make_session(**spec)
    pod = pod_of(render.render_session(session, make_settings(docker_image=image), TOKENS))
    assert "initContainers" not in pod
    assert {v["name"] for v in pod["volumes"]} == {"workspace", "config", "mise", "any-mcp"}
    ccw = pod["containers"][0]
    assert "DOCKER_HOST" not in {e["name"] for e in ccw["env"]}
    assert not {"docker-sock", "docker-cli"} & set(mounts(ccw))
    assert "privileged" not in str(pod)


def test_enabled_flag_without_setting_fails_loudly() -> None:
    # An enabled flag is never silently muted: no image means no pod, not a pod without docker.
    with pytest.raises(ValueError, match="ORCH_DOCKER_IMAGE"):
        render.render_session(make_session(docker=True), make_settings(), TOKENS)


def test_init_container_order_and_sidecar_shape() -> None:
    pod = docker_pod()
    cli, dind = pod["initContainers"]
    assert cli["name"] == "docker-cli"
    assert "restartPolicy" not in cli
    assert dind["name"] == "dind"
    assert dind["restartPolicy"] == "Always"
    assert cli["image"] == dind["image"] == IMAGE


def test_only_dind_is_privileged_root() -> None:
    pod = docker_pod()
    assert pod["securityContext"] == {"runAsUser": 1000, "fsGroup": 1000}
    dind = by_name(pod["initContainers"], "dind")
    assert dind["securityContext"] == {"privileged": True, "runAsUser": 0}
    others = [by_name(pod["initContainers"], "docker-cli"), *pod["containers"]]
    for c in others:
        assert "privileged" not in str(c.get("securityContext", {}))


def test_dind_command_unix_socket_only() -> None:
    dind = by_name(docker_pod()["initContainers"], "dind")
    assert dind["command"][:2] == ["dockerd-entrypoint.sh", "dockerd"]
    assert f"--host={SOCK}" in dind["command"]
    assert "--group=1000" in dind["command"]
    assert not any("tcp" in a or "--tls" in a for a in dind["command"])
    assert not any(a.startswith("--registry-mirror") for a in dind["command"])
    assert {"name": "DOCKER_TLS_CERTDIR", "value": ""} in dind["env"]
    assert "ports" not in dind


@pytest.mark.parametrize(
    ("mirror", "flag"),
    [
        ("harbor.example", "--registry-mirror=https://harbor.example"),
        ("http://m:5000", "--registry-mirror=http://m:5000"),
    ],
)
def test_registry_mirror_only_when_set(mirror: str, flag: str) -> None:
    dind = by_name(docker_pod(docker_registry_mirror=mirror)["initContainers"], "dind")
    assert flag in dind["command"]


def test_mirror_reuses_image_pull_secret_credentials() -> None:
    pod = docker_pod(docker_registry_mirror="harbor.example", image_pull_secret="regcred")
    vol = by_name(pod["volumes"], "docker-auth")
    assert vol["secret"]["secretName"] == "regcred"
    assert vol["secret"]["items"] == [{"key": ".dockerconfigjson", "path": "config.json"}]
    dind = by_name(pod["initContainers"], "dind")
    assert mounts(dind)["docker-auth"] == {
        "name": "docker-auth",
        "mountPath": "/root/.docker",
        "readOnly": True,
    }
    plain = docker_pod()
    assert "docker-auth" not in {v["name"] for v in plain["volumes"]}


def test_startup_probe_asks_dockerd() -> None:
    dind = by_name(docker_pod()["initContainers"], "dind")
    probe = dind["startupProbe"]
    assert probe["exec"]["command"] == ["docker", "-H", SOCK, "info"]
    assert probe["failureThreshold"] * probe["periodSeconds"] >= 60


def test_volumes() -> None:
    pod = docker_pod()
    vols = {v["name"]: v for v in pod["volumes"]}
    assert vols["docker-sock"] == {"name": "docker-sock", "emptyDir": {}}
    assert vols["docker-data"]["emptyDir"] == {"sizeLimit": "30Gi"}
    assert vols["docker-cli"] == {"name": "docker-cli", "emptyDir": {}}
    assert vols["lib-modules"]["hostPath"] == {"path": "/lib/modules"}
    assert "persistentVolumeClaim" not in vols["docker-data"]
    dind_mounts = mounts(by_name(pod["initContainers"], "dind"))
    assert dind_mounts["docker-sock"]["mountPath"] == "/var/run/docker"
    assert dind_mounts["docker-data"]["mountPath"] == "/var/lib/docker"
    assert dind_mounts["lib-modules"]["mountPath"] == "/lib/modules"
    assert dind_mounts["lib-modules"]["readOnly"] is True


def test_storage_size_limit_spec_over_settings() -> None:
    assert by_name(docker_pod({"docker_storage": "50Gi"})["volumes"], "docker-data")[
        "emptyDir"
    ] == {"sizeLimit": "50Gi"}
    pod = docker_pod(docker_storage="10Gi")
    assert by_name(pod["volumes"], "docker-data")["emptyDir"] == {"sizeLimit": "10Gi"}


def test_resources_defaults_and_spec_override() -> None:
    res = by_name(docker_pod()["initContainers"], "dind")["resources"]
    assert res == {
        "requests": {"cpu": "500m", "memory": "1Gi", "ephemeral-storage": "5Gi"},
        "limits": {"cpu": "4", "memory": "8Gi"},
    }
    res = by_name(
        docker_pod({"docker_cpu": "2", "docker_memory": "512Mi"})["initContainers"], "dind"
    )["resources"]
    assert res["limits"] == {"cpu": "2", "memory": "512Mi"}
    # a request never exceeds the limit
    assert res["requests"] == {"cpu": "500m", "memory": "512Mi", "ephemeral-storage": "5Gi"}


def test_cli_copy_init_container() -> None:
    cli = by_name(docker_pod()["initContainers"], "docker-cli")
    script = " ".join(cli["command"])
    assert "/usr/local/bin/docker" in script
    assert "/usr/local/libexec/docker/cli-plugins" in script
    assert mounts(cli)["docker-cli"]["mountPath"] == "/cli"


def test_agent_container_wiring() -> None:
    ccw = docker_pod()["containers"][0]
    m = mounts(ccw)
    assert m["docker-sock"]["mountPath"] == "/var/run/docker"
    cli = [x for x in ccw["volumeMounts"] if x["name"] == "docker-cli"]
    assert {(x["mountPath"], x["subPath"]) for x in cli} == {
        ("/usr/local/bin/docker", "docker"),
        ("/usr/local/lib/docker/cli-plugins", "cli-plugins"),
    }
    assert {"name": "DOCKER_HOST", "value": SOCK} in ccw["env"]
    assert "lib-modules" not in m and "docker-data" not in m


def test_image_pull_secret_still_first_key() -> None:
    pod = docker_pod()
    assert pod["imagePullSecrets"] == [{"name": "regcred"}]


def passthrough(session_kw: dict[str, Any]) -> list[str]:
    objs = render.render_session(make_session(**session_kw), settings(), TOKENS)
    (cm,) = (o for o in objs if o["kind"] == "ConfigMap")
    return yaml.safe_load(cm["data"]["config.yaml"])["claude"]["passthrough_env"]


def test_docker_host_in_passthrough_iff_docker_rendered() -> None:
    assert "DOCKER_HOST" in passthrough({"docker": True})
    assert "DOCKER_HOST" not in passthrough({})
    assert "DOCKER_HOST" not in passthrough({"docker": False})


def test_passthrough_keeps_kube_entries_and_stays_sorted() -> None:
    both = passthrough({"docker": True, "kube_access": True})
    assert {"DOCKER_HOST", "KUBERNETES_SERVICE_HOST", "KUBERNETES_SERVICE_PORT"} <= set(both)
    assert both == sorted(both)
    assert "DOCKER_HOST" not in passthrough({"kube_access": True})

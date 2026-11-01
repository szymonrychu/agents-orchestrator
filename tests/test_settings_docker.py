"""The docker (dind sidecar) settings: availability, semver image pin, defaults."""

import pytest

from agents_orchestrator.settings import Settings, SettingsError

DB = "postgresql://u:p@h/db"
IMAGE = "docker:27.3.1-dind"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    import os

    for k in list(os.environ):
        if k.startswith("ORCH_"):
            monkeypatch.delenv(k)
    monkeypatch.setenv("ORCH_DATABASE_URL", DB)


def test_unavailable_when_image_empty():
    s = Settings()
    assert s.docker_image == ""
    assert s.docker_available is False


def test_available_when_image_set():
    assert Settings(docker_image=IMAGE).docker_available is True


def test_image_from_env(monkeypatch):
    monkeypatch.setenv("ORCH_DOCKER_IMAGE", IMAGE)
    assert Settings().docker_image == IMAGE


@pytest.mark.parametrize(
    "image",
    [
        "docker:27.3.1-dind",
        "harbor.szymonrichert.pl/proxy-dockerhub/library/docker:27.3.1-dind",
        "registry:5000/library/docker:27.3.1-dind",
    ],
)
def test_semver_dind_tags_accepted(image):
    assert Settings(docker_image=image).docker_image == image


@pytest.mark.parametrize(
    "image",
    [
        "docker:dind",
        "docker:latest",
        "docker",
        "docker:27.3.1",
        "docker:27.3-dind",
        "docker:27.3.1-dind-rootless",
        "docker:abc1234",
        "docker@sha256:" + "a" * 64,
        "docker:27.3.1-dind@sha256:" + "a" * 64,
    ],
)
def test_non_semver_images_rejected(image):
    with pytest.raises(SettingsError) as exc:
        Settings(docker_image=image)
    assert "ORCH_DOCKER_IMAGE" in exc.value.problems


def test_defaults():
    s = Settings()
    assert s.docker_registry_mirror == ""
    assert s.docker_storage == "30Gi"
    assert s.docker_cpu_request == "500m"
    assert s.docker_cpu_limit == "4"
    assert s.docker_memory_request == "1Gi"
    assert s.docker_memory_limit == "8Gi"
    assert s.docker_ephemeral_request == "5Gi"
    assert s.max_docker_storage == "100Gi"
    assert s.max_docker_cpu == "8"
    assert s.max_docker_memory == "16Gi"


@pytest.mark.parametrize("mirror", ["harbor.example", "https://harbor.example:5000", "http://m/"])
def test_mirror_optional_and_settable(mirror):
    assert Settings(docker_registry_mirror=mirror).docker_registry_mirror == mirror


@pytest.mark.parametrize("mirror", ["harbor.example/proxy-dockerhub", "https://h/p?x=1", "ftp://h"])
def test_mirror_with_path_or_bad_scheme_rejected(mirror):
    # dockerd refuses a --registry-mirror with a path and would crash-loop the sidecar.
    with pytest.raises(SettingsError) as exc:
        Settings(docker_registry_mirror=mirror)
    assert "ORCH_DOCKER_REGISTRY_MIRROR" in exc.value.problems


def test_default_limits_within_caps():
    with pytest.raises(SettingsError) as exc:
        Settings(docker_storage="200Gi")
    assert "ORCH_DOCKER_STORAGE" in exc.value.problems
    with pytest.raises(SettingsError) as exc:
        Settings(docker_cpu_limit="16")
    assert "ORCH_DOCKER_CPU_LIMIT" in exc.value.problems
    with pytest.raises(SettingsError) as exc:
        Settings(docker_memory_limit="32Gi")
    assert "ORCH_DOCKER_MEMORY_LIMIT" in exc.value.problems

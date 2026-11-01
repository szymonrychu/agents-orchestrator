"""The docker spec options: sparse strict flag, orphan size fields, setting gate and caps."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec, SpecInvalid, validate_spec

GATE = "Docker is not configured (ORCH_DOCKER_IMAGE is unset)"
IMAGE = "docker:27.3.1-dind"
SIZES = ("docker_storage", "docker_cpu", "docker_memory")


def settings(**over: object) -> Settings:
    return Settings(database_url="postgresql://u:p@h/db", **over)  # type: ignore[arg-type]


def test_docker_options_are_sparse() -> None:
    for name in ("docker", *SIZES):
        assert getattr(SessionSpec(), name) is None
        assert name not in SessionSpec().model_dump(exclude_none=True)
        assert name not in SessionSpec().model_dump(exclude_unset=True)
    assert SessionSpec(docker=False).model_dump(exclude_none=True)["docker"] is False


@pytest.mark.parametrize("bad", ["yes", "true", 1, 0, 2.0])
def test_docker_is_strict_bool(bad: object) -> None:
    with pytest.raises(ValidationError, match="docker"):
        SessionSpec.model_validate({"docker": bad})


@pytest.mark.parametrize("name", SIZES)
@pytest.mark.parametrize("docker", [None, False])
def test_size_fields_need_docker_true(name: str, docker: bool | None) -> None:
    body: dict[str, object] = {name: "1"}
    if docker is not None:
        body["docker"] = docker
    with pytest.raises(ValidationError, match=f"{name} needs docker: true"):
        SessionSpec.model_validate(body)


def test_size_fields_accepted_with_docker() -> None:
    spec = SessionSpec(docker=True, docker_storage="40Gi", docker_cpu="2", docker_memory="4Gi")
    assert spec.model_dump(exclude_none=True)["docker_storage"] == "40Gi"


@pytest.mark.parametrize("name", SIZES)
@pytest.mark.parametrize("bad", ["lots", "0", "-1Gi", ""])
def test_invalid_quantity_refused(name: str, bad: str) -> None:
    with pytest.raises(ValidationError):
        SessionSpec.model_validate({"docker": True, name: bad})


def test_docker_refused_when_setting_off() -> None:
    with pytest.raises(SpecInvalid) as e:
        validate_spec(SessionSpec(docker=True), settings())
    assert GATE in e.value.errors


def test_docker_accepted_when_setting_on() -> None:
    validate_spec(SessionSpec(docker=True), settings(docker_image=IMAGE))


def test_docker_false_and_unset_pass_when_off() -> None:
    validate_spec(SessionSpec(docker=False), settings())
    validate_spec(SessionSpec(), settings())


def test_unchanged_from_stored_is_exempt() -> None:
    off = settings()
    validate_spec(SessionSpec(docker=True), off, stored=SessionSpec(docker=True))
    for stored in (SessionSpec(), SessionSpec(docker=False)):
        with pytest.raises(SpecInvalid):
            validate_spec(SessionSpec(docker=True), off, stored=stored)


@pytest.mark.parametrize(
    ("field", "over", "cap"),
    [
        ("docker_storage", "101Gi", "100Gi"),
        ("docker_cpu", "9", "8"),
        ("docker_memory", "17Gi", "16Gi"),
    ],
)
def test_sizes_over_cap_refused(field: str, over: str, cap: str) -> None:
    s = settings(docker_image=IMAGE)
    with pytest.raises(SpecInvalid) as e:
        validate_spec(SessionSpec.model_validate({"docker": True, field: over}), s)
    assert f"{field} {over} exceeds the maximum {cap}" in e.value.errors
    validate_spec(SessionSpec.model_validate({"docker": True, field: cap}), s)

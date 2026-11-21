"""spec.volumes: attachment shape rules (path, uniqueness, count) and the preset policy class."""

from __future__ import annotations

import uuid

import pytest

from agents_orchestrator.preset_policy import NON_SECURITY, SESSION_FIXED, classify
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec, SpecInvalid, validate_spec

V1, V2 = uuid.uuid4(), uuid.uuid4()


def errors(*atts: dict) -> list[str]:
    spec = SessionSpec.model_validate({"volumes": list(atts)})
    with pytest.raises(SpecInvalid) as exc:
        validate_spec(spec, Settings(database_url="postgresql://u:p@h/db"))
    return exc.value.errors


def att(volume=V1, **over) -> dict:
    return {"volume": str(volume), "mode": "ro", **over}


@pytest.mark.parametrize(
    ("atts", "text"),
    [
        ([att(), att()], "more than once"),
        ([att(mount_path="data")], "must be absolute"),
        ([att(mount_path="/a/../b")], "'..'"),
        ([att(mount_path="/")], "must not be '/'"),
        ([att(mount_path="/" + "a" * 200)], "longer than 200"),
        ([att(mount_path="/a//b")], "normalised"),
        ([att(mount_path="/a/"), att(V2, mount_path="/a")], "normalised"),
        ([att(mount_path="/a"), att(V2, mount_path="/a")], "used more than once"),
        ([att(uuid.uuid4()) for _ in range(9)], "at most 8"),
    ],
)
def test_volume_shape_refusals(atts, text):
    assert any(text in e for e in errors(*atts))


@pytest.mark.parametrize("path", ["/claude", "/home/agent/.claude/skills", "/volumes/x", None])
def test_any_absolute_path_is_accepted(path):
    spec = SessionSpec.model_validate({"volumes": [att(mount_path=path)]})
    validate_spec(spec, Settings(database_url="postgresql://u:p@h/db"))


def test_volumes_is_session_fixed_and_security():
    assert "volumes" in SESSION_FIXED
    assert classify(["volumes"]) == {"volumes": (True, False)}
    assert "volumes" not in NON_SECURITY

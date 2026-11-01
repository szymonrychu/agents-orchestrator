"""SessionSpec hive vocabulary: max_replicas, static and the derived hive rule."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec, SpecInvalid, validate_spec


def settings(**kw) -> Settings:
    return Settings(database_url="postgresql://unused/db", **kw)


def test_max_replicas_defaults_to_replicas() -> None:
    assert SessionSpec().max_replicas == 1
    assert SessionSpec(replicas=3).max_replicas == 3
    # a stored spec without the key loads as max_replicas == replicas
    assert SessionSpec.model_validate({"replicas": 4}).max_replicas == 4
    assert SessionSpec(replicas=2, max_replicas=5).max_replicas == 5


def test_cap_bounds() -> None:
    with pytest.raises(ValidationError):
        SessionSpec(replicas=3, max_replicas=2)
    with pytest.raises(ValidationError):
        SessionSpec(replicas=1, max_replicas=17)
    with pytest.raises(ValidationError):
        SessionSpec(replicas=1, max_replicas=0)
    with pytest.raises(SpecInvalid, match="max_replicas"):
        validate_spec(SessionSpec(replicas=1, max_replicas=5), settings(max_replicas=4))
    validate_spec(SessionSpec(replicas=1, max_replicas=4), settings(max_replicas=4))


def test_static_requires_fixed_size() -> None:
    with pytest.raises(ValidationError):
        SessionSpec(replicas=1, max_replicas=3, static=True)
    assert SessionSpec(replicas=3, max_replicas=3, static=True).static is True
    assert SessionSpec(replicas=3, static=True).max_replicas == 3


def test_derived_properties() -> None:
    alone = SessionSpec()
    assert (alone.hive, alone.alone, alone.scalable) == (False, True, False)
    grow = SessionSpec(replicas=1, max_replicas=3)
    assert (grow.hive, grow.alone, grow.scalable) == (True, False, True)
    fixed = SessionSpec(replicas=2, static=True)
    assert (fixed.hive, fixed.alone, fixed.scalable) == (True, False, False)
    assert SessionSpec(static=True).scalable is False


@pytest.mark.parametrize(
    "entry",
    [
        {"name": "orchestrator", "plugin": "x:Y"},
        {"name": "other", "plugin": "x:Y", "namespace": "orchestrator"},
    ],
)
def test_orchestrator_plugin_name_reserved(entry) -> None:
    spec = SessionSpec(any_mcp_config={"plugins": [entry]})
    with pytest.raises(SpecInvalid, match="orchestrator"):
        validate_spec(spec, settings())
    ok = SessionSpec(any_mcp_config={"plugins": [{"name": "fine", "plugin": "x:Y"}]})
    validate_spec(ok, settings())


def test_preset_stays_sparse() -> None:
    spec = SessionSpec.model_validate({"replicas": 1})
    assert spec.max_replicas == 1
    assert spec.model_dump(mode="json", exclude_unset=True) == {"replicas": 1}
    both = SessionSpec.model_validate({"max_replicas": 3, "static": False})
    assert both.model_dump(mode="json", exclude_unset=True) == {
        "max_replicas": 3,
        "static": False,
    }


def test_cap_stable_after_backfill() -> None:
    stored = {"replicas": 2, "max_replicas": 2}
    spec = SessionSpec.model_validate(stored)
    edited = SessionSpec.model_validate({**stored, "replicas": 1})
    assert spec.max_replicas == 2
    assert edited.max_replicas == 2
    assert edited.hive is True
    with pytest.raises(ValidationError):
        SessionSpec.model_validate({**stored, "replicas": 3})


@pytest.mark.parametrize("key", ["ORCH_AGENT_TOKEN", "ORCH_AGENT_URL", "ORCH_SESSION_ID", "ORCH_X"])
def test_orch_env_reserved(key: str) -> None:
    with pytest.raises(ValidationError):
        SessionSpec(env={key: {"value": "v"}})

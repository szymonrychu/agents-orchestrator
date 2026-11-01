"""WrapperSettings.permission_mode: the two accepted values, sparse dump, strict validation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from agents_orchestrator.spec import SessionSpec, WrapperSettings


@pytest.mark.parametrize("mode", ["bypassPermissions", "auto"])
def test_accepts_the_two_modes(mode):
    assert WrapperSettings(permission_mode=mode).permission_mode == mode
    assert SessionSpec(wrapper={"permission_mode": mode}).wrapper.permission_mode == mode


def test_none_is_default_and_sparse():
    assert WrapperSettings().permission_mode is None
    assert WrapperSettings(permission_mode=None).permission_mode is None
    assert WrapperSettings().model_dump(mode="json", exclude_none=True) == {}
    assert "permission_mode" not in SessionSpec().model_dump(mode="json", exclude_none=True).get(
        "wrapper", {}
    )


@pytest.mark.parametrize(
    "bad", ["default", "plan", "dontAsk", "acceptEdits", "", "AUTO", 1, True, []]
)
def test_rejects_other_values(bad):
    with pytest.raises(ValidationError):
        WrapperSettings(permission_mode=bad)


def test_existing_specs_unchanged():
    spec = SessionSpec(wrapper={"turn_max_seconds": 5})
    assert spec.wrapper.permission_mode is None
    assert spec.wrapper.model_dump(mode="json", exclude_none=True) == {"turn_max_seconds": 5.0}

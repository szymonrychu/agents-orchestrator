"""spec.inactivity: the per-preset reminder and close limits, and the removed idle field."""

from __future__ import annotations

import pytest
import pytest_asyncio
from pydantic import ValidationError
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import InactivityLimits, SessionSpec, SpecInvalid, validate_spec

P = "/api/v1/presets"


def _settings(**kw) -> Settings:
    kw.setdefault("database_url", "postgresql://x/y")
    kw.setdefault("agent_image", "img:1")
    return Settings(**kw)


def test_inactivity_sparse_round_trip():
    spec = SessionSpec.model_validate({"inactivity": {"reminder_seconds": 600}})
    assert spec.model_dump(mode="json", exclude_unset=True) == {
        "inactivity": {"reminder_seconds": 600}
    }
    assert "inactivity" not in SessionSpec().model_dump(exclude_unset=True)
    assert SessionSpec().inactivity is None
    assert InactivityLimits() == InactivityLimits(reminder_seconds=None, close_seconds=None)


@pytest.mark.parametrize("field", ["reminder_seconds", "close_seconds"])
@pytest.mark.parametrize("value", [0, 60, 2592000])
def test_inactivity_value_accepted(field, value):
    assert getattr(InactivityLimits(**{field: value}), field) == value


@pytest.mark.parametrize("field", ["reminder_seconds", "close_seconds"])
@pytest.mark.parametrize("value", [1, 59, 2592001, -1])
def test_inactivity_value_refused(field, value):
    with pytest.raises(ValidationError):
        InactivityLimits(**{field: value})


def test_inactivity_extra_key_refused():
    with pytest.raises(ValidationError):
        InactivityLimits.model_validate({"reminder_seconds": 60, "nope": 1})


def test_discard_after_idle_seconds_is_refused():
    with pytest.raises(ValidationError):
        SessionSpec.model_validate({"discard_after_idle_seconds": 600})


def _validate(settings: Settings, **inactivity) -> None:
    validate_spec(SessionSpec.model_validate({"inactivity": inactivity}), settings)


def test_validate_spec_effective_order():
    s = _settings(inactivity_reminder_seconds=3600, inactivity_close_seconds=14400)
    with pytest.raises(SpecInvalid) as e:
        _validate(s, reminder_seconds=14400)
    assert any("inactivity.reminder_seconds" in m for m in e.value.errors)
    with pytest.raises(SpecInvalid) as e:
        _validate(s, close_seconds=600)  # effective reminder 3600 >= 600
    assert any("inactivity.reminder_seconds 3600" in m for m in e.value.errors)
    _validate(s, reminder_seconds=0, close_seconds=600)
    _validate(s, reminder_seconds=600)
    _validate(_settings(), reminder_seconds=600)
    _validate(_settings(), close_seconds=600)
    _validate(s, reminder_seconds=600, close_seconds=0)


@pytest_asyncio.fixture
async def http(pool, dsn):
    app = create_app(make_settings(dsn, max_replicas=4), pool)
    async with client_for(app) as c:
        yield c


async def test_preset_with_bad_inactivity_is_422(http):
    r = await http.post(P, json={"name": "bad", "spec": {"inactivity": {"close_seconds": 30}}})
    assert_error(r, 422, "invalid_request")


async def test_preset_with_inactivity_is_stored_sparse(http):
    spec = {"inactivity": {"close_seconds": 600}}
    r = await http.post(P, json={"name": "ok", "spec": spec})
    assert r.status_code == 201, r.text
    assert r.json()["spec"] == spec

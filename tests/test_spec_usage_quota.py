import pytest
from pydantic import ValidationError

from agents_orchestrator.spec import SessionSpec, UsageQuota


def test_valid():
    spec = SessionSpec(usage_quota={"seven_day_pct": 80})
    assert spec.usage_quota == UsageQuota(five_hour_pct=None, seven_day_pct=80)
    assert spec.usage_quota.seven_day_pct == 80
    assert spec.usage_quota.five_hour_pct is None


@pytest.mark.parametrize("field", ["five_hour_pct", "seven_day_pct"])
@pytest.mark.parametrize("value", [0, 101, -1])
def test_bounds(field, value):
    with pytest.raises(ValidationError):
        SessionSpec(usage_quota={field: value})


@pytest.mark.parametrize("field", ["five_hour_pct", "seven_day_pct"])
@pytest.mark.parametrize("value", [1, 100])
def test_bounds_inclusive(field, value):
    assert getattr(SessionSpec(usage_quota={field: value}).usage_quota, field) == value


def test_extra_forbidden():
    with pytest.raises(ValidationError):
        SessionSpec(usage_quota={"monthly_pct": 1})


def test_sparse():
    assert "usage_quota" not in SessionSpec().model_dump(exclude_unset=True)
    assert SessionSpec().usage_quota is None

from datetime import UTC, datetime, timedelta, timezone

import pytest

from agents_orchestrator.credential import notice_key, notice_text, phase

E = datetime(2027, 10, 23, tzinfo=UTC)


def test_phase_unknown_without_date():
    assert phase(E, None, 10, 24) == "unknown"


@pytest.mark.parametrize(
    ("now", "want"),
    [
        (E - timedelta(days=10, seconds=1), "ok"),
        (E - timedelta(days=10), "warn"),
        (E - timedelta(hours=24, seconds=1), "warn"),
        (E - timedelta(hours=24), "refuse"),
        (E - timedelta(seconds=1), "refuse"),
        (E, "expired"),
        (E + timedelta(days=1), "expired"),
    ],
)
def test_phase_boundaries(now, want):
    assert phase(now, E, 10, 24) == want


def test_notice_key():
    assert notice_key("warn", E) == "credential-warn-2027-10-23"
    assert notice_key("auth_failed", None) == "credential-auth_failed-unset"
    local = datetime(2027, 10, 23, 1, 0, tzinfo=timezone(timedelta(hours=2)))
    assert notice_key("expired", local) == "credential-expired-2027-10-22"


def test_notice_text():
    title, body = notice_text("warn", E, 10, 24)
    assert title == "Claude credential ends 2027-10-23: rotate it"
    assert "2027-10-23T00:00:00+00:00" in body
    assert "claude setup-token" in body
    assert "claude.credentialExpiresAt" in body
    assert notice_text("refuse", E, 10, 24)[0] == (
        "Claude credential ends 2027-10-23: new sessions refused"
    )
    assert notice_text("expired", E, 10, 24)[0] == (
        "Claude credential expired 2027-10-23: dispatch held"
    )
    assert notice_text("auth_failed", None, 10, 24)[0] == "Claude login failed on an agent"
    assert "no end date configured" in notice_text("auth_failed", None, 10, 24)[1]

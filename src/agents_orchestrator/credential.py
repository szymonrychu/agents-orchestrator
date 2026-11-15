"""Claude credential expiry phases and notice texts (pure, stdlib only; no credential value)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Literal

Phase = Literal["unknown", "ok", "warn", "refuse", "expired"]
Threshold = Literal["warn", "refuse", "expired", "auth_failed"]

_STEPS = (
    "1. Run `claude setup-token`.\n"
    "2. In agents-helmfile replace `claude.token` in"
    " `values/agents-orchestrator/default.secrets.yaml` with the sops helper.\n"
    "3. Set `claude.credentialExpiresAt` in `values/agents-orchestrator/common.yaml` to the"
    " mint date + 365 days.\n"
    "4. Apply agents-orchestrator and agents-usage.\n"
    "5. Restart the sessions so agent pods read the new Secret.\n"
)


def phase(now: datetime, expires_at: datetime | None, warn_days: int, refuse_hours: int) -> Phase:
    if expires_at is None:
        return "unknown"
    if now >= expires_at:
        return "expired"
    if now >= expires_at - timedelta(hours=refuse_hours):
        return "refuse"
    if now >= expires_at - timedelta(days=warn_days):
        return "warn"
    return "ok"


def _date(expires_at: datetime | None) -> str:
    return "unset" if expires_at is None else expires_at.astimezone(UTC).strftime("%Y-%m-%d")


def notice_key(threshold: Threshold, expires_at: datetime | None) -> str:
    return f"credential-{threshold}-{_date(expires_at)}"


def notice_text(
    threshold: Threshold, expires_at: datetime | None, warn_days: int, refuse_hours: int
) -> tuple[str, str]:
    day = _date(expires_at)
    titles = {
        "warn": f"Claude credential ends {day}: rotate it",
        "refuse": f"Claude credential ends {day}: new sessions refused",
        "expired": f"Claude credential expired {day}: dispatch held",
        "auth_failed": "Claude login failed on an agent",
    }
    end = "no end date configured" if expires_at is None else expires_at.astimezone(UTC).isoformat()
    body = (
        f"Threshold: {threshold}.\n"
        f"Credential end instant: {end}.\n"
        f"Margins: warn {warn_days} days before the end, refuse new sessions and scale-ups"
        f" {refuse_hours} hours before the end, hold dispatch at and after the end.\n\n"
        f"Rotation steps:\n{_STEPS}"
    )
    return titles[threshold], body

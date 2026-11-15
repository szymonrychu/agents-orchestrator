"""The credential gate: refuse a start or scale-up from `expires_at - refuse_hours` on.

Pure and clock-injected; carries the end instant and the phase only, never a credential value.
"""

from __future__ import annotations

from datetime import datetime

from agents_orchestrator import credential
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.settings import Settings


def refusal(expires_at: datetime, ph: credential.Phase) -> ApiError:
    return ApiError(
        409,
        "credential_expiring",
        f"the Claude credential ends at {expires_at.isoformat()}: new sessions and scale-ups"
        " are refused until it is rotated",
        extra={"credential": {"expires_at": expires_at.isoformat(), "phase": ph}},
    )


def check(settings: Settings, now: datetime) -> None:
    expires_at = settings.claude_credential_expires_at
    ph = credential.phase(
        now, expires_at, settings.credential_warn_days, settings.credential_refuse_hours
    )
    if expires_at is not None and ph in ("refuse", "expired"):
        raise refusal(expires_at, ph)

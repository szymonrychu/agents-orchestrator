"""How far a session's start got: booting, ready or bootstrap_failed (T-79)."""

from __future__ import annotations

from typing import Any

START_REASON_MAX = 200


def start_state(ready: int, failures: list[dict[str, Any]]) -> str:
    """booting | ready | bootstrap_failed: a recorded start failure wins, else pod readiness."""
    if failures:
        return "bootstrap_failed"
    return "ready" if ready > 0 else "booting"


def start_reason(
    state: str, failures: list[dict[str, Any]], status_message: str = ""
) -> str | None:
    """One line saying why the session is not ready yet; None when it is."""
    if state == "bootstrap_failed":
        first = failures[0]
        line = (first.get("message") or "").strip().splitlines()
        return f"{first.get('step') or 'bootstrap'}: {line[0] if line else 'failed'}"[
            :START_REASON_MAX
        ]
    if state == "booting":
        return (status_message.strip() or "waiting for agent pods to become ready")[
            :START_REASON_MAX
        ]
    return None

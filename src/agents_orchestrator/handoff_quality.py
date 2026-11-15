"""The quality check of a submitted handoff: reason codes, empty list when it is acceptable.

Codes: `missing:<field>`, `too_short`, `not_text` and `unreferenced:<item id>` (a running
background task or subagent, or pod-local scratch, that no `in_flight` entry names and the agent
did not abandon). `check_text` is the length and text-ratio part alone (the shutdown path).
"""

from __future__ import annotations

from typing import Any

from agents_orchestrator import handoff_gate
from agents_orchestrator.wrapper.models import Inflight

REQUIRED = {
    "work": ("doing", "done", "next"),
    "chat": ("doing", "open_requests", "results", "next"),
}
TEXT_FIELDS = ("doing", "done", "in_flight", "next", "open_requests", "results", "gotchas")


def _entries(value: Any) -> list[str]:
    items = [value] if isinstance(value, str) else list(value or [])
    return [i for i in items if str(i).strip()]


def check_text(text: str, settings: Any) -> list[str]:
    """`too_short` and `not_text` of one free text."""
    reasons: list[str] = []
    if len(text) < settings.handoff_min_chars:
        reasons.append("too_short")
    if text:
        textual = sum(1 for c in text if c.isalnum() or c.isspace())
        if textual / len(text) < settings.handoff_min_text_ratio:
            reasons.append("not_text")
    return reasons


def check(
    body: Any, profile: str, inflight: Inflight | None, abandon: list[Any], settings: Any
) -> list[str]:
    reasons = [f"missing:{f}" for f in REQUIRED[profile] if not _entries(getattr(body, f))]
    text = "\n".join(s for f in TEXT_FIELDS for s in _entries(getattr(body, f)))
    reasons.extend(check_text(text, settings))
    in_flight = "\n".join(_entries(body.in_flight)).lower()
    with_results = "\n".join([in_flight, *_entries(body.results)]).lower()
    for item in handoff_gate.items(inflight):
        if item.kind not in ("task", "subagent", "scratch"):
            continue
        if handoff_gate.abandon_for(item, abandon) is not None:
            continue
        names = (item.id, *item.keys, *([item.path] if item.path else []))
        if item.kind == "subagent":
            names = ("subagent",)
        mentioned = with_results if item.kind == "scratch" else in_flight
        if not any(n.lower() in mentioned for n in names if n):
            reasons.append(f"unreferenced:{item.id}")
    return reasons

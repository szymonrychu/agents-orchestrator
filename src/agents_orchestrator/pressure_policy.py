"""The context pressure policy pushed to a wrapper (`PUT /v1/sessions/{id}/pressure`).

`build` makes the body of the wrapper's `PressurePolicy`: thresholds in tokens (percentages
only for a gauge without a window), per-level texts, the handoff state of the open row and the
resume and compaction texts. `policy_hash` identifies a policy apart from its version, so the
dispatcher bumps the version only when the content changed.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Literal

from agents_orchestrator import handoff_texts
from agents_orchestrator.db.handoffs import Handoff
from agents_orchestrator.dispatcher.hive import Levels, critical_tokens, l1_tokens, strong_tokens
from agents_orchestrator.wrapper.models import blocking_items

# The window the token thresholds are computed for when the gauge has none yet: the percent
# thresholds decide for such a gauge, and the policy is re-pushed once the window is known.
DEFAULT_WINDOW_TOKENS = 200_000
MAX_WAITING_ON = 20
MAX_WAITING_ON_CHARS = 200

HandoffState = Literal["none", "submitted", "blocked", "accepted"]


@dataclass(frozen=True)
class PolicyExtras:
    """Pieces filled by later steps (resume, compaction steering, resubmit)."""

    resume_pending: bool = False
    resume_text: str = ""
    compact_instructions: str = ""
    reinject_text: str = ""
    resubmit: bool = False


def waiting_on(row: Handoff | None) -> list[str]:
    if row is None:
        return []
    return [i[:MAX_WAITING_ON_CHARS] for i in blocking_items(row.inflight)][:MAX_WAITING_ON]


def handoff_state(row: Handoff | None) -> HandoffState:
    """none while pressuring, blocked when a submit waits on in-flight work, else accepted."""
    if row is None or row.phase == "pressuring":
        return "none"
    if row.phase == "submitted":
        return "blocked" if waiting_on(row) else "accepted"
    return "accepted"


def build(
    lv: Levels,
    window_tokens: int | None,
    l1_on: bool,
    profile: Literal["work", "chat"],
    row: Handoff | None,
    *,
    hive: bool,
    version: int,
    extras: PolicyExtras,
) -> dict[str, Any]:
    window = window_tokens if window_tokens and window_tokens > 0 else DEFAULT_WINDOW_TOKENS
    l2 = strong_tokens(lv, window)
    l3 = critical_tokens(lv, window)
    thresholds: dict[str, Any] = {
        "l1_tokens": l1_tokens(lv, window, l2) if l1_on else None,
        "l2_tokens": l2,
        "l3_tokens": l3,
        "compact_tokens": int(lv.compact_pct / 100 * window),
        "l1_pct": float(lv.fallback_pct) if l1_on else None,
        "l2_pct": float(lv.strong_pct),
        "l3_pct": float(lv.compact_pct - lv.critical_margin_pct),
        "compact_pct": float(lv.compact_pct),
    }
    state = handoff_state(row)
    texts = handoff_texts.level_texts(profile, hive)
    waiting = waiting_on(row)
    if state == "blocked":
        extra = " " + handoff_texts.waiting_on_text()
        texts = {k: {f: t + extra for f, t in v.items()} for k, v in texts.items()}
    elif extras.resubmit:
        extra = " " + handoff_texts.resubmit_text()
        texts = {k: {f: t + extra for f, t in v.items()} for k, v in texts.items()}
    return {
        "version": version,
        "thresholds": thresholds,
        "profile": profile,
        "texts": texts,
        "handoff_state": state,
        "waiting_on": waiting,
        "resume_pending": extras.resume_pending,
        "resume_text": extras.resume_text,
        "compact_instructions": extras.compact_instructions,
        "reinject_text": extras.reinject_text,
    }


def policy_hash(body: dict[str, Any]) -> str:
    """sha256 of the canonical JSON of the policy without its version."""
    content = {k: v for k, v in body.items() if k != "version"}
    raw = json.dumps(content, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(raw.encode()).hexdigest()

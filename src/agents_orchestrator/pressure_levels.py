"""Pure context-pressure level math (L1, L2, L3) shared by the push loop, the pressure policy,
the leadership pass and the API views.

A leaf module (settings and stdlib only): `dispatcher/hive.py` imports `db/leadership.py`, so the
leadership pass could not import these from `hive` without a cycle.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from agents_orchestrator.settings import Settings

Level = Literal["none", "l1", "l2", "l3"]

# Context % margin below the compaction point used as the percent fallback of the soft handover
# rule (an agent whose token gauge is unknown).
HANDOVER_MARGIN_PCT = 15


def compact_pct(auto_compact_pct: int | None, settings: Settings) -> int:
    """The effective compaction point: the spec's `auto_compact_pct`, else the pods' default."""
    return settings.default_auto_compact_pct if auto_compact_pct is None else auto_compact_pct


@dataclass(frozen=True)
class Levels:
    """The handover levels of one leader or solo agent."""

    strong_pct: float  # compact - handover_strong_margin_pct (the percent fallback)
    strong_headroom_tokens: int
    compact_pct: int
    fallback_pct: int  # weak, for an unknown token gauge: max(compact - HANDOVER_MARGIN_PCT, 1)
    start_tokens: int | None
    budget_tokens: int
    critical_margin_pct: float = 5  # L3: compact - critical_margin_pct (the percent fallback)
    critical_headroom_tokens: int = 25000


def levels(spec: dict[str, Any], start_tokens: int | None, settings: Settings) -> Levels:
    """The levels of an agent whose context was `start_tokens` when its tenure began."""
    compact = compact_pct(spec.get("auto_compact_pct"), settings)
    return Levels(
        strong_pct=compact - settings.handover_strong_margin_pct,
        strong_headroom_tokens=settings.handover_strong_headroom_tokens,
        compact_pct=compact,
        fallback_pct=max(compact - HANDOVER_MARGIN_PCT, 1),
        start_tokens=start_tokens,
        budget_tokens=settings.handover_budget_tokens,
        critical_margin_pct=settings.handover_critical_margin_pct,
        critical_headroom_tokens=settings.handover_critical_headroom_tokens,
    )


def _floored_below_compaction(
    pct: float, compact_pct: int, headroom_tokens: int, window_tokens: int
) -> float:
    """The smaller of the percent point and the token headroom below compaction; the headroom
    term never drops below half the percent point, so a low compaction point on a small window
    does not collapse the level to 0 (and the result stays continuous in the compaction point)."""
    point = pct / 100 * window_tokens
    return min(point, max(compact_pct / 100 * window_tokens - headroom_tokens, point / 2))


def strong_tokens(lv: Levels, window_tokens: int) -> int:
    """The context size from which level 2 (strong) applies: the smaller of the percent margin
    below compaction and the headroom in tokens below it, never below half the percent point;
    0 only for a non-positive percent."""
    if lv.strong_pct <= 0:
        return 0
    return int(
        _floored_below_compaction(
            lv.strong_pct, lv.compact_pct, lv.strong_headroom_tokens, window_tokens
        )
    )


def critical_tokens(lv: Levels, window_tokens: int) -> int:
    """The context size from which level 3 (critical) applies: the smaller of the percent margin
    and the token headroom below compaction (same floor), never below the level 2 point plus the
    gap between the two margins."""
    strong = strong_tokens(lv, window_tokens)
    gap = max(0, lv.compact_pct - lv.critical_margin_pct - lv.strong_pct) / 100 * window_tokens
    crit = _floored_below_compaction(
        lv.compact_pct - lv.critical_margin_pct,
        lv.compact_pct,
        lv.critical_headroom_tokens,
        window_tokens,
    )
    return max(int(strong + gap), int(crit))


def l1_tokens(lv: Levels, window_tokens: int, l2_tokens: int) -> int:
    """The context size from which L1 applies: the start-plus-budget point, or the percent
    fallback of the window when the tenure start is unknown, never above the L2 point."""
    if lv.start_tokens is not None:
        return min(lv.start_tokens + lv.budget_tokens, l2_tokens)
    return min(int(lv.fallback_pct / 100 * window_tokens), l2_tokens)


def due_level(
    lv: Levels,
    used_tokens: int | None,
    window_tokens: int | None,
    pct: float | None,
    tenure_seconds: float,
    min_tenure_seconds: int,
    rollout_pending: bool,
) -> Level:
    """The pressure level of an agent. `l3` at the critical point and `l2` at the strong point,
    whatever else holds; `l1` past its token budget (or the percent fallback) once the minimum
    tenure is over and its pod does not await a rollout; else `none`. Tokens decide when the
    window is known; the percentages only when it is not."""
    window = window_tokens if window_tokens is not None and window_tokens > 0 else None
    tokens = used_tokens is not None and window is not None
    cap = strong_tokens(lv, window) if window is not None else None
    crit = critical_tokens(lv, window) if window is not None else None
    by_pct = not tokens and pct is not None
    if by_pct and pct >= lv.compact_pct - lv.critical_margin_pct:
        return "l3"
    if tokens and used_tokens is not None and crit is not None and used_tokens >= crit:
        return "l3"
    if by_pct and pct >= lv.strong_pct:
        return "l2"
    if tokens and used_tokens is not None and cap is not None and used_tokens >= cap:
        return "l2"
    if tokens and used_tokens is not None and window is not None and cap is not None:
        hit = used_tokens >= l1_tokens(lv, window, cap)
    else:
        hit = by_pct and pct >= lv.fallback_pct
    if hit and tenure_seconds >= min_tenure_seconds and not rollout_pending:
        return "l1"
    return "none"

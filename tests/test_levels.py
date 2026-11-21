"""Handover levels L1-L3 (dispatcher/hive.py): tokens decide, percentages are the fallback."""

import math

import pytest

from agents_orchestrator import pressure_levels as pl
from agents_orchestrator.settings import Settings


def _lv(start=None, **over):
    s = Settings(database_url="postgresql://u:p@db/o", **over)
    return pl.levels({}, start, s)


def test_levels_200k_window():
    lv = _lv()
    assert pl.strong_tokens(lv, 200_000) == 130_000
    assert pl.critical_tokens(lv, 200_000) == 165_000
    assert int(lv.compact_pct / 100 * 200_000) == 190_000


def test_levels_1m_window():
    lv = _lv()
    assert pl.strong_tokens(lv, 1_000_000) == 850_000
    assert pl.critical_tokens(lv, 1_000_000) == 900_000


def test_l3_never_below_l2_small_window():
    lv = _lv()
    for window in (10_000, 50_000, 80_000, 100_000):
        assert pl.critical_tokens(lv, window) >= pl.strong_tokens(lv, window)
    big_headroom = _lv(handover_critical_headroom_tokens=500_000)
    gap = 200_000 * 5 // 100  # strong margin 10 minus critical margin 5
    assert pl.critical_tokens(big_headroom, 200_000) == (
        pl.strong_tokens(big_headroom, 200_000) + gap
    )


@pytest.mark.parametrize(
    ("compact", "window", "l2", "l3"),
    [
        (25, 200_000, 15_000, 25_000),
        (95, 200_000, 130_000, 165_000),
        (95, 1_000_000, 850_000, 900_000),
        (25, 1_000_000, 150_000, 200_000),
        (50, 200_000, 40_000, 75_000),
    ],
)
def test_levels_floor(compact, window, l2, l3):
    lv = pl.levels(
        {"auto_compact_pct": compact}, None, Settings(database_url="postgresql://u:p@db/o")
    )
    assert (
        pl.strong_tokens(lv, window),
        pl.critical_tokens(lv, window),
    ) == (l2, l3)


def _due(lv, used, window=200_000, pct=None, tenure=math.inf, min_tenure=0, rollout=False):
    return pl.due_level(lv, used, window, pct, tenure, min_tenure, rollout)


def test_due_level_l1_needs_tenure_and_no_rollout():
    lv = _lv(start=50_000, handover_budget_tokens=10_000)  # L1 from 60k tokens
    used = 60_000
    assert _due(lv, used - 1, tenure=5000, min_tenure=1200) == "none"
    assert _due(lv, used, tenure=5000, min_tenure=1200) == "l1"
    assert _due(lv, used, tenure=10, min_tenure=1200) == "none"
    assert _due(lv, used, tenure=5000, min_tenure=1200, rollout=True) == "none"


def test_due_level_levels_by_tokens_whatever_tenure_or_rollout():
    lv = _lv(start=10_000)
    assert _due(lv, 130_000, tenure=0, min_tenure=9999, rollout=True) == "l2"
    assert _due(lv, 164_999, tenure=0, min_tenure=9999, rollout=True) == "l2"
    assert _due(lv, 165_000, tenure=0, min_tenure=9999, rollout=True) == "l3"


def test_due_level_pct_fallback():
    lv = _lv()
    assert _due(lv, None, window=None, pct=79.0) == "none"
    assert _due(lv, None, window=None, pct=80.0) == "l1"
    assert _due(lv, None, window=None, pct=85.0) == "l2"
    assert _due(lv, None, window=None, pct=89.9) == "l2"
    assert _due(lv, None, window=None, pct=90.0) == "l3"
    assert _due(lv, None, window=None, pct=None) == "none"


def test_due_level_pct_ignored_when_tokens_and_window_known():
    lv = _lv()
    assert _due(lv, 10_000, window=200_000, pct=95.0) == "none"
    assert _due(lv, 10_000, window=200_000, pct=87.0) == "none"


def test_due_level_l1_pct_threshold_when_start_unknown():
    lv = _lv()  # fallback 80% of 1M = 800k, below L2 at 850k
    assert pl.l1_tokens(lv, 1_000_000, pl.strong_tokens(lv, 1_000_000)) == 800_000
    assert _due(lv, 800_000, window=1_000_000, pct=10.0) == "l1"
    assert _due(lv, 799_999, window=1_000_000, pct=99.0) == "none"


@pytest.mark.parametrize(
    ("compact", "window", "used", "room"),
    [
        (25, 200_000, 5_000, True),  # L2 too small for the margin: half of L2 caps the margin
        (25, 200_000, None, True),
        (95, 1_000_000, 10_000, True),
        (95, 1_000_000, 970_000, False),
    ],
)
def test_successor_has_room_small_l2(compact, window, used, room):
    lv = pl.levels(
        {"auto_compact_pct": compact}, None, Settings(database_url="postgresql://u:p@db/o")
    )
    l2 = pl.strong_tokens(lv, window)
    assert pl.successor_has_room(lv, used, window, None, 30_000) is room
    if compact == 25:
        assert pl.successor_has_room(lv, l2 // 2, window, None, 30_000)
        assert not pl.successor_has_room(lv, l2, window, None, 30_000)
    else:
        assert pl.successor_has_room(lv, l2 - 30_000, window, None, 30_000)
        assert not pl.successor_has_room(lv, l2 - 29_999, window, None, 30_000)

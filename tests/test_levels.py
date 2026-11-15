"""Handover levels L1-L3 (dispatcher/hive.py): tokens decide, percentages are the fallback."""

import math

from agents_orchestrator.dispatcher import hive
from agents_orchestrator.settings import Settings


def _lv(start=None, **over):
    s = Settings(database_url="postgresql://u:p@db/o", **over)
    return hive.levels({}, start, s)


def test_levels_200k_window():
    lv = _lv()
    assert hive.strong_tokens(lv, 200_000) == 130_000
    assert hive.critical_tokens(lv, 200_000) == 165_000
    assert int(lv.compact_pct / 100 * 200_000) == 190_000


def test_levels_1m_window():
    lv = _lv()
    assert hive.strong_tokens(lv, 1_000_000) == 850_000
    assert hive.critical_tokens(lv, 1_000_000) == 900_000


def test_l3_never_below_l2_small_window():
    lv = _lv()
    for window in (10_000, 50_000, 80_000, 100_000):
        assert hive.critical_tokens(lv, window) >= hive.strong_tokens(lv, window)
    big_headroom = _lv(handover_critical_headroom_tokens=500_000)
    assert hive.critical_tokens(big_headroom, 200_000) == hive.strong_tokens(big_headroom, 200_000)


def _due(lv, used, window=200_000, pct=None, tenure=math.inf, min_tenure=0, rollout=False):
    return hive.due_level(lv, used, window, pct, tenure, min_tenure, rollout)


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

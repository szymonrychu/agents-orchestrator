"""The save rule with a restore-only cache (empty save_key)."""

from __future__ import annotations

import pytest

from agents_orchestrator.cache.save_rule import RESTORE_ONLY, save_skip

RESTORED = {"cache": {"restore": {"state": "restored"}}, "replicas": 1}


def test_restore_only_constant():
    assert RESTORE_ONLY == "restore-only"


def test_restore_only_skips_the_workdir_save():
    assert save_skip(True, RESTORED, "workdir", restore_only=True) == RESTORE_ONLY
    assert save_skip(True, RESTORED, "workdir") is None


def test_restore_only_leaves_the_transcripts_save_alone():
    status = {"cache": {"restore": {"state": "restored"}, "sessions_restore": {"state": "miss"}}}
    assert save_skip(True, status, "sessions", restore_only=True) is None


def test_not_configured_wins_over_restore_only():
    assert save_skip(False, RESTORED, "workdir", restore_only=True) == "not_configured"


@pytest.mark.parametrize("state", ["restored", "miss", "reset"])
def test_fresh_start_and_miss_still_save(state):
    status = {"cache": {"restore": {"state": state, "reason": "fresh_start"}}, "replicas": 1}
    assert save_skip(True, status, "workdir") is None


@pytest.mark.parametrize("state", ["running", "failed"])
def test_unfinished_or_failed_init_skips_the_workdir_save(state):
    status = {**RESTORED, "init": {"state": state}}
    assert save_skip(True, status, "workdir") == f"init_{state}"


@pytest.mark.parametrize("init", [None, {"state": "skipped"}, {"state": "succeeded"}])
def test_no_init_or_successful_init_still_saves_the_workdir(init):
    status = {**RESTORED, **({"init": init} if init else {})}
    assert save_skip(True, status, "workdir") is None


def test_failed_init_does_not_affect_the_transcripts_save():
    status = {
        "cache": {"restore": {"state": "restored"}, "sessions_restore": {"state": "miss"}},
        "init": {"state": "failed"},
    }
    assert save_skip(True, status, "sessions") is None

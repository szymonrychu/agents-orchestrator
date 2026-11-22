"""The transcripts save rule (deploy-safe-agent-sessions D7)."""

from __future__ import annotations

import pytest

from agents_orchestrator.cache.save_rule import transcripts_skip


@pytest.mark.parametrize(
    ("restore", "converged", "skip"),
    [
        (None, False, "never_converged"),
        (None, True, None),
        ({"state": "skipped", "reason": "pods_exist"}, False, None),
        ({"state": "running"}, False, "restore_running"),
        ({"state": "miss"}, False, None),
        ({"state": "restored"}, False, None),
        ({"state": "unreadable"}, False, None),
        ({"state": "failed", "reason": "HTTP 403"}, False, "HTTP"),
        ({"state": "failed", "reason": None}, False, "restore_failed"),
        ({"state": "saved"}, False, "restore_saved"),
    ],
)
def test_transcripts_rule(restore, converged, skip):
    status: dict = {"backup": {"transcripts": {} if restore is None else {"restore": restore}}}
    if converged:
        status["replicas"] = 1
    assert transcripts_skip(True, status) == skip


def test_transcripts_not_configured_wins():
    assert transcripts_skip(False, {"backup": {"transcripts": {"restore": {"state": "miss"}}}}) == (
        "not_configured"
    )

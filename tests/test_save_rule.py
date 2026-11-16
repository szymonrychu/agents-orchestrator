"""The per-entry save plan and the transcripts save rule (S-68; deploy-safe-agent-sessions D7)."""

from __future__ import annotations

import pytest
from test_cache_jobs_render import make_session

from agents_orchestrator.cache.save_rule import (
    ORPHAN,
    RESTORE_ONLY,
    WORKSPACE_INDEX,
    EntryPlan,
    save_plan,
    transcripts_skip,
)

SKILLS = {"root": "claude", "path": "skills", "mode": "both", "name": "skills/{{ project }}"}
SRC = {"root": "workspace", "path": "src", "mode": "backup", "name": "src/x"}
SEED = {"root": "workspace", "path": "seed", "mode": "restore", "name": "seed"}


def session(entries, status=None):
    s = make_session(backup=entries)
    s.status.update(status or {})
    return s


def restored(*states):
    return {
        "replicas": 1,
        "backup": {"entries": [{"restore": {"state": st} if st else None} for st in states]},
    }


def test_save_plan_per_entry():
    plan = save_plan(session([SKILLS, SRC, SEED], restored("restored", None, "restored")))
    assert plan == [
        EntryPlan(0, "skills/{{ project }}", "save", None),
        EntryPlan(1, "src/x", "save", None),
        EntryPlan(2, "seed", "discard", RESTORE_ONLY),
    ]


def test_save_plan_lists_the_workspace_when_no_entry_saves_it():
    plan = save_plan(session([SKILLS], restored("restored")))
    assert plan[-1] == EntryPlan(WORKSPACE_INDEX, "", "discard", "workspace_not_listed")
    assert save_plan(session([SEED], restored("restored")))[-1].reason == "workspace_not_listed"
    assert save_plan(session(None)) == [
        EntryPlan(WORKSPACE_INDEX, "", "discard", "workspace_not_listed")
    ]


def test_save_plan_skips_an_entry_whose_restore_did_not_finish():
    plan = save_plan(session([SKILLS], restored("running")))
    assert (plan[0].action, plan[0].reason) == ("skip", "restore_pending")
    plan = save_plan(session([SKILLS], restored(None)))
    assert (plan[0].action, plan[0].reason) == ("skip", "restore_pending")


def test_save_plan_orphan_after_a_failed_restore():
    plan = save_plan(session([SKILLS], restored("failed")))
    assert (plan[0].action, plan[0].reason) == ("save", ORPHAN)
    plain = save_plan(session([SKILLS], restored("missing")))
    assert (plain[0].action, plain[0].reason) == ("save", None)


@pytest.mark.parametrize("init", ["running", "failed"])
def test_unfinished_or_failed_init_skips_workspace_entries_only(init):
    status = {**restored("restored", "restored"), "init": {"state": init}}
    plan = save_plan(session([SKILLS, SRC], status))
    assert [(p.action, p.reason) for p in plan[:2]] == [("save", None), ("skip", f"init_{init}")]


def test_not_configured_and_never_converged_skip_every_save():
    plan = save_plan(session([SKILLS, SRC], restored("restored", "restored")), False)
    assert {(p.action, p.reason) for p in plan if p.index >= 0} == {
        ("skip", "cache_not_configured")
    }
    plan = save_plan(
        session([SKILLS], {"backup": {"entries": [{"restore": {"state": "restored"}}]}})
    )
    assert (plan[0].action, plan[0].reason) == ("skip", "never_converged")


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

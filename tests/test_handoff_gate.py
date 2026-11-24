"""handoff_gate.evaluate: what blocks a clear or a move, and what the agent may abandon."""

from __future__ import annotations

import pytest

from agents_orchestrator import handoff_gate
from agents_orchestrator.api.schemas import Abandon
from agents_orchestrator.wrapper.models import Inflight


def ab(item: str, why: str = "not needed any more, reported") -> Abandon:
    return Abandon(item=item, why=why)


def test_clean_inflight_clear():
    got = handoff_gate.evaluate(Inflight(), [])
    assert (got.clear, got.waiting_on, got.abandoned) == (True, [], [])


def test_dirty_worktree_blocks():
    inflight = Inflight(worktrees=[{"path": "/workdir/code/web", "dirty": 3, "ahead": 0}])
    got = handoff_gate.evaluate(inflight, [])
    assert not got.clear and got.waiting_on == ["worktree:/workdir/code/web dirty 3"]


def test_ahead_and_no_upstream_block():
    inflight = Inflight(
        worktrees=[
            {"path": "/w/a", "dirty": 0, "ahead": 2},
            {"path": "/w/b", "dirty": 0, "ahead": 0, "upstream": False},
            {"path": "/w/c", "dirty": 0, "ahead": 0, "upstream": True},
        ]
    )
    got = handoff_gate.evaluate(inflight, [])
    assert got.waiting_on == ["worktree:/w/a ahead 2", "worktree:/w/b unpushed (no upstream)"]


def test_unknown_worktree_blocks_until_abandoned():
    inflight = Inflight(worktrees=[{"path": "/w/a", "error": "git status timed out"}])
    got = handoff_gate.evaluate(inflight, [])
    assert got.waiting_on == ["worktree:/w/a unknown (git status timed out)"]
    got = handoff_gate.evaluate(inflight, [ab("/w/a", "scan keeps timing out, nothing is changed")])
    assert got.clear and got.waiting_on == []
    assert got.abandoned == [
        {
            "item": "worktree:/w/a unknown (git status timed out)",
            "why": "scan keeps timing out, nothing is changed",
        }
    ]


def test_crons_never_block():
    inflight = Inflight(crons=[{"id": "c1", "schedule": "*/5 * * * *", "prompt": "check"}])
    assert handoff_gate.evaluate(inflight, []).clear


def test_wrapper_unreachable_blocks():
    got = handoff_gate.evaluate(None, [])
    assert not got.clear and got.waiting_on == ["inflight:unknown (wrapper unreachable)"]
    got = handoff_gate.evaluate(None, [ab("inflight:unknown (wrapper unreachable)")])
    assert got.clear


def test_task_and_subagent_block_and_abandon_by_id():
    inflight = Inflight(subagents=2, background_tasks=[{"id": "bf3a", "description": "CI watch"}])
    got = handoff_gate.evaluate(inflight, [])
    assert got.waiting_on == ["subagent:2 running", "task:bf3a (CI watch)"]
    got = handoff_gate.evaluate(inflight, [ab("bf3a")])
    assert got.waiting_on == ["subagent:2 running"]


def test_pod_local_scratch_blocks():
    inflight = Inflight(scratch=[{"path": "/tmp/claude-1/notes.md"}])
    got = handoff_gate.evaluate(inflight, [])
    assert got.waiting_on == ["scratch:/tmp/claude-1/notes.md (pod-local, not persisted)"]


def test_scratch_abandoned_with_reason_clears():
    inflight = Inflight(scratch=[{"path": "/tmp/claude-1/notes.md"}])
    for item in ("/tmp/claude-1/notes.md", "/tmp/claude-1", "/tmp/claude-1/"):
        assert handoff_gate.evaluate(inflight, [ab(item)]).clear, item
    assert not handoff_gate.evaluate(inflight, [ab("/tmp/claude-2")]).clear
    assert not handoff_gate.evaluate(inflight, [ab("/tmp/claude-1/notes")]).clear


def test_scratch_moved_to_shared_workdir_clears():
    # the re-scan no longer reports a file moved under the shared workdir
    assert handoff_gate.evaluate(Inflight(scratch=[]), []).clear


def test_subagent_abandoned_by_bare_word_whatever_the_count():
    inflight = Inflight(subagents=3)
    abandon = [Abandon(item="subagent", why="its result is not needed any more")]
    got = handoff_gate.evaluate(inflight, abandon)
    assert got.clear and got.abandoned[0]["item"] == "subagent:3 running"


TWO = Inflight(subagents=2, subagent_ids=["a1", "b2"])
NAMED = (
    "is not named in in_flight: list it there, or stop it with TaskStop, "
    "or abandon it with a reason"
)
RUNNING = "is running: finish it, stop it with TaskStop, or abandon it with a reason"


@pytest.mark.parametrize(
    ("inflight", "stored", "expected"),
    [
        (
            Inflight(scratch=[{"path": "/tmp/c (1)/n.md"}]),
            False,
            "/tmp/c (1)/n.md is not persisted: move it under .scratchpad/<name>/ "
            "or list it in results",
        ),
        (
            Inflight(scratch=[{"path": "/tmp/c (1)/n.md"}]),
            True,
            "/tmp/c (1)/n.md is not persisted: move it under .scratchpad/<name>/ "
            "or abandon it with a reason",
        ),
        (Inflight(background_tasks=[{"id": "bf3a"}]), False, f"task:bf3a {NAMED}"),
        (Inflight(background_tasks=[{"id": "bf3a"}]), True, f"task:bf3a {RUNNING}"),
        (TWO, False, f"subagent a1 {NAMED}"),
        (TWO, True, f"subagent a1 {RUNNING}"),
    ],
)
def test_refusal_words_by_kind_and_stage(inflight, stored, expected):
    item = handoff_gate.items(inflight)[0]
    assert handoff_gate.refusal_words(item.id, stored=stored) == expected


def test_abandoning_one_subagent_id_leaves_the_other_blocking():
    got = handoff_gate.evaluate(TWO, [ab("b2")])
    assert got.waiting_on == ["subagent:a1 running"]
    assert handoff_gate.evaluate(TWO, [ab("subagent")]).clear

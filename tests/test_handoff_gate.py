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


@pytest.mark.parametrize(
    ("inflight", "expected"),
    [
        (
            Inflight(scratch=[{"path": "/tmp/claude-1/n.md"}]),
            "/tmp/claude-1/n.md is not persisted: move it under .scratchpad/<name>/ "
            "or list it in results",
        ),
        (
            Inflight(background_tasks=[{"id": "bf3a"}]),
            "task:bf3a is running: finish it, stop it with TaskStop, or abandon it with a reason",
        ),
        (
            Inflight(subagents=2, subagent_ids=["a1", "b2"]),
            "subagent(s) a1,b2 running (name an id or the word subagent in in_flight): "
            "finish it, stop it with TaskStop, or abandon it with a reason",
        ),
    ],
)
def test_refusal_words_by_kind(inflight, expected):
    assert handoff_gate.refusal_words(handoff_gate.items(inflight)[0].id) == expected


def test_subagent_ids_name_and_abandon_the_item():
    inflight = Inflight(subagents=2, subagent_ids=["a1", "b2"])
    assert handoff_gate.evaluate(inflight, [ab("b2")]).clear

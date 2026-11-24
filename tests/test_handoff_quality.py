"""handoff_quality.check and check_text: reason codes of a rejected handoff."""

from __future__ import annotations

from types import SimpleNamespace

from agents_orchestrator import handoff_quality
from agents_orchestrator.api.schemas import Abandon, HandoffSubmitIn
from agents_orchestrator.wrapper.models import Inflight

SETTINGS = SimpleNamespace(handoff_min_chars=300, handoff_min_text_ratio=0.6)
LONG = "Implementing the submit route for the cooperative handoff and its quality check. " * 5


def body(**fields) -> HandoffSubmitIn:
    base = {
        "doing": LONG,
        "done": ["wrote the storage layer and its tests"],
        "next": ["wire the dispatcher pass"],
        "open_requests": ["answer alice about the release date"],
        "results": ["the subagent found two callers"],
    }
    return HandoffSubmitIn(**{**base, **fields})


def check(b, profile="work", inflight=None, abandon=None):
    return handoff_quality.check(b, profile, inflight or Inflight(), abandon or [], SETTINGS)


def test_thumbs_up_rejected_not_text_and_too_short():
    emoji = HandoffSubmitIn(doing="\U0001f44d")
    got = check(emoji)
    assert "not_text" in got and "too_short" in got
    word = HandoffSubmitIn(doing="[thumbs-up]")
    got = check(word)
    assert "too_short" in got and "missing:done" in got and "missing:next" in got


def test_one_liner_too_short():
    got = check(HandoffSubmitIn(doing="fixing the bug", done=["a"], next=["b"]))
    assert got == ["too_short"]


def test_work_missing_fields():
    got = check(HandoffSubmitIn(doing=LONG))
    assert got == ["missing:done", "missing:next"]
    assert "missing:doing" in check(body(doing="  "))


def test_chat_requires_open_requests_and_results():
    got = check(HandoffSubmitIn(doing=LONG, done=["x"], next=["y"]), profile="chat")
    assert got == ["missing:open_requests", "missing:results"]
    assert check(body(), profile="chat") == []


def test_unreferenced_background_task_rejected():
    inflight = Inflight(background_tasks=[{"id": "bf3a", "description": "CI watch"}])
    got = check(body(), inflight=inflight)
    assert got == ["unreferenced:task:bf3a (CI watch)"]
    assert check(body(in_flight=["bf3a polling the CI run of PR 7"]), inflight=inflight) == []
    assert check(body(in_flight=["the ci watch is running"]), inflight=inflight) == []


def test_unreferenced_subagent_rejected():
    inflight = Inflight(subagents=1)
    assert check(body(), inflight=inflight) == ["unreferenced:subagent:1 running"]
    assert check(body(in_flight=["one subagent reviewing the diff"]), inflight=inflight) == []


def test_each_subagent_id_needs_its_own_whole_word_reference():
    inflight = Inflight(subagents=2, subagent_ids=["a1", "b2"])
    got = check(body(in_flight=["a10 reviewing, a1 checking"]), inflight=inflight)
    assert got == ["unreferenced:subagent:b2 running"]
    assert check(body(in_flight=["a10 reviewing"]), inflight=inflight) == [
        "unreferenced:subagent:a1 running",
        "unreferenced:subagent:b2 running",
    ]


def test_abandoned_task_needs_no_reference():
    inflight = Inflight(background_tasks=[{"id": "bf3a", "description": "CI watch"}])
    gone = [Abandon(item="bf3a", why="the PR was closed, nothing to watch")]
    assert check(body(), inflight=inflight, abandon=gone) == []


def test_scratch_referenced_in_results_or_abandoned():
    inflight = Inflight(scratch=[{"path": "/tmp/claude-1/notes.md"}])
    assert check(body(), inflight=inflight) == [
        "unreferenced:scratch:/tmp/claude-1/notes.md (pod-local, not persisted)"
    ]
    assert check(body(results=["kept /tmp/claude-1/notes.md in scratch"]), inflight=inflight) == []
    gone = [Abandon(item="/tmp/claude-1", why="throwaway notes, nothing worth keeping")]
    assert check(body(), inflight=inflight, abandon=gone) == []


def test_valid_work_handoff_ok():
    assert check(body()) == []
    assert check(body(in_flight=[], gotchas=["uv run reverts the wrapper"])) == []


def test_check_text_length_and_ratio():
    assert handoff_quality.check_text(LONG, SETTINGS) == []
    assert handoff_quality.check_text("", SETTINGS) == ["too_short"]
    assert handoff_quality.check_text("!?*#" * 100, SETTINGS) == ["not_text"]

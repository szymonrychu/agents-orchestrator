"""pressure_policy.build: thresholds, handoff state mapping, the hash, the wrapper schema."""

import uuid
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from claude_code_wrapper.pressure import PressurePolicy

from agents_orchestrator import handoff_texts
from agents_orchestrator import pressure_policy as pp
from agents_orchestrator.db.handoffs import Handoff
from agents_orchestrator.dispatcher import hive
from agents_orchestrator.settings import Settings

SETTINGS = Settings(database_url="postgresql://u:p@db/o")
LV = hive.levels({}, 20_000, SETTINGS)
EXTRAS = pp.PolicyExtras()


def _row(phase="pressuring", inflight=None) -> Handoff:
    now = datetime.now(UTC)
    return Handoff(
        id=1, session_id=uuid.uuid4(), agent_id=uuid.uuid4(), trigger="limit", phase=phase,
        level=2, profile="work", handoff=None, handoff_text=None, rejects=0, reject_reasons=[],
        inflight=inflight, gate_blocks=0, submitted_at=None, escalated_at=None, nudged_level=0,
        l2_tokens_at=None, submit_tokens=None, progress_at=now, stuck_reported_at=None,
        leadership_request_id=None, successor_agent_id=None, delivered_at=None,
        discuss_since=None, clear_command_id=None, clear_attempts=0, cleared_at=None,
        return_message_id=None, created_at=now, closed_at=None, close_reason=None,
    )  # fmt: skip


def _build(row=None, *, l1_on=True, window=200_000, profile="work", hive_=True, extras=EXTRAS):
    return pp.build(LV, window, l1_on, profile, row, hive=hive_, version=3, extras=extras)


def test_policy_thresholds_and_state_mapping():
    body = _build()
    th = body["thresholds"]
    assert (th["l2_tokens"], th["l3_tokens"], th["compact_tokens"]) == (130_000, 165_000, 190_000)
    assert th["l1_tokens"] == 130_000  # min(20k + 150k, l2)
    assert (th["l1_pct"], th["l2_pct"], th["l3_pct"], th["compact_pct"]) == (80, 85, 90, 95)
    assert body["version"] == 3 and body["profile"] == "work"
    assert body["handoff_state"] == "none"
    big = pp.build(LV, 1_000_000, True, "work", None, hive=True, version=1, extras=EXTRAS)
    assert (big["thresholds"]["l2_tokens"], big["thresholds"]["l3_tokens"]) == (850_000, 900_000)

    blocking = {"subagents": 1, "background_tasks": [{"id": "bf3a", "description": "CI"}]}
    cases = [
        (None, "none", []),
        (_row("pressuring"), "none", []),
        (_row("submitted", {}), "accepted", []),
        (_row("submitted", {"subagents": 0}), "accepted", []),
        (_row("submitted", blocking), "blocked", ["1 subagent(s)", "background task bf3a CI"]),
        (_row("moving"), "accepted", []),
        (_row("clearing"), "accepted", []),
        (_row("discussing"), "accepted", []),
        (_row("returning"), "accepted", []),
    ]
    for row, state, waiting in cases:
        got = _build(row)
        assert (got["handoff_state"], got["waiting_on"]) == (state, waiting), row and row.phase


def test_policy_l1_off_clears_the_l1_thresholds():
    th = _build(l1_on=False)["thresholds"]
    assert th["l1_tokens"] is None and th["l1_pct"] is None


def test_policy_without_start_tokens_uses_the_percent_fallback_point():
    lv = hive.levels({}, None, SETTINGS)
    body = pp.build(lv, 200_000, True, "work", None, hive=False, version=1, extras=EXTRAS)
    assert body["thresholds"]["l1_tokens"] == 130_000  # min(fallback 80% = 160k, l2)


def test_policy_blocked_texts_carry_waiting_on_placeholder_and_extras():
    row = _row("submitted", {"subagents": 2})
    body = _build(row, extras=pp.PolicyExtras(resume_pending=True, resume_text="Resume now: 1."))
    assert "{waiting_on}" in body["texts"]["l2"]["full"]
    assert body["resume_pending"] is True and body["resume_text"] == "Resume now: 1."
    assert "{waiting_on}" not in _build()["texts"]["l2"]["full"]


def test_policy_hash_ignores_version():
    a, b = _build(), _build()
    b["version"] = 99
    assert pp.policy_hash(a) == pp.policy_hash(b)
    c = _build(profile="chat")
    assert pp.policy_hash(a) != pp.policy_hash(c)
    assert len(pp.policy_hash(a)) == 64


@pytest.mark.parametrize("profile", ["work", "chat"])
@pytest.mark.parametrize("state", [None, "pressuring", "blocked"])
def test_policy_body_validates_against_the_wrapper_schema(profile, state):
    row = {
        None: None,
        "pressuring": _row("pressuring"),
        "blocked": _row("submitted", {"subagents": 1}),
    }[state]
    body = _build(row, profile=profile)
    model = PressurePolicy.model_validate(body)
    assert model.max_text_len() <= 4000
    assert model.handoff_state == body["handoff_state"]
    unknown = _build(window=None)
    PressurePolicy.model_validate(unknown)


def test_l1_threshold_shared_with_due_level():
    lv = hive.levels({}, None, Settings(database_url="postgresql://u:p@db/o"))
    body = pp.build(
        lv, 1_000_000, True, "work", None, hive=False, version=1, extras=pp.PolicyExtras()
    )
    l1 = body["thresholds"]["l1_tokens"]
    assert l1 == 800_000
    assert hive.due_level(lv, l1, 1_000_000, None, 9e9, 0, False) == "l1"
    assert hive.due_level(lv, l1 - 1, 1_000_000, None, 9e9, 0, False) == "none"


# ---------------------------------------------------------------- compaction steering (spec 5.8)

PRESERVE = (
    "Preserve verbatim: open requests, in-flight items, the latest handoff (accepted or draft), "
    "claimed task ids and the todo."
)
TODO = ["open: wire the policy", "done: write the texts"]


def _accepted() -> Handoff:
    busy = {"subagents": 1, "background_tasks": [{"id": "bf3a", "description": "CI"}]}
    row = _row("submitted", busy)
    return replace(row, handoff={"doing": "steering compaction", "next": ["open the PR"],
                                 "open_requests": ["answer Anna about the rollout"]})  # fmt: skip


def test_compact_instructions_present_while_cycle_open():
    text = handoff_texts.compact_instructions(_accepted(), ["T-437", "T-423"], "work", todo=TODO)
    assert text.startswith(PRESERVE)
    for needle in ("T-437", "T-423", "open: wire the policy", "1 subagent(s)",
                   "background task bf3a CI", "steering compaction", "open the PR",
                   "answer Anna about the rollout"):  # fmt: skip
        assert needle in text, needle
    body = _build(_accepted(), extras=pp.PolicyExtras(compact_instructions=text))
    assert body["compact_instructions"] == text
    PressurePolicy.model_validate(body)
    # an open row with nothing else still carries the instruction
    assert handoff_texts.compact_instructions(_row(), [], "work").startswith(PRESERVE)


def test_compact_instructions_capped():
    big = replace(_accepted(), handoff={"doing": "x" * 20_000})
    text = handoff_texts.compact_instructions(big, [f"T-{i}" for i in range(50)], "work",
                                              todo=["open: " + "y" * 500] * 100)  # fmt: skip
    assert len(text) <= 9000 and text.startswith(PRESERVE) and text.isascii()
    reinject = handoff_texts.reinject_text(big, ["T-1"], ["open: " + "y" * 500] * 100, "work")
    assert len(reinject) <= 9000 and reinject.isascii()
    PressurePolicy.model_validate(
        _build(big, extras=pp.PolicyExtras(compact_instructions=text, reinject_text=reinject))
    )


def test_reinject_has_task_todo_inflight():
    text = handoff_texts.reinject_text(_accepted(), ["T-437"], TODO, "work")
    for needle in ("T-437", "open: wire the policy", "done: write the texts", "1 subagent(s)",
                   "background task bf3a CI", "steering compaction", "open the PR"):  # fmt: skip
        assert needle in text, needle
    chat = handoff_texts.reinject_text(_accepted(), [], [], "chat")
    assert "answer Anna about the rollout" in chat and "room" in chat


def test_no_steering_without_cycle():
    assert handoff_texts.compact_instructions(None, ["T-1"], "work", todo=TODO) == ""
    assert handoff_texts.reinject_text(None, ["T-1"], TODO, "work") == ""
    body = _build(None)
    assert body["compact_instructions"] == "" and body["reinject_text"] == ""


def test_steering_texts_have_no_time_words():
    import re

    for text in (
        handoff_texts.compact_instructions(_accepted(), ["T-1"], "work", todo=TODO),
        handoff_texts.reinject_text(_accepted(), ["T-1"], TODO, "chat"),
    ):
        assert not re.search(r"minute|deadline|second", text, re.I), text
        assert not re.search(r"\bin \d+", text), text

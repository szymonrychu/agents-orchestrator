"""handoff_texts: no time words, the checklists per profile, the submit tool named."""

import re

import pytest

from agents_orchestrator import handoff_texts as t

PROFILES = ("work", "chat")


def _all_texts() -> list[str]:
    out = [
        t.PRESSURE_LINE,
        t.SHUTDOWN_NOTICE,
        t.SHUTDOWN_REQUEST,
        t.resubmit_text(),
        t.resume_text(["T-1", "T-2"], "open the PR"),
        t.resume_text([], None),
        t.waiting_on_text(),
        t.escalation_text("s", "agent-s-0", 3, 97.0, ["next"], ["background task bf3a"]),
        t.escalation_text("s", "agent-s-0", 2, None, [], []),
        t.shutdown_successor("agent-s-0", "text"),
    ]
    for level in (1, 2, 3):
        out.append(t.nudge_text(level))
    for profile in PROFILES:
        for hive in (True, False):
            for level in (1, 2, 3):
                out.append(t.checklist(profile, level, hive=hive))
            for forms in t.level_texts(profile, hive).values():
                out.extend(forms.values())
    return out


def test_no_time_words_in_any_text():
    for text in _all_texts():
        assert not re.search(r"minute|deadline|second", text, re.I), text
        assert not re.search(r"\bin \d+", text), text


def test_all_texts_plain_ascii():
    for text in _all_texts():
        assert text.isascii(), text


def test_chat_checklist_mentions_open_requests_and_never_room_reply():
    text = t.checklist("chat", 2, hive=True)
    assert "every open member request" in text
    assert "open request" in text and "who asked" in text
    assert "never send the handoff as a room reply" in text
    assert "room notes" in text


def test_work_checklist_mentions_commit_push_task_todo():
    text = t.checklist("work", 2, hive=False)
    for word in ("commit and push every worktree", "task and todo", "background task", "subagent"):
        assert word in text


def test_l2_texts_name_the_submit_tool():
    for profile in PROFILES:
        for hive in (True, False):
            texts = t.level_texts(profile, hive)
            assert set(texts) == {"l1", "l2", "l3"}
            for key in ("l2", "l3"):
                for form in ("full", "short"):
                    assert "orchestrator_handoff_submit" in texts[key][form], (key, form)
            assert "orchestrator_handoff_submit" in t.nudge_text(2)
    for level in (2, 3):
        assert "orchestrator_handoff_submit" in t.checklist("work", level, hive=True)


def test_level_texts_carry_the_line_with_the_level_name():
    texts = t.level_texts("work", True)
    assert "Level: plan." in texts["l1"]["full"]
    assert "Level: hand off." in texts["l2"]["short"]
    assert "Level: critical." in texts["l3"]["full"]
    assert "{level}" not in texts["l3"]["full"]
    assert "{pct}" in texts["l2"]["full"]


def test_shutdown_request_starts_with_write_your_handoff_now():
    assert t.SHUTDOWN_REQUEST.startswith("Write your handoff now")


def test_nudge_text_opens_with_safe_point():
    assert t.nudge_text(2).startswith("You are at a safe point: hand off now")


def test_texts_tell_agent_to_persist_scratch():
    for text in (
        t.checklist("work", 2, hive=False),
        t.checklist("chat", 3, hive=True),
        t.waiting_on_text(),
        t.SHUTDOWN_NOTICE,
        t.SHUTDOWN_REQUEST,
    ):
        assert ".scratchpad/<name>/" in text and "/tmp" in text, text


def test_checklist_rejects_unknown_level():
    with pytest.raises(ValueError):
        t.checklist("work", 4, hive=True)


def test_resume_text_is_an_instruction_list():
    text = t.resume_text(["T-9"], "finish step 3")
    assert text.startswith("Resume now: 1. call orchestrator_me")
    assert "T-9" in text and "finish step 3" in text

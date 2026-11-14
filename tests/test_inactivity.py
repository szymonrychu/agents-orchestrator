"""Effective inactivity limits and the notice texts (hive-inactivity-limits, spec 3.3/3.4)."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from agents_orchestrator import inactivity
from agents_orchestrator.inactivity import Limits
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec

LAST = datetime(2026, 11, 2, 10, 0, tzinfo=UTC)
NOW = datetime(2026, 11, 2, 11, 0, tzinfo=UTC)


def _settings(reminder: int = 3600, close: int = 14400) -> Settings:
    return Settings(
        database_url="postgresql://x/y",
        inactivity_reminder_seconds=reminder,
        inactivity_close_seconds=close,
    )


def _reminder(**overrides) -> str:
    kwargs = {
        "last_update": LAST,
        "now": NOW,
        "limits": Limits(3600, 14400),
        "pods": 3,
        "chat": True,
        "taskboard": True,
    }
    kwargs.update(overrides)
    return inactivity.reminder_text(**kwargs)


def _close(**overrides) -> str:
    kwargs = {
        "quiet_seconds": 14400,
        "deadline": datetime(2026, 11, 2, 14, 5, tzinfo=UTC),
        "grace_seconds": 30,
        "chat": True,
        "taskboard": True,
        "workdir_saved": True,
    }
    kwargs.update(overrides)
    return inactivity.close_text(**kwargs)


def test_constants():
    assert inactivity.NOTICE_REMINDER == "inactivity_reminder"
    assert inactivity.NOTICE_CLOSE == "inactivity_close"
    assert inactivity.REMINDER_PREFIX == "Orchestrator notice (inactivity reminder):"
    assert inactivity.CLOSE_PREFIX == "Orchestrator notice (inactivity limit):"
    assert inactivity.NOTICE_AUTOCLOSE == "autoclose"
    assert inactivity.AUTOCLOSE_PREFIX == "Orchestrator notice (session lifetime):"


def test_effective_uses_spec_then_setting():
    settings = _settings(3600, 14400)
    assert inactivity.effective(SessionSpec(), settings) == Limits(3600, 14400)
    assert inactivity.effective(SessionSpec(inactivity={}), settings) == Limits(3600, 14400)
    spec = SessionSpec(inactivity={"reminder_seconds": 0, "close_seconds": 0})
    assert inactivity.effective(spec, settings) == Limits(0, 0)
    spec = SessionSpec(inactivity={"reminder_seconds": 600})
    assert inactivity.effective(spec, settings) == Limits(600, 14400)
    spec = SessionSpec(inactivity={"close_seconds": 3300})
    assert inactivity.effective(spec, settings) == Limits(3600, 3300)
    assert inactivity.effective(SessionSpec(), _settings(0, 0)) == Limits(0, 0)


def test_effective_exempts_superleaders():
    spec = SessionSpec(session_messaging=True, inactivity={"reminder_seconds": 600})
    assert inactivity.effective(spec, _settings()) == Limits(0, 0)
    spec = SessionSpec(session_messaging=False)
    assert inactivity.effective(spec, _settings()) == Limits(3600, 14400)


@pytest.mark.parametrize(
    ("seconds", "text"),
    [
        (0, "0 s"),
        (30, "30 s"),
        (59, "59 s"),
        (60, "1 min"),
        (600, "10 min"),
        (3300, "55 min"),
        (3599, "59 min"),
        (3600, "1 h"),
        (5400, "1.5 h"),
        (14400, "4 h"),
        (14405, "4 h"),
        (7190, "1.9 h"),
        (2592000, "720 h"),
    ],
)
def test_humanize(seconds, text):
    assert inactivity.humanize(seconds) == text


def test_stamp_and_clock_are_utc():
    t = datetime(2026, 11, 2, 15, 5, 59, tzinfo=timezone(timedelta(hours=1)))
    assert inactivity.stamp(t) == "2026-11-02 14:05Z"
    assert inactivity.clock(t) == "14:05Z"


def test_reminder_text_example():
    assert _reminder() == (
        "Orchestrator notice (inactivity reminder): this session has sent no update to the user"
        " and done no tool work for 1 h (last activity 2026-11-02 10:00Z) while holding 3 agent"
        " pods and a session slot."
        " If the work is still going, please post a short progress update now with"
        " orchestrator_chat_send or a comment on your task (orchestrator_task_comment), and keep"
        " posting at least every 1 h. If the work is finished, post a final status and call"
        " orchestrator_self_close to free the hive. Without an update or activity the"
        " orchestrator will ask this session to close at about 2026-11-02 14:00Z; it is closed"
        " only once idle, never mid-turn. Replying here alone does not count as an update."
    )


def test_reminder_text_invites_self_close_only_when_finished():
    text = _reminder()
    assert "If the work is finished" in text
    assert "done or blocked" not in text
    assert "never mid-turn" in text


def test_reminder_text_without_close_and_tools():
    text = _reminder(limits=Limits(600, 0))
    assert "ask this session to close" not in text
    assert "at least every 10 min." in text

    text = _reminder(taskboard=False)
    assert "orchestrator_task_comment" not in text
    assert "progress update now with orchestrator_chat_send, and keep posting" in text

    text = _reminder(chat=False)
    assert "orchestrator_chat_send" not in text
    assert "now with a comment on your task (orchestrator_task_comment), and keep" in text

    text = _reminder(chat=False, taskboard=False)
    assert "orchestrator_chat_send" not in text
    assert "orchestrator_task_comment" not in text
    assert "orchestrator_self_close" in text
    assert (
        " If the work is still going, report progress in your answer to the next user message. "
        in text
    )

    text = _reminder(pods=1)
    assert "while holding 1 agent pod and a session slot." in text


def test_close_text_example():
    assert _close() == (
        "Orchestrator notice (inactivity limit): this session has sent no update and done no tool"
        " work for 4 h, so the orchestrator asks you to close it. If your work is finished: post a"
        " final status (orchestrator_chat_send or a task comment) and since this session saves its"
        " workspace, write handoff notes into the workdir, then call orchestrator_self_close. If"
        " you are still working, post a progress update now to keep the session. It is never"
        " closed while you work; if it is idle after 14:05Z without a self-close or an update, it"
        " is discarded 30 s later."
    )
    assert "workdir" not in _close(workdir_saved=False)
    assert "post a final status (orchestrator_chat_send or a task comment), then call" in (
        _close(workdir_saved=False)
    )


def test_close_text_asks_self_close():
    text = _close()
    assert "orchestrator_self_close" in text
    assert "post a progress update" in text
    assert "never closed while you work" in text
    assert "even mid-turn" not in text


def test_close_text_tools():
    assert "post a final status (orchestrator_chat_send), then" in _close(
        taskboard=False, workdir_saved=False
    )
    assert "post a final status (a task comment with orchestrator_task_comment), then" in _close(
        chat=False, workdir_saved=False
    )
    text = _close(chat=False, taskboard=False)
    assert "post a final status" not in text
    assert "If your work is finished: since this session saves its workspace, write handoff" in text
    text = _close(chat=False, taskboard=False, workdir_saved=False)
    assert "hand off" not in text and "handoff" not in text
    assert "If your work is finished, call orchestrator_self_close." in text


def _autoclose(**overrides) -> str:
    kwargs = {
        "after_seconds": 3600,
        "deadline": datetime(2026, 11, 2, 14, 5, tzinfo=UTC),
        "grace_seconds": 30,
        "chat": True,
        "taskboard": True,
        "workdir_saved": True,
        "can_self_close": True,
    }
    kwargs.update(overrides)
    return inactivity.autoclose_text(**kwargs)


def test_autoclose_text():
    assert _autoclose() == (
        "Orchestrator notice (session lifetime): this session reached its lifetime of 1 h set by"
        " its preset, so the orchestrator asks you to wrap up. Finish or checkpoint your work,"
        " post a final status (orchestrator_chat_send or a task comment) and since this session"
        " saves its workspace, write handoff notes into the workdir, then call"
        " orchestrator_self_close. The session is never closed while you work; if it is idle"
        " after 14:05Z without a self-close, it is discarded 30 s later. Only the user can keep"
        " it."
    )
    plain = _autoclose(chat=False, taskboard=False, workdir_saved=False)
    assert "Finish or checkpoint your work, then call orchestrator_self_close." in plain
    text = _autoclose(can_self_close=False)
    assert "orchestrator_self_close" not in text and "Finish or checkpoint" not in text
    assert "it is discarded 30 s later" in text


def test_autoclose_reason():
    assert inactivity.autoclose_reason(3600) == (
        "Lifetime of 1 h reached (autoclose_after_seconds 3600)"
    )


def test_texts_are_ascii():
    texts = [
        _reminder(),
        _reminder(limits=Limits(600, 0), chat=False, taskboard=False, pods=1),
        _close(),
        _close(chat=False, taskboard=False, workdir_saved=False),
        _autoclose(),
        _autoclose(can_self_close=False),
        inactivity.close_reason(14400, 14400),
        inactivity.autoclose_reason(3600),
    ]
    for text in texts:
        assert text.isascii()
        assert "--" not in text
        assert "\n" not in text


def test_close_reason():
    assert inactivity.close_reason(14400, 14400) == "No update for 4 h (inactivity limit 14400 s)"
    assert inactivity.close_reason(3330, 3300) == "No update for 55 min (inactivity limit 3300 s)"

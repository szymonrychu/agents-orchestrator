import uuid
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from agents_orchestrator.runaway import (
    AgentWindow,
    Breach,
    Limits,
    breach,
    client_key,
    effective,
    parked_notice,
    task_body,
    task_title,
)
from agents_orchestrator.settings import Settings, SettingsError
from agents_orchestrator.spec import SessionSpec

AID = uuid.UUID("00000000-0000-0000-0000-0000000000a1")
SID = uuid.UUID("00000000-0000-0000-0000-0000000000b2")
T0 = datetime(2026, 11, 14, 10, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    import os

    for k in list(os.environ):
        if k.startswith("ORCH_"):
            monkeypatch.delenv(k)
    monkeypatch.setenv("ORCH_DATABASE_URL", "postgresql://u:p@h/db")


def make_settings() -> Settings:
    return Settings()


def test_runaway_limits_sparse_and_bounds():
    assert "runaway" not in SessionSpec().model_dump(exclude_none=True)
    assert (
        SessionSpec.model_validate(
            {"runaway": {"max_turns_per_task": 0}}
        ).runaway.max_turns_per_task
        == 0
    )
    for field, bad in [
        ("max_turns_per_task", 9),
        ("max_tokens_per_task", 999999),
        ("max_stop_continuations", 4),  # the wrapper needs more than pressure l3_stop_blocks + 1
        ("max_stop_continuations", 1001),
        ("max_agent_spawns", 100001),
        ("tool_failure_repeats", 1),
        ("max_concurrent_subagents", 101),
        ("max_subagent_spawn_depth", 11),
    ]:
        with pytest.raises(ValidationError, match="must be 0 \\(off\\) or"):
            SessionSpec.model_validate({"runaway": {field: bad}})


def test_settings_defaults():
    s = make_settings()
    assert (
        s.runaway_max_turns_per_task,
        s.runaway_max_tokens_per_task,
        s.runaway_max_stop_continuations,
        s.runaway_max_agent_spawns,
        s.runaway_tool_failure_repeats,
    ) == (300, 60000000, 25, 300, 3)


def test_settings_bounds(monkeypatch):
    monkeypatch.setenv("ORCH_RUNAWAY_MAX_TURNS_PER_TASK", "5")
    with pytest.raises(
        (SettingsError, ValidationError), match="ORCH_RUNAWAY_MAX_TURNS_PER_TASK|max_turns_per_task"
    ):
        Settings()
    monkeypatch.setenv("ORCH_RUNAWAY_MAX_TURNS_PER_TASK", "0")
    assert Settings().runaway_max_turns_per_task == 0


def test_effective_spec_over_settings():
    lim = effective(
        SessionSpec.model_validate(
            {"runaway": {"max_turns_per_task": 0, "max_concurrent_subagents": 8}}
        ),
        make_settings(),
    )
    assert lim == Limits(0, 60000000, 25, 300, 3, 8, 0)


def test_breach_strictly_greater_and_precedence():
    lim = Limits(300, 60000000, 25, 300, 3, 0, 0)
    a = AgentWindow(AID, "agent-x-0", T0, 300, 60000000, None)
    assert breach(lim, [a]) is None
    assert breach(lim, [replace(a, turns=301)]).reason == "turns"
    assert breach(lim, [replace(a, turns=301, tokens=60000001)]).reason == "tokens"
    b = breach(lim, [replace(a, turns=301, tokens=60000001, stop_loop_at=T0)])
    assert (b.reason, b.value, b.limit) == ("stop_loop", 25, 25)
    off = Limits(0, 0, 0, 300, 3, 0, 0)
    assert breach(off, [replace(a, turns=10**6, tokens=10**12, stop_loop_at=T0)]) is None


def test_breach_first_agent_wins():
    lim = Limits(300, 60000000, 25, 300, 3, 0, 0)
    a = AgentWindow(AID, "agent-x-0", T0, 301, 0, None)
    other = AgentWindow(uuid.uuid4(), "agent-x-1", T0, 0, 0, T0)
    assert breach(lim, [a, other]).pod == "agent-x-0"


def test_texts():
    b = Breach("turns", AID, "agent-x-0", 301, 300, T0)
    assert parked_notice(b).startswith(
        "Orchestrator notice (runaway guard): this session was parked on agent-x-0 "
        "by the turn ceiling: 301 turns"
    )
    assert len(task_title("s" * 300, b)) == 200
    assert client_key(SID, datetime(2026, 11, 14, tzinfo=UTC)) == f"runaway:{SID}:1794614400"
    body = task_body(
        session_id=SID,
        slug="x",
        preset_name=None,
        b=b,
        limits=effective(SessionSpec(), make_settings()),
    )
    assert f"DELETE /api/v1/sessions/{SID}/park" in body
    assert b.detail()["window_at"] == T0.isoformat()

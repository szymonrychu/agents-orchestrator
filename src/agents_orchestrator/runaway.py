"""Runaway-guard rules: effective limits, the breach test and the texts of the park notice and
the task. Pure: no database, no I/O."""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec

NOTICE_PARKED = "runaway_parked"
PARKED_PREFIX = "Orchestrator notice (runaway guard):"
TASK_LABEL = "runaway-guard"
_TITLE_MAX = 200


@dataclass(frozen=True)
class Limits:
    """0 = off (for the two subagent caps: the Claude default)."""

    max_turns_per_task: int
    max_tokens_per_task: int
    max_stop_continuations: int
    max_agent_spawns: int
    tool_failure_repeats: int
    max_concurrent_subagents: int
    max_subagent_spawn_depth: int


def effective(spec: SessionSpec, settings: Settings) -> Limits:
    """The spec's limits where set, else the settings'; the two Claude caps default to 0.
    Superleaders are not exempt."""
    own = spec.runaway

    def pick(name: str, default: int) -> int:
        v = getattr(own, name) if own else None
        return default if v is None else v

    return Limits(
        pick("max_turns_per_task", settings.runaway_max_turns_per_task),
        pick("max_tokens_per_task", settings.runaway_max_tokens_per_task),
        pick("max_stop_continuations", settings.runaway_max_stop_continuations),
        pick("max_agent_spawns", settings.runaway_max_agent_spawns),
        pick("tool_failure_repeats", settings.runaway_tool_failure_repeats),
        pick("max_concurrent_subagents", 0),
        pick("max_subagent_spawn_depth", 0),
    )


@dataclass(frozen=True)
class AgentWindow:
    agent_id: uuid.UUID
    pod: str
    window_at: datetime | None
    turns: int
    tokens: int
    stop_loop_at: datetime | None


@dataclass(frozen=True)
class Breach:
    reason: Literal["stop_loop", "tokens", "turns"]
    agent_id: uuid.UUID
    pod: str
    value: int
    limit: int
    window_at: datetime | None

    def detail(self) -> dict[str, Any]:
        return {
            "agent_id": str(self.agent_id),
            "pod": self.pod,
            "value": self.value,
            "limit": self.limit,
            "window_at": self.window_at.isoformat() if self.window_at else None,
        }


def breach(limits: Limits, agents: Sequence[AgentWindow]) -> Breach | None:
    """The first agent (in the given order) over a limit; within it stop_loop, tokens, turns."""
    for a in agents:
        if limits.max_stop_continuations > 0 and a.stop_loop_at is not None:
            n = limits.max_stop_continuations
            return Breach("stop_loop", a.agent_id, a.pod, n, n, a.window_at)
        if limits.max_tokens_per_task > 0 and a.tokens > limits.max_tokens_per_task:
            return Breach(
                "tokens", a.agent_id, a.pod, a.tokens, limits.max_tokens_per_task, a.window_at
            )
        if limits.max_turns_per_task > 0 and a.turns > limits.max_turns_per_task:
            return Breach(
                "turns", a.agent_id, a.pod, a.turns, limits.max_turns_per_task, a.window_at
            )
    return None


def reason_text(b: Breach) -> str:
    if b.reason == "turns":
        return f"turn ceiling: {b.value} turns since the last delivered message (limit {b.limit})"
    if b.reason == "tokens":
        return (
            f"token ceiling: {b.value} tokens since the last delivered message "
            f"(limit {b.limit}, cache reads not counted)"
        )
    return f"Stop hook loop: a turn was continued {b.limit} times"


def parked_notice(b: Breach) -> str:
    return (
        f"{PARKED_PREFIX} this session was parked on {b.pod} by the {reason_text(b)}. "
        "A person resumed it. Check the working tree and your task state before you continue, "
        "and do not repeat the loop that led here."
    )


def task_title(slug: str, b: Breach) -> str:
    return f"Session {slug} parked by the runaway guard: {b.reason}"[:_TITLE_MAX]


def task_body(
    *,
    session_id: uuid.UUID,
    slug: str,
    preset_name: str | None,
    b: Breach,
    limits: Limits,
) -> str:
    window = b.window_at.isoformat() if b.window_at else "unknown"
    return "\n".join(
        [
            f"- Session: {session_id} ({slug})",
            f"- Preset: {preset_name or 'none'}",
            f"- Reason: {reason_text(b)}",
            f"- Window start: {window}",
            f"- Limits: max_turns_per_task {limits.max_turns_per_task}, "
            f"max_tokens_per_task {limits.max_tokens_per_task}, "
            f"max_stop_continuations {limits.max_stop_continuations}, "
            f"max_agent_spawns {limits.max_agent_spawns}, "
            f"tool_failure_repeats {limits.tool_failure_repeats}",
            "Resume: the Resume button in the web UI, "
            f"or DELETE /api/v1/sessions/{session_id}/park.",
        ]
    )


def client_key(session_id: uuid.UUID, parked_at: datetime) -> str:
    return f"runaway:{session_id}:{int(parked_at.timestamp())}"

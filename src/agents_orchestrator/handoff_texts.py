"""Texts of the cooperative context-pressure handoff (plain ASCII, one place).

The wrapper renders `PRESSURE_LINE` with live numbers at delivery; the orchestrator pushes the
level texts, checklists and the nudge and escalation texts. No text names a time or a deadline:
the facts are the context size and what is still open.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

Profile = Literal["work", "chat"]

PRESSURE_LINE = (
    "Context {pct}% ({used_k}k of {window_k}k tokens). Auto-compaction at {compact_pct}% "
    "(~{left_k}k tokens left). Level: {level}."
)
LEVEL_NAMES = {1: "plan", 2: "hand off", 3: "critical"}

SUBMIT_TOOL = "orchestrator_handoff_submit"
SCRATCH_SENTENCE = (
    "Persist any scratch file you still need into the shared workdir scratchpad "
    "(.scratchpad/<name>/); do not leave it under /tmp or anywhere else on the pod."
)

SHUTDOWN_NOTICE = (
    "This pod is shutting down. Finish or park your current step now: commit and push, update "
    "your task and todo. You will be asked for a handoff next. " + SCRATCH_SENTENCE
)
SHUTDOWN_REQUEST = (
    "Write your handoff now as your reply: what you were doing, what is done (commits, PRs), "
    "what is next and in which order, open decisions and gotchas. Your reply text is the "
    "handoff; do not start new work. " + SCRATCH_SENTENCE
)

_WORK_STEPS = (
    "finish or park the step you are in",
    "commit and push every worktree",
    "update your task and todo",
    "wait for or abandon (with a reason) every background task and subagent",
)
_CHAT_STEPS = (
    "answer or acknowledge every open member request first",
    "wait for running subagents and post or store their results (room notes)",
    "put every open request, with who asked and what, into the handoff",
    "never send the handoff as a room reply",
)


def _steps(profile: Profile) -> tuple[str, ...]:
    return _CHAT_STEPS if profile == "chat" else _WORK_STEPS


def checklist(profile: Profile, level: int, *, hive: bool) -> str:
    """The checklist of one level (1..3) for a profile; `hive` adds the leadership sentence."""
    if level not in (1, 2, 3):
        raise ValueError(f"level must be 1, 2 or 3, got {level}")
    steps = _steps(profile)
    if level == 1:
        text = (
            "Plan your handoff: at a natural boundary, "
            + ", ".join(steps[:-1])
            + f", and {steps[-1]}. Then call {SUBMIT_TOOL}. Nothing happens if you keep working."
        )
    else:
        head = "Context is critical: hand off now." if level == 3 else "Hand off as soon as safe."
        text = (
            f"{head} In order: "
            + "; ".join(f"{i}. {s}" for i, s in enumerate(steps, 1))
            + f"; {len(steps) + 1}. call {SUBMIT_TOOL} with every field filled in. "
            "Auto-compaction is the last resort and loses detail."
        )
    if hive and level >= 2:
        text += " Leadership moves to a successor once your handoff is accepted."
    elif level >= 2:
        text += " Your context is cleared once your handoff is accepted and nothing is in flight."
    if level >= 2:
        text += " " + SCRATCH_SENTENCE
    return text


def _short(level: int) -> str:
    name = LEVEL_NAMES[level]
    if level == 1:
        return f"Context pressure ({name}): plan your handoff at a natural boundary."
    return f"Context pressure ({name}): finish the checklist and call {SUBMIT_TOOL}."


def level_texts(profile: Profile, hive: bool) -> dict[str, dict[str, str]]:
    """The wrapper's per-level texts: `full` = pressure line plus checklist, `short` a one-liner."""
    out: dict[str, dict[str, str]] = {}
    for level in (1, 2, 3):
        line = PRESSURE_LINE.replace("{level}", LEVEL_NAMES[level])
        out[f"l{level}"] = {
            "full": line + " " + checklist(profile, level, hive=hive),
            "short": line + " " + _short(level),
        }
    return out


def nudge_text(level: int) -> str:
    """The one message per level to an idle agent with nothing in flight."""
    return (
        f"You are at a safe point: hand off now (context level {LEVEL_NAMES.get(level, level)}). "
        f"Call {SUBMIT_TOOL}. An idle context does not grow, but a handoff taken now is the "
        "one you write; auto-compaction is not."
    )


def escalation_text(
    slug: str, pod: str, level: int, pct: float | None, missing: list[str], waiting_on: list[str]
) -> str:
    """The human-facing report of an agent that does not hand off under pressure."""
    shown = "unknown" if pct is None else f"{pct:g}%"
    lines = [
        f"Agent {pod} of session {slug} is at context level {LEVEL_NAMES.get(level, level)} "
        f"({shown}) and has not handed off."
    ]
    if missing:
        lines.append("Its last handoff was rejected, missing: " + ", ".join(missing) + ".")
    if waiting_on:
        lines.append("It is waiting on: " + ", ".join(waiting_on) + ".")
    lines.append("It keeps receiving pressure; nothing was cleared or moved.")
    return " ".join(lines)


def waiting_on_text() -> str:
    """Appended to the texts of a submitted-but-blocked handoff; the wrapper fills the
    `{waiting_on}` placeholder from the policy's `waiting_on` list."""
    return (
        "Your handoff is stored but still waiting on: {waiting_on}"
        + f". Finish or abandon each (with a reason), then call {SUBMIT_TOOL} again. "
        + SCRATCH_SENTENCE
    )


def resubmit_text() -> str:
    """Sent when the blockers cleared: results may change the handoff."""
    return (
        "Everything you were waiting on is done. Re-read the results, update your handoff if "
        f"they change it, and call {SUBMIT_TOOL} again."
    )


def resume_text(task_ids: list[str], next_first: str | None) -> str:
    """The resume instruction list for a fresh context: instructions, not data."""
    steps = ["call orchestrator_me"]
    if task_ids:
        steps.append("read your tasks with orchestrator_task_get: " + ", ".join(task_ids))
    steps.append("read your room notes and recent chat if you have a room")
    steps.append("recreate the session crons listed in the handoff")
    steps.append(f"continue with: {next_first}" if next_first else "continue with the next item")
    return "Resume now: " + "; ".join(f"{i}. {s}" for i, s in enumerate(steps, 1)) + "."


_FIELDS = (
    ("doing", "Doing"),
    ("done", "Done"),
    ("in_flight", "In flight"),
    ("next", "Next, in order"),
    ("open_requests", "Open requests"),
    ("results", "Results"),
    ("gotchas", "Gotchas"),
)


def render_handoff(handoff: Mapping[str, Any]) -> str:
    """The fields of an accepted handoff as plain text, in the submit's order; abandoned items
    with their reasons last."""
    lines: list[str] = []
    for key, title in _FIELDS:
        value = handoff.get(key)
        if not value:
            continue
        if isinstance(value, str):
            lines.append(f"{title}: {value}")
        else:
            lines.append(f"{title}:")
            lines.extend(f"- {item}" for item in value)
    abandoned = handoff.get("abandoned") or []
    if abandoned:
        lines.append("Abandoned:")
        lines.extend(f"- {a.get('item')}: {a.get('why')}" for a in abandoned)
    return "\n".join(lines)


def return_text(handoff: Mapping[str, Any], *, cleared: bool) -> str:
    """The accepted handoff sent back to a solo agent after its clear (`cleared`), or after a
    clear that failed (the context was kept)."""
    nxt = handoff.get("next") or []
    if cleared:
        head = "Your context was cleared after your accepted handoff. " + resume_text(
            [], str(nxt[0]) if nxt else None
        )
    else:
        head = (
            "The clear after your accepted handoff failed and your context was kept. "
            "Your handoff, for reference:"
        )
    return head + "\n\n" + render_handoff(handoff)


def shutdown_successor(pod: str, text: str) -> str:
    """A leader's pod-shutdown handoff sent to its successor on its own, when the successor's
    handover notice already went out without it."""
    return f"Handoff from {pod} (written by it before its pod shut down):\n{text}"

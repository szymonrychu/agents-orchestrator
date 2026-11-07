"""Rendered transcript of the predecessor for a leadership handover and the notice text
(voluntary-handover, spec 3.4). Pure: no database, no I/O; the api sweeper calls it with rows of
the `events` table (`id`, `ts`, `type`, `data`, ascending, main chain of the predecessor).

Every peer-originated string is escaped with `html.escape` (newlines kept), so the file and the
notice cannot forge an `<attachment>` or `<session-message ...>` tag.
"""

from __future__ import annotations

import html
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

NOTICE_HANDOVER = "handover"
# The web recognises the notice in the chat log by this prefix.
HANDOVER_PREFIX = "Orchestrator notice (leadership handover):"
PROSE_CHARS = 4000
CALL_CHARS = 200
_LAUNCH_TOOLS = ("Agent", "Task")
_VOLUNTARY = "handover:"
_FORCED_ROLLOUT = "rollout"


@dataclass(frozen=True)
class Header:
    slug: str
    epoch: int
    reason: str
    predecessor_pod: str
    changed_at: datetime
    summary: str | None


@dataclass(frozen=True)
class InFlight:
    queued_messages: int
    locks: list[str]


def summary_of(reason: str) -> str | None:
    """The leader's summary when the move was voluntary (`handover: x`), else None.
    The reconciler's forced rollout request (`handover: rollout`) is a forced move."""
    if reason.startswith(_VOLUNTARY):
        text = reason[len(_VOLUNTARY) :].strip()
        return None if text == _FORCED_ROLLOUT else text or None
    return None


def attachment_name(epoch: int) -> str:
    return f"handover-{epoch}.md"


def _esc(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=False)


def _cut(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[:limit] + "..."


def _truncated(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return f"{text[:limit]}...[truncated {len(text) - limit} chars]"


def _when(ts: Any) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S") if isinstance(ts, datetime) else _esc(ts)


def _one_line(text: str, limit: int) -> str:
    return _esc(_cut(" ".join(text.split()), limit))


def _reason_label(header: Header) -> str:
    if header.summary is not None:
        return "handover"
    if header.reason == f"{_VOLUNTARY} {_FORCED_ROLLOUT}":
        return _FORCED_ROLLOUT
    return _esc(header.reason)


def _call_line(data: Mapping[str, Any]) -> str:
    tool = str(data.get("tool_name") or "?")
    tin = data.get("tool_input")
    tin = tin if isinstance(tin, Mapping) else {}
    path = tin.get("file_path") or tin.get("notebook_path") or tin.get("path")
    if tool == "Bash" and tin.get("command") is not None:
        detail = str(tin["command"])
    elif path:
        detail = str(path)
    else:
        detail = str(tin.get("pattern") or tin.get("url") or tin.get("description") or "")
    return f"- tool {_esc(tool)}: {_one_line(detail, CALL_CHARS)}".rstrip(": ")


def _launch_prompts(rows: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    prompts: dict[str, str] = {}
    for row in rows:
        data = row.get("data") or {}
        if row["type"] == "tool_call" and data.get("tool_name") in _LAUNCH_TOOLS:
            tin = data.get("tool_input")
            prompt = tin.get("prompt") if isinstance(tin, Mapping) else None
            if data.get("tool_use_id") and isinstance(prompt, str):
                prompts[str(data["tool_use_id"])] = prompt
    return prompts


def _in_flight_lines(rows: Sequence[Mapping[str, Any]], in_flight: InFlight) -> list[str]:
    prompts = _launch_prompts(rows)
    running: dict[str, Mapping[str, Any]] = {}
    linked: dict[str, str] = {}  # agent_id -> parent tool_use_id, from `subagent_linked`
    for row in rows:
        data = row.get("data") or {}
        if row["type"] == "subagent_started" and data.get("agent_id"):
            running[str(data["agent_id"])] = row
        elif row["type"] == "subagent_linked" and data.get("parent_tool_use_id"):
            linked[str(data.get("agent_id"))] = str(data["parent_tool_use_id"])
        elif row["type"] == "subagent_stopped":
            running.pop(str(data.get("agent_id")), None)
    lines = ["## In flight", ""]
    lines.append("Background subagents started and not stopped:")
    if not running:
        lines.append("- none")
    for agent_id, row in running.items():
        data = row["data"]
        parent = data.get("parent_tool_use_id") or linked.get(agent_id)
        prompt = prompts.get(str(parent), "")
        first = next(iter(prompt.strip().splitlines()), "")
        text = f"- {_esc(data.get('agent_type') or 'subagent')} {_esc(agent_id)}"
        text += f", started {_when(row.get('ts'))}"
        if first:
            text += f": {_one_line(first, CALL_CHARS)}"
        lines.append(text)
    lines.append(f"Queued messages for the session: {in_flight.queued_messages}")
    lines.append("Held locks: " + (", ".join(_esc(x) for x in in_flight.locks) or "none"))
    return lines


def _body_line(row: Mapping[str, Any], output_chars: int) -> str | None:
    kind, data = row["type"], row.get("data") or {}
    if kind == "gap":
        return "[ingest gap: events are missing here]"
    if kind in ("subagent_started", "subagent_stopped"):
        verb = kind.removeprefix("subagent_")
        who = f"{_esc(data.get('agent_type') or '?')} {verb} ({_esc(data.get('agent_id'))})"
        text = f"- subagent {who}"
        last = data.get("last_assistant_message")
        if kind == "subagent_stopped" and isinstance(last, str) and last:
            text += ": " + _esc(_truncated(last, output_chars))
        return text
    if data.get("agent_id"):
        return None  # sidechain rows are not the predecessor's own chain
    if kind == "compacted":
        return "[context compacted here]"
    if kind in ("user_message", "assistant_text"):
        who = (
            "assistant"
            if kind == "assistant_text"
            else f"user ({_esc(data.get('origin') or 'user')})"
        )
        prose = _esc(_cut(str(data.get("text") or ""), PROSE_CHARS))
        return f"### {_when(row.get('ts'))} {who}\n{prose}"
    if kind == "tool_call":
        return _call_line(data)
    if kind == "tool_result":
        status = "error" if data.get("is_error") else "ok"
        text = _truncated(str(data.get("response") or ""), output_chars)
        return f"  result ({status}): {_esc(text)}"
    if kind == "turn_completed":
        return "[turn ended]"
    return None


def render_transcript(
    header: Header,
    rows: Sequence[Mapping[str, Any]],
    in_flight: InFlight,
    *,
    budget_bytes: int,
    output_chars: int,
) -> str:
    """Markdown: header, "In flight", then the body oldest first, the oldest body lines dropped
    until the UTF-8 size fits `budget_bytes` (header and "In flight" are never dropped)."""
    top = [
        f"# Handover transcript: hive {_esc(header.slug)}",
        "",
        f"- Change: epoch {header.epoch - 1} to {header.epoch}",
        f"- Reason: {_reason_label(header)}",
        f"- Predecessor pod: {_esc(header.predecessor_pod)}",
        f"- Time: {_when(header.changed_at)}",
    ]
    if header.summary is not None:
        top += ["", "## Summary of the previous leader", "", _esc(header.summary)]
    top += [""] + _in_flight_lines(rows, in_flight) + ["", "## Transcript (newest at the end)", ""]
    fixed = "\n".join(top) + "\n"
    body = [line for line in (_body_line(r, output_chars) for r in rows) if line is not None]
    sizes = [len(line.encode()) + 1 for line in body]
    total = len(fixed.encode()) + sum(sizes)
    dropped = 0
    while total > budget_bytes and dropped < len(body):
        total -= sizes[dropped]
        dropped += 1
        # the marker line is re-measured below; loop until the marker fits too
        marker = f"[earlier history omitted ({dropped} events)]\n"
        if total + len(marker.encode()) <= budget_bytes:
            break
    out = fixed
    if dropped:
        out += f"[earlier history omitted ({dropped} events)]\n"
    return out + "".join(line + "\n" for line in body[dropped:])


def _size(n: int) -> str:
    return f"{n // 1024} KiB" if n % 1024 == 0 else f"{n} bytes"


def notice_text(header: Header, budget_bytes: int = 65536) -> str:
    summary = _esc(header.summary) if header.summary is not None else "none (forced move)"
    return (
        f"{HANDOVER_PREFIX} You now lead hive {_esc(header.slug)} (epoch {header.epoch}, "
        f"{_reason_label(header)}), taking over from {_esc(header.predecessor_pod)}. "
        f"Their summary: {summary}. The attached file is their rendered transcript "
        f"(at most {_size(budget_bytes)}, newest at the end): "
        "read it, starting with the 'In flight' section. "
        "Do not restart work that is in flight; check its outputs. "
        "Then continue with the user's goal."
    )

"""Handover file of the predecessor for a leadership handover and the notice text
(voluntary-handover, spec 3.4). Pure: no database, no I/O; the api sweeper calls it with the
generated State section and rows of the `events` table (`id`, `ts`, `type`, `data`, ascending,
main chain of the predecessor, the last `HANDOVER_TURNS` turns only).

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
REREAD_RULE = (
    "Decision and approval states are never taken from a summary or transcript: re-read them "
    "from the source (orchestrator_task_get, the PR, the proposal) before acting."
)
PROSE_CHARS = 2000
CALL_CHARS = 200
ERROR_CHARS = 200
HANDOVER_TURNS = 3
_VOLUNTARY = "handover:"
_FORCED_ROLLOUT = "rollout"
_FORCED = (_FORCED_ROLLOUT, "limit")


@dataclass(frozen=True)
class Header:
    slug: str
    epoch: int
    reason: str
    predecessor_pod: str
    changed_at: datetime
    summary: str | None


def summary_of(reason: str) -> str | None:
    """The leader's summary when the move was voluntary (`handover: x`), else None.
    The reconciler's forced rollout request (`handover: rollout`) is a forced move."""
    if reason.startswith(_VOLUNTARY):
        text = reason[len(_VOLUNTARY) :].strip()
        return None if text in _FORCED else text or None
    return None


def kind_of(reason: str) -> str:
    """`handover` for a voluntary move, `rollout` or `limit` for a forced request, else the reason
    itself (`scale_down`, `leader_lost`, `user`, `initial`)."""
    for forced in _FORCED:
        if reason == f"{_VOLUNTARY} {forced}":
            return forced
    return "handover" if reason.startswith(_VOLUNTARY) else reason


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
    kind = kind_of(header.reason)
    return kind if kind in _FORCED else _esc(header.reason)


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
        if not data.get("is_error"):
            return None
        text = _truncated(str(data.get("response") or ""), ERROR_CHARS)
        return f"  result (error): {_esc(text)}"
    if kind == "turn_completed":
        return "[turn ended]"
    return None


def render_transcript(
    header: Header,
    state_md: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    omitted_turns: int,
    budget_bytes: int,
    output_chars: int,
) -> str:
    """Markdown: header, the State section, the previous leader's judgment, then the last turns
    oldest first. The oldest body lines are dropped until the UTF-8 size fits `budget_bytes`;
    header, State and judgment are never dropped (the State may exceed the budget alone)."""
    judgment = (
        _esc(header.summary)
        if header.summary is not None
        else f"none (forced move: {_esc(kind_of(header.reason))})"
    )
    top = [
        f"# Handover transcript: hive {_esc(header.slug)}",
        "",
        f"- Change: epoch {header.epoch - 1} to {header.epoch}",
        f"- Reason: {_reason_label(header)}",
        f"- Predecessor pod: {_esc(header.predecessor_pod)}",
        f"- Time: {_when(header.changed_at)}",
        "",
        state_md.rstrip("\n"),
        "",
        "## Judgment of the previous leader",
        "",
        judgment,
        "",
        "## Last turns (newest at the end)",
        "",
    ]
    if omitted_turns > 0:
        top.append(
            f"[{omitted_turns} earlier turns omitted; full log: the session events API or"
            " agents-history]"
        )
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


def notice_text(header: Header, budget_bytes: int = 16384) -> str:
    return (
        f"{HANDOVER_PREFIX} You now lead hive {_esc(header.slug)} (epoch {header.epoch}, "
        f"{_reason_label(header)}), taking over from {_esc(header.predecessor_pod)}. "
        f"The attached file (turns capped at {_size(budget_bytes)}) holds a generated State "
        "section as of the change, the previous leader's judgment and its last turns. "
        "Call orchestrator_me first: its leader_state is the live state. "
        f"{REREAD_RULE} "
        "Do not restart work that is in flight; check its outputs. "
        "Then continue with the user's goal."
    )

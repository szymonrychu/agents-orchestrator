"""The generated, factual state of a hive for its leader: open decisions, unhandled incoming
session messages, members, pending leadership requests, rollout, queue depth and held locks.

`collect` reads the database only (the taskboard call is made by the caller and set as
`decisions`); `render_state` is pure. The api sweeper puts the rendering at the top of the
handover file and `GET /agent/v1/me` serves it live to the current leader. Every peer-originated
string goes through `handover_transcript._esc`, so a message text cannot forge a tag.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from agents_orchestrator.db import file_locks
from agents_orchestrator.handover_transcript import CALL_CHARS, _cut, _esc, _one_line
from agents_orchestrator.taskboard_client import OpenDecision

STATE_MESSAGES = 20
STATE_MESSAGE_CHARS = 4000
STATE_MORE_IDS = 100
STATE_DECISIONS = 30
STATE_TEXT_CHARS = 500
REPORT_WINDOW_HOURS = 24
DECISIONS_UNAVAILABLE = "- taskboard unavailable: call orchestrator_task_list(waiting=true)"


@dataclass(frozen=True)
class IncomingMessage:
    id: int
    sender_slug: str | None
    created_at: datetime
    text: str
    total_chars: int
    project_post_id: int | None = None  # set for a project room fan-out row


@dataclass(frozen=True)
class SubagentInfo:
    agent_type: str | None
    prompt_line: str  # first line of the launching prompt, "" when unknown


_LAUNCH_TOOLS = ("Agent", "Task")


@dataclass(frozen=True)
class MemberState:
    pod_name: str
    leader: bool
    state: str
    since: datetime | None
    context_pct: float | None
    context_used_tokens: int | None
    background: dict[str, str]
    last_report_at: datetime | None
    subagents: dict[str, SubagentInfo] = field(default_factory=dict)  # by background subagent id


@dataclass(frozen=True)
class PendingRequest:
    id: int
    kind: str
    reason: str | None
    from_pod: str | None
    to_pod: str | None
    created_at: datetime


@dataclass(frozen=True)
class LeaderState:
    generated_at: datetime
    messages: list[IncomingMessage]
    more_message_ids: list[int]
    more_message_count: int
    members: list[MemberState]
    requests: list[PendingRequest]
    rollout: dict[str, Any] | None
    queued_messages: int
    locks: list[str]
    decisions: list[OpenDecision] | None = None  # None: taskboard unavailable


_UNHANDLED = (
    "FROM messages WHERE session_id = %s AND sender_session_id IS NOT NULL AND notice IS NULL"
    " AND handled_at IS NULL AND status <> 'cancelled'"
)
_UNHANDLED_ROOM_COUNTS = (
    "SELECT p.project, count(*) AS n FROM messages m"
    " JOIN project_posts p ON p.id = m.project_post_id"
    " WHERE m.session_id = %s AND m.sender_session_id IS NOT NULL AND m.notice IS NULL"
    " AND m.handled_at IS NULL AND m.status <> 'cancelled' GROUP BY p.project"
)
_MEMBERS = f"""
    SELECT a.id AS agent_id, a.pod_name, coalesce(l.agent_id = a.id, false) AS leader,
           coalesce(st.state, 'unknown') AS state,
           CASE WHEN st.state = 'idle' THEN st.free_since ELSE t.started_at END AS since,
           st.context_pct, st.context_used_tokens,
           coalesce(st.background_subagents, '{{}}'::jsonb) AS background,
           (SELECT max(e.ts) FROM events e
             WHERE e.session_id = a.session_id AND e.agent_id = l.agent_id
               AND e.type = 'peer_message_received'
               AND e.ts > now() - make_interval(hours => {REPORT_WINDOW_HOURS})
               AND starts_with(e.data->>'from', a.pod_name || '/')) AS last_report_at
    FROM agents a
    LEFT JOIN agent_status st ON st.agent_id = a.id
    LEFT JOIN turns t ON t.turn_id = st.current_turn_id AND t.agent_id = a.id
    LEFT JOIN session_leaders l ON l.session_id = a.session_id
    WHERE a.session_id = %s AND a.gone_at IS NULL
    ORDER BY a.ordinal
"""
_REQUESTS = """
    SELECT r.id, r.kind, r.reason, fa.pod_name AS from_pod, ta.pod_name AS to_pod, r.created_at
    FROM leadership_requests r
    LEFT JOIN agents fa ON fa.id = r.from_agent_id
    LEFT JOIN agents ta ON ta.id = r.to_agent_id
    WHERE r.session_id = %s AND r.status = 'queued' ORDER BY r.id
"""


_SUBAGENT_STARTS = """
    SELECT DISTINCT ON (data->>'agent_id') data->>'agent_id' AS sid, data->>'agent_type' AS kind,
           data->>'parent_tool_use_id' AS parent
    FROM events WHERE session_id = %s AND agent_id = %s AND type = 'subagent_started'
      AND data->>'agent_id' = ANY(%s) ORDER BY data->>'agent_id', id DESC
"""
_SUBAGENT_LINKS = """
    SELECT DISTINCT ON (data->>'agent_id') data->>'agent_id' AS sid,
           data->>'parent_tool_use_id' AS parent
    FROM events WHERE session_id = %s AND agent_id = %s AND type = 'subagent_linked'
      AND data->>'agent_id' = ANY(%s) AND data->>'parent_tool_use_id' IS NOT NULL
    ORDER BY data->>'agent_id', id DESC
"""
_LAUNCH_PROMPTS = """
    SELECT DISTINCT ON (data->>'tool_use_id') data->>'tool_use_id' AS tuid,
           data->'tool_input'->>'prompt' AS prompt
    FROM events WHERE session_id = %s AND agent_id = %s AND type = 'tool_call'
      AND data->>'tool_name' = ANY(%s) AND data->>'tool_use_id' = ANY(%s)
      AND jsonb_typeof(data->'tool_input'->'prompt') = 'string'
    ORDER BY data->>'tool_use_id', id DESC
"""


async def _subagent_infos(
    cur: psycopg.AsyncCursor, session_id: uuid.UUID, agent_id: uuid.UUID, ids: list[str]
) -> dict[str, SubagentInfo]:
    """Agent type and launching-prompt first line of the member's running background subagents,
    from its `subagent_started` / `subagent_linked` events and the launching tool call."""
    await cur.execute(_SUBAGENT_STARTS, (session_id, agent_id, ids))
    started = {r["sid"]: r for r in await cur.fetchall()}
    await cur.execute(_SUBAGENT_LINKS, (session_id, agent_id, ids))
    linked = {r["sid"]: r["parent"] for r in await cur.fetchall()}
    parents = {sid: (row["parent"] or linked.get(sid)) for sid, row in started.items()}
    wanted = sorted({p for p in parents.values() if p})
    prompts: dict[str, str] = {}
    if wanted:
        await cur.execute(_LAUNCH_PROMPTS, (session_id, agent_id, list(_LAUNCH_TOOLS), wanted))
        prompts = {r["tuid"]: r["prompt"] for r in await cur.fetchall()}
    return {
        sid: SubagentInfo(
            row["kind"],
            next(iter((prompts.get(parents[sid] or "", "")).strip().splitlines()), ""),
        )
        for sid, row in started.items()
    }


async def unhandled_room_counts(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID
) -> dict[str, int]:
    """Unhandled project room fan-out rows of the session, per project."""
    async with conn.cursor() as cur:
        await cur.execute(_UNHANDLED_ROOM_COUNTS, (session_id,))
        return {project: n for project, n in await cur.fetchall()}


async def collect(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> LeaderState:
    """The state of the session's hive, read from the database only (`decisions` stays None)."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT id, sender_slug, created_at, text, length(text) AS total_chars, project_post_id"
            f" {_UNHANDLED} ORDER BY id DESC",
            (session_id,),
        )
        newest = await cur.fetchall()
        await cur.execute(_MEMBERS, (session_id,))
        members = await cur.fetchall()
        infos = {
            m["agent_id"]: await _subagent_infos(
                cur, session_id, m["agent_id"], list(m["background"])
            )
            for m in members
            if m["background"]
        }
        await cur.execute(_REQUESTS, (session_id,))
        requests = await cur.fetchall()
        await cur.execute(
            "SELECT status->'rollout' AS rollout, now() AS generated_at FROM sessions"
            " WHERE id = %s",
            (session_id,),
        )
        head = await cur.fetchone()
        await cur.execute(
            "SELECT count(*) AS n FROM messages WHERE session_id = %s AND status = 'queued'",
            (session_id,),
        )
        queued = await cur.fetchone()
    older = sorted(m["id"] for m in newest[STATE_MESSAGES:])
    locks = await file_locks.list_live(conn, session_id)
    return LeaderState(
        generated_at=head["generated_at"] if head else datetime.now().astimezone(),
        messages=[
            IncomingMessage(
                m["id"],
                m["sender_slug"],
                m["created_at"],
                m["text"][:STATE_MESSAGE_CHARS],
                m["total_chars"],
                m["project_post_id"],
            )
            for m in reversed(newest[:STATE_MESSAGES])
        ],
        more_message_ids=older[:STATE_MORE_IDS],
        more_message_count=len(older),
        members=[
            MemberState(
                **{k: v for k, v in m.items() if k != "agent_id"},
                subagents=infos.get(m["agent_id"], {}),
            )
            for m in members
        ],
        requests=[PendingRequest(**r) for r in requests],
        rollout=(head["rollout"] or None) if head else None,
        queued_messages=queued["n"] if queued else 0,
        locks=[f"{lock.path} ({lock.agent})" for lock in locks],
    )


def _when(ts: datetime | None) -> str:
    return "-" if ts is None else ts.strftime("%Y-%m-%d %H:%M:%S")


def _subagent_text(m: MemberState, sid: str, since: str) -> str:
    info = m.subagents.get(sid)
    kind = f" ({_esc(info.agent_type)})" if info and info.agent_type else ""
    prompt = f": {_one_line(info.prompt_line, CALL_CHARS)}" if info and info.prompt_line else ""
    return f"{_esc(sid)}{kind} since {_esc(since)}{prompt}"


def _member_line(m: MemberState) -> str:
    parts = [f"- {_esc(m.pod_name)}{' (leader)' if m.leader else ''}: {_esc(m.state)}"]
    if m.since is not None:
        parts.append(f"since {_when(m.since)}")
    if m.context_pct is not None:
        used = f", {m.context_used_tokens} tokens" if m.context_used_tokens is not None else ""
        parts.append(f"context {m.context_pct:.0f}%{used}")
    if m.background:
        started = ", ".join(_subagent_text(m, k, v) for k, v in m.background.items())
        parts.append(f"background subagents: {started}")
    parts.append(f"last report {_when(m.last_report_at)}")
    return "; ".join(parts)


def _request_line(r: PendingRequest) -> str:
    who = f" from {_esc(r.from_pod)}" if r.from_pod else ""
    who += f" to {_esc(r.to_pod)}" if r.to_pod else ""
    reason = f": {_esc(_cut(r.reason, STATE_TEXT_CHARS))}" if r.reason else ""
    return f"- {r.id} {_esc(r.kind)}{who}, queued {_when(r.created_at)}{reason}"


def _message_block(m: IncomingMessage) -> list[str]:
    room = f", room post p{m.project_post_id}" if m.project_post_id is not None else ""
    heading = f"### message {m.id} from {_esc(m.sender_slug)}{room}, {_when(m.created_at)}"
    lines = [heading, _esc(m.text)]
    if m.total_chars > len(m.text):
        lines.append(f"[truncated at {STATE_MESSAGE_CHARS} of {m.total_chars} chars]")
    return lines


def _decisions_lines(decisions: list[OpenDecision] | None) -> list[str]:
    if decisions is None:
        return [DECISIONS_UNAVAILABLE]
    lines = [
        f"- {_esc(d.task_key)} decision {_esc(d.decision_id)}: "
        f"{_esc(_cut(d.question, STATE_TEXT_CHARS))}"
        for d in decisions[:STATE_DECISIONS]
    ]
    rest = decisions[STATE_DECISIONS:]
    if rest:
        ids = ", ".join(f"{_esc(d.task_key)}:{_esc(d.decision_id)}" for d in rest[:STATE_MORE_IDS])
        extra = f", +{len(rest) - STATE_MORE_IDS} not listed" if len(rest) > STATE_MORE_IDS else ""
        lines.append(f"and {len(rest)} more (ids {ids}{extra})")
    return lines or ["- none"]


def render_state(state: LeaderState) -> str:
    """Markdown starting `## State (generated <time>)`; see the module docstring."""
    out = [f"## State (generated {state.generated_at.isoformat()})", "", "### Open decisions"]
    out += _decisions_lines(state.decisions)
    out += ["", "### Unhandled session messages"]
    if not state.messages and not state.more_message_count:
        out.append("- none")
    for m in state.messages:
        out += _message_block(m)
    if state.more_message_count:
        ids = ", ".join(str(i) for i in state.more_message_ids)
        out.append(f"Older unhandled message ids: {ids}")
        if state.more_message_count > len(state.more_message_ids):
            out.append(f"({state.more_message_count - len(state.more_message_ids)} more)")
    out += ["", "### Members"]
    out += [_member_line(m) for m in state.members] or ["- none"]
    out += ["", "### Pending leadership requests"]
    out += [_request_line(r) for r in state.requests] or ["- none"]
    out += ["", "### Rollout"]
    rollout = state.rollout
    out.append("- none" if not rollout else "- " + _esc(_rollout_text(rollout)))
    out += ["", f"Queued messages: {state.queued_messages}"]
    out.append("Held locks: " + (", ".join(_esc(x) for x in state.locks) or "none"))
    return "\n".join(out) + "\n"


def _rollout_text(rollout: dict[str, Any]) -> str:
    return ", ".join(f"{k} {rollout[k]}" for k in sorted(rollout))

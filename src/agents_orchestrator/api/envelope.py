"""`POST /agent/v1/envelope/stop`, served only on the agent port (S-70, S-71).

The onStop webhook of the claude-code-wrapper: before the agent's turn ends, its own text blocks
of the turn are parsed for envelopes (`ingester.envelopes.REGISTRY`, chat and hive tags). A
malformed envelope, one the caller may not send (`only the leader may send <tag>`; any agent may
send `<agent-send>`), or one the relay refused (an `<agent-send>` to an unknown agent, itself,
over its rate or into a full queue included) blocks the Stop once with a numbered reason so the
agent resends only the corrected ones. Every later Stop of the turn is allowed. When the retry
did not repair a `<chat-send>` failure of the leader and nothing was delivered to the target
room, one `notice` row queues the fixed apology the user sees (`chat_protocol_failed`; a member
posts none). The endpoint never fails on a body it cannot use: the hook answers allow on any
error (`on_error: allow`).
"""

from __future__ import annotations

import logging
import re
import uuid
from dataclasses import dataclass
from typing import Any

import psycopg
from agents_common.envelope import parse
from fastapi import APIRouter, Request
from pydantic import BaseModel

from agents_orchestrator.api.agent import Caller
from agents_orchestrator.api.sessions import Pool
from agents_orchestrator.db import envelope_outbox, leadership
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.db.envelope_outbox import Row
from agents_orchestrator.envelope_outbox.worker import tell_row
from agents_orchestrator.ingester.envelopes import (
    REGISTRY,
    has_marker,
    kind_of,
    needs_leader,
)

log = logging.getLogger(__name__)

router = APIRouter(prefix="/agent/v1/envelope", tags=["agent"])

_ROOM = re.compile(r'\broom="([^"]*)"')
_DELIVERING = ("queued", "sending", "accepted")


class StopOut(BaseModel):
    decision: str = "allow"
    reason: str | None = None


def _blocks(data: dict[str, Any]) -> list[tuple[str, str]]:
    """(message id, text) of every usable block of the payload; anything else is dropped."""
    out: list[tuple[str, str]] = []
    for item in data.get("assistant_texts") or []:
        if isinstance(item, dict) and isinstance(item.get("text"), str):
            out.append((str(item.get("message_id") or ""), item["text"]))
    return out


@dataclass(frozen=True)
class Problem:
    """One envelope of a block that cannot be delivered, before the outbox row is looked up."""

    message_id: str
    index: int
    tag: str
    message: str
    snippet: str = ""


_TAG = re.compile(r"<([a-z]+-[a-z]+)")


def _parse(blocks: list[tuple[str, str]], leads: bool) -> tuple[list[Problem], list[str]]:
    """(problems, tags of the deliverable envelopes) over the blocks."""
    problems: list[Problem] = []
    valid: list[str] = []
    for message_id, text in blocks:
        if not has_marker(text):
            continue
        parsed = parse(text, REGISTRY)
        for err in parsed.errors:
            found = _TAG.match(err.snippet)
            tag = found.group(1) if found else "chat-send"
            problems.append(Problem(message_id, err.index, tag, err.message, err.snippet))
        for env in parsed.envelopes:
            if needs_leader(env.tag) and not leads:
                msg = f"only the leader may send <{env.tag}>"
            else:
                valid.append(env.tag)
                continue
            problems.append(Problem(message_id, env.index, env.tag, msg))
    return problems, valid


def _ref(rows: list[dict[str, Any]], p: Problem) -> str:
    """`a<outbox id> ` of the one row of this envelope (same kind and index, narrowed to its
    assistant message when rows carry one), or '' when it is not exactly one: never a guess."""
    if p.index is None:
        return ""
    kind = kind_of(p.tag)
    same = [r for r in rows if r["envelope_index"] == p.index and r["kind"] == kind]
    if len(same) > 1 and p.message_id:
        same = [r for r in same if r.get("message_id") == p.message_id]
    return f"a{same[0]['id']} " if len(same) == 1 else ""


def _line(n: int, rows: list[dict[str, Any]], p: Problem) -> str:
    where = f" of message {p.message_id}" if p.message_id else ""
    return f"{n}) {_ref(rows, p)}envelope {p.index}{where}: {p.message}"


def _refused_line(n: int, row: dict[str, Any]) -> str:
    index = row["envelope_index"]
    what = f"envelope {index}" if index is not None else "an envelope"
    return f"{n}) a{row['id']} {what}: refused ({row['error_code'] or 'refused'})"


def _reason(
    problems: list[Problem], refused: list[dict[str, Any]], rows: list[dict[str, Any]]
) -> str:
    lines = [_line(i, rows, p) for i, p in enumerate(problems, 1)]
    lines += [_refused_line(len(lines) + i, r) for i, r in enumerate(refused, 1)]
    return (
        "Some envelopes were not delivered: "
        + " ".join(lines)
        + " The others were delivered; do not resend them. Resend only the corrected ones now."
    )


def _target_room(problems: list[Problem], refused: list[dict[str, Any]]) -> str | None:
    """The room the failed envelopes named; None when none did (the connector's default)."""
    for p in problems:
        found = _ROOM.search(p.snippet)
        if found:
            return found.group(1)
    for row in refused:
        if row["room"]:
            return row["room"]
    return None


async def _check(conn: psycopg.AsyncConnection, agent_id: uuid.UUID, turn_id: str):
    cur = await conn.execute(
        "SELECT blocked_at, blocks_seen FROM envelope_stop_checks"
        " WHERE agent_id = %s AND turn_id = %s",
        (agent_id, turn_id),
    )
    return await cur.fetchone()


async def _record_allowed(conn: psycopg.AsyncConnection, agent_id: uuid.UUID, turn_id: str) -> None:
    await conn.execute(
        "INSERT INTO envelope_stop_checks (agent_id, turn_id, allowed_at) VALUES (%s, %s, now())"
        " ON CONFLICT (agent_id, turn_id) DO UPDATE SET allowed_at = now()",
        (agent_id, turn_id),
    )


async def _record_blocked(
    conn: psycopg.AsyncConnection, agent_id: uuid.UUID, turn_id: str, blocks_seen: int
) -> None:
    await conn.execute(
        "INSERT INTO envelope_stop_checks (agent_id, turn_id, blocked_at, blocks_seen)"
        " VALUES (%s, %s, now(), %s)"
        " ON CONFLICT (agent_id, turn_id) DO UPDATE SET blocked_at = now(), blocks_seen = %s,"
        " allowed_at = NULL",
        (agent_id, turn_id, blocks_seen, blocks_seen),
    )


def _delivering(rows: list[dict[str, Any]], room: str | None) -> bool:
    return any(
        r["kind"] in ("send", "notice") and r["status"] in _DELIVERING and r["room"] == room
        for r in rows
    )


async def _tell_missed(
    conn: psycopg.AsyncConnection, agent_id: uuid.UUID, turn_id: str, seen: list[dict[str, Any]]
) -> None:
    """Close the race between the read of the turn's rows and the allowed record: a row the
    relay refused in between saw no ended turn (no notice) and is in no Stop reason. Once the
    allowed record is committed the relay does tell, so only rows refused earlier can be lost;
    re-read and send the relay's own notice for each refused row this Stop did not see. A
    duplicate notice is possible, a lost one is not."""
    known = {r["id"] for r in seen if r["status"] == "refused"}
    for row in await envelope_outbox.rows_for_turn(conn, agent_id, turn_id):
        if row["status"] == "refused" and row["id"] not in known:
            await tell_row(conn, row, "refused", row["error_code"] or "refused")


async def decide(
    conn: psycopg.AsyncConnection,
    session_id: uuid.UUID,
    agent_id: uuid.UUID,
    body: dict[str, Any],
) -> StopOut:
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    turn = data.get("turn_id") or body.get("turn_id")
    turn_id = str(turn) if turn else None
    blocks = _blocks(data)
    if turn_id is None:
        return StopOut()
    session = await sessions_repo.get(conn, session_id)
    leads = session is not None and await leadership.leads(
        conn, session_id, agent_id, alone=session.spec.alone
    )
    if not blocks:
        await _record_allowed(conn, agent_id, turn_id)
        return StopOut()
    check = await _check(conn, agent_id, turn_id)
    rows = await envelope_outbox.rows_for_turn(conn, agent_id, turn_id)
    refused = [r for r in rows if r["status"] == "refused"]
    if check is None or check[0] is None:
        problems, _ = _parse(blocks, leads)
        if not problems and not refused:
            await _record_allowed(conn, agent_id, turn_id)
            await _tell_missed(conn, agent_id, turn_id, rows)
            return StopOut()
        await _record_blocked(conn, agent_id, turn_id, len(blocks))
        return StopOut(decision="block", reason=_reason(problems, refused, rows))
    # a later Stop of the turn: always allowed; judge the chat retry
    seen = check[1]
    first, _ = _parse(blocks[:seen], leads)
    later, later_valid = _parse(blocks[seen:], leads)
    first_chat = [p for p in first if p.tag == "chat-send"]
    later_chat = [p for p in later if p.tag == "chat-send"]
    refused_chat = [r for r in refused if r["kind"] == "send"]
    later_valid_chat = [t for t in later_valid if t.startswith("chat-")]
    failed = bool(later_chat) or (bool(first_chat or refused_chat) and not later_valid_chat)
    await _record_allowed(conn, agent_id, turn_id)
    await _tell_missed(conn, agent_id, turn_id, rows)
    if not failed:
        return StopOut()
    room = _target_room(later_chat or first_chat, refused_chat)
    already_told = any(r["kind"] == "notice" for r in rows)
    notify = leads and not already_told and not _delivering(rows, room)
    if notify:
        await envelope_outbox.insert_rows(
            conn,
            [
                Row(
                    session_id=session_id,
                    kind="notice",
                    agent_id=agent_id,
                    turn_id=turn_id,
                    room=room,
                )
            ],
        )
    log.warning(
        "chat_protocol_failed agent=%s turn=%s errors=%d refused=%d notice=%s",
        agent_id,
        turn_id,
        len(first_chat) + len(later_chat),
        len(refused_chat),
        notify,
    )
    return StopOut()


@router.post(
    "/stop",
    response_model=StopOut,
    response_model_exclude_none=True,
    summary="onStop webhook: block once on envelopes that were not delivered",
)
async def stop(request: Request, agent: Caller, pool: Pool) -> StopOut:
    """Never fails on a body it cannot use: an unreadable one is an allow."""
    try:
        body = await request.json()
    except ValueError:
        return StopOut()
    if not isinstance(body, dict):
        return StopOut()
    async with pool.connection() as conn:
        return await decide(conn, agent.session_id, agent.id, body)

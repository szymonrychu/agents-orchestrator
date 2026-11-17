"""`POST /agent/v1/envelope/stop`, served only on the agent port (S-70).

The onStop webhook of the claude-code-wrapper: before the agent's turn ends, its own text blocks
of the turn are parsed for chat envelopes (`agents_common.envelope`). A malformed envelope, or one
the chat refused, blocks the Stop once with a numbered reason so the agent resends only the
corrected ones. Every later Stop of the turn is allowed. When the retry did not repair the
failure and nothing was delivered to the target room, one `notice` row queues the fixed apology
the user sees (`chat_protocol_failed`). The endpoint never fails on a body it cannot use: the
hook answers allow on any error (`on_error: allow`).
"""

from __future__ import annotations

import logging
import re
import uuid
from typing import Any

import psycopg
from agents_common.envelope import CHAT_TAGS, EnvelopeError, parse
from fastapi import APIRouter, Request
from pydantic import BaseModel

from agents_orchestrator.api.agent import Caller
from agents_orchestrator.api.sessions import Pool
from agents_orchestrator.db import envelope_outbox, leadership
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.db.envelope_outbox import Row

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


def _parse(blocks: list[tuple[str, str]]) -> tuple[list[tuple[str, EnvelopeError]], int]:
    """(parse errors with their message id, number of valid envelopes) over the blocks."""
    errors: list[tuple[str, EnvelopeError]] = []
    valid = 0
    for message_id, text in blocks:
        if "<chat-" not in text:
            continue
        parsed = parse(text, CHAT_TAGS)
        valid += len(parsed.envelopes)
        errors.extend((message_id, e) for e in parsed.errors)
    return errors, valid


def _line(n: int, message_id: str, err: EnvelopeError) -> str:
    where = f" of message {message_id}" if message_id else ""
    return f"{n}) envelope {err.index}{where}: {err.message}"


def _refused_line(n: int, row: dict[str, Any]) -> str:
    index = row["envelope_index"]
    what = f"envelope {index}" if index is not None else "an envelope"
    return f"{n}) {what}: refused by the chat ({row['error_code'] or 'refused'})"


def _reason(errors: list[tuple[str, EnvelopeError]], refused: list[dict[str, Any]]) -> str:
    lines = [_line(i, mid, e) for i, (mid, e) in enumerate(errors, 1)]
    lines += [_refused_line(len(lines) + i, r) for i, r in enumerate(refused, 1)]
    return (
        "Some chat envelopes were not delivered: "
        + " ".join(lines)
        + " The others were delivered; do not resend them. Resend only the corrected ones now."
    )


def _target_room(
    errors: list[tuple[str, EnvelopeError]], refused: list[dict[str, Any]]
) -> str | None:
    """The room the failed envelopes named; None when none did (the connector's default)."""
    for _, err in errors:
        found = _ROOM.search(err.snippet)
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
        " ON CONFLICT (agent_id, turn_id) DO UPDATE SET blocked_at = now(), blocks_seen = %s",
        (agent_id, turn_id, blocks_seen, blocks_seen),
    )


def _delivering(rows: list[dict[str, Any]], room: str | None) -> bool:
    return any(
        r["kind"] in ("send", "notice") and r["status"] in _DELIVERING and r["room"] == room
        for r in rows
    )


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
    if not blocks or not leads:
        await _record_allowed(conn, agent_id, turn_id)
        return StopOut()
    check = await _check(conn, agent_id, turn_id)
    rows = await envelope_outbox.rows_for_turn(conn, agent_id, turn_id)
    refused = [r for r in rows if r["status"] == "refused"]
    if check is None or check[0] is None:
        errors, _ = _parse(blocks)
        if not errors and not refused:
            await _record_allowed(conn, agent_id, turn_id)
            return StopOut()
        await _record_blocked(conn, agent_id, turn_id, len(blocks))
        return StopOut(decision="block", reason=_reason(errors, refused))
    # a later Stop of the turn: always allowed; judge the retry
    seen = check[1]
    first_errors, _ = _parse(blocks[:seen])
    later_errors, later_valid = _parse(blocks[seen:])
    failed = bool(later_errors) or (bool(first_errors or refused) and later_valid == 0)
    await _record_allowed(conn, agent_id, turn_id)
    if not failed:
        return StopOut()
    room = _target_room(later_errors or first_errors, refused)
    already_told = any(r["kind"] == "notice" for r in rows)
    if not already_told and not _delivering(rows, room):
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
        len(first_errors) + len(later_errors),
        len(refused),
        not already_told and not _delivering(rows, room),
    )
    return StopOut()


@router.post(
    "/stop",
    response_model=StopOut,
    response_model_exclude_none=True,
    summary="onStop webhook: block once on chat envelopes that were not delivered",
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

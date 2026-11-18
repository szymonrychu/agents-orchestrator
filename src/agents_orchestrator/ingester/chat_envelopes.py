"""Chat envelopes in an agent's own text, projected to `envelope_outbox` (S-70).

A fresh main-chain `assistant_text` is parsed with `agents_common.envelope`; each envelope
becomes a `queued` row and each parse error an `invalid` row, in the transaction that stores the
event. A caller that does not lead its session gets `invalid` `not_leader` rows (the same rule as
the chat relay). Rows are keyed by (agent, wrapper session, event seq, index), so a replay of the
event inserts nothing.
"""

from __future__ import annotations

import logging
import re
import uuid
from typing import Any

import psycopg
from agents_common.envelope import CHAT_TAGS, Envelope, EnvelopeError, parse

from agents_orchestrator.db import envelope_outbox, leadership
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.db.envelope_outbox import Row

_ERROR_TAG = re.compile(r"<chat-react\b")

log = logging.getLogger(__name__)


def is_main_chain(data: dict[str, Any]) -> bool:
    """Missing `sidechain` and `agent_id` (an older wrapper) count as the main chain."""
    return not data.get("sidechain") and data.get("agent_id") is None


def _row(env: Envelope, common: dict[str, Any]) -> Row:
    attrs = env.attrs
    if env.tag == "chat-react":
        return Row(
            kind="react",
            envelope_index=env.index,
            reply_to=attrs.get("to"),
            react_key=attrs.get("key"),
            **common,
        )
    files = attrs.get("files")
    return Row(
        kind="send",
        envelope_index=env.index,
        room=attrs.get("room"),
        reply_to=attrs.get("reply_to"),
        files=tuple(f for f in files.split(",") if f) if files else None,
        body=env.body,
        **common,
    )


def _error_row(err: EnvelopeError, common: dict[str, Any]) -> Row:
    # err.snippet is the opening tag (EnvelopeError has no tag field): prefix match is reliable.
    kind = "react" if _ERROR_TAG.match(err.snippet) else "send"
    return Row(
        kind=kind,
        envelope_index=err.index,
        status="invalid",
        error_code="parse_error",
        error=err.message,
        **common,
    )


async def project_assistant_text(
    conn: psycopg.AsyncConnection,
    *,
    session_id: uuid.UUID,
    agent_id: uuid.UUID,
    wrapper_session_id: str,
    event_seq: int,
    data: dict[str, Any],
) -> None:
    text = data.get("text")
    if not isinstance(text, str) or "<chat-" not in text or not is_main_chain(data):
        return
    parsed = parse(text, CHAT_TAGS)
    if not parsed.envelopes and not parsed.errors:
        return
    turn_id = data.get("turn_id")
    common: dict[str, Any] = {
        "session_id": session_id,
        "agent_id": agent_id,
        "wrapper_session_id": wrapper_session_id,
        "event_seq": event_seq,
        "turn_id": str(turn_id) if turn_id else None,
    }
    rows = [_row(e, common) for e in parsed.envelopes] + [
        _error_row(e, common) for e in parsed.errors
    ]
    session = await sessions_repo.get(conn, session_id)
    if session is None or not await leadership.leads(
        conn, session_id, agent_id, alone=session.spec.alone
    ):
        rows = [
            Row(
                **{
                    **common,
                    "kind": r.kind,
                    "envelope_index": r.envelope_index,
                    "status": "invalid",
                    "error_code": "not_leader",
                    "error": "only the leader of a session can use the chat",
                }
            )
            for r in rows
        ]
    await envelope_outbox.insert_rows(conn, rows)
    if common["turn_id"] is None:
        # Without a turn the Stop gate cannot see the row and no notice can reach the agent.
        for r in rows:
            if r.status == "invalid":
                log.warning(
                    "invalid envelope row without turn_id agent_id=%s event_seq=%s"
                    " envelope_index=%s error_code=%s",
                    agent_id,
                    event_seq,
                    r.envelope_index,
                    r.error_code,
                )

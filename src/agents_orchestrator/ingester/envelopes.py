"""Envelopes in an agent's own text, projected to `envelope_outbox` (S-70, S-71).

A fresh main-chain `assistant_text` is parsed with `agents_common.envelope` over the chat and
hive tags (`REGISTRY`, one index space); each envelope becomes a `queued` row and each parse
error an `invalid` row, in the transaction that stores the event. The chat tags, `<session-send>`
and `<project-post>` need the session's leadership: a caller that does not lead gets `invalid`
`not_leader` rows for those tags only. `<agent-send>` is not routed yet (`not_supported`). Rows
are keyed by (agent, wrapper session, event seq, index), so a replay of the event inserts
nothing.
"""

from __future__ import annotations

import logging
import re
import uuid
from typing import Any

import psycopg
from agents_common.envelope import (
    CHAT_TAGS,
    HIVE_TAGS,
    Envelope,
    EnvelopeError,
    merge_registries,
    parse,
)

from agents_orchestrator.db import envelope_outbox, leadership
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.db.envelope_outbox import Row

REGISTRY = merge_registries(CHAT_TAGS, HIVE_TAGS)
MARKERS = tuple(f"<{family}" for family in REGISTRY.families)

_ERROR_TAG = re.compile(r"<([a-z]+-[a-z]+)")

log = logging.getLogger(__name__)


def has_marker(text: str) -> bool:
    """The fast path: True when the text may hold an envelope of a registered family."""
    return any(m in text for m in MARKERS)


def kind_of(tag: str) -> str:
    """The outbox kind of a tag, by its family for an unknown tag of a known family."""
    if tag == "chat-react":
        return "react"
    if tag.startswith("session-"):
        return "session"
    if tag.startswith("agent-"):
        return "agent"
    if tag.startswith("project-"):
        return "project_post"
    return "send"


def needs_leader(tag: str) -> bool:
    return not tag.startswith("agent-")


def is_main_chain(data: dict[str, Any]) -> bool:
    """Missing `sidechain` and `agent_id` (an older wrapper) count as the main chain."""
    return not data.get("sidechain") and data.get("agent_id") is None


def _split(value: str | None) -> tuple[str, ...] | None:
    return tuple(v for v in value.split(",") if v) if value else None


def _row(env: Envelope, common: dict[str, Any]) -> Row:
    attrs = env.attrs
    kind = kind_of(env.tag)
    if kind == "react":
        return Row(
            kind=kind,
            envelope_index=env.index,
            reply_to=attrs.get("to"),
            react_key=attrs.get("key"),
            **common,
        )
    if kind == "send":
        return Row(
            kind=kind,
            envelope_index=env.index,
            room=attrs.get("room"),
            reply_to=attrs.get("reply_to"),
            files=_split(attrs.get("files")),
            body=env.body,
            **common,
        )
    if kind == "agent":
        return Row(
            kind=kind,
            envelope_index=env.index,
            status="invalid",
            error_code="not_supported",
            error="agent-send is not available yet",
            **common,
        )
    handled = _split(attrs.get("handled"))
    reply_to = attrs.get("reply_to")
    return Row(
        kind=kind,
        envelope_index=env.index,
        target=attrs.get("to"),
        lane=attrs.get("lane"),
        handled=tuple(int(h) for h in handled) if handled else None,
        project=attrs.get("project"),
        reply_to=reply_to.removeprefix("p") if reply_to else None,
        body=env.body,
        **common,
    )


def _error_tag(err: EnvelopeError) -> str:
    # err.snippet is the opening tag (EnvelopeError has no tag field): prefix match is reliable.
    match = _ERROR_TAG.match(err.snippet)
    return match.group(1) if match else "chat-send"


def _error_row(err: EnvelopeError, common: dict[str, Any]) -> Row:
    return Row(
        kind=kind_of(_error_tag(err)),
        envelope_index=err.index,
        status="invalid",
        error_code="parse_error",
        error=err.message,
        **common,
    )


def _not_leader(r: Row, tag: str, common: dict[str, Any]) -> Row:
    return Row(
        **common,
        kind=r.kind,
        envelope_index=r.envelope_index,
        status="invalid",
        error_code="not_leader",
        error=f"only the leader of a session may send <{tag}>",
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
    if not isinstance(text, str) or not has_marker(text) or not is_main_chain(data):
        return
    parsed = parse(text, REGISTRY)
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
    tagged = [(e.tag, _row(e, common)) for e in parsed.envelopes] + [
        (_error_tag(e), _error_row(e, common)) for e in parsed.errors
    ]
    if any(needs_leader(tag) for tag, _ in tagged):
        session = await sessions_repo.get(conn, session_id)
        leads = session is not None and await leadership.leads(
            conn, session_id, agent_id, alone=session.spec.alone
        )
        if not leads:
            tagged = [
                (tag, _not_leader(r, tag, common) if needs_leader(tag) else r) for tag, r in tagged
            ]
    rows = [r for _, r in tagged]
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

"""Cooperative context-pressure handoff state machine (T-423, spec 5.10): `handoff_pass`,
dispatcher process only.

Per session, under the session's claim advisory lock (the one `leadership_pass` takes; skipped
when another dispatcher holds it) and in one transaction, every open `agent_handoffs` row of the
session moves at most along its phases and every limit subject (hive leader or solo agent, see
`hive._subject_rows`) at L1 or above without an open row gets one. Every transition is a
status-guarded update; an enqueue happens in a savepoint with the guarded update it belongs to,
so a lost race leaves no orphan message, command or request.

Nothing here forces anything on an agent because of its context (decision 716): no deadline, no
request for a handoff, no capture of reply text, no interrupt. A `clear` command or a `limit`
leadership request is queued only after the agent submitted a handoff that was accepted with
its in-flight gate clear (`api/agent.handoff_submit`), and only while the gate is still clear.
The pressure itself is the policy the push loop puts to the wrapper (`hive._push_pressure`).

1. No open row, level >= L1, agent live and not stopping: `open_pressure` (`pressuring`, the
   profile detected). While a row is open before `moving` its level follows the gauge up
   (`set_level`).
2. pressuring: nothing is forced. An idle agent (no turn, no background subagent, no running
   background task in its inventory) gets one regular-lane nudge per level >= 2 (`nudge`). At
   L3 (pressuring; or submitted with `handoff_max_rejects` rejects or a blocked gate) the row is
   escalated once per cycle (`escalate`): a session message to every live superleader session
   and, through `notify_escalations`, a taskboard notice to the user. Neither clears, moves or
   interrupts anything.
3. submitted: the wrapper's in-flight inventory is fetched on every pass before the lock
   (bounded by `handoff_inflight_timeout_seconds`; on a timeout the session is skipped this
   pass) and judged by `handoff_gate.evaluate` with the submit's `abandon` list.
   - blocked: the inventory is stored with its `waiting_on` (the policy says `blocked`).
   - blocked before, clear now: stored with `resubmit` (the policy tells the agent to submit
     again, results may change the handoff); the row stays until the agent re-submits.
   - accepted clear (the route stored a non-blocking inventory) and still clear: a hive leader
     with an eligible successor gets a `handover` request with reason `limit` (to the submit's
     `to` when that agent is eligible), `moving`; a solo agent, or a hive leader that is the only
     live agent, gets a `clear` once it is idle (no current turn, no background subagent, no
     running background task in the inventory), `clearing`.
4. moving: the leadership pass's `handoffs.on_leader_moved` carries the row to `discussing`. A
   `limit` request rejected while the agent still leads falls back to the solo order (a clear
   once idle and the gate clear); one rejected after leadership moved closes the row
   `cancelled not_leader`.
5. discussing (hive ex-leader): cleared only when the session is quiet (`_QUIET`;
   `quiet_cap_seconds` after the move only drops the session-quiet requirement, never the
   ex-leader's own: past the cap it is still cleared only once idle) AND its own in-flight gate
   is clear; until then it keeps its policy (`accepted`, with `waiting_on`). Only the row's own
   `limit` move or the leader's voluntary handover reaches this phase: a rollout or user move
   closes the row before it (`handoffs.on_leader_moved`), so it is never cleared. An ex-leader
   that leads again (a 2-agent hive's ping-pong, a user pick, a leader_lost move) has its row
   closed `cancelled leader_again` in that move, so it is never cleared while it leads; so does
   a `clearing` row whose clear is still queued (the clear cancelled with it).
6. clearing: a `session_cleared` after the clear closes the row `done cleared` (hive order) or,
   solo, queues the accepted handoff back (`handoff_texts.return_text`) with the move to
   `returning`, exactly once. A clear refused with `subagents_running` or lost (no
   `session_cleared` after `clear_timeout_seconds` idle) is queued again, MAX_CLEAR_ATTEMPTS in
   all, and only while no background subagent runs and the gate is clear; an ex-leader leading
   again is never re-cleared (its row closes `cancelled leader_again`); any other failure, or
   the last attempt, closes the row `cancelled clear_failed` (solo: the handoff still comes back,
   worded for a kept context); no new row opens for that agent for
   `clear_failed_block_seconds` unless its context is cleared or compacted first.
7. returning: closed `done returned` once the message is sent and its turn ended; until then the
   policy carries `resume_pending` and the resume text (`hive._pressure_targets`).

A transition of a `submitted` row is guarded on the `submitted_at` the pass read
(`seen_submit`): a resubmit committed since, blocked or accepted, makes it lose, so neither a
move nor a clear follows a refused submit, and a fresh accepted submit is never overwritten.

A `moving` row (its `limit` request not applied yet) closes on a context reset like the
phases before it, its queued request rejected `superseded` with it (row-116 class).

Before `moving`: a `compacted` event since the row opened, or the gauge back below L1, closes
the row `done compacted` (a voluntary row is not judged by the gauge); a `session_cleared` by
anyone `cancelled cleared`; no longer a limit subject `cancelled not_leader`; a voluntary row
still `pressuring` below L1 (a submit that never landed) `cancelled stale`. In any phase: the
agent gone or stopping `cancelled shutdown`, the session closing `cancelled session_closing`.
Queued nudges and the queued return of a closed row are cancelled with it.

The claim (`db/queue._handoff_gate`) holds the agent's other messages only while a solo row is
`clearing`/`returning` (and while a shutdown row waits for its clear), so the clear and the
return run with nothing in between.

`shutdown` rows belong to the pod-shutdown path (plan 5.8) and keep their flow.
"""

from __future__ import annotations

import asyncio
import logging
import math
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import httpx
import psycopg
from psycopg.rows import dict_row

from agents_orchestrator import (
    handoff_gate,
    handoff_package,
    handoff_profile,
    handoff_quality,
    handoff_texts,
    taskboard_client,
)
from agents_orchestrator.api.scope import scope_of
from agents_orchestrator.db import handoffs, leadership, queue
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.db.eligibility import (
    BACKGROUND_BUSY,
    ELIGIBLE,
    HOLDS_SESSION,
    background_busy,
)
from agents_orchestrator.db.queue import _CLAIM_LOCK_NAMESPACE, StoredAttachment
from agents_orchestrator.db.sessions import quiet_clock_sql
from agents_orchestrator.dispatcher import hive
from agents_orchestrator.handoff_texts import shutdown_successor
from agents_orchestrator.settings import Settings
from agents_orchestrator.wrapper.client import WrapperClient
from agents_orchestrator.wrapper.models import (
    Inflight,
    WrapperError,
    WrapperTimeout,
    WrapperUnreachable,
)

log = logging.getLogger(__name__)

LIMIT_REASON = handoffs.LIMIT_REASON
MAX_CLEAR_ATTEMPTS = 5
BEFORE_MOVING = ("pressuring", "submitted")
# A context reset before `moving` closes the row, with this reason. `compacting` alone is not
# one: a PreCompact whose compaction then fails leaves the context as it was.
RESET_EVENTS = {"compacted": "compacted", "session_cleared": "cleared"}
# The phases whose pass reads the wrapper's in-flight inventory.
GATED = ("submitted", "moving", "discussing", "clearing")

# (session id, agent url, wrapper session id) -> the inventory, None when the wrapper did not
# answer usably (the gate then blocks); raises TimeoutError when it did not answer in time.
InflightFetch = Callable[[uuid.UUID, str, str], Awaitable[Inflight | None]]


@dataclass(frozen=True)
class HandoffPolicy:
    quiet_seconds: int
    quiet_cap_seconds: int
    clear_timeout_seconds: int
    clear_failed_block_seconds: int
    inflight_timeout_seconds: int

    @classmethod
    def from_settings(cls, settings: Settings) -> HandoffPolicy:
        return cls(
            settings.handover_quiet_seconds,
            settings.handover_quiet_cap_seconds,
            settings.handover_clear_timeout_seconds,
            settings.handover_clear_failed_block_seconds,
            settings.handoff_inflight_timeout_seconds,
        )


def inflight_fetcher(
    client_factory: Callable[[uuid.UUID, str], WrapperClient], timeout: float
) -> InflightFetch:
    """The wrapper's `GET /inflight` through `client_factory` (the dispatcher's clients)."""

    async def fetch(session_id: uuid.UUID, url: str, wrapper_session_id: str) -> Inflight | None:
        client = client_factory(session_id, url)
        try:
            async with asyncio.timeout(timeout):
                return await client.get_inflight(wrapper_session_id, timeout=timeout)
        except WrapperTimeout as exc:
            raise TimeoutError from exc
        except (WrapperError, WrapperUnreachable):
            return None

    return fetch


# A stopping mark older than this is not looked at for a shutdown handoff: the routine reports
# within the pod grace, and the pass runs every few seconds.
SHUTDOWN_WINDOW_SECONDS = 3600

# The agent's newest `shutdown_handoff` with a handoff since its stopping mark that no shutdown
# row was opened for yet (aliases `a` agents, `s` its agent_status row): its text, else NULL.
_SHUTDOWN_TEXT = f"""
SELECT e.data->>'final_text' FROM events e
WHERE e.agent_id = a.id AND e.type = 'shutdown_handoff' AND e.created_at >= s.stopping_since
  AND {handoffs.SHUTDOWN_TEXT}
  AND NOT EXISTS (SELECT 1 FROM agent_handoffs h WHERE h.agent_id = a.id
      AND h.trigger = 'shutdown' AND h.created_at >= e.created_at)
ORDER BY e.id DESC LIMIT 1
"""
_STOPPING = (
    "s.stopping_since IS NOT NULL AND s.stopping_since > now() - make_interval(secs => %(window)s)"
)

# Sessions a pass can do something for: active unparked hive sessions with a leader, active
# unparked solo sessions with a live agent, any session with an open row (closing ones included,
# to cancel it) and any session with a shutdown handoff to store. A parked session (T-224) opens
# no new handoff: its queue is held.
_SESSIONS = f"""
SELECT c.id FROM sessions c WHERE c.desired = 'active' AND c.parked_at IS NULL AND (
    ({leadership.HIVE} AND EXISTS (SELECT 1 FROM session_leaders l WHERE l.session_id = c.id))
    OR ({leadership.SOLO} AND EXISTS (
        SELECT 1 FROM agents a WHERE a.session_id = c.id AND a.gone_at IS NULL)))
UNION
SELECT h.session_id FROM agent_handoffs h WHERE h.phase = ANY(%(open)s)
UNION
SELECT a.session_id FROM agents a JOIN agent_status s ON s.agent_id = a.id
WHERE {_STOPPING} AND EXISTS ({_SHUTDOWN_TEXT})
ORDER BY 1
"""

# The session's agents with a shutdown handoff to store, oldest mark first.
_SHUTDOWNS = f"""
SELECT a.id AS agent_id, s.stopping_since, ({_SHUTDOWN_TEXT}) AS text
FROM agents a JOIN agent_status s ON s.agent_id = a.id
WHERE a.session_id = %(session)s AND {_STOPPING}
ORDER BY s.stopping_since, a.id
"""

# The open, gated rows of a session whose agent can be asked for its inventory.
_GATED_AGENTS = """
SELECT h.agent_id, a.url, a.wrapper_session_id FROM agent_handoffs h
JOIN agents a ON a.id = h.agent_id
WHERE h.session_id = %s AND h.phase = ANY(%s) AND h.trigger <> 'shutdown'
  AND a.gone_at IS NULL
"""

# Pressuring rows of a session that may get an idle nudge this pass (level 2 or more, not nudged
# at it yet, the agent idle by its status): their agent's inventory is read for background
# tasks. A failed or slow read only skips the nudge, never the pass.
_NUDGE_AGENTS = f"""
SELECT h.agent_id, a.url, a.wrapper_session_id FROM agent_handoffs h
JOIN agents a ON a.id = h.agent_id
LEFT JOIN agent_status s ON s.agent_id = a.id
WHERE h.session_id = %s AND h.phase = 'pressuring' AND h.trigger <> 'shutdown'
  AND h.level >= 2 AND h.nudged_level < h.level AND a.gone_at IS NULL
  AND coalesce(s.state = 'idle' AND s.current_turn_id IS NULL AND NOT {BACKGROUND_BUSY}, false)
"""

# The live superleader sessions (`spec.session_messaging`) besides `%s`, the escalated
# session. A superleader's scope is every project (`api/scope.scope_of`), so each covers it.
_SUPERLEADERS = """
SELECT c.id FROM sessions c
WHERE c.desired = 'active' AND c.id <> %s
  AND COALESCE((c.spec->>'session_messaging')::boolean, false)
  AND EXISTS (SELECT 1 FROM agents a WHERE a.session_id = c.id AND a.gone_at IS NULL)
ORDER BY c.id
"""
ESCALATION_LABEL = "context-pressure"

# What the pass needs to know about one agent. `idle`: idle, no turn, no background subagent.
# `compact_stale`: a compaction event newer than the last `agent_status` write (the gauge still
# shows the pre-compaction context). `clear_failed`: the agent's last row closed `clear_failed`
# less than `block` seconds ago and its context was neither cleared nor compacted since.
_FACTS = f"""
SELECT a.id, a.gone_at IS NOT NULL AS gone, {HOLDS_SESSION} AS live,
    s.stopping_since IS NOT NULL AS stopping, s.stopping_since,
    coalesce({BACKGROUND_BUSY}, false) AS background_busy,
    coalesce(s.state = 'idle' AND s.current_turn_id IS NULL AND NOT {BACKGROUND_BUSY}, false)
        AS idle,
    EXISTS (SELECT 1 FROM events e WHERE e.agent_id = a.id AND e.type = 'compacted'
        AND e.created_at > coalesce(s.updated_at, '-infinity')) AS compact_stale,
    coalesce((SELECT h.close_reason = 'clear_failed'
        AND h.closed_at > now() - make_interval(secs => %(block)s) AND NOT EXISTS (
            SELECT 1 FROM events e WHERE e.agent_id = a.id AND e.created_at > h.closed_at
            AND e.type = ANY(%(context_reset)s))
        FROM agent_handoffs h WHERE h.agent_id = a.id AND h.closed_at IS NOT NULL
        ORDER BY h.closed_at DESC LIMIT 1), false) AS clear_failed
FROM agents a LEFT JOIN agent_status s ON s.agent_id = a.id
WHERE a.id = %(agent)s
"""

_QUIET = f"""
SELECT
    NOT EXISTS (
        SELECT 1 FROM agents a LEFT JOIN agent_status st ON st.agent_id = a.id
        WHERE a.session_id = c.id AND a.ready AND a.gone_at IS NULL
          AND (st.state IS DISTINCT FROM 'idle' OR st.current_turn_id IS NOT NULL
               OR st.waiting_since IS NOT NULL OR {background_busy("st")}))
    AND NOT EXISTS (
        SELECT 1 FROM messages m
        WHERE m.session_id = c.id AND m.status IN ('queued', 'dispatching', 'verifying'))
    AND GREATEST(
        h.discuss_since,
        {quiet_clock_sql("c")},
        (SELECT max(GREATEST(st.free_since, st.waiting_since))
           FROM agents a JOIN agent_status st ON st.agent_id = a.id
           WHERE a.session_id = c.id AND a.gone_at IS NULL),
        (SELECT max(t.completed_at) FROM agents a JOIN turns t ON t.agent_id = a.id
           WHERE a.session_id = c.id)
    ) <= now() - make_interval(secs => %(quiet)s) AS quiet,
    coalesce(h.discuss_since <= now() - make_interval(secs => %(cap)s), false) AS capped
FROM agent_handoffs h JOIN sessions c ON c.id = h.session_id
WHERE h.id = %(row)s
"""

# `timed_out`: the agent has sat idle, with no turn and no background subagent, for `timeout`
# seconds with no `session_cleared` since the clear was accepted (`done`: a wrapper or claude
# restart lost it), claimed (`running`: the dispatcher died before completing it) or queued (no
# dispatcher claimed it). Measured from the later of that mark and the agent's last free mark,
# since the wrapper runs /clear only after a running turn's Stop.
_CLEAR_STATE = f"""
SELECT k.status, k.result,
    EXISTS (SELECT 1 FROM events e WHERE e.agent_id = k.agent_id
        AND e.type = 'session_cleared' AND e.created_at >= k.created_at) AS cleared,
    coalesce(k.status IN ('queued', 'running', 'done') AND s.state = 'idle'
        AND s.current_turn_id IS NULL AND NOT {BACKGROUND_BUSY}
        AND GREATEST(coalesce(k.finished_at, k.started_at, k.created_at), s.free_since)
            <= now() - make_interval(secs => %(timeout)s),
        false) AS timed_out,
    (SELECT coalesce(e.data->'record'->>'error', e.data->'record'->>'state', 'failed')
       FROM events e WHERE e.agent_id = k.agent_id AND e.type = 'command_failed'
         AND e.created_at >= k.created_at
         AND e.data->'record'->>'id' = k.result->'response'->>'id'
       ORDER BY e.id DESC LIMIT 1) AS wrapper_error
FROM commands k LEFT JOIN agent_status s ON s.agent_id = k.agent_id WHERE k.id = %(command)s
"""

# The submit's preferred successor when it is eligible (aliases as in `leadership._SUCCESSOR`).
_TARGET = f"""
SELECT a.id FROM agents a LEFT JOIN agent_status s ON s.agent_id = a.id
JOIN sessions c ON c.id = a.session_id
WHERE a.session_id = %(session)s AND a.pod_name = %(pod)s AND a.id <> %(other)s AND {ELIGIBLE}
"""


def _hive_order(row: handoffs.Handoff) -> bool:
    """The row's handoff went to a successor, so nothing comes back after the clear: a leader's
    discussion (`discuss_since` set) or a leader's shutdown row with its successor recorded."""
    return row.discuss_since is not None or (
        row.trigger == "shutdown" and row.successor_agent_id is not None
    )


def _snapshot(
    inflight: Inflight | None, gate: handoff_gate.GateResult, *, resubmit: bool = False
) -> dict[str, Any]:
    """The inventory as the submit route stores it, plus the gate's verdict."""
    snap = inflight.model_dump(mode="json") if inflight is not None else {"unknown": True}
    snap = {**snap, "waiting_on": gate.waiting_on, "abandoned": gate.abandoned}
    if resubmit:
        snap["resubmit"] = True
    return snap


class _Lost(Exception):
    """A guarded update lost to a concurrent writer: roll back the savepoint's enqueue."""


async def _one(conn: psycopg.AsyncConnection, query: str, params: Any) -> dict | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        return await cur.fetchone()


async def _all(conn: psycopg.AsyncConnection, query: str, params: Any) -> list[dict]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        return await cur.fetchall()


def _reasons(row: handoffs.Handoff) -> list[str]:
    """The row's reject reasons, each once, in first-seen order."""
    return list(dict.fromkeys(str(r) for r in row.reject_reasons or []))


async def escalation_text(
    conn: psycopg.AsyncConnection,
    row: handoffs.Handoff,
    level: int,
    pct: float | None,
    waiting_on: list[str],
) -> str:
    """`handoff_texts.escalation_text` for the row: session slug, agent pod, level, context %,
    reject reasons and what it waits on."""
    got = await _one(
        conn,
        "SELECT c.slug, a.pod_name FROM agents a JOIN sessions c ON c.id = a.session_id"
        " WHERE a.id = %s",
        (row.agent_id,),
    )
    slug = got["slug"] if got else str(row.session_id)
    pod = (got["pod_name"] if got else None) or str(row.agent_id)
    return handoff_texts.escalation_text(slug, pod, level, pct, _reasons(row), waiting_on)


async def handoff_sessions(conn: psycopg.AsyncConnection) -> list[uuid.UUID]:
    """The sessions the dispatcher runs `handoff_pass` for on this wake (one query)."""
    async with conn.cursor() as cur:
        await cur.execute(
            _SESSIONS, {"open": list(handoffs.OPEN_PHASES), "window": SHUTDOWN_WINDOW_SECONDS}
        )
        return [r[0] for r in await cur.fetchall()]


class _Pass:
    """One pass over one session, inside its transaction and lock."""

    def __init__(
        self,
        conn: psycopg.AsyncConnection,
        session: dict,
        policy: HandoffPolicy,
        settings: Settings,
        inflight: dict[uuid.UUID, Inflight | None],
        nudge_inflight: dict[uuid.UUID, Inflight | None] | None = None,
        taskboard: handoff_package.TaskboardFetch | None = None,
        chat: handoff_package.ChatFetch | None = None,
    ) -> None:
        self.taskboard = taskboard or handoff_package.default_taskboard(settings)
        self.chat = chat or handoff_package.default_chat(settings)
        self.conn = conn
        self.session = session
        self.policy = policy
        self.settings = settings
        self.inflight = inflight
        # the inventories read for idle-nudge candidates (absent: not read or not in time)
        self.nudge_inflight = nudge_inflight or {}
        self.out: list[str] = []

    @property
    def active(self) -> bool:
        return bool(self.session["desired"] == "active")

    async def facts(self, agent_id: uuid.UUID) -> dict:
        row = await _one(
            self.conn,
            _FACTS,
            {
                "agent": agent_id,
                "block": self.policy.clear_failed_block_seconds,
                "context_reset": list(RESET_EVENTS),
            },
        )
        return row or {
            "gone": True,
            "live": False,
            "stopping": False,
            "stopping_since": None,
            "background_busy": False,
            "idle": False,
            "compact_stale": False,
            "clear_failed": False,
        }

    async def message(
        self,
        text: str,
        agent_id: uuid.UUID,
        notice: str | None,
        attachments: list[StoredAttachment] | None = None,
    ) -> int:
        return await queue.enqueue_message(
            self.conn,
            self.session["id"],
            text,
            agent_id,
            lane="regular",
            notice=notice,
            attachments=attachments or [],
        )

    async def package(self, row: handoffs.Handoff) -> handoff_package.Package:
        """The successor package of the row (the claimed tasks and room notes asked through
        the pass's fetchers, each bounded by its client's own timeout)."""
        session = await sessions_repo.get(self.conn, self.session["id"])
        assert session is not None
        return await handoff_package.build_package(
            self.conn, row, session, self.settings, taskboard=self.taskboard, chat=self.chat
        )

    def shutdown_rejected(self, row: handoffs.Handoff) -> bool:
        """A pod-shutdown text that fails the quality check is not delivered as the handoff."""
        return row.handoff is None and bool(
            handoff_quality.check_text(row.handoff_text or "", self.settings)
        )

    async def guarded(self, work: Callable[[], Awaitable[bool]]) -> bool:
        """Run `work` (enqueues plus the guarded row update it returns the outcome of) in a
        savepoint: when the update loses, the enqueued message, command or request is rolled
        back with it, so a lost race never leaves an orphan behind."""
        try:
            async with self.conn.transaction():
                if not await work():
                    raise _Lost
        except _Lost:
            return False
        return True

    def _pass_policy(self) -> leadership.PassPolicy:
        return leadership.PassPolicy(
            self.settings.default_auto_compact_pct,
            self.settings.rollout_max_defer_seconds,
            self.settings.handover_max_wait_seconds,
        )

    def gate(self, row: handoffs.Handoff) -> handoff_gate.GateResult:
        """The in-flight gate of the row's agent as of this pass (no answer: blocked)."""
        abandon = [
            SimpleNamespace(item=str(a.get("item") or ""), why=str(a.get("why") or ""))
            for a in (row.handoff or {}).get("abandon") or []
        ]
        return handoff_gate.evaluate(self.inflight.get(row.agent_id), abandon)

    def background_tasks(self, agent_id: uuid.UUID) -> bool:
        got = self.inflight.get(agent_id)
        return got is None or bool(got.background_tasks)

    @staticmethod
    def seen_submit(row: handoffs.Handoff) -> datetime | None:
        """The submit a `submitted` row's transition is judged on: the update loses when the
        agent submitted again since this pass read the row (the route takes no lock)."""
        return row.submitted_at if row.phase == "submitted" else None

    async def store_inflight(self, row: handoffs.Handoff, snap: dict[str, Any]) -> bool:
        """Keep the latest inventory on the row (only when it changed: `progress_at` moves)."""
        if snap == (row.inflight or {}):
            return False
        return await handoffs.advance(
            self.conn,
            row.id,
            [row.phase],
            row.phase,
            seen_submit=self.seen_submit(row),
            inflight=snap,
        )

    # ------------------------------------------------------------ 1. open

    async def open(self, subject: dict) -> None:
        agent_id = subject["agent_id"]
        level = hive.LEVEL_NUMBER[hive._row_level(subject, self.settings)[1]]
        if level < 1:
            return
        facts = await self.facts(agent_id)
        if not facts["live"] or facts["stopping"] or facts["compact_stale"]:
            return
        if facts["clear_failed"]:
            return  # its last cycle could not clear: none again until a clear or compaction
        tenure = float(subject["tenure"] or 0)  # 0: the tenure start is unknown
        profile = await handoff_profile.detect(
            self.conn,
            {"id": self.session["id"], "spec": subject["spec"]},
            agent_id,
            datetime.now(UTC) - timedelta(seconds=tenure) if tenure > 0 else None,
        )
        try:
            async with self.conn.transaction():
                row = await handoffs.open_pressure(
                    self.conn,
                    self.session["id"],
                    agent_id,
                    level,
                    profile,
                    used_tokens=subject["context_used_tokens"],
                )
        except psycopg.errors.UniqueViolation:
            return  # a row opened since the pass read them (the submit route's voluntary row)
        log.info("handoff %s: agent %s under pressure (level %s)", row.id, agent_id, level)
        self.out.append(f"pressuring:{agent_id}")
        await self.escalate(row, level, subject)  # opened right at L3

    async def _now(self) -> Any:
        row = await _one(self.conn, "SELECT now() AS t", ())
        assert row is not None
        return row["t"]

    # ------------------------------------------------------------ closing

    async def close(
        self, row: handoffs.Handoff, phase: str, reason: str, *, messages: bool = True
    ) -> None:
        """Close the row; with `messages`, its queued nudges and return are cancelled too, and a
        clear of it still queued (so a dying pod is not cleared after its row closed)."""
        if not await handoffs.close(self.conn, row.id, phase, reason):  # type: ignore[arg-type]
            return
        for message_id in await handoffs.queued_messages(self.conn, row.id) if messages else []:
            await queue.cancel_message(self.conn, message_id)
        if messages and row.clear_command_id is not None:
            await queue.cancel_queued_command(self.conn, row.clear_command_id)
        log.info("handoff %s: agent %s %s (%s)", row.id, row.agent_id, phase, reason)
        self.out.append(f"{phase}:{row.agent_id}:{reason}")

    async def cancel(self, row: handoffs.Handoff, reason: str, *, messages: bool = True) -> None:
        await self.close(row, "cancelled", reason, messages=messages)

    def _below_l1(self, subject: dict) -> bool:
        """The gauge alone is below L1: infinite tenure and no rollout, so `none` means below L1
        by context (with no start context, by the percent fallback)."""
        used, pct = subject["context_used_tokens"], subject["context_pct"]
        if used is None and pct is None:
            return False  # no reading: nothing to judge by
        lv = hive._row_level(subject, self.settings)[0]
        level = hive.due_level(
            lv, used, subject["context_window_tokens"], pct, math.inf, 0, rollout_pending=False
        )
        return level == "none"

    async def _reset_since(self, row: handoffs.Handoff) -> str | None:
        """The close reason of a context reset (compaction or a /clear from anyone) since the
        row opened, if any. Before `moving` the row has no clear of its own."""
        found = await _one(
            self.conn,
            "SELECT e.type FROM events e, agent_handoffs h WHERE h.id = %s"
            " AND e.agent_id = h.agent_id AND e.type = ANY(%s) AND e.created_at >= h.created_at"
            " ORDER BY e.id LIMIT 1",
            (row.id, list(RESET_EVENTS)),
        )
        return RESET_EVENTS[found["type"]] if found is not None else None

    async def close_reason(
        self, row: handoffs.Handoff, subject: dict | None, facts: dict
    ) -> tuple[str, str] | None:
        """(phase, reason) to close the row with, or None."""
        if not self.active:
            return "cancelled", "session_closing"
        if facts["gone"] or self._sigterm_since(row, facts):
            return "cancelled", "shutdown"
        if row.phase == "moving":
            # its limit request still waits (or was rejected): a reset first makes the handoff
            # stale, so the row closes and the request is rejected `superseded` with it
            reset = await self._reset_since(row)
            if reset is None:
                return None
            return ("done", "compacted") if reset == "compacted" else ("cancelled", reset)
        if row.phase not in BEFORE_MOVING:
            return None
        if subject is None:
            return "cancelled", "not_leader"
        reset = await self._reset_since(row)
        if reset == "compacted":
            return "done", "compacted"
        if reset is not None:
            return "cancelled", reset
        level = hive.LEVEL_NUMBER[hive._row_level(subject, self.settings)[1]]
        if row.trigger != "voluntary" and self._below_l1(subject):
            return "done", "compacted"
        if row.trigger == "voluntary" and row.phase == "pressuring" and level < 1:
            return "cancelled", "stale"  # a voluntary submit that never landed
        return None

    @staticmethod
    def _sigterm_since(row: handoffs.Handoff, facts: dict) -> bool:
        """The agent's pod got SIGTERM while the row is open: before `moving` any stopping mark,
        later one set after the row opened (a shutdown row's own pod set its mark before it).
        A `returning` row stays: its return waits for the next pod (the claim takes nothing
        for a stopping agent) and goes out there."""
        if not facts["stopping"]:
            return False
        if row.phase in BEFORE_MOVING:
            return True
        return row.phase != "returning" and facts["stopping_since"] >= row.created_at

    # ------------------------------------------------------------ the phases

    async def step(self, row: handoffs.Handoff, subject: dict | None) -> None:
        facts = await self.facts(row.agent_id)
        if row.trigger == "shutdown" and row.phase == "handed":
            await self.shutdown_handed(row, facts)
            return
        closing = await self.close_reason(row, subject, facts)
        if closing is not None:
            await self.close(row, *closing)
            return
        if subject is not None and row.phase in BEFORE_MOVING:
            level = hive.LEVEL_NUMBER[hive._row_level(subject, self.settings)[1]]
            if level > row.level and await handoffs.set_level(
                self.conn, row.id, level, subject["context_used_tokens"]
            ):
                self.out.append(f"level:{row.agent_id}:{level}")
            level = max(level, row.level)
            await self.escalate(row, level, subject)
            await self.nudge(row, level, facts)
        if row.phase == "submitted":
            await self.submitted(row, subject, facts)
        elif row.phase == "moving":
            await self.moving(row, facts)
        elif row.phase == "discussing":
            await self.discussing(row, facts)
        elif row.phase == "clearing":
            await self.clearing(row, facts)
        elif row.phase == "returning":
            await self.returning(row)

    # ------------------------------------------------------------ escalation and nudge

    async def escalate(self, row: handoffs.Handoff, level: int, subject: dict) -> None:
        """Decision 717: an agent at L3 that does not hand off is reported once per cycle
        (`mark_escalated`) to every live superleader session (a regular-lane session message,
        notice `handoff_escalation`) and, through `notify_escalations`, to the user on the
        taskboard. Due at L3 while `pressuring`, or `submitted` with `handoff_max_rejects`
        rejects or the gate blocked. Nothing is cleared, moved or interrupted: at the compact
        point the steered auto-compaction is the fallback."""
        if row.escalated_at is not None or level < 3 or row.trigger == "shutdown":
            return
        waiting_on = list((row.inflight or {}).get("waiting_on") or [])
        if row.phase == "submitted":
            gate = self.gate(row)
            if gate.clear and row.rejects < self.settings.handoff_max_rejects:
                return  # accepted and still clear: the move or the clear follows
            waiting_on = gate.waiting_on
        elif row.phase != "pressuring":
            return
        text = await escalation_text(self.conn, row, level, subject["context_pct"], waiting_on)
        targets = await _all(self.conn, _SUPERLEADERS, (self.session["id"],))

        async def send() -> bool:
            ids = []
            for target in targets:
                try:
                    ids.append(
                        await queue.enqueue_message(
                            self.conn,
                            target["id"],
                            text,
                            lane="regular",
                            notice="handoff_escalation",
                        )
                    )
                except queue.SessionBeingDeleted:
                    continue  # that superleader is going away; the others and the user remain
            return await handoffs.mark_escalated(self.conn, row.id, ids)

        if await self.guarded(send):
            log.info(
                "handoff %s: agent %s escalated to %d superleader session(s)",
                row.id,
                row.agent_id,
                len(targets),
            )

    async def nudge(self, row: handoffs.Handoff, level: int, facts: dict) -> None:
        """One regular-lane message per level >= 2 (`mark_nudged`) to an idle agent whose row is
        `pressuring`: no turn, no background subagent and no running background task in the
        inventory read this pass. A busy agent gets its pressure from the wrapper; an idle
        context does not grow, so one message per level is all."""
        if row.phase != "pressuring" or level < 2 or row.nudged_level >= level:
            return
        if row.trigger == "shutdown" or not facts["idle"]:
            return
        inventory = self.nudge_inflight.get(row.agent_id)
        if inventory is None or inventory.background_tasks:
            return  # not read, unusable or background work running: no nudge this pass

        async def send() -> bool:
            await self.message(handoff_texts.nudge_text(level), row.agent_id, "handoff_nudge")
            return await handoffs.mark_nudged(self.conn, row.id, level)

        if await self.guarded(send):
            log.info("handoff %s: idle agent %s nudged at level %s", row.id, row.agent_id, level)

    async def submitted(self, row: handoffs.Handoff, subject: dict | None, facts: dict) -> None:
        gate = self.gate(row)
        before = row.inflight or {}
        if not gate.clear:
            if await self.store_inflight(row, _snapshot(self.inflight.get(row.agent_id), gate)):
                self.out.append(f"blocked:{row.agent_id}")
            return
        if before.get("waiting_on") or before.get("resubmit"):
            # blocked at the submit or since: the agent re-submits (results may change it)
            snap = _snapshot(self.inflight.get(row.agent_id), gate, resubmit=True)
            if await self.store_inflight(row, snap):
                self.out.append(f"resubmit:{row.agent_id}")
            return
        # accepted with the gate clear, and still clear
        if subject is not None and not subject["solo"]:
            leader = await leadership.current_leader(self.conn, self.session["id"])
            if (
                leader is not None
                and leader.agent_id == row.agent_id
                and await self.request_move(row, leader)
            ):
                return
        await self.clear_when_idle(row, ["submitted"], facts)

    async def request_move(self, row: handoffs.Handoff, leader: leadership.Leader) -> bool:
        """A `limit` handover to the submit's `to` (when eligible) or the pass's pick; False
        when no successor exists (the leader is the only live agent: the solo order)."""
        session_id = self.session["id"]
        to = (row.handoff or {}).get("to")
        target = None
        if to:
            got = await _one(
                self.conn, _TARGET, {"session": session_id, "pod": to, "other": row.agent_id}
            )
            target = got["id"] if got is not None else None
        if target is None and (
            await leadership.successor(self.conn, session_id, row.agent_id, self._pass_policy())
            is None
        ):
            return False

        async def move() -> bool:
            request_id = await leadership.insert_request(
                self.conn, session_id, "handover", row.agent_id, target, LIMIT_REASON, leader.epoch
            )
            return await handoffs.advance(
                self.conn,
                row.id,
                ["submitted"],
                "moving",
                seen_submit=self.seen_submit(row),
                leadership_request_id=request_id,
            )

        if await self.guarded(move):
            log.info("handoff %s: limit handover requested for agent %s", row.id, row.agent_id)
            self.out.append(f"moving:{row.agent_id}")
        return True

    async def clear_when_idle(
        self, row: handoffs.Handoff, from_phases: list[str], facts: dict
    ) -> None:
        """The solo order: a clear once the agent is idle (no turn, no background subagent, no
        background task in this pass's inventory). The caller checked the gate."""
        if facts["idle"] and not self.background_tasks(row.agent_id):
            await self.clear(row, from_phases)

    async def moving(self, row: handoffs.Handoff, facts: dict) -> None:
        got = await _one(
            self.conn,
            "SELECT status FROM leadership_requests WHERE id = %s",
            (row.leadership_request_id,),
        )
        if got is not None and got["status"] == "queued":
            return  # the leadership pass decides; its apply moves the row on
        leader = await leadership.current_leader(self.conn, self.session["id"])
        if leader is None or leader.agent_id != row.agent_id:
            await self.cancel(row, "not_leader")  # moved by something else
            return
        # rejected while still leading: the solo order, gate permitting
        gate = self.gate(row)
        if not gate.clear:
            await self.store_inflight(row, _snapshot(self.inflight.get(row.agent_id), gate))
            return
        await self.clear_when_idle(row, ["moving"], facts)

    async def clear(self, row: handoffs.Handoff, from_phases: list[str]) -> None:
        async def start() -> bool:
            command_id = await queue.enqueue_command(self.conn, row.agent_id, "clear")
            return await handoffs.start_clear(
                self.conn, row.id, from_phases, command_id, seen_submit=self.seen_submit(row)
            )

        if await self.guarded(start):
            log.info("handoff %s: clearing agent %s", row.id, row.agent_id)
            label = "clear_retry" if from_phases == ["clearing"] else "clearing"
            self.out.append(f"{label}:{row.agent_id}")

    async def discussing(self, row: handoffs.Handoff, facts: dict) -> None:
        gate = self.gate(row)
        snap = _snapshot(self.inflight.get(row.agent_id), gate)
        if not gate.clear:
            if await self.store_inflight(row, snap):
                self.out.append(f"blocked:{row.agent_id}")
            return
        got = await _one(
            self.conn,
            _QUIET,
            {
                "row": row.id,
                "quiet": self.policy.quiet_seconds,
                "cap": self.policy.quiet_cap_seconds,
            },
        )
        # the cap drops only the session-quiet requirement: the ex-leader's own turn boundary
        # always holds, with no cap (no clear lands on a working agent, decision 716)
        idle = facts["idle"] and not self.background_tasks(row.agent_id)
        if got is not None and (got["quiet"] or got["capped"]) and idle:
            await self.clear(row, ["discussing"])
        elif (row.inflight or {}).get("waiting_on"):
            await self.store_inflight(row, snap)  # its blockers are done: the policy says so

    async def clearing(self, row: handoffs.Handoff, facts: dict) -> None:
        state = await _one(
            self.conn,
            _CLEAR_STATE,
            {"command": row.clear_command_id, "timeout": self.policy.clear_timeout_seconds},
        )
        if state is None:
            error: str | None = "command_missing"
        elif state["cleared"]:
            await self.cleared(row)
            return
        elif state["status"] == "failed":
            error = str((state["result"] or {}).get("error") or "failed")
        elif state["status"] == "cancelled":
            error = "cancelled"
        elif state["wrapper_error"] is not None:
            error = state["wrapper_error"]
        else:
            error = "clear_timeout" if state["timed_out"] else None
        if error is None:
            return
        if error == "clear_timeout":
            await self.drop_lost_clear(row.clear_command_id)
        retryable = "subagents_running" in error or error == "clear_timeout"
        if retryable and row.clear_attempts < MAX_CLEAR_ATTEMPTS:
            if await self.leads_again(row):
                await self.cancel(row, "leader_again")  # its retry would wipe the lead's context
                return
            gate_clear = row.trigger == "shutdown" or self.gate(row).clear
            if not facts["background_busy"] and gate_clear:
                await self.clear(row, ["clearing"])
            return
        log.info("handoff %s: clear of agent %s failed (%s)", row.id, row.agent_id, error)
        if not _hive_order(row):
            await self.send_return(row, cleared=False)  # the solo agent still gets its handoff
        await self.cancel(row, "clear_failed", messages=False)

    async def leads_again(self, row: handoffs.Handoff) -> bool:
        """An ex-leader's discussion row whose agent leads again (a user pick, a leader_lost
        move or its successor's own limit move): its context now holds the lead's handoff."""
        if row.discuss_since is None or row.trigger == "shutdown":
            return False
        leader = await leadership.current_leader(self.conn, self.session["id"])
        return leader is not None and leader.agent_id == row.agent_id

    async def drop_lost_clear(self, command_id: int | None) -> None:
        """Finish a clear that timed out, still queued or running, as `failed` with the retryable
        `clear_timeout` (never `cancelled`, which reads as non-retryable on a later pass),
        status-guarded, so it never runs after its retry or the return; a late completion of a
        running one then changes nothing."""
        if command_id is not None:
            await queue.fail_unfinished_command(self.conn, command_id, {"error": "clear_timeout"})

    async def cleared(self, row: handoffs.Handoff) -> None:
        if _hive_order(row):  # nothing comes back
            if await handoffs.advance(
                self.conn, row.id, ["clearing"], "clearing", cleared_at=await self._now()
            ) and await handoffs.close(self.conn, row.id, "done", "cleared"):
                log.info("handoff %s: agent %s cleared", row.id, row.agent_id)
                self.out.append(f"done:{row.agent_id}")
            return
        if await self.send_return(row, cleared=True):
            self.out.append(f"returning:{row.agent_id}")

    async def return_message(
        self, row: handoffs.Handoff, *, cleared: bool
    ) -> tuple[str, list[StoredAttachment] | None]:
        """What goes back to a solo agent: after its clear the successor package of the accepted
        handoff (resume list first, the inventory attached as `handoff-<row id>.md`); after a
        failed clear (the context was kept) the handoff for reference; a pod-shutdown text as
        today, unless it failed the quality check (then the package, with the reasons)."""
        if self.shutdown_rejected(row) or (row.handoff is not None and cleared):
            package = await self.package(row)
            attachment = StoredAttachment(
                f"handoff-{row.id}.md", "text/markdown", package.attachment.encode()
            )
            return package.text, [attachment]
        if row.handoff is not None:
            return handoff_texts.return_text(row.handoff, cleared=cleared), None
        text = row.handoff_text or ""
        why = "pod shutdown" if row.trigger == "shutdown" else "handoff"
        if not cleared:
            return f"Handoff from your current context ({why}, the clear failed):\n\n{text}", None
        return f"Handoff from your previous context ({why}, cleared just now):\n\n{text}", None

    async def send_return(self, row: handoffs.Handoff, *, cleared: bool) -> bool:
        """Queue the handoff back to the agent with the move to `returning` (exactly once)."""

        async def send() -> bool:
            text, attachments = await self.return_message(row, cleared=cleared)
            message_id = await self.message(text, row.agent_id, "handoff_return", attachments)
            fields: dict[str, Any] = {"return_message_id": message_id}
            if cleared:
                fields["cleared_at"] = await self._now()
                # a leader that kept the lead (L3, rejected limit, shutdown) starts afresh
                await leadership.reset_start_context(self.conn, self.session["id"], row.agent_id)
            return await handoffs.advance(self.conn, row.id, ["clearing"], "returning", **fields)

        return await self.guarded(send)

    # ------------------------------------------------------------ pod shutdown

    async def shutdown(self, agent_id: uuid.UUID, stopping_since: Any, text: str) -> None:
        """Store the agent's pod-shutdown handoff (its `shutdown_handoff` event; plan 5.8).

        Whatever row is open closes `cancelled shutdown` (a `returning` row keeps its queued
        return, which then follows this one on the next pod). The new row opens `handed` with
        the text. A member (L2) closes it at once `done member`: its next pod resumes as today.
        A leader's text goes to its successor and a solo agent's back to itself on its next pod
        (`shutdown_handed`)."""
        row = await handoffs.open_row(self.conn, agent_id)
        if row is not None:
            await self.cancel(row, "shutdown", messages=row.phase != "returning")
        role = await self._shutdown_role(agent_id, stopping_since)
        new = await handoffs.insert(
            self.conn, self.session["id"], agent_id, "shutdown", "handed", handoff_text=text
        )
        if role == "member":
            await handoffs.close(self.conn, new.id, "done", "member")
            log.info("handoff %s: member %s shutdown handoff stored only", new.id, agent_id)
            self.out.append(f"shutdown_member:{agent_id}")
            return
        log.info("handoff %s: agent %s shutdown handoff stored (%s)", new.id, agent_id, role)
        self.out.append(f"shutdown_handed:{agent_id}")
        await self.shutdown_handed(new, await self.facts(agent_id))

    async def _shutdown_role(self, agent_id: uuid.UUID, stopping_since: Any) -> str:
        """`solo` (a non-hive session), `leader` (it leads, or it led until a move made within
        the pod grace before its stopping mark: leader_lost or scale_down) or `member`."""
        got = await _one(
            self.conn,
            f"SELECT {leadership.SOLO} AS solo,"
            " l.agent_id = %(agent)s OR (l.previous_agent_id = %(agent)s"
            "  AND l.since >= %(since)s - make_interval(secs => %(grace)s)) AS led"
            " FROM sessions c LEFT JOIN session_leaders l ON l.session_id = c.id"
            " WHERE c.id = %(session)s",
            {
                "agent": agent_id,
                "since": stopping_since,
                "grace": self.settings.agent_termination_grace_seconds,
                "session": self.session["id"],
            },
        )
        if got is not None and got["solo"]:
            return "solo"
        return "leader" if got is not None and got["led"] else "member"

    async def shutdown_handed(self, row: handoffs.Handoff, facts: dict) -> None:
        """A shutdown row waits in `handed` for the agent's next pod.

        Leadership moved off the agent: the new leader is recorded as successor, whose
        handover notice then leads with the text (`deliver_handover`), or the pass sends it on
        its own when that notice already went out. Once the next pod is ready and attached
        (live, not stopping): `clear`, then nothing more when a successor took the text, else
        (solo, or a leader that kept the lead) the text comes back after the clear. A gone
        agent (scaled away) closes `done gone` once nothing is left to deliver."""
        if not self.active:
            await self.cancel(row, "session_closing")
            return
        if self._sigterm_since(row, facts):
            await self.cancel(row, "shutdown")  # the next pod is stopping again
            return
        leader = await leadership.current_leader(self.conn, self.session["id"])
        if (
            row.successor_agent_id is None
            and leader is not None
            and leader.agent_id != row.agent_id
            and await handoffs.advance(
                self.conn, row.id, ["handed"], "handed", successor_agent_id=leader.agent_id
            )
        ):
            row = replace(row, successor_agent_id=leader.agent_id)
        pending = await self.deliver(row, leader)
        if facts["gone"]:
            if not pending and await handoffs.close(self.conn, row.id, "done", "gone"):
                self.out.append(f"done:{row.agent_id}")
            return
        if facts["live"] and not facts["stopping"]:
            await self.clear(row, ["handed"])

    async def deliver(self, row: handoffs.Handoff, leader: Any) -> bool:
        """The successor's copy of a shutdown text not delivered yet; True while the notice
        sweeper may still lead the successor's notice with it (its epoch is not served).

        Once that notice went out without it, the text is sent to the successor here, marked
        delivered in the same savepoint (`mark_delivered` takes it once, against the sweeper
        too). A successor that no longer leads gets nothing (logged)."""
        if row.successor_agent_id is None or row.delivered_at is not None:
            return False
        if leader is None or leader.agent_id != row.successor_agent_id:
            log.info("handoff %s: successor no longer leads, text not delivered", row.id)
            return False
        served = await _one(
            self.conn,
            "SELECT handover_notice_epoch >= %s AS served FROM sessions WHERE id = %s",
            (leader.epoch, self.session["id"]),
        )
        if not (served and served["served"]):
            return True
        pod = await _one(self.conn, "SELECT pod_name FROM agents WHERE id = %s", (row.agent_id,))
        assert row.handoff_text is not None

        async def send() -> bool:
            if self.shutdown_rejected(row):
                package = await self.package(row)
                text = package.text
                attachments = [
                    StoredAttachment(
                        f"handoff-{row.id}.md", "text/markdown", package.attachment.encode()
                    )
                ]
            else:
                pod_name = pod["pod_name"] if pod else str(row.agent_id)
                text, attachments = shutdown_successor(pod_name, row.handoff_text), None
            await self.message(text, leader.agent_id, None, attachments)
            return await handoffs.mark_delivered(self.conn, row.id)

        if await self.guarded(send):
            log.info("handoff %s: shutdown text sent to successor %s", row.id, leader.agent_id)
            self.out.append(f"handoff_sent:{leader.agent_id}")
        return False

    async def return_turn_running(self, row: handoffs.Handoff, msg: dict | None) -> bool:
        """The turn the return message started has not ended: the agent's current turn is it."""
        turn = await _one(
            self.conn,
            "SELECT m.turn_id IS NOT NULL AND s.current_turn_id = m.turn_id AS running"
            " FROM messages m LEFT JOIN agent_status s ON s.agent_id = %s WHERE m.id = %s",
            (row.agent_id, row.return_message_id),
        )
        return bool(turn and turn["running"])

    async def returning(self, row: handoffs.Handoff) -> None:
        msg = await _one(
            self.conn, "SELECT status FROM messages WHERE id = %s", (row.return_message_id,)
        )
        status = msg["status"] if msg is not None else "failed"
        if status == "sent":
            if await self.return_turn_running(row, msg):
                return  # the policy keeps asking for the resume check until the turn ends
            if await handoffs.close(self.conn, row.id, "done", "returned"):
                log.info("handoff %s: agent %s got its handoff back", row.id, row.agent_id)
                self.out.append(f"done:{row.agent_id}")
        elif status in ("failed", "cancelled"):
            await self.cancel(row, "return_failed")


async def _inventories(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, fetch: InflightFetch | None
) -> tuple[dict[uuid.UUID, Inflight | None], dict[uuid.UUID, Inflight | None]]:
    """The in-flight inventory of every agent with a gated row, and of every idle-nudge
    candidate, read with no transaction or lock held across the wrapper calls. Raises
    TimeoutError when a gated one did not answer in time; a nudge candidate that did not is
    left out (no nudge this pass)."""
    async with conn.transaction():
        gated = await _all(conn, _GATED_AGENTS, (session_id, list(GATED)))
        nudges = await _all(conn, _NUDGE_AGENTS, (session_id,))
    out: dict[uuid.UUID, Inflight | None] = {a["agent_id"]: None for a in gated}
    if fetch is None:
        return out, {}

    def usable(agents: list[dict]) -> list[dict]:
        return [a for a in agents if a["url"] and a["wrapper_session_id"]]

    async def nudge_read(a: dict) -> tuple[uuid.UUID, Inflight | None] | None:
        try:
            return a["agent_id"], await fetch(session_id, a["url"], a["wrapper_session_id"])
        except TimeoutError:
            return None

    asked = usable(gated)
    got, nudged = await asyncio.gather(
        asyncio.gather(*(fetch(session_id, a["url"], a["wrapper_session_id"]) for a in asked)),
        asyncio.gather(*(nudge_read(a) for a in usable(nudges))),
    )
    out.update({a["agent_id"]: inv for a, inv in zip(asked, got, strict=True)})
    return out, dict(n for n in nudged if n is not None)


async def handoff_pass(
    conn: psycopg.AsyncConnection,
    session_id: uuid.UUID,
    policy: HandoffPolicy,
    settings: Settings,
    fetch: InflightFetch | None = None,
    taskboard: handoff_package.TaskboardFetch | None = None,
    chat: handoff_package.ChatFetch | None = None,
) -> list[str]:
    """Move the session's handoff rows on and open new ones; the transitions made, in order
    (e.g. `pressuring:<agent>`, `done:<agent>:compacted`). Empty when another dispatcher holds
    the session's claim lock, an in-flight inventory did not arrive in time (the session is
    skipped this pass) or nothing changed. `fetch` reads the wrapper's in-flight inventory;
    without it every gate reads as blocked."""
    try:
        inflight, nudge_inflight = await _inventories(conn, session_id, fetch)
    except TimeoutError:
        log.info("handoff pass for session %s skipped: in-flight inventory timed out", session_id)
        return []
    async with conn.transaction():
        lock = await _one(
            conn,
            "SELECT pg_try_advisory_xact_lock(%s, hashtext(%s::text)) AS got",
            (_CLAIM_LOCK_NAMESPACE, session_id),
        )
        if not (lock and lock["got"]):
            return []
        session = await _one(conn, "SELECT id, desired FROM sessions WHERE id = %s", (session_id,))
        if session is None:
            return []
        run = _Pass(conn, session, policy, settings, inflight, nudge_inflight, taskboard, chat)
        subjects = await hive._subject_rows(conn, session_id=session_id) if run.active else []
        by_agent = {s["agent_id"]: s for s in subjects}
        touched: set[uuid.UUID] = set()
        for row in await handoffs.open_rows(conn, session_id):
            touched.add(row.agent_id)
            await run.step(row, by_agent.get(row.agent_id))
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                _SHUTDOWNS, {"session": session_id, "window": SHUTDOWN_WINDOW_SECONDS}
            )
            shutdowns = await cur.fetchall()
        for got in shutdowns:
            if got["text"] is not None:
                touched.add(got["agent_id"])
                await run.shutdown(got["agent_id"], got["stopping_since"], got["text"])
        for subject in subjects:
            if subject["agent_id"] not in touched:
                await run.open(subject)
        return run.out


async def notify_escalations(
    conn: psycopg.AsyncConnection,
    settings: Settings,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> list[str]:
    """The user's side of an escalation (decision 717): for every open escalated row whose
    notice is not posted yet, a comment on the session's claimed task, or, when it has none, a
    notice task (`client_key` per row, so a retry is a no-op on the taskboard). Marked
    `escalation_notified_at` once posted, so a dispatcher restart never posts it twice; a
    failed post is retried on the next call. No transaction is held across the HTTP calls. A
    row that closed meanwhile is not notified (the cycle ended). Returns `notified:<agent>`
    entries."""
    if not settings.taskboard_available:
        return []
    async with conn.transaction():
        pending = []
        for row in await handoffs.escalations_to_notify(conn):
            session = await sessions_repo.get(conn, row.session_id)
            if session is None:
                continue
            pct = await _one(
                conn,
                "SELECT context_pct FROM agent_status WHERE agent_id = %s",
                (row.agent_id,),
            )
            text = await escalation_text(
                conn,
                row,
                row.level,
                pct["context_pct"] if pct else None,
                list((row.inflight or {}).get("waiting_on") or []),
            )
            pod = await _one(conn, "SELECT pod_name FROM agents WHERE id = %s", (row.agent_id,))
            pending.append((row, session, text, (pod or {}).get("pod_name") or str(row.agent_id)))
    out: list[str] = []
    for row, session, text, pod in pending:
        scope = scope_of(session)
        try:
            key = await taskboard_client.claimed_task(
                settings, session.id, session.slug, scope, transport=transport
            )
        except taskboard_client.TaskboardUnavailable as exc:
            log.warning("handoff %s: escalation notice not posted (%s); retrying", row.id, exc)
            continue
        if key is not None:
            ok = await taskboard_client.add_comment(
                settings, session.id, session.slug, scope, key, text, transport=transport
            )
        else:
            ok = await taskboard_client.create_notice_task(
                settings,
                session.id,
                session.slug,
                scope,
                title=handoff_texts.escalation_title(session.slug, pod),
                body=text,
                client_key=f"handoff-escalation-{row.id}",
                labels=[ESCALATION_LABEL],
                transport=transport,
            )
        if not ok:
            log.warning("handoff %s: escalation notice not posted; retrying", row.id)
            continue
        async with conn.transaction():
            await handoffs.mark_escalation_notified(conn, row.id)
        log.info("handoff %s: escalation of agent %s posted (%s)", row.id, row.agent_id, key)
        out.append(f"notified:{row.agent_id}")
    return out

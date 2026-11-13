"""Strong-limit handoff state machine (T-256, plan 5.5): `handoff_pass`, dispatcher process only.

Per session, under the session's claim advisory lock (the one `leadership_pass` takes; skipped
when another dispatcher holds it) and in one transaction, every open `agent_handoffs` row of the
session moves at most along its phases and every limit subject (hive leader or solo agent, see
`hive._subject_rows`) at the strong limit without an open row gets one:

1. strong, no open row, agent live and not stopping: row `notified` with `deadline_at = now() +
   notify`, plus a `handoff_notice` message (regular lane, targeted). The push loop repeats the
   text as the keyed note `handoff_notice` while the row is notified or extended.
2. notified/extended: past `deadline_at`, or (notified only) once the notice's turn ended and the
   agent stayed idle with no background subagent for `idle_proceed_seconds`: a `handoff_request`
   message (regular lane: it never interrupts a running turn), phase `requested`.
3. requested: once the request's turn ended (or the message failed, or its turn record was lost
   and the agent sat idle), its `final_text` is the handoff (blank: `handoff_empty` is logged),
   phase `handed`. A hive leader with an eligible successor then gets a forced `handover`
   request (reason `limit`; the leadership pass applies it and moves the row on); anyone else
   (solo, or a hive whose leader is the only live agent, L3) follows the solo order: a `clear`
   command, phase `clearing`. A `limit` request rejected while the agent still leads falls back
   to the solo order; one rejected after leadership moved closes the row `not_leader`.
4. discussing (hive order, `discuss_since` set): once the session was quiet for `quiet_seconds`
   (no live agent busy, waiting or with background work, nothing queued or in flight, and
   neither the quiet clock, an agent's free/wait mark nor a turn end moved since) or
   `quiet_cap_seconds` after `discuss_since` whatever the activity: `clear`, phase `clearing`.
5. clearing: a `session_cleared` event after the clear command closes the row `done cleared`
   (hive order) or, in the solo order (`discuss_since` NULL), queues the `handoff_return`
   message in the same transaction as the move to `returning`, so it is sent exactly once;
   `returning` closes `done returned` once that message is sent. A clear refused with
   `subagents_running` (the wrapper's 409, or its `command_failed` event) is queued again once
   the agent has no background subagent, and an accepted clear with no `session_cleared` after
   the agent sat idle `clear_timeout_seconds` (a wrapper restart lost it) is queued again,
   MAX_CLEAR_ATTEMPTS times in all; any other failure, or the last attempt, closes the row
   `cancelled clear_failed` (solo order: after queuing the return anyway, worded for a kept
   context); no new row opens for that agent for `clear_failed_block_seconds` unless its
   context is cleared or compacted first.

Cancellation before `handed`: the session is closing (`session_closing`), the agent is gone or
stopping (`shutdown`), it is no longer a limit subject (`not_leader`), its context was
compacted (a `compacted` event since the row opened, or the gauge fell below the weak level:
`compacted`) or cleared by anyone (a `session_cleared` event since the row opened: `cleared`).
Its queued notice or request message is cancelled with it (status-guarded). After `handed`
only a closing session or a gone agent cancels a row. Every enqueue happens in a savepoint
with the guarded row update, so a lost update leaves no orphan message or command.

The claim (`db/queue.py`) guards the same rules at dispatch time, since the message drain can
run before the next pass: a notice or request whose row moved on, or whose agent's context was
reset since the row opened, is cancelled instead of sent; and while a row is `requested`, or
`clearing`/`returning` in the solo order, every other message for that agent waits, so the
handoff, the clear and the return run with nothing in between.

`shutdown` rows belong to the shutdown path and are left alone here.
"""

from __future__ import annotations

import logging
import math
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from agents_orchestrator.db import handoffs, leadership, queue
from agents_orchestrator.db.eligibility import BACKGROUND_BUSY, HOLDS_SESSION, background_busy
from agents_orchestrator.db.queue import _CLAIM_LOCK_NAMESPACE
from agents_orchestrator.db.sessions import quiet_clock_sql
from agents_orchestrator.dispatcher import hive
from agents_orchestrator.handoff_texts import STRONG_REQUEST, strong_notice, strong_return
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)

LIMIT_REASON = "limit"
MAX_CLEAR_ATTEMPTS = 5
BEFORE_HANDED = ("notified", "extended", "requested")
# A context reset before `handed` cancels the row, with this close reason. `compacting` alone is
# not one: a PreCompact whose compaction then fails leaves the context as it was.
RESET_EVENTS = {"compacted": "compacted", "session_cleared": "cleared"}


@dataclass(frozen=True)
class HandoffPolicy:
    notify_seconds: int
    extension_seconds: int
    idle_proceed_seconds: int
    quiet_seconds: int
    quiet_cap_seconds: int
    clear_timeout_seconds: int
    clear_failed_block_seconds: int

    @classmethod
    def from_settings(cls, settings: Settings) -> HandoffPolicy:
        return cls(
            settings.handover_notify_seconds,
            settings.handover_extension_seconds,
            settings.handover_idle_proceed_seconds,
            settings.handover_quiet_seconds,
            settings.handover_quiet_cap_seconds,
            settings.handover_clear_timeout_seconds,
            settings.handover_clear_failed_block_seconds,
        )


# Sessions a pass can do something for: active hive sessions with a leader, active solo sessions
# with a live agent, and any session with an open row (closing ones included, to cancel it).
_SESSIONS = f"""
SELECT c.id FROM sessions c WHERE c.desired = 'active' AND (
    ({leadership.HIVE} AND EXISTS (SELECT 1 FROM session_leaders l WHERE l.session_id = c.id))
    OR ({leadership.SOLO} AND EXISTS (
        SELECT 1 FROM agents a WHERE a.session_id = c.id AND a.gone_at IS NULL)))
UNION
SELECT h.session_id FROM agent_handoffs h
WHERE h.phase = ANY(%(open)s) AND h.trigger <> 'shutdown'
ORDER BY 1
"""

# What the pass needs to know about one agent. `idle_long`: idle, no turn, no background
# subagent, free for `idle` seconds. `compact_stale`: a compaction event newer than the last
# `agent_status` write; the ingester writes that row on every event, so in production this
# rarely holds and no reopen on the pre-compaction gauge relies on the wrapper's null `context`
# event right after `compacted` (the gauge becomes unknown, which never reaches strong).
# `clear_failed`: the agent's last row closed `clear_failed` less than `block` seconds ago and
# its context was neither cleared nor compacted since (a new cycle would likely fail the same
# way; after the block a later strong crossing starts one).
_FACTS = f"""
SELECT a.id, a.gone_at IS NOT NULL AS gone, {HOLDS_SESSION} AS live,
    s.stopping_since IS NOT NULL AS stopping,
    coalesce({BACKGROUND_BUSY}, false) AS background_busy,
    coalesce(s.state = 'idle' AND s.current_turn_id IS NULL AND NOT {BACKGROUND_BUSY}
        AND s.free_since <= now() - make_interval(secs => %(idle)s), false) AS idle_long,
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


class _Lost(Exception):
    """A guarded update lost to a concurrent writer: roll back the savepoint's enqueue."""


async def _one(conn: psycopg.AsyncConnection, query: str, params: Any) -> dict | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        return await cur.fetchone()


async def handoff_sessions(conn: psycopg.AsyncConnection) -> list[uuid.UUID]:
    """The sessions the dispatcher runs `handoff_pass` for on this wake (one query)."""
    async with conn.cursor() as cur:
        await cur.execute(_SESSIONS, {"open": list(handoffs.OPEN_PHASES)})
        return [r[0] for r in await cur.fetchall()]


class _Pass:
    """One pass over one session, inside its transaction and lock."""

    def __init__(
        self,
        conn: psycopg.AsyncConnection,
        session: dict,
        policy: HandoffPolicy,
        settings: Settings,
    ) -> None:
        self.conn = conn
        self.session = session
        self.policy = policy
        self.settings = settings
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
                "idle": self.policy.idle_proceed_seconds,
                "block": self.policy.clear_failed_block_seconds,
                "context_reset": list(RESET_EVENTS),
            },
        )
        return row or {
            "gone": True,
            "live": False,
            "stopping": False,
            "background_busy": False,
            "idle_long": False,
            "compact_stale": False,
            "clear_failed": False,
        }

    async def message(self, text: str, agent_id: uuid.UUID, notice: str) -> int:
        return await queue.enqueue_message(
            self.conn, self.session["id"], text, agent_id, lane="regular", notice=notice
        )

    async def guarded(self, work: Callable[[], Awaitable[bool]]) -> bool:
        """Run `work` (enqueues plus the guarded row update it returns the outcome of) in a
        savepoint: when the update loses, the enqueued message or command is rolled back with
        it, so a lost race never leaves an orphan behind."""
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

    async def _has_successor(self, agent_id: uuid.UUID) -> bool:
        policy = self._pass_policy()
        return (
            await leadership.successor(self.conn, self.session["id"], agent_id, policy) is not None
        )

    # ------------------------------------------------------------ 1. open

    async def open(self, subject: dict) -> None:
        agent_id = subject["agent_id"]
        if hive._row_level(subject, self.settings)[1] != "strong":
            return
        facts = await self.facts(agent_id)
        if not facts["live"] or facts["stopping"] or facts["compact_stale"]:
            return
        if facts["clear_failed"]:
            return  # its last cycle could not clear: none again until a clear or compaction
        row = await handoffs.insert(
            self.conn,
            self.session["id"],
            agent_id,
            "strong",
            "notified",
            deadline_at=await self._in(self.policy.notify_seconds),
        )
        # the wording is decided now: a hive whose leader is the only live agent follows the
        # solo order (L3), so it is not promised a successor
        moves = not subject["solo"] and await self._has_successor(agent_id)
        text = strong_notice(
            math.ceil(self.policy.notify_seconds / 60),
            math.ceil(self.policy.extension_seconds / 60),
            hive=moves,
        )
        await self.message(text, agent_id, "handoff_notice")
        log.info("handoff %s: agent %s notified (strong limit)", row.id, agent_id)
        self.out.append(f"notified:{agent_id}")

    async def _in(self, seconds: int) -> Any:
        row = await _one(self.conn, "SELECT now() + make_interval(secs => %s) AS t", (seconds,))
        assert row is not None
        return row["t"]

    # ------------------------------------------------------------ cancellation

    async def cancel(self, row: handoffs.Handoff, reason: str, *, messages: bool = True) -> None:
        """Close the row `cancelled`; with `messages`, its queued messages are cancelled too."""
        if not await handoffs.close(self.conn, row.id, "cancelled", reason):
            return
        for message_id in await self._queued_messages(row) if messages else []:
            await queue.cancel_message(self.conn, message_id)
        log.info("handoff %s: agent %s cancelled (%s)", row.id, row.agent_id, reason)
        self.out.append(f"cancelled:{row.agent_id}:{reason}")

    async def _queued_messages(
        self, row: handoffs.Handoff, *, notices_only: bool = False
    ) -> list[int]:
        return await handoffs.queued_messages(self.conn, row.id, notices_only=notices_only)

    def _below_weak(self, subject: dict) -> bool:
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
        row opened, if any. Before `handed` the row has no clear of its own."""
        found = await _one(
            self.conn,
            "SELECT e.type FROM events e, agent_handoffs h WHERE h.id = %s"
            " AND e.agent_id = h.agent_id AND e.type = ANY(%s) AND e.created_at >= h.created_at"
            " ORDER BY e.id LIMIT 1",
            (row.id, list(RESET_EVENTS)),
        )
        return RESET_EVENTS[found["type"]] if found is not None else None

    async def cancel_reason(
        self, row: handoffs.Handoff, subject: dict | None, facts: dict
    ) -> str | None:
        if not self.active:
            return "session_closing"
        if facts["gone"]:
            return "shutdown"
        if row.phase not in BEFORE_HANDED:
            return None
        if facts["stopping"]:
            return "shutdown"
        if subject is None:
            return "not_leader"
        reset = await self._reset_since(row)
        if reset is not None:
            return reset
        if self._below_weak(subject):
            return "compacted"
        return None

    # ------------------------------------------------------------ the phases

    async def step(self, row: handoffs.Handoff, subject: dict | None) -> None:
        facts = await self.facts(row.agent_id)
        reason = await self.cancel_reason(row, subject, facts)
        if reason is not None:
            await self.cancel(row, reason)
            return
        if row.phase in ("notified", "extended"):
            await self.countdown(row, facts)
        elif row.phase == "requested":
            await self.requested(row, facts)
        elif row.phase == "handed":
            await self.handed(row)
        elif row.phase == "discussing":
            await self.discussing(row)
        elif row.phase == "clearing":
            await self.clearing(row, facts)
        elif row.phase == "returning":
            await self.returning(row)

    async def countdown(self, row: handoffs.Handoff, facts: dict) -> None:
        due = await _one(
            self.conn,
            "SELECT deadline_at <= now() AS due FROM agent_handoffs WHERE id = %s",
            (row.id,),
        )
        proceed = bool(due and due["due"])
        if not proceed and row.phase == "notified" and facts["idle_long"]:
            proceed = await self._notice_turn_ended(row)
        if not proceed:
            return

        async def request() -> bool:
            message_id = await self.message(STRONG_REQUEST, row.agent_id, "handoff_request")
            return await handoffs.advance(
                self.conn, row.id, [row.phase], "requested", request_message_id=message_id
            )

        if not await self.guarded(request):
            return
        # a notice still queued (the agent was busy all countdown) would arrive right before
        # the request: drop it
        for message_id in await self._queued_messages(row, notices_only=True):
            await queue.cancel_message(self.conn, message_id)
        log.info("handoff %s: agent %s asked for its handoff", row.id, row.agent_id)
        self.out.append(f"requested:{row.agent_id}")

    async def _notice_turn_ended(self, row: handoffs.Handoff) -> bool:
        found = await _one(
            self.conn,
            "SELECT t.completed_at IS NOT NULL AS ended FROM messages m"
            " JOIN agent_handoffs h ON h.id = %s"
            " LEFT JOIN turns t ON t.turn_id = m.turn_id"
            " WHERE m.target_agent_id = h.agent_id AND m.notice = 'handoff_notice'"
            " AND m.created_at >= h.created_at AND m.status = 'sent'"
            " ORDER BY m.id LIMIT 1",
            (row.id,),
        )
        return bool(found and found["ended"])

    async def requested(self, row: handoffs.Handoff, facts: dict) -> None:
        msg = await _one(
            self.conn,
            "SELECT m.status, t.completed_at IS NOT NULL AS ended, t.final_text,"
            " s.current_turn_id IS NOT DISTINCT FROM m.turn_id AS current"
            " FROM messages m LEFT JOIN turns t ON t.turn_id = m.turn_id"
            " LEFT JOIN agent_status s ON s.agent_id = m.target_agent_id WHERE m.id = %s",
            (row.request_message_id,),
        )
        if msg is None or msg["status"] in ("failed", "cancelled"):
            text = None  # the request never ran: carry on without a handoff
        elif msg["status"] == "sent" and (
            msg["ended"] or (facts["idle_long"] and not msg["current"])
        ):
            text = msg["final_text"]
        else:
            return
        if text is not None and not text.strip():
            text = None
        if not await handoffs.advance(
            self.conn, row.id, ["requested"], "handed", handoff_text=text
        ):
            return
        if text is None:
            log.info("handoff_empty: handoff %s of agent %s has no text", row.id, row.agent_id)
        self.out.append(f"handed:{row.agent_id}")
        await self.handed(row)

    async def handed(self, row: handoffs.Handoff) -> None:
        session_id = self.session["id"]
        leader = await leadership.current_leader(self.conn, session_id)
        if row.trigger != "strong" or leader is None:
            await self.clear(row, ["handed"])  # solo order
            return
        request = await _one(
            self.conn,
            "SELECT r.status FROM leadership_requests r, agent_handoffs h WHERE h.id = %s"
            " AND r.session_id = h.session_id AND r.kind = 'handover' AND r.reason = %s"
            " AND r.from_agent_id = h.agent_id AND r.created_at >= h.created_at"
            " ORDER BY r.id DESC LIMIT 1",
            (row.id, LIMIT_REASON),
        )
        status = request["status"] if request is not None else None
        if status in ("queued", "applied"):
            return  # the leadership pass decides; its apply moves the row on
        if leader.agent_id != row.agent_id:
            await self.cancel(row, "not_leader")  # moved by something else
            return
        if status == "rejected":
            await self.clear(row, ["handed"])  # still the leader: the solo order
            return
        if not await self._has_successor(row.agent_id):
            await self.clear(row, ["handed"])  # L3: the only live agent
            return
        await leadership.insert_request(
            self.conn, session_id, "handover", row.agent_id, None, LIMIT_REASON, leader.epoch
        )
        log.info("handoff %s: limit handover requested for agent %s", row.id, row.agent_id)
        self.out.append(f"handover_requested:{row.agent_id}")

    async def clear(self, row: handoffs.Handoff, from_phases: list[str]) -> None:
        async def start() -> bool:
            command_id = await queue.enqueue_command(self.conn, row.agent_id, "clear")
            return await handoffs.start_clear(self.conn, row.id, from_phases, command_id)

        if await self.guarded(start):
            log.info("handoff %s: clearing agent %s", row.id, row.agent_id)
            label = "clear_retry" if from_phases == ["clearing"] else "clearing"
            self.out.append(f"{label}:{row.agent_id}")

    async def discussing(self, row: handoffs.Handoff) -> None:
        got = await _one(
            self.conn,
            _QUIET,
            {
                "row": row.id,
                "quiet": self.policy.quiet_seconds,
                "cap": self.policy.quiet_cap_seconds,
            },
        )
        if got is not None and (got["quiet"] or got["capped"]):
            await self.clear(row, ["discussing"])

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
            if not facts["background_busy"]:
                await self.clear(row, ["clearing"])
            return
        log.info("handoff %s: clear of agent %s failed (%s)", row.id, row.agent_id, error)
        if row.discuss_since is None:
            await self.send_return(row, cleared=False)  # the solo agent still gets its handoff
        await self.cancel(row, "clear_failed", messages=False)

    async def drop_lost_clear(self, command_id: int | None) -> None:
        """Finish a clear that timed out, still queued or running, as `failed` with the retryable
        `clear_timeout` (never `cancelled`, which reads as non-retryable on a later pass),
        status-guarded, so it never runs after its retry or the return; a late completion of a
        running one then changes nothing."""
        await self.conn.execute(
            "UPDATE commands SET finished_at = now(), result = %s, status = 'failed'"
            " WHERE id = %s AND status IN ('queued', 'running')",
            (Jsonb({"error": "clear_timeout"}), command_id),
        )

    async def cleared(self, row: handoffs.Handoff) -> None:
        if row.discuss_since is not None:  # hive order: nothing comes back
            if await handoffs.advance(
                self.conn, row.id, ["clearing"], "clearing", cleared_at=await self._in(0)
            ) and await handoffs.close(self.conn, row.id, "done", "cleared"):
                log.info("handoff %s: agent %s cleared", row.id, row.agent_id)
                self.out.append(f"done:{row.agent_id}")
            return
        if await self.send_return(row, cleared=True):
            self.out.append(f"returning:{row.agent_id}")

    async def send_return(self, row: handoffs.Handoff, *, cleared: bool) -> bool:
        """Queue the handoff back to the agent with the move to `returning` (exactly once).

        `cleared` False: the clear failed and the agent kept its context (other wording)."""

        async def send() -> bool:
            message_id = await self.message(
                strong_return(row.handoff_text, cleared=cleared), row.agent_id, "handoff_return"
            )
            fields: dict[str, Any] = {"return_message_id": message_id}
            if cleared:
                fields["cleared_at"] = await self._in(0)
            return await handoffs.advance(self.conn, row.id, ["clearing"], "returning", **fields)

        return await self.guarded(send)

    async def returning(self, row: handoffs.Handoff) -> None:
        msg = await _one(
            self.conn, "SELECT status FROM messages WHERE id = %s", (row.return_message_id,)
        )
        status = msg["status"] if msg is not None else "failed"
        if status == "sent":
            if await handoffs.close(self.conn, row.id, "done", "returned"):
                log.info("handoff %s: agent %s got its handoff back", row.id, row.agent_id)
                self.out.append(f"done:{row.agent_id}")
        elif status in ("failed", "cancelled"):
            await self.cancel(row, "return_failed")


async def handoff_pass(
    conn: psycopg.AsyncConnection,
    session_id: uuid.UUID,
    policy: HandoffPolicy,
    settings: Settings,
) -> list[str]:
    """Move the session's handoff rows on and open new ones; the transitions made, in order
    (e.g. `notified:<agent>`, `cancelled:<agent>:<reason>`). Empty when another dispatcher holds
    the session's claim lock (the pass is skipped) or nothing changed."""
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
        run = _Pass(conn, session, policy, settings)
        subjects = await hive._subject_rows(conn, session_id=session_id) if run.active else []
        by_agent = {s["agent_id"]: s for s in subjects}
        touched: set[uuid.UUID] = set()
        for row in await handoffs.open_rows(conn, session_id):
            touched.add(row.agent_id)
            if row.trigger == "shutdown":
                continue
            await run.step(row, by_agent.get(row.agent_id))
        for subject in subjects:
            if subject["agent_id"] not in touched:
                await run.open(subject)
        return run.out

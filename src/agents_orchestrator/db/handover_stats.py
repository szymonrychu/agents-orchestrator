"""Read-only handover measurements for `orch handover-stats`: how long leaders last and how many
bytes a successor takes in. `handover_stats` reads `handover_log` (written by the api sweeper);
`baseline_stats` derives the same numbers from the existing `handover` notices, for the
before/after comparison. `pressure_stats` reads the cooperative handoff rows (`agent_handoffs`)
for `orch handover-stats --pressure`."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime

import psycopg

SHORT_TENURE_SECONDS = 1200

_LOG = "SELECT kind, tenure_seconds, intake_bytes, delivered_at FROM handover_log"
# One row per handover notice: the tenure is the gap to the session's previous notice (the first
# notice of a session has none), the intake is the notice text plus its attachment sizes.
_NOTICES = """
    SELECT 'unknown' AS kind, extract(epoch FROM n.gap) AS tenure_seconds,
           n.bytes AS intake_bytes, n.created_at AS delivered_at
    FROM (SELECT m.created_at,
                 m.created_at - lag(m.created_at) OVER (PARTITION BY m.session_id ORDER BY m.id)
                     AS gap,
                 octet_length(m.text)
                     + coalesce((SELECT sum(a.size) FROM message_attachments a
                                 WHERE a.message_id = m.id), 0) AS bytes
          FROM messages m WHERE m.notice = 'handover') n
"""


@dataclass(frozen=True)
class HandoverStats:
    days: int
    handovers: int
    by_kind: dict[str, int]
    median_tenure_seconds: float | None
    tenures: int
    tenures_under_20_min: int
    intake_bytes_by_day: list[tuple[date, int]]
    handoffs_by_trigger: dict[str, int] = field(default_factory=dict)
    handoffs_by_close_reason: dict[str, int] = field(default_factory=dict)
    median_notified_to_handed_seconds: float | None = None


async def _stats(conn: psycopg.AsyncConnection, days: int, source: str) -> HandoverStats:
    window = f"FROM ({source}) h WHERE h.delivered_at > now() - make_interval(days => %(days)s)"
    params = {"days": days, "short": SHORT_TENURE_SECONDS}
    kinds = await conn.execute(f"SELECT h.kind, count(*) {window} GROUP BY h.kind", params)
    tenure = await conn.execute(
        "SELECT count(h.tenure_seconds),"
        " percentile_cont(0.5) WITHIN GROUP (ORDER BY h.tenure_seconds),"
        f" count(*) FILTER (WHERE h.tenure_seconds < %(short)s) {window}",
        params,
    )
    per_day = await conn.execute(
        "SELECT date_trunc('day', h.delivered_at)::date AS day, sum(h.intake_bytes)"
        f" {window} GROUP BY 1 ORDER BY 1",
        params,
    )
    by_kind = {kind: count for kind, count in await kinds.fetchall()}
    tenures, median, short = await tenure.fetchone() or (0, None, 0)
    return HandoverStats(
        days=days,
        handovers=sum(by_kind.values()),
        by_kind=by_kind,
        median_tenure_seconds=median,
        tenures=tenures,
        tenures_under_20_min=short,
        intake_bytes_by_day=[(day, int(total)) for day, total in await per_day.fetchall()],
    )


async def _handoff_stats(
    conn: psycopg.AsyncConnection, days: int
) -> tuple[dict[str, int], dict[str, int], float | None]:
    """Counts of `agent_handoffs` rows created in the window by trigger and by close reason, and
    the median seconds from the row's opening to the delivery of its handoff to the successor."""
    window = "FROM agent_handoffs WHERE created_at > now() - make_interval(days => %s)"
    triggers = await conn.execute(f"SELECT trigger, count(*) {window} GROUP BY trigger", (days,))
    reasons = await conn.execute(
        f"SELECT close_reason, count(*) {window} AND close_reason IS NOT NULL"
        " GROUP BY close_reason",
        (days,),
    )
    median = await conn.execute(
        "SELECT percentile_cont(0.5) WITHIN GROUP"
        f" (ORDER BY extract(epoch FROM delivered_at - created_at)) {window}"
        " AND delivered_at IS NOT NULL",
        (days,),
    )
    row = await median.fetchone()
    return (
        {name: count for name, count in await triggers.fetchall()},
        {name: count for name, count in await reasons.fetchall()},
        row[0] if row else None,
    )


async def handover_stats(conn: psycopg.AsyncConnection, days: int) -> HandoverStats:
    """Statistics of the handovers delivered in the last `days` days, from `handover_log`, and
    of the agent handoffs (`agent_handoffs`) of the same window."""
    stats = await _stats(conn, days, _LOG)
    by_trigger, by_reason, median = await _handoff_stats(conn, days)
    return replace(
        stats,
        handoffs_by_trigger=by_trigger,
        handoffs_by_close_reason=by_reason,
        median_notified_to_handed_seconds=median,
    )


async def baseline_stats(conn: psycopg.AsyncConnection, days: int) -> HandoverStats:
    """The same statistics over the `handover` notices of the last `days` days (kind unknown)."""
    return await _stats(conn, days, _NOTICES)


# --- context-pressure handoffs (T-423 step 15) ---------------------------------------------

# A return turn that used a tool within this long of the return message, with no human message
# of the session in between, resumed the work without a human.
RESUME_WINDOW_SECONDS = 600
_GATE_KINDS = ("subagent", "task", "worktree", "scratch", "inflight")


@dataclass(frozen=True)
class PressureStats:
    since: datetime
    #: pressure and voluntary cycles closed in the window, `done:returned`, `cancelled:<reason>`
    cycles_by_close: dict[str, int]
    submits_accepted: int
    rejects_by_reason: dict[str, int]
    gate_blocks: int
    #: the in-flight kind (`subagent`, `task`, `worktree`, `scratch`, `inflight`) of every item
    #: the stored inventory of a blocked row waited on
    gate_blocks_by_kind: dict[str, int]
    escalations: int
    #: tokens used between the first L2 sighting and the accepted submit
    tokens_l2_to_submit_median: float | None
    tokens_l2_to_submit_p90: float | None
    #: share of accepted submits preceded by a `pressure_delivered` event of the agent
    pressure_seen_before_submit_share: float | None
    returns: int
    resumes_without_human: int


_WINDOW = "FROM agent_handoffs h WHERE h.created_at >= %(since)s AND h.trigger <> 'shutdown'"
_RESUMES = """
WITH r AS (
    SELECT h.session_id, m.created_at AS sent_at,
           (SELECT min(e.created_at) FROM events e
            WHERE e.agent_id = h.agent_id AND e.turn_id = m.turn_id AND e.type = 'tool_call'
              AND e.created_at <= m.created_at + make_interval(secs => %(window)s)) AS first_tool
    FROM agent_handoffs h LEFT JOIN messages m ON m.id = h.return_message_id
    WHERE h.created_at >= %(since)s AND h.trigger <> 'shutdown' AND h.close_reason = 'returned')
SELECT count(*),
       count(*) FILTER (WHERE r.first_tool IS NOT NULL AND NOT EXISTS (
           SELECT 1 FROM messages u
           WHERE u.session_id = r.session_id AND u.created_at >= r.sent_at
             AND u.created_at <= r.first_tool AND u.notice IS NULL
             AND u.origin_service IS NULL AND u.sender_session_id IS NULL))
FROM r
"""


async def pressure_stats(conn: psycopg.AsyncConnection, since: datetime) -> PressureStats:
    """Cooperative handoff cycles created since `since`, from `agent_handoffs`, the agents'
    `pressure_delivered` events and the return turns. Shutdown rows are not pressure cycles."""
    p = {"since": since, "window": RESUME_WINDOW_SECONDS}
    closes = await conn.execute(
        f"SELECT h.phase || ':' || h.close_reason, count(*) {_WINDOW}"
        " AND h.phase IN ('done', 'cancelled') AND h.close_reason IS NOT NULL GROUP BY 1",
        p,
    )
    rejects = await conn.execute(
        "SELECT r, count(*) FROM agent_handoffs h, jsonb_array_elements_text(h.reject_reasons) r"
        " WHERE h.created_at >= %(since)s AND h.trigger <> 'shutdown' GROUP BY r",
        p,
    )
    totals = await conn.execute(
        "SELECT count(*) FILTER (WHERE h.submitted_at IS NOT NULL),"
        " coalesce(sum(h.gate_blocks), 0), count(*) FILTER (WHERE h.escalated_at IS NOT NULL),"
        " percentile_cont(0.5) WITHIN GROUP (ORDER BY h.submit_tokens - h.l2_tokens_at),"
        " percentile_cont(0.9) WITHIN GROUP (ORDER BY h.submit_tokens - h.l2_tokens_at)"
        f" {_WINDOW}",
        p,
    )
    kinds = await conn.execute(
        "SELECT split_part(w, ':', 1), count(*)"
        " FROM agent_handoffs h, jsonb_array_elements_text(h.inflight -> 'waiting_on') w"
        " WHERE h.created_at >= %(since)s AND h.trigger <> 'shutdown' AND h.gate_blocks > 0"
        " GROUP BY 1",
        p,
    )
    seen = await conn.execute(
        "SELECT count(*) FILTER (WHERE EXISTS (SELECT 1 FROM events e WHERE"
        " e.agent_id = h.agent_id AND e.type = 'pressure_delivered'"
        " AND e.created_at >= h.created_at AND e.created_at <= h.submitted_at))"
        f" {_WINDOW} AND h.submitted_at IS NOT NULL",
        p,
    )
    resumes = await conn.execute(_RESUMES, p)
    submits, blocks, escalations, median, p90 = await totals.fetchone()  # type: ignore[misc]
    seen_count = (await seen.fetchone())[0]  # type: ignore[index]
    returns, alone = await resumes.fetchone()  # type: ignore[misc]
    by_kind: dict[str, int] = {}
    for kind, count in await kinds.fetchall():
        key = kind if kind in _GATE_KINDS else "inflight"
        by_kind[key] = by_kind.get(key, 0) + count
    return PressureStats(
        since=since,
        cycles_by_close={name: count for name, count in await closes.fetchall()},
        submits_accepted=submits,
        rejects_by_reason={name: count for name, count in await rejects.fetchall()},
        gate_blocks=int(blocks),
        gate_blocks_by_kind=by_kind,
        escalations=escalations,
        tokens_l2_to_submit_median=median,
        tokens_l2_to_submit_p90=p90,
        pressure_seen_before_submit_share=seen_count / submits if submits else None,
        returns=returns,
        resumes_without_human=alone,
    )

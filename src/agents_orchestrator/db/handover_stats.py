"""Read-only handover measurements for `orch handover-stats`: how long leaders last and how many
bytes a successor takes in. `handover_stats` reads `handover_log` (written by the api sweeper);
`baseline_stats` derives the same numbers from the existing `handover` notices, for the
before/after comparison."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date

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
    the median seconds from the notice to the delivery of the handoff text to the successor."""
    window = "FROM agent_handoffs WHERE created_at > now() - make_interval(days => %s)"
    triggers = await conn.execute(f"SELECT trigger, count(*) {window} GROUP BY trigger", (days,))
    reasons = await conn.execute(
        f"SELECT close_reason, count(*) {window} AND close_reason IS NOT NULL"
        " GROUP BY close_reason",
        (days,),
    )
    median = await conn.execute(
        "SELECT percentile_cont(0.5) WITHIN GROUP"
        f" (ORDER BY extract(epoch FROM delivered_at - notified_at)) {window}"
        " AND notified_at IS NOT NULL AND delivered_at IS NOT NULL",
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

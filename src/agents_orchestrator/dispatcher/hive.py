"""Hive state push (D3, D9, D23; hive-decoupling D1, D2).

`push_once` runs in the dispatcher process only, never in the singleton ingester. For each hive
session it takes the session's advisory lock (try; skip when held) around the DB read, releases
it, pushes notes to the agents' wrappers with no transaction open, then takes the lock again
around the DB write. Pushes are best effort: a wrapper that is down, stopped or hanging costs
that agent its note for this tick and nothing else.

State kept here:
- `session_leaders.state_hash` is a JSON map `{agent_id: hash}` of the hive state each agent
  last accepted, so one failing agent never causes re-pushes to the others. The dispatcher is
  the only writer. `state_pushed_at` moves only when every receiver took the current state.

The hash covers only the structure (leader, epoch, members, wrapper sessions, readiness), so a
busy/idle flip or a context change pushes nothing. Members pull live state with
`orchestrator_me` and peers' activity with `orchestrator_activity`; nothing is streamed.

Limit subjects (hive-decoupling D4, T-256, T-423): a hive leader and the single agent of a
non-hive session (a "solo" agent, see `_subject_rows`) are under context pressure from L1 on
(`due_level`: L1 once its context grew by ORCH_HANDOVER_BUDGET_TOKENS on top of the context it had
when its tenure began, after ORCH_HANDOVER_MIN_TENURE_SECONDS and never while its pod awaits a
rollout; L2 at the strong point, L3 at the critical point, neither needing tenure). The start
context is `session_leaders.start_context_tokens` for a leader and
`agent_limits.start_context_tokens` for a solo agent (recorded on the first tick with a gauge and
again after each clear); the dispatcher fills both.

Pressure is a policy, not a note (`_push_pressure`, spec 5.2): every limit subject at L1 or above,
every agent with an open handoff row (an ex-leader waiting for its clear included) and every
agent pushed before gets the policy of `pressure_policy.build` with `PUT /v1/sessions/{id}/pressure`
whenever its content hash changes; the wrapper evaluates the level from its own gauge and
delivers the texts. Every PRESSURE_CHECK_SECONDS a `GET` compares the wrapper's version with the
last push and re-puts it when they differ (a wrapper restart lost it). The version is per agent
and strictly increasing: max(wrapper reported, last pushed) + 1. A wrapper without the route (404)
or with pressure disabled (409 `pressure_disabled`) is logged once per agent and wrapper session
and skipped. Nothing here names a time or asks for a handoff: the agent submits its own
(`dispatcher/handoff.py`).

The leader's keyed `rollout_due` note (voluntary handover 3.7) tells it a rollout waits on it: sent
once per (leader, epoch, revision) while the reconciler's `status.rollout.leader_since` is set and
the current leader's own ordinal is in `status.rollout.pending` (the stored status may still name
a previous leader's pod), retried on the next tick when the push fails, forgotten once the window
closes or the leader's pod is current.

The other keyed note is the requester's `leadership_request` outcome (hive-decoupling D6): each
`handover` request an agent made that the dispatcher decided (applied or rejected) within the
last OUTCOME_WINDOW_SECONDS is reported once per request id and process to the agent that made
it. The decision is read from `leadership_requests`, so any number of dispatchers can send it.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from agents_orchestrator import handoff_texts
from agents_orchestrator.db import handoffs, leadership
from agents_orchestrator.db.eligibility import HANDOVER_MARGIN_PCT
from agents_orchestrator.db.queue import _CLAIM_LOCK_NAMESPACE
from agents_orchestrator.dispatcher import feed
from agents_orchestrator.handover_transcript import REREAD_RULE
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec, legacy_context
from agents_orchestrator.wrapper.client import NoteOutcome, WrapperClient
from agents_orchestrator.wrapper.models import WrapperError, WrapperUnreachable

log = logging.getLogger(__name__)

# Module constants by design (D14).
PUSH_INTERVAL_SECONDS = 10
PUSH_TIMEOUT_SECONDS = 3
STATE_KEY = "hive_state"
ROLLOUT_KEY = "rollout_due"
OUTCOME_KEY = "leadership_request"
OUTCOME_WINDOW_SECONDS = 300
PRESSURE_CHECK_SECONDS = 60
PULL_SENTENCE = (
    "Call orchestrator_me for live state and context, orchestrator_activity for what peers did."
)

ClientFactory = Callable[[uuid.UUID, str], WrapperClient]

# What the dispatcher last told a leader about a rollout: (leader agent id, epoch, revision).
RolloutKey = tuple[uuid.UUID, int, str]


Level = Literal["none", "l1", "l2", "l3"]
LEVEL_NUMBER: dict[str, int] = {"none": 0, "l1": 1, "l2": 2, "l3": 3}


@dataclass(frozen=True)
class Steering:
    """A session's claimed task ids and todo lines as the last handoff pass read them from the
    taskboard (the push loop never calls the taskboard itself); `at` is a monotonic clock."""

    task_ids: tuple[str, ...]
    todo: tuple[str, ...]
    at: float


@dataclass
class HiveState:
    """In-memory push state of one dispatcher: the throttle clocks, the last `rollout_due` key and
    the `leadership_request` outcomes already sent (request ids, pruned to the window) per
    session, and per agent the pressure policy last pushed. A restart re-sends each of them at
    most once more."""

    last_tick: dict[uuid.UUID, float] = field(default_factory=dict)
    rollout_sent: dict[uuid.UUID, RolloutKey] = field(default_factory=dict)
    outcome_sent: dict[uuid.UUID, set[int]] = field(default_factory=dict)
    # per agent: (policy hash, version, clock of the last GET check) of the last successful PUT
    pressure_pushed: dict[uuid.UUID, tuple[str, int, float]] = field(default_factory=dict)
    # per agent: the wrapper session the entry above belongs to
    pressure_session: dict[uuid.UUID, str] = field(default_factory=dict)
    # per agent: the wrapper session that answered 404 or 409 pressure_disabled (logged once)
    pressure_off: dict[uuid.UUID, str] = field(default_factory=dict)
    pressure_tick: float | None = None
    # per session: its claimed tasks and todo as the handoff pass last read them (written by
    # the pass, read by the policy build; the push loop never asks the taskboard)
    steering: dict[uuid.UUID, Steering] = field(default_factory=dict)
    # per agent: the row id whose accepted handoff was last pushed as the post-compaction
    # reminder (the reminder goes out once)
    reminded: dict[uuid.UUID, int] = field(default_factory=dict)


# ---------------------------------------------------------------- the note text


def compact_pct(auto_compact_pct: int | None, settings: Settings) -> int:
    """The effective compaction point: the spec's `auto_compact_pct`, else the pods' default."""
    return settings.default_auto_compact_pct if auto_compact_pct is None else auto_compact_pct


@dataclass(frozen=True)
class Levels:
    """The handover levels of one leader or solo agent."""

    strong_pct: float  # compact - handover_strong_margin_pct (the percent fallback)
    strong_headroom_tokens: int
    compact_pct: int
    fallback_pct: int  # weak, for an unknown token gauge: max(compact - HANDOVER_MARGIN_PCT, 1)
    start_tokens: int | None
    budget_tokens: int
    critical_margin_pct: float = 5  # L3: compact - critical_margin_pct (the percent fallback)
    critical_headroom_tokens: int = 25000


def levels(spec: dict[str, Any], start_tokens: int | None, settings: Settings) -> Levels:
    """The levels of an agent whose context was `start_tokens` when its tenure began."""
    compact = compact_pct(spec.get("auto_compact_pct"), settings)
    return Levels(
        strong_pct=compact - settings.handover_strong_margin_pct,
        strong_headroom_tokens=settings.handover_strong_headroom_tokens,
        compact_pct=compact,
        fallback_pct=max(compact - HANDOVER_MARGIN_PCT, 1),
        start_tokens=start_tokens,
        budget_tokens=settings.handover_budget_tokens,
        critical_margin_pct=settings.handover_critical_margin_pct,
        critical_headroom_tokens=settings.handover_critical_headroom_tokens,
    )


def strong_tokens(lv: Levels, window_tokens: int) -> int:
    """The context size from which level 2 (strong) applies: the smaller of the percent margin
    below compaction and the headroom in tokens below it, floored at 0."""
    return max(
        0,
        int(
            min(
                lv.strong_pct / 100 * window_tokens,
                lv.compact_pct / 100 * window_tokens - lv.strong_headroom_tokens,
            )
        ),
    )


def critical_tokens(lv: Levels, window_tokens: int) -> int:
    """The context size from which level 3 (critical) applies: the smaller of the percent margin
    and the token headroom below compaction, never below the level 2 point."""
    return max(
        strong_tokens(lv, window_tokens),
        int(
            min(
                (lv.compact_pct - lv.critical_margin_pct) / 100 * window_tokens,
                lv.compact_pct / 100 * window_tokens - lv.critical_headroom_tokens,
            )
        ),
    )


def l1_tokens(lv: Levels, window_tokens: int, l2_tokens: int) -> int:
    """The context size from which L1 applies: the start-plus-budget point, or the percent
    fallback of the window when the tenure start is unknown, never above the L2 point."""
    if lv.start_tokens is not None:
        return min(lv.start_tokens + lv.budget_tokens, l2_tokens)
    return min(int(lv.fallback_pct / 100 * window_tokens), l2_tokens)


def due_level(
    lv: Levels,
    used_tokens: int | None,
    window_tokens: int | None,
    pct: float | None,
    tenure_seconds: float,
    min_tenure_seconds: int,
    rollout_pending: bool,
) -> Level:
    """The pressure level of an agent. `l3` at the critical point and `l2` at the strong point,
    whatever else holds; `l1` past its token budget (or the percent fallback) once the minimum
    tenure is over and its pod does not await a rollout; else `none`. Tokens decide when the
    window is known; the percentages only when it is not."""
    window = window_tokens if window_tokens is not None and window_tokens > 0 else None
    tokens = used_tokens is not None and window is not None
    cap = strong_tokens(lv, window) if window is not None else None
    crit = critical_tokens(lv, window) if window is not None else None
    by_pct = not tokens and pct is not None
    if by_pct and pct >= lv.compact_pct - lv.critical_margin_pct:
        return "l3"
    if tokens and used_tokens is not None and crit is not None and used_tokens >= crit:
        return "l3"
    if by_pct and pct >= lv.strong_pct:
        return "l2"
    if tokens and used_tokens is not None and cap is not None and used_tokens >= cap:
        return "l2"
    if tokens and used_tokens is not None and window is not None and cap is not None:
        hit = used_tokens >= l1_tokens(lv, window, cap)
    else:
        hit = by_pct and pct >= lv.fallback_pct
    if hit and tenure_seconds >= min_tenure_seconds and not rollout_pending:
        return "l1"
    return "none"


def hive_state_payload(
    you: str,
    leader: str,
    epoch: int,
    members: list[dict[str, Any]],
    max_replicas: int,
    static: bool,
) -> dict[str, Any]:
    """The hive_state schema: leader, epoch, you, members, max_replicas, static."""
    return {
        "leader": leader,
        "epoch": epoch,
        "you": you,
        "members": [
            {
                "pod_name": m["pod_name"],
                "ready": m["ready"],
                "state": m["state"],
                "context_pct": m["context_pct"],
                "leader": m["pod_name"] == leader,
            }
            for m in members
        ],
        "max_replicas": max_replicas,
        "static": static,
    }


def handover_advice(lv: Levels, settings: Settings) -> str:
    """The standing handover guidance of the hive-state note."""
    return (
        "Leadership is yours until you hand it over. Context pressure tells you when: after "
        f"{round(lv.budget_tokens / 1000)}k tokens of work on top of your context at the start "
        f"of leadership and at least {round(settings.handover_min_tenure_seconds / 60)} minutes "
        f"as leader, and always before {lv.strong_pct:g}% context. Hand off with "
        f"orchestrator_handoff_submit. {REREAD_RULE}"
    )


def render_hive_state(payload: dict[str, Any], slug: str, advice: str) -> str:
    """The `<hive-state>` text block pushed under the key `hive_state`."""
    clean = feed._clean
    role = "leader" if payload["you"] == payload["leader"] else "member"
    members = []
    for m in payload["members"]:
        parts = ["leader" if m["leader"] else "member", "ready" if m["ready"] else "not ready"]
        parts.append(clean(m["state"]))
        if m["context_pct"] is not None:
            parts.append(f"{round(m['context_pct'])}% context")
        members.append(f"{clean(m['pod_name'])} ({', '.join(parts)})")
    lines = [
        "<hive-state>",
        f"You are {clean(payload['you'])} ({role}) in hive {clean(slug)} of up to "
        f"{payload['max_replicas']} agents. Leader: {clean(payload['leader'])} "
        f"(epoch {payload['epoch']}). Members: {', '.join(members)}."
        + (" The hive is static: agents are not added or removed." if payload["static"] else ""),
    ]
    lines.append(advice)
    lines.append(PULL_SENTENCE)
    lines.append("</hive-state>")
    return "\n".join(lines)


def render_rollout_due(epoch: int, window_seconds: int) -> str:
    """The `<rollout-due>` text block pushed to the leader under the key `rollout_due`."""
    return (
        "<rollout-due>A new version of this hive is deployed and your pod is the last on the "
        "old one. Hand leadership over at your next natural boundary with "
        f"orchestrator_leadership_handover(epoch={epoch}, reason=<state summary>). "
        f"After {round(window_seconds / 60)} minutes the orchestrator moves it at your next "
        "turn boundary once your background agents have reported.</rollout-due>"
    )


def render_outcome(outcome: dict[str, Any], leader: str, epoch: int) -> str:
    """The `leadership_request` note: an applied request names the leader as of now."""
    head = f"handover request {outcome['id']}"
    if outcome["status"] == "applied":
        return f"{head} applied: {feed._clean(leader)} leads at epoch {epoch}"
    return f"{head} rejected: {feed._clean(outcome['reject_reason'])}"


def state_hash(leader: str, epoch: int, members: list[dict[str, Any]]) -> str:
    """Changes with leader, epoch, member set, wrapper sessions and ready; never with a member's
    state or context, which members pull with `orchestrator_me`."""
    key = [leader, epoch, [[m["pod_name"], m["wrapper_session_id"], m["ready"]] for m in members]]
    return hashlib.sha256(json.dumps(key).encode()).hexdigest()


# ---------------------------------------------------------------- one session, three phases


@dataclass
class _Plan:
    session_id: uuid.UUID
    slug: str
    levels: Levels
    advice: str
    # the leader's context when its leadership began, to record when still unknown
    fill_start: int | None
    max_replicas: int
    static: bool
    leader: str
    leader_id: uuid.UUID
    leader_ordinal: int
    epoch: int
    hashes: dict[str, str]  # per agent, the state hash it last accepted
    members: list[dict[str, Any]]
    scalable: bool
    # handover requests decided within OUTCOME_WINDOW_SECONDS that an agent made, by id
    outcomes: list[dict[str, Any]]
    # the reconciler's status.rollout while only the leader's pod is left (leader_since set)
    rollout: dict[str, Any] | None = None

    @property
    def receivers(self) -> list[dict[str, Any]]:
        return [m for m in self.members if m["url"] and m["wrapper_session_id"]]


async def _try_lock(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> bool:
    cur = await conn.execute(
        "SELECT pg_try_advisory_xact_lock(%s, hashtext(%s::text))",
        (_CLAIM_LOCK_NAMESPACE, session_id),
    )
    row = await cur.fetchone()
    return bool(row and row[0])


async def _read(
    conn: psycopg.AsyncConnection, session: dict[str, Any], settings: Settings
) -> _Plan | None:
    """Phase 1: under the lock, read the leader row and the members."""
    cid = session["id"]
    spec = session["spec"]
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT a.pod_name AS leader, a.ordinal, l.agent_id, l.epoch, l.state_hash,"
            " l.start_context_tokens"
            " FROM session_leaders l JOIN agents a ON a.id = l.agent_id WHERE l.session_id = %s",
            (cid,),
        )
        leader = await cur.fetchone()
        if leader is None:
            return None
        await cur.execute(
            "SELECT a.id, a.pod_name, a.url, a.wrapper_session_id, a.ready,"
            " coalesce(s.state, 'unknown') AS state, s.context_pct,"
            " s.context_used_tokens, s.context_window_tokens,"
            f" {_GAUGE_STALE} AS gauge_stale"
            " FROM agents a LEFT JOIN agent_status s ON s.agent_id = a.id"
            " WHERE a.session_id = %s AND a.gone_at IS NULL ORDER BY a.ordinal",
            (cid,),
        )
        members = await cur.fetchall()
        lead = next((m for m in members if m["pod_name"] == leader["leader"]), None)
        if lead is not None and lead["gauge_stale"]:  # predates the last clear: no reading yet
            lead["context_pct"] = lead["context_used_tokens"] = None
        used = lead["context_used_tokens"] if lead is not None else None
        fill_start = used if leader["start_context_tokens"] is None else None
        lv = levels(
            spec, leader["start_context_tokens"] if fill_start is None else fill_start, settings
        )
        rollout = (session.get("status") or {}).get("rollout") or {}
        await cur.execute(
            "SELECT id, from_agent_id, status, reject_reason FROM leadership_requests"
            " WHERE session_id = %s AND kind = 'handover' AND from_agent_id IS NOT NULL"
            " AND status IN ('applied', 'rejected')"
            " AND decided_at > now() - make_interval(secs => %s) ORDER BY id",
            (cid, OUTCOME_WINDOW_SECONDS),
        )
        outcomes = await cur.fetchall()
    typed = SessionSpec.model_validate(spec, context=legacy_context(session.get("secret_versions")))
    return _Plan(
        session_id=cid,
        slug=session["slug"],
        levels=lv,
        advice=handover_advice(lv, settings),
        fill_start=fill_start,
        max_replicas=spec.get("max_replicas") or spec.get("replicas") or 1,
        static=bool(spec.get("static")),
        leader=leader["leader"],
        leader_id=leader["agent_id"],
        leader_ordinal=leader["ordinal"],
        epoch=leader["epoch"],
        hashes=_load_hashes(leader["state_hash"]),
        members=members,
        scalable=typed.scalable,
        outcomes=outcomes,
        rollout=rollout if rollout.get("leader_since") else None,
    )


def _load_hashes(raw: str | None) -> dict[str, str]:
    try:
        data = json.loads(raw) if raw else {}
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


async def _note(
    client: WrapperClient, wrapper_session_id: str, text: str, key: str | None = None
) -> NoteOutcome:
    try:
        async with asyncio.timeout(PUSH_TIMEOUT_SECONDS):
            return await client.post_note(
                wrapper_session_id, text, key=key, timeout=PUSH_TIMEOUT_SECONDS
            )
    except TimeoutError:
        log.info("note to session %s timed out", wrapper_session_id)
        return "failed"
    except Exception:
        log.exception("note to session %s failed", wrapper_session_id)
        return "failed"


async def _push_agent(
    plan: _Plan, member: dict[str, Any], client_factory: ClientFactory
) -> NoteOutcome:
    """Push the current hive_state to one member."""
    payload = hive_state_payload(
        member["pod_name"],
        plan.leader,
        plan.epoch,
        plan.members,
        plan.max_replicas,
        plan.static,
    )
    text = render_hive_state(payload, plan.slug, plan.advice)
    client = client_factory(plan.session_id, member["url"])
    return await _note(client, member["wrapper_session_id"], text, STATE_KEY)


async def _push_rollout(
    plan: _Plan, state: HiveState, settings: Settings, client_factory: ClientFactory
) -> int:
    """Tell the leader a rollout waits on it, once per (leader, epoch, revision).

    Runs on every tick while `status.rollout.leader_since` is set and the leader's own pod is
    pending; otherwise the key is forgotten. A failed push is not remembered: the next tick
    retries it.
    """
    if plan.rollout is None or plan.leader_ordinal not in plan.rollout.get("pending", []):
        state.rollout_sent.pop(plan.session_id, None)
        return 0
    leader = next((m for m in plan.receivers if m["pod_name"] == plan.leader), None)
    if leader is None:
        return 0
    key: RolloutKey = (plan.leader_id, plan.epoch, str(plan.rollout.get("revision")))
    if state.rollout_sent.get(plan.session_id) == key:
        return 0
    text = render_rollout_due(plan.epoch, settings.rollout_handover_seconds)
    client = client_factory(plan.session_id, leader["url"])
    outcome = await _note(client, leader["wrapper_session_id"], text, ROLLOUT_KEY)
    if outcome != "failed":
        state.rollout_sent[plan.session_id] = key
    return int(outcome == "ok")


async def _push_outcomes(plan: _Plan, state: HiveState, client_factory: ClientFactory) -> int:
    """Tell each requester how its decided handover request ended, once per request id.

    Only a requester that is not gone and has a wrapper session is told. The sent ids are kept
    per session and pruned to the requests still inside the window; a failed push is not
    remembered, so the next tick retries it.
    """
    sent = state.outcome_sent.get(plan.session_id, set()) & {o["id"] for o in plan.outcomes}
    state.outcome_sent[plan.session_id] = sent
    receivers = {m["id"]: m for m in plan.receivers}
    todo = [o for o in plan.outcomes if o["id"] not in sent and o["from_agent_id"] in receivers]
    pushes = []
    for o in todo:
        member = receivers[o["from_agent_id"]]
        client = client_factory(plan.session_id, member["url"])
        text = render_outcome(o, plan.leader, plan.epoch)
        pushes.append(_note(client, member["wrapper_session_id"], text, OUTCOME_KEY))
    outcomes = await asyncio.gather(*pushes)
    for o, outcome in zip(todo, outcomes, strict=True):
        if outcome != "failed":
            sent.add(o["id"])
    return sum(outcome == "ok" for outcome in outcomes)


async def _claim(conn: psycopg.AsyncConnection, plan: _Plan, current: str) -> list[dict[str, Any]]:
    """Under the lock, before any push: record the work as taken, so a second dispatcher that
    reads while this one is on the wire finds nothing left to push.

    Marks every receiver whose state is not current as holding `current` and returns those
    receivers (the ones to push to). A failed push is undone in `_release`.
    """
    live = {str(m["id"]) for m in plan.members}
    due = [m for m in plan.receivers if plan.hashes.get(str(m["id"])) != current]
    hashes = {k: v for k, v in plan.hashes.items() if k in live}
    hashes.update({str(m["id"]): current for m in due})
    await conn.execute(
        "UPDATE session_leaders SET state_hash = %s WHERE session_id = %s",
        (json.dumps(hashes), plan.session_id),
    )
    if plan.fill_start is not None:
        await conn.execute(
            "UPDATE session_leaders SET start_context_tokens = %s"
            " WHERE session_id = %s AND epoch = %s AND start_context_tokens IS NULL",
            (plan.fill_start, plan.session_id, plan.epoch),
        )
    return due


async def _release(
    pool: AsyncConnectionPool, plan: _Plan, current: str, failed: set[str], all_ok: bool
) -> None:
    """After the pushes: take back the claim of failed receivers (they retry alone on the next
    tick) and stamp `state_pushed_at` when every push of this tick was taken."""
    async with pool.connection() as conn, conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT state_hash FROM session_leaders WHERE session_id = %s FOR UPDATE",
                (plan.session_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return
        hashes = _load_hashes(row["state_hash"])
        for agent_id in failed:
            if hashes.get(agent_id) == current:
                del hashes[agent_id]
        await conn.execute(
            "UPDATE session_leaders SET state_hash = %s,"
            " state_pushed_at = CASE WHEN %s THEN now() ELSE state_pushed_at END"
            " WHERE session_id = %s",
            (json.dumps(hashes), all_ok, plan.session_id),
        )


async def _push_session(
    pool: AsyncConnectionPool,
    session: dict[str, Any],
    settings: Settings,
    client_factory: ClientFactory,
    state: HiveState,
    clock: Callable[[], float],
) -> int:
    cid = session["id"]
    async with pool.connection() as conn, conn.transaction():
        if not await _try_lock(conn, cid):
            return 0
        state.last_tick[cid] = clock()
        plan = await _read(conn, session, settings)
        if plan is None:
            return 0
        current = state_hash(plan.leader, plan.epoch, plan.members)
        due = await _claim(conn, plan, current)
    sent = 0
    if due:
        outcomes = await asyncio.gather(*(_push_agent(plan, m, client_factory) for m in due))
        # disabled counts as taken (nothing to retry)
        failed = {
            str(m["id"]) for m, outcome in zip(due, outcomes, strict=True) if outcome == "failed"
        }
        await _release(pool, plan, current, failed, all_ok=not failed)
        sent = sum(outcome == "ok" for outcome in outcomes)
    sent += await _push_rollout(plan, state, settings, client_factory)
    return sent + await _push_outcomes(plan, state, client_factory)


# ---------------------------------------------------------------- limit subjects

# Gauge rows of the agents the limits apply to. A hive leader keeps its start context and
# tenure on `session_leaders` (the start is reset when the solo order clears it, see
# `handoff._Pass.send_return`). A solo agent (the one live agent of a session that is not a
# hive) keeps them in `agent_limits`; a row older than the agent's last `session_cleared` is
# stale (the context was cleared since), so its start is unknown again and is recorded afresh.
# For both, a gauge not updated since the clear still shows the old context and is read as no
# gauge (`_GAUGE_STALE`, aliases `a` the agent and `s` its agent_status row).
_GAUGE_STALE = """EXISTS (SELECT 1 FROM events x WHERE x.agent_id = a.id
        AND x.type = 'session_cleared' AND x.created_at > coalesce(s.updated_at, '-infinity'))"""
_SUBJECT_COLUMNS = """
    c.id AS session_id, c.slug, c.spec, c.status, a.id AS agent_id, a.ordinal, a.pod_name,
    a.url, a.wrapper_session_id, a.ready,
    coalesce(s.state, 'unknown') AS state, s.context_pct,
    s.context_used_tokens, s.context_window_tokens
"""
_HIVE_SUBJECTS = f"""
SELECT {_SUBJECT_COLUMNS}, false AS solo, {_GAUGE_STALE} AS gauge_stale, l.epoch,
    l.start_context_tokens AS start_tokens,
    extract(epoch FROM now() - l.since) AS tenure
FROM sessions c
JOIN session_leaders l ON l.session_id = c.id
JOIN agents a ON a.id = l.agent_id AND a.gone_at IS NULL
LEFT JOIN agent_status s ON s.agent_id = a.id
WHERE c.desired = 'active' AND {leadership.HIVE}
"""
_SOLO_SUBJECTS = f"""
SELECT {_SUBJECT_COLUMNS}, true AS solo, 0 AS epoch, {_GAUGE_STALE} AS gauge_stale,
    CASE WHEN fresh.ok THEN lim.start_context_tokens END AS start_tokens,
    CASE WHEN fresh.ok THEN extract(epoch FROM now() - lim.since) ELSE 0 END AS tenure
FROM sessions c
JOIN LATERAL (
    SELECT x.* FROM agents x WHERE x.session_id = c.id AND x.gone_at IS NULL
    ORDER BY x.ordinal LIMIT 1) a ON true
LEFT JOIN agent_status s ON s.agent_id = a.id
LEFT JOIN agent_limits lim ON lim.agent_id = a.id
LEFT JOIN LATERAL (
    SELECT lim.since IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM events e WHERE e.agent_id = a.id AND e.type = 'session_cleared'
        AND e.created_at > lim.since) AS ok) fresh ON true
WHERE c.desired = 'active' AND {leadership.SOLO}
"""


async def _subject_rows(
    conn: psycopg.AsyncConnection, *, session_id: uuid.UUID | None = None, solo: bool | None = None
) -> list[dict[str, Any]]:
    """The hive leaders and solo agents of active sessions (one session when `session_id`)."""
    queries = []
    if solo is not True:
        queries.append(_HIVE_SUBJECTS)
    if solo is not False:
        queries.append(_SOLO_SUBJECTS)
    rows: list[dict[str, Any]] = []
    for query in queries:
        where = " AND c.id = %(session)s" if session_id is not None else ""
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query + where + " ORDER BY c.id", {"session": session_id})
            rows.extend(await cur.fetchall())
    for row in rows:
        if row["gauge_stale"]:  # the gauge predates the last clear: no reading yet
            row["context_pct"] = row["context_used_tokens"] = None
    return rows


def _pending(row: dict[str, Any]) -> bool:
    """The agent's pod awaits its rollout (its ordinal is in `status.rollout.pending`)."""
    rollout = (row["status"] or {}).get("rollout") or {}
    return row["ordinal"] in (rollout.get("pending") or [])


def _row_level(row: dict[str, Any], settings: Settings) -> tuple[Levels, Level, int | None]:
    """The levels, the level and the start context to record (None when it is known already)."""
    fill = row["context_used_tokens"] if row["start_tokens"] is None else None
    lv = levels(row["spec"], row["start_tokens"] if fill is None else fill, settings)
    level = due_level(
        lv,
        row["context_used_tokens"],
        row["context_window_tokens"],
        row["context_pct"],
        float(row["tenure"]),
        settings.handover_min_tenure_seconds,
        _pending(row),
    )
    return lv, level, fill


@dataclass(frozen=True)
class PressureSubject:
    """The live pressure reading of one leader or solo agent (`Me` and the submit route)."""

    levels: Levels
    level: int
    used_tokens: int | None
    window_tokens: int | None
    context_pct: float | None
    tenure_seconds: float


async def pressure_subject(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, agent_id: uuid.UUID, settings: Settings
) -> PressureSubject | None:
    """The reading of `agent_id` when it is the leader or solo agent of an active session, else
    None. The level is computed in tokens (unknown gauge: 0)."""
    for row in await _subject_rows(conn, session_id=session_id):
        if row["agent_id"] != agent_id:
            continue
        lv, level, _ = _row_level(row, settings)
        return PressureSubject(
            levels=lv,
            level=LEVEL_NUMBER[level],
            used_tokens=row["context_used_tokens"],
            window_tokens=row["context_window_tokens"],
            context_pct=row["context_pct"],
            tenure_seconds=float(row["tenure"]),
        )
    return None


async def _fill_solo_start(
    pool: AsyncConnectionPool,
    session_id: uuid.UUID,
    settings: Settings,
    state: HiveState,
    clock: Callable[[], float],
) -> None:
    """Under the session lock, record a solo agent's start context when unknown (its first
    gauge, or the first after a clear). Solo agents get no notes: their pressure is the policy."""
    async with pool.connection() as conn, conn.transaction():
        if not await _try_lock(conn, session_id):
            return
        state.last_tick[session_id] = clock()
        rows = await _subject_rows(conn, session_id=session_id, solo=True)
        if not rows:
            return
        fill = _row_level(rows[0], settings)[2]
        if fill is not None:
            await handoffs.set_limits_start(conn, rows[0]["agent_id"], fill)


# ---------------------------------------------------------------- pressure policy push

# The open rows' agents that are not limit subjects (an ex-leader waiting for its clear): their
# gauge and session, to build their policy.
_ROW_AGENTS = f"""
SELECT c.id AS session_id, c.spec, a.id AS agent_id, a.url, a.wrapper_session_id,
    s.context_used_tokens, s.context_window_tokens, {_GAUGE_STALE} AS gauge_stale
FROM agents a JOIN sessions c ON c.id = a.session_id
LEFT JOIN agent_status s ON s.agent_id = a.id
WHERE a.id = ANY(%(agents)s) AND a.gone_at IS NULL AND c.desired = 'active'
"""


@dataclass(frozen=True)
class _PressureTarget:
    agent_id: uuid.UUID
    session_id: uuid.UUID
    url: str
    wrapper_session_id: str
    body: dict[str, Any]  # the policy with version 0
    hash: str
    reminder_row: int | None = None  # the row whose handoff this policy reminds of


async def _pressure_targets(
    conn: psycopg.AsyncConnection, settings: Settings, state: HiveState
) -> list[_PressureTarget]:
    """Every limit subject at L1 or above, with an open row, or pushed before; every other agent
    with an open handoff row (an ex-leader). Shutdown rows have their own path."""
    from agents_orchestrator import handoff_profile, pressure_policy  # cycle: they import hive

    rows = {h.agent_id: h for h in await handoffs.open_rows_all(conn) if h.trigger != "shutdown"}
    out: list[_PressureTarget] = []

    def add(
        agent_id: uuid.UUID,
        session_id: uuid.UUID,
        url: str | None,
        wsid: str | None,
        body: dict[str, Any],
        reminder_row: int | None = None,
    ) -> None:
        if url and wsid:
            h = pressure_policy.policy_hash(body)
            out.append(_PressureTarget(agent_id, session_id, url, wsid, body, h, reminder_row))

    reminders = {
        a: r
        for a, r in (await handoffs.unapplied_compacted(conn)).items()
        if r.id > state.reminded.get(a, 0)
    }

    def extras(
        row: handoffs.Handoff | None, session_id: uuid.UUID, reminder: handoffs.Handoff | None
    ) -> pressure_policy.PolicyExtras:
        resubmit = bool(row is not None and (row.inflight or {}).get("resubmit"))
        cached = state.steering.get(session_id)
        task_ids, todo = (cached.task_ids, cached.todo) if cached else ((), ())
        steer = row if row is not None else reminder
        profile: handoff_texts.Profile = (
            "chat" if steer is not None and steer.profile == "chat" else "work"
        )
        compact = handoff_texts.compact_instructions(row, task_ids, profile, todo=todo)
        reinject = handoff_texts.reinject_text(steer, task_ids, todo, profile)
        if row is None or row.phase != "returning" or row.handoff is None:
            return pressure_policy.PolicyExtras(
                resubmit=resubmit, compact_instructions=compact, reinject_text=reinject
            )
        # a solo agent's fresh context: the resume check until the return turn ends
        first = [str(n) for n in row.handoff.get("next") or [] if str(n).strip()]
        text = handoff_texts.resume_text(
            [], first[0] if first else None, chat=row.profile == "chat"
        )
        return pressure_policy.PolicyExtras(
            resume_pending=True,
            resume_text=text,
            compact_instructions=compact,
            reinject_text=reinject,
        )

    seen: set[uuid.UUID] = set()
    for subject in await _subject_rows(conn):
        agent_id = subject["agent_id"]
        lv, level, _ = _row_level(subject, settings)
        row = rows.get(agent_id)
        seen.add(agent_id)
        reminder = reminders.get(agent_id) if row is None else None
        if (
            level == "none"
            and row is None
            and agent_id not in state.pressure_pushed
            and reminder is None
        ):
            continue
        known = row.profile if row is not None else None
        profile: handoff_texts.Profile
        if known == "work" or known == "chat":
            profile = known
        else:
            tenure = float(subject["tenure"] or 0)  # 0: the tenure start is unknown
            tenure_start = await _since(conn, tenure) if tenure > 0 else None
            profile = await handoff_profile.detect(
                conn, {"id": subject["session_id"], "spec": subject["spec"]}, agent_id, tenure_start
            )
        l1_on = float(subject["tenure"]) >= settings.handover_min_tenure_seconds and not _pending(
            subject
        )
        body = pressure_policy.build(
            lv,
            subject["context_window_tokens"],
            l1_on,
            profile,
            row,
            hive=not subject["solo"],
            version=0,
            extras=extras(row, subject["session_id"], reminder),
        )
        add(
            agent_id,
            subject["session_id"],
            subject["url"],
            subject["wrapper_session_id"],
            body,
            reminder.id if reminder is not None and body["reinject_text"] else None,
        )
    others = [a for a in rows if a not in seen]
    if others:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(_ROW_AGENTS, {"agents": others})
            for got in await cur.fetchall():
                row = rows[got["agent_id"]]
                window = None if got["gauge_stale"] else got["context_window_tokens"]
                body = pressure_policy.build(
                    levels(got["spec"], None, settings),
                    window,
                    False,
                    "chat" if row.profile == "chat" else "work",
                    row,
                    hive=True,
                    version=0,
                    extras=extras(row, got["session_id"], None),
                )
                add(got["agent_id"], got["session_id"], got["url"], got["wrapper_session_id"], body)
    return out


async def _since(conn: psycopg.AsyncConnection, seconds: float) -> Any:
    cur = await conn.execute("SELECT now() - make_interval(secs => %s)", (seconds,))
    row = await cur.fetchone()
    return row[0] if row else None


def _off(state: HiveState, target: _PressureTarget, exc: WrapperError) -> bool:
    """404 (no route, or an unknown session) or 409 pressure_disabled: the wrapper takes no
    policy. Logged once per agent and wrapper session; True when it was such an answer."""
    if exc.status != 404 and exc.code != "pressure_disabled":
        return False
    if state.pressure_off.get(target.agent_id) != target.wrapper_session_id:
        log.info(
            "pressure policy for agent %s skipped: wrapper answered %s %s",
            target.agent_id,
            exc.status,
            exc.code,
        )
    state.pressure_off[target.agent_id] = target.wrapper_session_id
    return True


async def _push_policy(
    target: _PressureTarget,
    state: HiveState,
    client_factory: ClientFactory,
    now: float,
) -> bool:
    """PUT the target's policy when its content changed since the last successful push, or when
    the wrapper's version differs from it at the periodic check; True when a PUT was taken."""
    agent_id, wsid = target.agent_id, target.wrapper_session_id
    if state.pressure_off.get(agent_id) == wsid:
        return False
    last = state.pressure_pushed.get(agent_id)
    last_version = last[1] if last is not None else 0  # per agent, across wrapper sessions
    if last is not None and state.pressure_session.get(agent_id) != wsid:
        last = None  # a new wrapper session holds nothing of the old one
    if last is not None and last[0] == target.hash and now - last[2] < PRESSURE_CHECK_SECONDS:
        return False
    client = client_factory(target.session_id, target.url)
    try:
        async with asyncio.timeout(PUSH_TIMEOUT_SECONDS * 3):
            reported = (await client.get_pressure(wsid)).version
            if last is not None and last[0] == target.hash and reported == last[1]:
                state.pressure_pushed[agent_id] = (last[0], last[1], now)
                return False
            for attempt in (1, 2):
                version = max(reported or 0, last_version) + 1
                try:
                    await client.put_pressure(wsid, {**target.body, "version": version})
                    break
                except WrapperError as exc:
                    if exc.code != "stale_version" or attempt == 2:
                        raise
                    reported = (await client.get_pressure(wsid)).version
    except WrapperError as exc:
        if not _off(state, target, exc):
            log.info("pressure policy for agent %s not pushed: %s", agent_id, exc)
        return False
    except (WrapperUnreachable, TimeoutError) as exc:
        log.info("pressure policy for agent %s not pushed: %s", agent_id, type(exc).__name__)
        return False
    state.pressure_pushed[agent_id] = (target.hash, version, now)
    if target.reminder_row is not None:
        state.reminded[agent_id] = target.reminder_row
    state.pressure_session[agent_id] = wsid
    state.pressure_off.pop(agent_id, None)
    return True


async def _drop_policy(
    agent_id: uuid.UUID, state: HiveState, pool: AsyncConnectionPool, client_factory: ClientFactory
) -> None:
    """An agent pushed before that is neither a limit subject nor has an open row (a member now):
    its wrapper's policy is removed, so a member never gets pressure. Best effort, forgotten
    either way."""
    wsid = state.pressure_session.pop(agent_id, None)
    state.pressure_pushed.pop(agent_id, None)
    state.pressure_off.pop(agent_id, None)
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT session_id, url, wrapper_session_id FROM agents"
            " WHERE id = %s AND gone_at IS NULL",
            (agent_id,),
        )
        got = await cur.fetchone()
    if got is None or not got["url"] or got["wrapper_session_id"] != wsid or wsid is None:
        return
    client = client_factory(got["session_id"], got["url"])
    try:
        async with asyncio.timeout(PUSH_TIMEOUT_SECONDS):
            await client.delete_pressure(wsid)
    except (WrapperError, WrapperUnreachable, TimeoutError) as exc:
        log.info("pressure policy of agent %s not removed: %s", agent_id, type(exc).__name__)


async def _push_pressure(
    pool: AsyncConnectionPool,
    settings: Settings,
    client_factory: ClientFactory,
    state: HiveState,
    clock: Callable[[], float],
) -> int:
    """The pressure policy push, at most every PUSH_INTERVAL_SECONDS (by `clock`); the PUTs
    taken. Read in one short transaction, pushed with none open."""
    now = clock()
    if state.pressure_tick is not None and now - state.pressure_tick < PUSH_INTERVAL_SECONDS:
        return 0
    state.pressure_tick = now
    async with pool.connection() as conn, conn.transaction():
        targets = await _pressure_targets(conn, settings, state)
    wanted = {t.agent_id for t in targets}
    for agent_id in [a for a in state.pressure_pushed if a not in wanted]:
        await _drop_policy(agent_id, state, pool, client_factory)
    taken = await asyncio.gather(*(_push_policy(t, state, client_factory, now) for t in targets))
    return sum(taken)


async def push_once(
    pool: AsyncConnectionPool,
    settings: Settings,
    client_factory: ClientFactory,
    state: HiveState | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> int:
    """One push pass over every hive session and solo agent; returns the number of notes accepted
    (pressure policies are not notes and are not counted).

    A session is evaluated at most once per PUSH_INTERVAL_SECONDS (by `clock`). One failing
    session is logged and never holds up the others.
    """
    state = state if state is not None else HiveState()
    async with pool.connection() as conn:
        sessions = await leadership.hive_sessions(conn)
        solo_ids = [r["session_id"] for r in await _subject_rows(conn, solo=True)]
    total = 0
    for session_id in solo_ids:
        last = state.last_tick.get(session_id)
        if last is not None and clock() - last < PUSH_INTERVAL_SECONDS:
            continue
        try:
            await _fill_solo_start(pool, session_id, settings, state, clock)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("limit push for session %s failed", session_id)
    for session in sessions:
        last = state.last_tick.get(session["id"])
        if last is not None and clock() - last < PUSH_INTERVAL_SECONDS:
            continue
        try:
            total += await _push_session(pool, session, settings, client_factory, state, clock)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("hive push for session %s failed", session["id"])
    try:
        await _push_pressure(pool, settings, client_factory, state, clock)
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("pressure push failed")
    return total

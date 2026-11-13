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

The one exception is the keyed `handover_due` note (hive-decoupling D4, T-256): a hive leader
or the single agent of a non-hive session (a "solo" agent, see `_subject_rows`) that reaches
the weak limit (see `due_level`) is told once per agent, epoch, 5% bucket and candidate list to
wrap up; a leader also gets its candidates, a solo agent the voluntary handoff. The weak limit is
reached once its context grew by ORCH_HANDOVER_BUDGET_TOKENS on top of the context it had when
its tenure began and it has been there ORCH_HANDOVER_MIN_TENURE_SECONDS, never while its pod
awaits a rollout; without a token gauge the percent rule (compact - HANDOVER_MARGIN_PCT)
applies. The strong limit (`strong_tokens`) needs neither tenure nor rollout. The start context
is `session_leaders.start_context_tokens` for a leader and `agent_limits.start_context_tokens`
for a solo agent (recorded on the first tick with a gauge and again after each clear); the
dispatcher fills both. It is evaluated on every push tick, outside the hash and claim path
above, and remembered per session in `HiveState`. Solo agents get only this note (no hive
state, rollout or outcome pushes).

While a strong-limit handoff row (dispatcher/handoff.py) of a leader or solo agent counts down
(phase notified or extended), the agent also gets the keyed `handoff_notice` note with the
countdown text and the minutes left, once per row and phase (so again after an extension); a
failed push is retried next tick. Members never get it.

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
import math
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.db import handoffs, leadership
from agents_orchestrator.db.eligibility import (
    ELIGIBLE,
    HANDOVER_MARGIN_PCT,
    HEADROOM,
    SUCCESSOR_ORDER,
)
from agents_orchestrator.db.queue import _CLAIM_LOCK_NAMESPACE
from agents_orchestrator.dispatcher import feed
from agents_orchestrator.handoff_texts import (
    WEAK_LEADER_CANDIDATES,
    WEAK_LEADER_NONE,
    WEAK_SOLO,
    WEAK_WRAP_UP,
    strong_notice,
)
from agents_orchestrator.handover_transcript import REREAD_RULE
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec, legacy_context
from agents_orchestrator.wrapper.client import NoteOutcome, WrapperClient

log = logging.getLogger(__name__)

# Module constants by design (D14).
PUSH_INTERVAL_SECONDS = 10
PUSH_TIMEOUT_SECONDS = 3
STATE_KEY = "hive_state"
DUE_KEY = "handover_due"
ROLLOUT_KEY = "rollout_due"
OUTCOME_KEY = "leadership_request"
NOTICE_KEY = "handoff_notice"
OUTCOME_WINDOW_SECONDS = 300
BUCKET_PCT = 5
PULL_SENTENCE = (
    "Call orchestrator_me for live state and context, orchestrator_activity for what peers did."
)

ClientFactory = Callable[[uuid.UUID, str], WrapperClient]

# Who could take over from a leader that is due: ELIGIBLE agents other than the leader that have
# headroom (eligibility.HEADROOM; a candidate without it would hand leadership straight back), in
# the leadership pass's successor order (eligibility.SUCCESSOR_ORDER), so the first one is the
# agent the pass picks for an untargeted handover. Params: session, leader, compact (the pods'
# default compaction point).
_CANDIDATES = f"""
SELECT a.pod_name, a.wrapper_session_id, coalesce(s.state, 'unknown') AS state, s.context_pct
FROM agents a LEFT JOIN agent_status s ON s.agent_id = a.id
JOIN sessions c ON c.id = a.session_id
WHERE a.session_id = %(session)s AND a.id <> %(leader)s AND {ELIGIBLE} AND {HEADROOM}
ORDER BY {SUCCESSOR_ORDER}
"""

# What the dispatcher last told a leader or solo agent (weak note): (agent id, epoch, 5% bucket,
# candidate identity).
DueKey = tuple[uuid.UUID, int, int, tuple[tuple[str, str], ...]]

# What the dispatcher last told a leader about a rollout: (leader agent id, epoch, revision).
RolloutKey = tuple[uuid.UUID, int, str]


@dataclass
class HiveState:
    """In-memory push state of one dispatcher: the throttle clock, the last `handover_due` and
    `rollout_due` keys and the `leadership_request` outcomes already sent (request ids, pruned to
    the window) per session. A restart re-sends each of them at most once more."""

    last_tick: dict[uuid.UUID, float] = field(default_factory=dict)
    due_sent: dict[uuid.UUID, DueKey] = field(default_factory=dict)
    rollout_sent: dict[uuid.UUID, RolloutKey] = field(default_factory=dict)
    outcome_sent: dict[uuid.UUID, set[int]] = field(default_factory=dict)
    # the strong countdown note last sent per session: (handoff row id, phase)
    notice_sent: dict[uuid.UUID, tuple[int, str]] = field(default_factory=dict)


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
    )


def strong_tokens(lv: Levels, window_tokens: int) -> int:
    """The context size from which the strong limit applies: the smaller of the percent margin
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


def due_level(
    lv: Levels,
    used_tokens: int | None,
    window_tokens: int | None,
    pct: float | None,
    tenure_seconds: float,
    min_tenure_seconds: int,
    rollout_pending: bool,
) -> Literal["none", "weak", "strong"]:
    """The limit an agent is at: `strong` at the strong limit whatever else holds, `weak` past
    its token budget (or the percent fallback) once the minimum tenure is over and its pod does
    not await a rollout, else `none`."""
    window = window_tokens if window_tokens is not None and window_tokens > 0 else None
    tokens = used_tokens is not None and window is not None
    cap = strong_tokens(lv, window) if window is not None else None
    if pct is not None and pct >= lv.strong_pct:
        return "strong"
    if tokens and used_tokens is not None and cap is not None and used_tokens >= cap:
        return "strong"
    if tokens and lv.start_tokens is not None and used_tokens is not None and cap is not None:
        hit = used_tokens >= min(lv.start_tokens + lv.budget_tokens, cap)
    else:
        hit = pct is not None and pct >= lv.fallback_pct
    if hit and tenure_seconds >= min_tenure_seconds and not rollout_pending:
        return "weak"
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
        "Leadership is yours until you hand it over. The orchestrator sends a handover_due note "
        f"when you should: after {round(lv.budget_tokens / 1000)}k tokens of work on top of your "
        "context at the start of leadership and at least "
        f"{round(settings.handover_min_tenure_seconds / 60)} minutes as leader, and always "
        f"before {lv.strong_pct:g}% context. {REREAD_RULE}"
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


def render_handover_due(epoch: int, candidates: list[dict[str, Any]], *, solo: bool) -> str:
    """The `<handover-due>` text block pushed under the key `handover_due` at the weak limit.

    Every agent is told to wrap up; the rest depends on who it is: a hive leader gets its
    candidates (or the lack of them), a solo agent the voluntary handoff.
    """
    lines = ["<handover-due>", WEAK_WRAP_UP]
    if solo:
        lines.append(WEAK_SOLO)
    elif not candidates:
        lines.append(WEAK_LEADER_NONE)
    else:
        names = ", ".join(feed._clean(c["pod_name"]) for c in candidates)
        lines.append(f"Candidates, best first: {names}. Current epoch: {epoch}.")
        lines.append(WEAK_LEADER_CANDIDATES)
    lines.append("</handover-due>")
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
    level: Literal["none", "weak", "strong"]
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
    # ELIGIBLE successors, best first; None while the leader is not due
    candidates: list[dict[str, Any]] | None = None
    # the reconciler's status.rollout while only the leader's pod is left (leader_since set)
    rollout: dict[str, Any] | None = None
    # a session of one agent: the agent is its own leader, there is no hive state to push
    solo: bool = False
    # the leader's strong row while it counts down: (row id, phase, minutes left)
    countdown: tuple[int, str, int] | None = None
    # while it counts down: a successor exists (`leadership.successor`, the one the handoff
    # pass and `handed()` use, so the note promises what the notice message promised)
    successor: bool = False

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
            " l.start_context_tokens, extract(epoch FROM now() - l.since) AS tenure"
            " FROM session_leaders l JOIN agents a ON a.id = l.agent_id WHERE l.session_id = %s",
            (cid,),
        )
        leader = await cur.fetchone()
        if leader is None:
            return None
        await cur.execute(
            "SELECT a.id, a.pod_name, a.url, a.wrapper_session_id, a.ready,"
            " coalesce(s.state, 'unknown') AS state, s.context_pct,"
            " s.context_used_tokens, s.context_window_tokens"
            " FROM agents a LEFT JOIN agent_status s ON s.agent_id = a.id"
            " WHERE a.session_id = %s AND a.gone_at IS NULL ORDER BY a.ordinal",
            (cid,),
        )
        members = await cur.fetchall()
        lead = next((m for m in members if m["pod_name"] == leader["leader"]), None)
        used = lead["context_used_tokens"] if lead is not None else None
        fill_start = used if leader["start_context_tokens"] is None else None
        lv = levels(
            spec, leader["start_context_tokens"] if fill_start is None else fill_start, settings
        )
        rollout = (session.get("status") or {}).get("rollout") or {}
        level = "none"
        if lead is not None:
            level = due_level(
                lv,
                used,
                lead["context_window_tokens"],
                lead["context_pct"],
                float(leader["tenure"]),
                settings.handover_min_tenure_seconds,
                leader["ordinal"] in (rollout.get("pending") or []),
            )
        candidates = None
        if level != "none":
            await cur.execute(
                _CANDIDATES,
                {
                    "session": cid,
                    "leader": leader["agent_id"],
                    "compact": settings.default_auto_compact_pct,
                },
            )
            candidates = await cur.fetchall()
        await cur.execute(
            "SELECT id, from_agent_id, status, reject_reason FROM leadership_requests"
            " WHERE session_id = %s AND kind = 'handover' AND from_agent_id IS NOT NULL"
            " AND status IN ('applied', 'rejected')"
            " AND decided_at > now() - make_interval(secs => %s) ORDER BY id",
            (cid, OUTCOME_WINDOW_SECONDS),
        )
        outcomes = await cur.fetchall()
    countdown = await handoffs.countdown(conn, leader["agent_id"])
    successor = countdown is not None and (
        await leadership.successor(
            conn,
            cid,
            leader["agent_id"],
            leadership.PassPolicy(default_auto_compact_pct=settings.default_auto_compact_pct),
        )
        is not None
    )
    typed = SessionSpec.model_validate(spec, context=legacy_context(session.get("secret_versions")))
    return _Plan(
        session_id=cid,
        slug=session["slug"],
        levels=lv,
        level=level,
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
        candidates=candidates,
        rollout=rollout if rollout.get("leader_since") else None,
        countdown=countdown,
        successor=successor,
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


async def _push_due(plan: _Plan, state: HiveState, client_factory: ClientFactory) -> int:
    """Tell an agent at the weak limit to wrap up, once per due key.

    Runs on every tick, whatever the hive_state hash says. When the agent is below the weak limit
    (or there is no leader to tell) nothing is sent and the key is forgotten, so an agent that
    compacted and climbs again is told again. At the strong limit the weak note is not repeated
    (the strong limit has its own handling). A failed push is not remembered: the next tick
    retries it.
    """
    leader = next((m for m in plan.receivers if m["pod_name"] == plan.leader), None)
    if plan.candidates is None or leader is None:
        if plan.candidates is None:
            state.due_sent.pop(plan.session_id, None)
        return 0
    if plan.level != "weak":
        return 0
    identity = tuple((c["pod_name"], c["wrapper_session_id"]) for c in plan.candidates)
    used, window = leader["context_used_tokens"], leader["context_window_tokens"]
    pct = leader["context_pct"]
    if pct is None:  # due by tokens alone
        pct = 100 * used / window
    key: DueKey = (plan.leader_id, plan.epoch, int(pct // BUCKET_PCT), identity)
    if state.due_sent.get(plan.session_id) == key:
        return 0
    text = render_handover_due(plan.epoch, plan.candidates, solo=plan.solo)
    client = client_factory(plan.session_id, leader["url"])
    outcome = await _note(client, leader["wrapper_session_id"], text, DUE_KEY)
    if outcome != "failed":
        state.due_sent[plan.session_id] = key
    return int(outcome == "ok")


async def _push_countdown(
    plan: _Plan, state: HiveState, settings: Settings, client_factory: ClientFactory
) -> int:
    """Repeat the strong countdown notice as the keyed `handoff_notice` note, once per row and
    phase; forgotten once nothing counts down. A failed push is retried on the next tick."""
    if plan.countdown is None:
        state.notice_sent.pop(plan.session_id, None)
        return 0
    leader = next((m for m in plan.receivers if m["pod_name"] == plan.leader), None)
    row_id, phase, minutes = plan.countdown
    if leader is None or state.notice_sent.get(plan.session_id) == (row_id, phase):
        return 0
    # decided at send time: a hive with no eligible successor follows the solo order (L3)
    text = strong_notice(
        minutes,
        math.ceil(settings.handover_extension_seconds / 60),
        hive=not plan.solo and plan.successor,
    )
    client = client_factory(plan.session_id, leader["url"])
    outcome = await _note(client, leader["wrapper_session_id"], text, NOTICE_KEY)
    if outcome != "failed":
        state.notice_sent[plan.session_id] = (row_id, phase)
    return int(outcome == "ok")


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
    sent += await _push_due(plan, state, client_factory)
    sent += await _push_countdown(plan, state, settings, client_factory)
    sent += await _push_rollout(plan, state, settings, client_factory)
    return sent + await _push_outcomes(plan, state, client_factory)


# ---------------------------------------------------------------- limit subjects

# Gauge rows of the agents the limits apply to. A hive leader keeps its start context and
# tenure on `session_leaders`. A solo agent (the one live agent of a session that is not a
# hive) keeps them in `agent_limits`; a row older than the agent's last `session_cleared` is
# stale (the context was cleared since), so its start is unknown again and is recorded afresh;
# a gauge not updated since the clear still shows the old context and is read as no gauge.
_SUBJECT_COLUMNS = """
    c.id AS session_id, c.slug, c.spec, c.status, a.id AS agent_id, a.ordinal, a.pod_name,
    a.url, a.wrapper_session_id, a.ready,
    coalesce(s.state, 'unknown') AS state, s.context_pct,
    s.context_used_tokens, s.context_window_tokens
"""
_HIVE_SUBJECTS = f"""
SELECT {_SUBJECT_COLUMNS}, false AS solo, false AS gauge_stale, l.epoch,
    l.start_context_tokens AS start_tokens,
    extract(epoch FROM now() - l.since) AS tenure
FROM sessions c
JOIN session_leaders l ON l.session_id = c.id
JOIN agents a ON a.id = l.agent_id AND a.gone_at IS NULL
LEFT JOIN agent_status s ON s.agent_id = a.id
WHERE c.desired = 'active' AND {leadership.HIVE}
"""
_SOLO_SUBJECTS = f"""
SELECT {_SUBJECT_COLUMNS}, true AS solo, 0 AS epoch,
    EXISTS (SELECT 1 FROM events x WHERE x.agent_id = a.id AND x.type = 'session_cleared'
        AND x.created_at > coalesce(s.updated_at, '-infinity')) AS gauge_stale,
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


def _row_level(
    row: dict[str, Any], settings: Settings
) -> tuple[Levels, Literal["none", "weak", "strong"], int | None]:
    """The levels, the level and the start context to record (None when it is known already)."""
    fill = row["context_used_tokens"] if row["start_tokens"] is None else None
    lv = levels(row["spec"], row["start_tokens"] if fill is None else fill, settings)
    rollout = (row["status"] or {}).get("rollout") or {}
    level = due_level(
        lv,
        row["context_used_tokens"],
        row["context_window_tokens"],
        row["context_pct"],
        float(row["tenure"]),
        settings.handover_min_tenure_seconds,
        row["ordinal"] in (rollout.get("pending") or []),
    )
    return lv, level, fill


async def _push_solo_session(
    pool: AsyncConnectionPool,
    session_id: uuid.UUID,
    settings: Settings,
    client_factory: ClientFactory,
    state: HiveState,
    clock: Callable[[], float],
) -> int:
    """The solo counterpart of `_push_session`: under the lock, record the start context when
    unknown (first gauge, or the first after a clear) and decide the level; then, with no
    transaction open, send the weak note."""
    async with pool.connection() as conn, conn.transaction():
        if not await _try_lock(conn, session_id):
            return 0
        state.last_tick[session_id] = clock()
        rows = await _subject_rows(conn, session_id=session_id, solo=True)
        if not rows:
            return 0
        row = rows[0]
        lv, level, fill = _row_level(row, settings)
        if fill is not None:
            await handoffs.set_limits_start(conn, row["agent_id"], fill)
        candidates = [] if level != "none" else None
        countdown = await handoffs.countdown(conn, row["agent_id"])
    member = {k: row[k] for k in ("pod_name", "url", "wrapper_session_id", "ready", "state")}
    member.update(
        id=row["agent_id"],
        context_pct=row["context_pct"],
        context_used_tokens=row["context_used_tokens"],
        context_window_tokens=row["context_window_tokens"],
    )
    plan = _Plan(
        session_id=session_id,
        slug=row["slug"],
        levels=lv,
        level=level,
        advice="",
        fill_start=None,
        max_replicas=1,
        static=True,
        leader=row["pod_name"],
        leader_id=row["agent_id"],
        leader_ordinal=row["ordinal"],
        epoch=0,
        hashes={},
        members=[member],
        scalable=False,
        outcomes=[],
        candidates=candidates,
        solo=True,
        countdown=countdown,
    )
    sent = await _push_due(plan, state, client_factory)
    return sent + await _push_countdown(plan, state, settings, client_factory)


async def push_once(
    pool: AsyncConnectionPool,
    settings: Settings,
    client_factory: ClientFactory,
    state: HiveState | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> int:
    """One push pass over every hive session and solo agent; returns the number of notes accepted.

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
            total += await _push_solo_session(
                pool, session_id, settings, client_factory, state, clock
            )
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
    return total

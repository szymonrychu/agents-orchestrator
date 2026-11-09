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

The one exception is the leader's keyed `handover_due` note (hive-decoupling D4): a leader that is
due (see `due_level`) is told, once per leader, epoch, 5% bucket, level and candidate list, to
hand leadership over and to whom. It is due softly once its context grew by
ORCH_HANDOVER_BUDGET_TOKENS on top of the context it had when its leadership began
(`session_leaders.start_context_tokens`, filled by the dispatcher on the first tick the gauge is
known) and it has led for ORCH_HANDOVER_MIN_TENURE_SECONDS, never while its pod awaits a rollout;
without a token gauge the percent rule (compact - HANDOVER_MARGIN_PCT) applies. It is due hard
at compact - 5, whatever the tenure or rollout; the successor gets the leader's transcript and
a generated state, so there is no briefing protocol. It is evaluated on every push tick, outside
the hash and claim path above, and remembered per session in `HiveState`.

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

from agents_orchestrator.db import leadership
from agents_orchestrator.db.eligibility import (
    ELIGIBLE,
    FIRM_MARGIN_PCT,
    HANDOVER_MARGIN_PCT,
    HEADROOM,
    SUCCESSOR_ORDER,
)
from agents_orchestrator.db.queue import _CLAIM_LOCK_NAMESPACE
from agents_orchestrator.dispatcher import feed
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

# What the dispatcher last told a leader: (leader agent id, epoch, 5% bucket, firm level,
# candidate identity).
DueKey = tuple[uuid.UUID, int, int, bool, tuple[tuple[str, str], ...]]

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


# ---------------------------------------------------------------- the note text


def compact_pct(auto_compact_pct: int | None, settings: Settings) -> int:
    """The effective compaction point: the spec's `auto_compact_pct`, else the pods' default."""
    return settings.default_auto_compact_pct if auto_compact_pct is None else auto_compact_pct


@dataclass(frozen=True)
class Levels:
    """The handover levels of one leader."""

    hard_pct: int  # compact - FIRM_MARGIN_PCT
    fallback_pct: int  # max(compact - HANDOVER_MARGIN_PCT, 1), for an unknown token gauge
    start_tokens: int | None
    budget_tokens: int


def levels(spec: dict[str, Any], start_tokens: int | None, settings: Settings) -> Levels:
    """The levels of a leader whose context was `start_tokens` when its leadership began."""
    compact = compact_pct(spec.get("auto_compact_pct"), settings)
    return Levels(
        hard_pct=compact - FIRM_MARGIN_PCT,
        fallback_pct=max(compact - HANDOVER_MARGIN_PCT, 1),
        start_tokens=start_tokens,
        budget_tokens=settings.handover_budget_tokens,
    )


def due_level(
    lv: Levels,
    used_tokens: int | None,
    window_tokens: int | None,
    pct: float | None,
    tenure_seconds: float,
    min_tenure_seconds: int,
    rollout_pending: bool,
) -> Literal["none", "soft", "hard"]:
    """Whether a leader should hand over: `hard` at the hard limit whatever else holds, `soft`
    past its token budget (or the percent fallback) once the minimum tenure is over and its pod
    does not await a rollout, else `none`."""
    tokens = used_tokens is not None and window_tokens is not None and window_tokens > 0
    hard_tokens = lv.hard_pct / 100 * window_tokens if tokens and window_tokens else None
    if pct is not None and pct >= lv.hard_pct:
        return "hard"
    if (
        tokens
        and used_tokens is not None
        and hard_tokens is not None
        and used_tokens >= hard_tokens
    ):
        return "hard"
    if (
        tokens
        and lv.start_tokens is not None
        and used_tokens is not None
        and hard_tokens is not None
    ):
        hit = used_tokens >= min(lv.start_tokens + lv.budget_tokens, hard_tokens)
    else:
        hit = pct is not None and pct >= lv.fallback_pct
    if hit and tenure_seconds >= min_tenure_seconds and not rollout_pending:
        return "soft"
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
        f"before {lv.hard_pct}% context. {REREAD_RULE}"
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


def render_handover_due(
    context_pct: float,
    used_tokens: int | None,
    lv: Levels,
    epoch: int,
    candidates: list[dict[str, Any]],
    *,
    firm: bool,
    scalable: bool,
) -> str:
    """The `<handover-due>` text block pushed to the leader under the key `handover_due`.

    The leader hands over with its judgment as the reason; the successor receives the transcript
    and a generated state, so no briefing happens over ccw. `firm` adds the note that compaction
    is near.
    """
    parts = []
    if used_tokens is not None:
        parts.append(f"{round(used_tokens / 1000)}k tokens")
        if lv.start_tokens is not None:
            parts.append(f"started at {round(lv.start_tokens / 1000)}k")
            parts.append(f"budget to {round((lv.start_tokens + lv.budget_tokens) / 1000)}k")
    parts.append(f"hard limit {lv.hard_pct}%")
    lines = [
        "<handover-due>",
        f"Your context is {round(context_pct)}% ({'; '.join(parts)}); "
        "hand leadership over before the hard limit.",
    ]
    if not candidates:
        hint = "; scale up with orchestrator_scale" if scalable else ""
        lines.append(f"Candidates: none - no ready successor; keep leading{hint}.")
    else:
        names = ", ".join(feed._clean(c["pod_name"]) for c in candidates)
        lines.append(f"Candidates, best first: {names}.")
        if firm:
            lines.append(
                "You will compact soon and keep leading with a summary; "
                "hand over now unless the work is about to finish."
            )
        lines.append(
            "Finish the step you are in, then call "
            f"orchestrator_leadership_handover(to=<pod_name>, epoch={epoch}, "
            "reason=<your judgment for the successor: priorities, risks, what you would do next; "
            "facts such as open decisions, unhandled messages and members are generated for "
            "it>). It applies when your turn has ended and your background agents have "
            "reported; the successor receives your transcript. Do not brief it over ccw."
        )
        lines.append(
            "Pass handled=[ids] for session messages you answered or that need no answer; "
            "the rest carry over."
        )
        lines.append(REREAD_RULE)
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
    level: Literal["none", "soft", "hard"]
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
    """Tell a due leader to hand over, once per due key.

    Runs on every tick, whatever the hive_state hash says. When the leader is not due (or with no
    leader to tell) nothing is sent and the key is forgotten, so a leader that compacted and
    climbs again is told again. A failed push is not remembered: the next tick retries it.
    """
    leader = next((m for m in plan.receivers if m["pod_name"] == plan.leader), None)
    if plan.candidates is None or leader is None:
        if plan.candidates is None:
            state.due_sent.pop(plan.session_id, None)
        return 0
    identity = tuple((c["pod_name"], c["wrapper_session_id"]) for c in plan.candidates)
    used, window = leader["context_used_tokens"], leader["context_window_tokens"]
    pct = leader["context_pct"]
    if pct is None:  # due by tokens alone
        pct = 100 * used / window
    firm = plan.level == "hard"
    key: DueKey = (plan.leader_id, plan.epoch, int(pct // BUCKET_PCT), firm, identity)
    if state.due_sent.get(plan.session_id) == key:
        return 0
    text = render_handover_due(
        pct, used, plan.levels, plan.epoch, plan.candidates, firm=firm, scalable=plan.scalable
    )
    client = client_factory(plan.session_id, leader["url"])
    outcome = await _note(client, leader["wrapper_session_id"], text, DUE_KEY)
    if outcome != "failed":
        state.due_sent[plan.session_id] = key
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
    sent += await _push_rollout(plan, state, settings, client_factory)
    return sent + await _push_outcomes(plan, state, client_factory)


async def push_once(
    pool: AsyncConnectionPool,
    settings: Settings,
    client_factory: ClientFactory,
    state: HiveState | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> int:
    """One push pass over every hive session; returns the number of notes accepted.

    A session is evaluated at most once per PUSH_INTERVAL_SECONDS (by `clock`). One failing
    session is logged and never holds up the others.
    """
    state = state if state is not None else HiveState()
    async with pool.connection() as conn:
        sessions = await leadership.hive_sessions(conn)
    total = 0
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

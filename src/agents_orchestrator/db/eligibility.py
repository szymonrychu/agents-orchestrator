"""The pick's predicate and the handover trigger, shared by the pass, `set_replicas` and the hold.

They live apart from `db/leadership.py` because that module imports `db/queue.py`, and the
claim hold in the queue needs the same SQL. Every eligibility and handover-pending predicate is
spelled once here; `leadership`, `queue` and `sessions` embed these fragments and never respell
them.

Design D4 (deploy-safe-agent-sessions): an agent whose `agent_status.state` is `dead` never
leads. `DEAD` is that one test; `READY_STAYS` (so `ELIGIBLE` too) excludes it, and the leadership
pass reads it for the leader itself, so a dead leader is handled as a lost leader.
"""

from __future__ import annotations

# An agent stays when its ordinal is below its session's `spec.replicas` (a spec without one
# never removes anyone). It can lead when it also is ready, not gone, not dead and has a wrapper
# session. An agent without an `agent_status` row is not dead.
# `STAYS`, `DEAD`, `READY_STAYS` and `ELIGIBLE` take the agent alias `a`; `ELIGIBLE` is the pick's
# predicate and `sessions.set_replicas` and the claim hold share it, `READY_STAYS` is the manual
# make-leader target's (no wrapper session required). `REPLICAS` takes the session alias `c`.
# Used inside a subquery, an inner `a` shadows an outer one, which is what the callers rely on.
NO_LIMIT = 2147483647
REPLICAS = f"coalesce((c.spec->>'replicas')::int, {NO_LIMIT})"
STAYS = (
    "a.ordinal < coalesce((SELECT (cs.spec->>'replicas')::int FROM sessions cs"
    f" WHERE cs.id = a.session_id), {NO_LIMIT})"
)
DEAD = "EXISTS (SELECT 1 FROM agent_status ds WHERE ds.agent_id = a.id AND ds.state = 'dead')"
READY_STAYS = f"a.ready AND a.gone_at IS NULL AND NOT {DEAD} AND {STAYS}"
# Pod shutdown (T-256): the agent's wrapper announced its shutdown routine and its stream has not
# attached to the next pod yet. A stopping agent is never handed a message (the claim's pick)
# and never picked as a successor. Alias `a`.
NOT_STOPPING = (
    "NOT EXISTS (SELECT 1 FROM agent_status ss WHERE ss.agent_id = a.id"
    " AND ss.stopping_since IS NOT NULL)"
)
ELIGIBLE = f"{READY_STAYS} AND a.wrapper_session_id IS NOT NULL AND {NOT_STOPPING}"
# The agents the dispatcher can hand a message to now (T-167): ready, not gone, not dead and
# holding a wrapper session. Not `STAYS`: a leaving agent still holds the message it is given.
# Behind the summary's `ready_with_session`. Alias `a`.
HOLDS_SESSION = f"a.ready AND a.gone_at IS NULL AND NOT {DEAD} AND a.wrapper_session_id IS NOT NULL"

# The handover trigger: the leader's pod is leaving. Aliases: `l` the `session_leaders` row, `a`
# the leader's agent, `c` the session. `LEAVING` is the ordinal test alone (the pass reads it as
# a column); the trigger also requires the agent not gone.
LEAVING = f"a.ordinal >= {REPLICAS}"
HANDOVER_TRIGGER = f"(a.gone_at IS NULL AND {LEAVING})"

# The notice hold: after a leader change the new leader gets the handover notice first, so
# untargeted messages wait until the api sweeper has queued it (it then sets
# `sessions.handover_notice_epoch`). The hold ends at the latest NOTICE_HOLD_SECONDS after the
# change, so a down sweeper cannot stall the session. Aliases `l` (session_leaders), `c` (sessions).
NOTICE_HOLD_SECONDS = 60
NOTICE_PENDING = (
    "(l.previous_agent_id IS NOT NULL AND l.epoch > c.handover_notice_epoch"
    f" AND l.since > now() - make_interval(secs => {NOTICE_HOLD_SECONDS}))"
)

# A running background subagent keeps its leader from being moved.
# `agent_status.background_subagents` maps subagent id -> start time; an entry older than
# BACKGROUND_STALE_SECONDS is ignored, so a lost stop event cannot pin a leader forever.
# Alias `s` is a LEFT JOINed `agent_status` row.
BACKGROUND_STALE_SECONDS = 7200


def background_busy(alias: str) -> str:
    """The background-busy SQL predicate over the `agent_status` row aliased `alias`."""
    return (
        f"coalesce((SELECT bool_or(b.value::timestamptz"
        f" > now() - make_interval(secs => {BACKGROUND_STALE_SECONDS}))"
        f" FROM jsonb_each_text({alias}.background_subagents) b), false)"
    )


BACKGROUND_BUSY = background_busy("s")

# Hard limit: the effective compaction point (the spec's `auto_compact_pct`, else `%(compact)s`,
# ORCH_DEFAULT_AUTO_COMPACT_PCT) less FIRM_MARGIN_PCT. Alias `c` (sessions).
# `dispatcher/hive.levels` is the same formula in Python.
FIRM_MARGIN_PCT = 5
HARD_PCT = f"(coalesce((c.spec->>'auto_compact_pct')::int, %(compact)s) - {FIRM_MARGIN_PCT})"

# A successor must start with room: its context at least MIN_HEADROOM_TOKENS below the hard limit
# (MIN_HEADROOM_PCT points when the token gauge is unknown), so it is not due at once. Aliases
# `s` (LEFT JOINed agent_status) and `c` (sessions).
MIN_HEADROOM_TOKENS = 50000
MIN_HEADROOM_PCT = 10
_HARD_TOKENS = f"({HARD_PCT} / 100.0 * s.context_window_tokens)"
# The margin is clamped to half the hard limit, so a small window or a low compaction point never
# drives the threshold negative.
HEADROOM = (
    "(CASE WHEN s.context_used_tokens IS NOT NULL AND s.context_window_tokens > 0"
    f" THEN s.context_used_tokens <= {_HARD_TOKENS}"
    f" - least({MIN_HEADROOM_TOKENS}, 0.5 * {_HARD_TOKENS})"
    f" ELSE coalesce(s.context_pct, 0) <= {HARD_PCT} - {MIN_HEADROOM_PCT} END)"
)

# True when the agent's pod awaits its rollout (its ordinal is in `status.rollout.pending`, JSON
# ints). Aliases `a` (agents) and `c` (sessions).
ROLLOUT_PENDING = "coalesce(c.status->'rollout'->'pending' @> to_jsonb(a.ordinal), false)"

# One successor rule (voluntary-handover 3.2), shared by every pick of the leadership pass and
# the leader's `handover_due` candidate list, so a moved leader is always the agent the note
# would have named and nobody hands leadership straight back. Among ELIGIBLE agents: not awaiting
# a rollout first, not background-busy, idle first, lowest `context_pct` (unknown last), then
# ordinal. Aliases `a` (agents), `s` (LEFT JOINed agent_status) and `c` (sessions).
SUCCESSOR_ORDER = (
    f"({ROLLOUT_PENDING}), ({BACKGROUND_BUSY}), (s.state = 'idle') IS NOT TRUE,"
    " s.context_pct ASC NULLS LAST, a.ordinal"
)

# Context % margin below the compaction point used as the percent fallback of the soft handover
# rule (a leader whose token gauge is unknown). `dispatcher/hive.levels` applies it.
HANDOVER_MARGIN_PCT = 15

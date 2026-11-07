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
ELIGIBLE = f"{READY_STAYS} AND a.wrapper_session_id IS NOT NULL"

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
BACKGROUND_BUSY = (
    "coalesce((SELECT bool_or(b.value::timestamptz"
    f" > now() - make_interval(secs => {BACKGROUND_STALE_SECONDS}))"
    " FROM jsonb_each_text(s.background_subagents) b), false)"
)

# One successor rule (voluntary-handover 3.2), shared by every pick of the leadership pass and
# the leader's `handover_due` candidate list, so a moved leader is always the agent the note
# would have named and nobody hands leadership straight back. Among ELIGIBLE agents: not
# background-busy first, idle first, lowest `context_pct` (unknown last), then ordinal.
# Aliases `a` (agents) and `s` (LEFT JOINed agent_status).
SUCCESSOR_ORDER = (
    f"({BACKGROUND_BUSY}), (s.state = 'idle') IS NOT TRUE, s.context_pct ASC NULLS LAST, a.ordinal"
)

# Context % above which a leader should hand over instead of compacting: the effective compaction
# point (the spec's `auto_compact_pct`, else `%(compact)s`, ORCH_DEFAULT_AUTO_COMPACT_PCT) less
# HANDOVER_MARGIN_PCT, at least 1. Alias `c` (sessions). `dispatcher/hive.handover_threshold` is
# the same formula in Python.
HANDOVER_MARGIN_PCT = 15
THRESHOLD = (
    "greatest(coalesce((c.spec->>'auto_compact_pct')::int, %(compact)s)"
    f" - {HANDOVER_MARGIN_PCT}, 1)"
)

-- Graceful agent handover (T-256). `agent_handoffs` holds one handover of an agent's working
-- context per row: a strong context limit, a shutdown or a voluntary request. At most one open
-- row per agent (partial unique index). Written through db/handoffs.py only, by the dispatcher
-- (decisions) and the agent route (inserts, extend), always with status-guarded updates.
-- `agent_limits` records the context size an agent started with (the weak/strong thresholds are
-- relative to it). `agent_status.stopping_since` marks an agent that is being stopped.
CREATE TABLE IF NOT EXISTS agent_handoffs (
    id bigserial PRIMARY KEY,
    session_id uuid NOT NULL REFERENCES sessions (id) ON DELETE CASCADE,
    agent_id uuid NOT NULL REFERENCES agents (id) ON DELETE CASCADE,
    trigger text NOT NULL CHECK (trigger IN ('strong', 'shutdown', 'voluntary')),
    phase text NOT NULL CHECK (phase IN (
        'notified', 'extended', 'requested', 'handed', 'discussing', 'clearing', 'returning',
        'done', 'cancelled')),
    notified_at timestamptz,
    deadline_at timestamptz,
    extended_at timestamptz,
    request_message_id bigint,
    handoff_text text,
    successor_agent_id uuid,
    delivered_at timestamptz,
    discuss_since timestamptz,
    clear_command_id bigint,
    clear_attempts int NOT NULL DEFAULT 0,
    cleared_at timestamptz,
    return_message_id bigint,
    created_at timestamptz NOT NULL DEFAULT now(),
    closed_at timestamptz,
    close_reason text
);
CREATE UNIQUE INDEX IF NOT EXISTS agent_handoffs_one_open ON agent_handoffs (agent_id)
    WHERE phase NOT IN ('done', 'cancelled');

CREATE TABLE IF NOT EXISTS agent_limits (
    agent_id uuid PRIMARY KEY REFERENCES agents (id) ON DELETE CASCADE,
    start_context_tokens int,
    since timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE agent_status ADD COLUMN IF NOT EXISTS stopping_since timestamptz;

-- Notice kinds: the union of 0045's kinds plus the three handoff kinds.
ALTER TABLE messages DROP CONSTRAINT IF EXISTS messages_notice_check;
ALTER TABLE messages ADD CONSTRAINT messages_notice_check
    CHECK (notice IN (
        'inactivity_reminder', 'inactivity_close', 'handover', 'proposal', 'project_grant',
        'handoff_notice', 'handoff_request', 'handoff_return'));

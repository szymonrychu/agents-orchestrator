-- Cooperative context-pressure handoff (T-423). The strong-limit countdown, its deadline and
-- extension are gone: an agent under context pressure submits its own handoff and the
-- orchestrator clears it only after an accepted submit. `agent_handoffs` is written only by
-- db/handoffs.py, `chat_activity` only by db/chat_activity.py, `messages` only by db/queue.py.

-- 1. Legacy rows of the removed flow are closed. Their queued messages go first (the guard reads
-- the still-open rows). Shutdown rows belong to the pod-shutdown path and are left alone.
UPDATE messages SET status = 'cancelled'
WHERE status = 'queued'
  AND notice IN ('handoff_notice', 'handoff_request', 'handoff_return')
  AND target_agent_id IN (
      SELECT agent_id FROM agent_handoffs
      WHERE trigger IN ('strong', 'voluntary')
        AND phase IN ('notified', 'extended', 'requested', 'handed', 'discussing', 'clearing',
                      'returning'));

UPDATE agent_handoffs
SET phase = 'cancelled', close_reason = 'superseded', closed_at = now()
WHERE trigger IN ('strong', 'voluntary')
  AND phase IN ('notified', 'extended', 'requested', 'handed', 'discussing', 'clearing',
                'returning');

-- 3. Queued forced-limit leadership requests (the row-116 class) are rejected, status-guarded.
UPDATE leadership_requests
SET status = 'rejected', reject_reason = 'superseded', decided_at = now()
WHERE status = 'queued' AND kind = 'handover' AND reason = 'limit';

-- 4. Columns.
ALTER TABLE agent_handoffs
    DROP COLUMN IF EXISTS deadline_at,
    DROP COLUMN IF EXISTS extended_at,
    DROP COLUMN IF EXISTS notified_at,
    DROP COLUMN IF EXISTS request_message_id;
ALTER TABLE agent_handoffs
    ADD COLUMN IF NOT EXISTS level smallint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS profile text,
    ADD COLUMN IF NOT EXISTS handoff jsonb,
    ADD COLUMN IF NOT EXISTS rejects int NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS reject_reasons jsonb NOT NULL DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS inflight jsonb,
    ADD COLUMN IF NOT EXISTS gate_blocks int NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS submitted_at timestamptz,
    ADD COLUMN IF NOT EXISTS escalated_at timestamptz,
    ADD COLUMN IF NOT EXISTS nudged_level smallint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS l2_tokens_at int,
    ADD COLUMN IF NOT EXISTS submit_tokens int,
    ADD COLUMN IF NOT EXISTS progress_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS stuck_reported_at timestamptz,
    ADD COLUMN IF NOT EXISTS leadership_request_id bigint;
ALTER TABLE agent_handoffs DROP CONSTRAINT IF EXISTS agent_handoffs_profile_check;
ALTER TABLE agent_handoffs ADD CONSTRAINT agent_handoffs_profile_check
    CHECK (profile IN ('work', 'chat'));

-- 5. Triggers and phases. `handed` stays for shutdown rows only. The partial unique index
-- `agent_handoffs_one_open` (agent_id, phase not done or cancelled) is unchanged.
ALTER TABLE agent_handoffs DROP CONSTRAINT IF EXISTS agent_handoffs_trigger_check;
ALTER TABLE agent_handoffs ADD CONSTRAINT agent_handoffs_trigger_check
    CHECK (trigger IN ('strong', 'shutdown', 'voluntary', 'pressure'));
ALTER TABLE agent_handoffs DROP CONSTRAINT IF EXISTS agent_handoffs_phase_check;
ALTER TABLE agent_handoffs ADD CONSTRAINT agent_handoffs_phase_check
    CHECK (phase IN (
        'pressuring', 'submitted', 'moving', 'clearing', 'discussing', 'returning', 'handed',
        'done', 'cancelled'));

-- 6. Notice kinds: the union of 0049's kinds plus the two new ones (history rows keep the old).
ALTER TABLE messages DROP CONSTRAINT IF EXISTS messages_notice_check;
ALTER TABLE messages ADD CONSTRAINT messages_notice_check
    CHECK (notice IN (
        'inactivity_reminder', 'inactivity_close', 'handover', 'proposal', 'project_grant',
        'handoff_notice', 'handoff_request', 'handoff_return', 'autoclose', 'runaway_parked',
        'handoff_nudge', 'handoff_escalation'));

-- 7. The service that posted a message (null for a user), read for chat activity.
ALTER TABLE messages ADD COLUMN IF NOT EXISTS origin_service text;

-- 8. Chat activity: an inbound connector message or an outbound chat post of an agent.
CREATE TABLE IF NOT EXISTS chat_activity (
    id bigserial PRIMARY KEY,
    session_id uuid NOT NULL REFERENCES sessions (id) ON DELETE CASCADE,
    agent_id uuid REFERENCES agents (id) ON DELETE CASCADE,
    direction text NOT NULL CHECK (direction IN ('in', 'out')),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS chat_activity_session_created
    ON chat_activity (session_id, created_at);

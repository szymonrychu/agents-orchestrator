-- Ask-then-fallback closes (session-rotation, T-362). Written only by the api and the ingester;
-- idempotent.
-- sessions.autoclose_asked_at: when the autoclose lifetime was last asked; a kept session buys one
-- more full lifetime from it.
-- agent_status.activity_at: the newest tool or subagent event of the agent, from the wrapper
-- event time; the inactivity quiet clock reads it.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS autoclose_asked_at timestamptz;
ALTER TABLE agent_status ADD COLUMN IF NOT EXISTS activity_at timestamptz;

ALTER TABLE sessions DROP CONSTRAINT IF EXISTS sessions_close_kind_check;
ALTER TABLE sessions ADD CONSTRAINT sessions_close_kind_check
    CHECK (close_kind IN ('self', 'inactivity', 'autoclose'));

-- Notice kinds: the union of 0047's kinds plus autoclose.
ALTER TABLE messages DROP CONSTRAINT IF EXISTS messages_notice_check;
ALTER TABLE messages ADD CONSTRAINT messages_notice_check
    CHECK (notice IN (
        'inactivity_reminder', 'inactivity_close', 'handover', 'proposal', 'project_grant',
        'handoff_notice', 'handoff_request', 'handoff_return', 'autoclose'));

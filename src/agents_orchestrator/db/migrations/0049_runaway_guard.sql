-- Runaway guard (T-224). agent_status guard_* columns: written only by the ingester.
-- sessions park columns and guard_reset_at: written only by the api process.
ALTER TABLE agent_status
    ADD COLUMN IF NOT EXISTS guard_window_at timestamptz,
    ADD COLUMN IF NOT EXISTS guard_turns int NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS guard_tokens bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS guard_stop_loop_at timestamptz;
ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS parked_at timestamptz,
    ADD COLUMN IF NOT EXISTS parked_reason text,
    ADD COLUMN IF NOT EXISTS parked_detail jsonb,
    ADD COLUMN IF NOT EXISTS park_notified_at timestamptz,
    ADD COLUMN IF NOT EXISTS guard_reset_at timestamptz;
ALTER TABLE sessions DROP CONSTRAINT IF EXISTS sessions_parked_reason_check;
ALTER TABLE sessions ADD CONSTRAINT sessions_parked_reason_check
    CHECK (parked_reason IN ('turns', 'tokens', 'stop_loop'));

-- Notice kinds: the union of 0048's kinds plus runaway_parked.
ALTER TABLE messages DROP CONSTRAINT IF EXISTS messages_notice_check;
ALTER TABLE messages ADD CONSTRAINT messages_notice_check
    CHECK (notice IN (
        'inactivity_reminder', 'inactivity_close', 'handover', 'proposal', 'project_grant',
        'handoff_notice', 'handoff_request', 'handoff_return', 'autoclose', 'runaway_parked'));

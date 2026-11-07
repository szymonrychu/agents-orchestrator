-- Voluntary leadership handover (T-54). Idempotent.
-- agent_status.background_subagents: ingester-written map of running background subagent id to
-- start time; a leader with live entries is never picked as successor.
-- session_leaders.previous_agent_id: dispatcher-written predecessor of the current leader row.
-- sessions.handover_notice_epoch: api-sweeper-written last epoch a handover notice was sent for;
-- backfilled from the current leader epoch so a deploy sends no notice for a pre-migration leader.
ALTER TABLE agent_status
    ADD COLUMN IF NOT EXISTS background_subagents jsonb NOT NULL DEFAULT '{}';
ALTER TABLE session_leaders
    ADD COLUMN IF NOT EXISTS previous_agent_id uuid REFERENCES agents (id) ON DELETE SET NULL;
ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS handover_notice_epoch int NOT NULL DEFAULT 0;
UPDATE sessions s SET handover_notice_epoch = l.epoch
FROM session_leaders l
WHERE l.session_id = s.id AND s.handover_notice_epoch < l.epoch;

-- A third orchestrator-written notice kind.
ALTER TABLE messages DROP CONSTRAINT IF EXISTS messages_notice_check;
ALTER TABLE messages ADD CONSTRAINT messages_notice_check
    CHECK (notice IN ('inactivity_reminder', 'inactivity_close', 'handover'));

-- leader_lease_seconds is removed (no alias): strip it from stored specs.
UPDATE sessions SET spec = spec - 'leader_lease_seconds' WHERE spec ? 'leader_lease_seconds';
UPDATE presets SET spec = spec - 'leader_lease_seconds' WHERE spec ? 'leader_lease_seconds';

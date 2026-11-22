-- Hive envelopes (S-71): agent-to-agent and project-room envelopes the agent writes in its own
-- text. envelope_outbox gains the kinds session, agent and project_post plus their routing
-- columns; messages gain a channel so agent-channel rows never count as session messages.
ALTER TABLE envelope_outbox DROP CONSTRAINT IF EXISTS envelope_outbox_kind_check;
ALTER TABLE envelope_outbox ADD CONSTRAINT envelope_outbox_kind_check
    CHECK (kind IN ('send', 'react', 'notice', 'session', 'agent', 'project_post'));
ALTER TABLE envelope_outbox ADD COLUMN IF NOT EXISTS target text;
ALTER TABLE envelope_outbox ADD COLUMN IF NOT EXISTS lane text;
ALTER TABLE envelope_outbox ADD COLUMN IF NOT EXISTS notify text;
ALTER TABLE envelope_outbox ADD COLUMN IF NOT EXISTS handled integer[];
ALTER TABLE envelope_outbox ADD COLUMN IF NOT EXISTS project text;
ALTER TABLE envelope_outbox ADD COLUMN IF NOT EXISTS message_id bigint
    REFERENCES messages (id) ON DELETE SET NULL;

ALTER TABLE messages ADD COLUMN IF NOT EXISTS channel text NOT NULL DEFAULT 'session'
    CHECK (channel IN ('session', 'agent'));
ALTER TABLE messages ADD COLUMN IF NOT EXISTS sender_agent_id uuid
    REFERENCES agents (id) ON DELETE SET NULL;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS notify text CHECK (notify IN ('idle'));
ALTER TABLE messages ADD COLUMN IF NOT EXISTS envelope_ref text;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS notified_at timestamptz;

DROP INDEX IF EXISTS messages_unhandled_incoming;
CREATE INDEX messages_unhandled_incoming ON messages (session_id, id)
    WHERE sender_session_id IS NOT NULL AND channel = 'session' AND notice IS NULL
    AND handled_at IS NULL;

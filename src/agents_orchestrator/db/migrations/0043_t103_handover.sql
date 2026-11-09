-- T-103 cheaper hive leader handovers. Writers: session_leaders.start_context_tokens and
-- previous_since belong to the dispatcher; messages.handled_at to the api; handover_log to the
-- api sweeper. handover_log has no foreign key on purpose: the measurements outlive a purged
-- session.
ALTER TABLE session_leaders ADD COLUMN IF NOT EXISTS start_context_tokens integer;
ALTER TABLE session_leaders ADD COLUMN IF NOT EXISTS previous_since timestamptz;

ALTER TABLE messages ADD COLUMN IF NOT EXISTS handled_at timestamptz;
CREATE INDEX IF NOT EXISTS messages_unhandled_incoming ON messages (session_id, id)
    WHERE sender_session_id IS NOT NULL AND notice IS NULL AND handled_at IS NULL;

CREATE TABLE IF NOT EXISTS handover_log (
    id bigserial PRIMARY KEY,
    session_id uuid NOT NULL,
    epoch int NOT NULL,
    kind text NOT NULL,
    tenure_seconds double precision,
    intake_bytes int NOT NULL,
    delivered_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (session_id, epoch)
);
CREATE INDEX IF NOT EXISTS handover_log_delivered_at ON handover_log (delivered_at);

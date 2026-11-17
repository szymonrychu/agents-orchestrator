-- Chat envelopes the agent writes in its own text (S-70). Rows are inserted by the ingester and
-- the onStop endpoint; status changes only through status-guarded updates (db/envelope_outbox.py).
CREATE TABLE IF NOT EXISTS envelope_outbox (
    id bigserial PRIMARY KEY,
    session_id uuid NOT NULL REFERENCES sessions (id) ON DELETE CASCADE,
    agent_id uuid REFERENCES agents (id) ON DELETE CASCADE,
    wrapper_session_id text,
    event_seq bigint,
    envelope_index integer,
    turn_id text,
    kind text NOT NULL CHECK (kind IN ('send', 'react', 'notice')),
    room text,
    reply_to text,
    files text[],
    body text,
    react_key text,
    status text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'sending', 'accepted', 'refused', 'failed', 'invalid')),
    error_code text,
    error text,
    send_id text,
    attempts integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS envelope_outbox_event_unique
    ON envelope_outbox (agent_id, wrapper_session_id, event_seq, envelope_index)
    WHERE event_seq IS NOT NULL;
CREATE INDEX IF NOT EXISTS envelope_outbox_session_status
    ON envelope_outbox (session_id, status, id);
CREATE INDEX IF NOT EXISTS envelope_outbox_turn
    ON envelope_outbox (agent_id, turn_id) WHERE turn_id IS NOT NULL;

-- Written only by the onStop endpoint.
CREATE TABLE IF NOT EXISTS envelope_stop_checks (
    agent_id uuid NOT NULL REFERENCES agents (id) ON DELETE CASCADE,
    turn_id text NOT NULL,
    blocked_at timestamptz NOT NULL DEFAULT now(),
    blocks_seen integer NOT NULL DEFAULT 1,
    PRIMARY KEY (agent_id, turn_id)
);

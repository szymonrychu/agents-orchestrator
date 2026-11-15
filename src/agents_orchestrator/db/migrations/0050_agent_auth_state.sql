-- Per agent auth state reported by the wrapper (its T-225 release), written only by the
-- ingester: auth_state 'failed' while claude shows a re-login screen, the wrapper's reason, and
-- when it was first seen. All null otherwise; a new pod's snapshot clears them.
ALTER TABLE agent_status
    ADD COLUMN IF NOT EXISTS auth_state text CHECK (auth_state IN ('failed')),
    ADD COLUMN IF NOT EXISTS auth_reason text,
    ADD COLUMN IF NOT EXISTS auth_failed_at timestamptz;

-- A leader's request to close its own session when idle (hive-self-close). Written only by the
-- api: set by POST /agent/v1/close, cleared by DELETE /api/v1/sessions/{id}/close.
ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS close_requested_at timestamptz,
    ADD COLUMN IF NOT EXISTS close_reason text;

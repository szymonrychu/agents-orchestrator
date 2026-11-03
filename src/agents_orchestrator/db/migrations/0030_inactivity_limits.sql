-- Inactivity limits (hive-inactivity-limits). Written only by the api; idempotent.
-- sessions.last_update_at is the api-written "last user-visible update" stamp; DEFAULT now() fills
-- existing rows with the migration time, so a deploy neither reminds nor closes a long-lived session.
ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS last_update_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS inactivity_reminded_at timestamptz,
    ADD COLUMN IF NOT EXISTS close_kind text,
    ADD COLUMN IF NOT EXISTS close_deadline_at timestamptz;
ALTER TABLE sessions DROP CONSTRAINT IF EXISTS sessions_close_kind_check;
ALTER TABLE sessions ADD CONSTRAINT sessions_close_kind_check CHECK (close_kind IN ('self', 'inactivity'));
UPDATE sessions SET close_kind = 'self' WHERE close_requested_at IS NOT NULL AND close_kind IS NULL;

-- An orchestrator-written message carries a notice kind; user, trigger and peer messages are null.
ALTER TABLE messages ADD COLUMN IF NOT EXISTS notice text;
ALTER TABLE messages DROP CONSTRAINT IF EXISTS messages_notice_check;
ALTER TABLE messages ADD CONSTRAINT messages_notice_check
    CHECK (notice IN ('inactivity_reminder', 'inactivity_close'));

-- discard_after_idle_seconds is replaced by inactivity.close_seconds (no alias).
UPDATE presets SET spec = (spec - 'discard_after_idle_seconds') || jsonb_build_object(
    'inactivity',
    COALESCE(spec->'inactivity', '{}'::jsonb)
        || jsonb_build_object('close_seconds', spec->'discard_after_idle_seconds'))
WHERE spec ? 'discard_after_idle_seconds';
UPDATE sessions SET spec = (spec - 'discard_after_idle_seconds') || jsonb_build_object(
    'inactivity',
    COALESCE(spec->'inactivity', '{}'::jsonb)
        || jsonb_build_object('close_seconds', spec->'discard_after_idle_seconds'))
WHERE spec ? 'discard_after_idle_seconds';

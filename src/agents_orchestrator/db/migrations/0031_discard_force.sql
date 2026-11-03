-- Forced discard (T-26). Written only by the api; idempotent.
-- discard_forced_at is set by DELETE ...?force=true (also on a session already deleting); the
-- reconciler then abandons every owed pre-discard hook with reason "forced" and finishes the
-- discard without waiting for them.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS discard_forced_at timestamptz;

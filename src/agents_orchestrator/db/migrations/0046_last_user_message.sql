-- When the user last messaged a session, from any chat path (T-137). Written only by the api
-- (POST /sessions/{id}/messages with from_user); idempotent. Nullable, no default, no backfill:
-- nothing recorded authorship before.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS last_user_message_at timestamptz;

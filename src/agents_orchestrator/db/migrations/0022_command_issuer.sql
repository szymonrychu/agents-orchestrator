-- Who issued a command and which held message it releases (superleader-crud O8 fix). The
-- issuer scopes the admin command read to the superleader that made it; the note message is a
-- self-restart note held until the last restart command naming it finished. Additive and
-- idempotent like 0006; NULL for every command the main port or a user made.
ALTER TABLE commands ADD COLUMN IF NOT EXISTS issued_by_session uuid;
ALTER TABLE commands ADD COLUMN IF NOT EXISTS note_message_id bigint;

-- Slash messages (D1): `kind` is 'command' when the api stored a slash command text
-- (agents_orchestrator.slash.parse matched), else 'turn'; the api writes it once, at insert, and
-- the text stays raw. `wrapper_command_id` is the claude-code-wrapper command id of a dispatched
-- command message, written by the dispatcher when it marks the message sent; NULL for turns and
-- for every message that existed before this column. The `commands` table is untouched.
-- Additive and idempotent like 0006.
ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS kind text NOT NULL DEFAULT 'turn' CHECK (kind IN ('turn', 'command'));
ALTER TABLE messages ADD COLUMN IF NOT EXISTS wrapper_command_id text;

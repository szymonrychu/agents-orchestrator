-- Free-form labels on presets and sessions (agents-taskboard matches a task to a session when
-- they share a label). A session's labels are a snapshot of its preset's at creation, then
-- editable on their own. No registry table. Additive and idempotent like 0006; PG16-compatible.
ALTER TABLE presets ADD COLUMN IF NOT EXISTS labels text[] NOT NULL DEFAULT '{}';
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS labels text[] NOT NULL DEFAULT '{}';
CREATE INDEX IF NOT EXISTS sessions_labels_gin ON sessions USING gin (labels);

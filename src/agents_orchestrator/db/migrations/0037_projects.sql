-- Projects (T-19 projects epic, stage 1). Written only by the api. A project groups presets and
-- sessions; it is archived, never deleted, so a slug stored in another service never dangles.
-- 'default' is seeded and can never be archived (enforced by the repository). Every existing
-- preset and session lands in 'default' through the column default: no data rewrite.
-- Numbered 0037 after the sibling epics' reserved 0032-0036; the migrator keys on a set.
-- Idempotent like 0027.
CREATE TABLE IF NOT EXISTS projects (
    slug text PRIMARY KEY CHECK (slug ~ '^[a-z0-9][a-z0-9-]{0,39}$'),
    name text NOT NULL CHECK (char_length(name) BETWEEN 1 AND 80),
    description text NOT NULL DEFAULT '' CHECK (char_length(description) <= 4000),
    archived_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO projects (slug, name) VALUES ('default', 'Default') ON CONFLICT DO NOTHING;
ALTER TABLE presets ADD COLUMN IF NOT EXISTS project text NOT NULL DEFAULT 'default'
    REFERENCES projects (slug);
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS project text NOT NULL DEFAULT 'default'
    REFERENCES projects (slug);
CREATE INDEX IF NOT EXISTS presets_project_idx ON presets (project);
CREATE INDEX IF NOT EXISTS sessions_project_idx ON sessions (project);

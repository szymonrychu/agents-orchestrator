-- Named, versioned directory backups (S-68, T-505). Written only by db/backups.py.
CREATE TABLE IF NOT EXISTS backups (
    name text PRIMARY KEY,
    current_version uuid,
    last_used_at timestamptz,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS backup_versions (
    id uuid PRIMARY KEY,
    name text NOT NULL REFERENCES backups (name) ON DELETE CASCADE,
    object_key text UNIQUE NOT NULL,
    size bigint NOT NULL,
    digest text NOT NULL,
    manifest jsonb,
    base_version uuid,
    origin text NOT NULL CHECK (origin IN ('save', 'copy', 'migration')),
    session_slug text,
    preset_id uuid,
    project text,
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    state text NOT NULL CHECK (state IN ('current', 'superseded', 'orphan'))
);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'backups_current_version_fkey') THEN
        ALTER TABLE backups ADD CONSTRAINT backups_current_version_fkey
            FOREIGN KEY (current_version) REFERENCES backup_versions (id)
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS backup_versions_name_created_idx
    ON backup_versions (name, created_at DESC);

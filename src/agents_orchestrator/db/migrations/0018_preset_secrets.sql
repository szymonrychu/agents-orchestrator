-- Write-only secrets of a preset (superleader-crud D5/D7). The values live in immutable
-- Kubernetes Secrets named ps-<preset id8>-<hash6 of name>-v<version>; this table holds only
-- the metadata. The api is the only writer. No foreign key to presets: a deleted preset's rows
-- stay until every version a live session still references is collected (retired_at set).
CREATE TABLE preset_secrets (
    preset_id uuid NOT NULL,
    name text NOT NULL,
    version integer NOT NULL CHECK (version >= 1),
    -- versions up to this one are known deleted (bounds the delete calls of a collection pass)
    swept integer NOT NULL DEFAULT 0,
    retired_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (preset_id, name)
);

-- The Secret names a session was created with, {ENV_NAME: secret_name}; NULL: none. Set once at
-- insert by the api; the collection pass keeps every Secret a stored session still names.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS secret_versions jsonb;

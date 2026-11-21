-- Volumes as first-class objects (S-77). Written only by db/volumes.py.
CREATE TABLE IF NOT EXISTS volumes (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    projects text[] NOT NULL CHECK (cardinality(projects) >= 1),
    labels text[] NOT NULL DEFAULT '{}',
    description text NOT NULL DEFAULT '',
    exclude text[] NOT NULL DEFAULT '{}',
    size text NOT NULL,
    storage_class text NOT NULL,
    pvc_name text NOT NULL,
    state text NOT NULL CHECK (state IN (
        'provisioning', 'ready', 'resizing', 'restoring', 'saving', 'deleting', 'failed')),
    state_reason text,
    content_version uuid,
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz
);
-- A name and its PVC name are unique among live rows: both are reusable after the delete.
CREATE UNIQUE INDEX IF NOT EXISTS volumes_name_live ON volumes (name) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS volumes_pvc_name_live ON volumes (pvc_name) WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS volume_mounts (
    id bigserial PRIMARY KEY,
    volume_id uuid NOT NULL REFERENCES volumes (id) ON DELETE CASCADE,
    session_id uuid NOT NULL,
    preset_id uuid,
    mode text NOT NULL CHECK (mode IN ('ro', 'rw')),
    backup_restore boolean NOT NULL DEFAULT false,
    claimed_at timestamptz NOT NULL DEFAULT now(),
    released_at timestamptz,
    UNIQUE (volume_id, session_id)
);
CREATE INDEX IF NOT EXISTS volume_mounts_unreleased
    ON volume_mounts (volume_id) WHERE released_at IS NULL;
CREATE INDEX IF NOT EXISTS volume_mounts_session ON volume_mounts (session_id) WHERE released_at IS NULL;

CREATE TABLE IF NOT EXISTS volume_events (
    id bigserial PRIMARY KEY,
    volume_id uuid NOT NULL REFERENCES volumes (id) ON DELETE CASCADE,
    at timestamptz NOT NULL DEFAULT clock_timestamp(),
    actor_kind text NOT NULL CHECK (actor_kind IN ('user', 'agent', 'system')),
    actor text NOT NULL,
    kind text NOT NULL CHECK (kind IN (
        'created', 'resized', 'labels_updated', 'exclude_updated', 'projects_updated', 'attached', 'detached',
        'mounted', 'released', 'restore_started', 'restored', 'restore_skipped_mounted',
        'restore_failed', 'save_started', 'saved', 'save_unchanged', 'save_failed',
        'delete_refused', 'deleted')),
    detail jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS volume_events_volume_at ON volume_events (volume_id, at);

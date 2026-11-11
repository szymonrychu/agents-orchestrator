-- Extra projects (T-120 slice 1). Written only by the api. A session or preset belongs to its
-- home `project` plus every slug in `extra_projects`, with the same access in each. Projects are
-- never deleted (0037), so a slug here never dangles and no FK is needed (an array cannot carry
-- one). At most 9 extras: the taskboard accepts a scope of at most 10 projects, home first.
-- A grant by a superleader is a change proposal of kind session_projects or preset_projects;
-- the granted session gets a project_grant notice. Idempotent.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS extra_projects text[] NOT NULL DEFAULT '{}';
ALTER TABLE presets ADD COLUMN IF NOT EXISTS extra_projects text[] NOT NULL DEFAULT '{}';
ALTER TABLE sessions DROP CONSTRAINT IF EXISTS sessions_extra_projects_max;
ALTER TABLE sessions ADD CONSTRAINT sessions_extra_projects_max
    CHECK (cardinality(extra_projects) <= 9);
ALTER TABLE presets DROP CONSTRAINT IF EXISTS presets_extra_projects_max;
ALTER TABLE presets ADD CONSTRAINT presets_extra_projects_max
    CHECK (cardinality(extra_projects) <= 9);
CREATE INDEX IF NOT EXISTS sessions_extra_projects_idx ON sessions USING gin (extra_projects);

ALTER TABLE change_proposals DROP CONSTRAINT IF EXISTS change_proposals_kind_check;
ALTER TABLE change_proposals ADD CONSTRAINT change_proposals_kind_check
    CHECK (kind IN ('preset', 'schedule', 'session', 'session_projects', 'preset_projects'));

-- Notice kinds: the union of 0038's kinds plus project_grant.
ALTER TABLE messages DROP CONSTRAINT IF EXISTS messages_notice_check;
ALTER TABLE messages ADD CONSTRAINT messages_notice_check
    CHECK (notice IN (
        'inactivity_reminder', 'inactivity_close', 'handover', 'proposal', 'project_grant'));

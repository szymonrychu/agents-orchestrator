-- Project-level secrets (T-19 projects epic, stage 2, plan 3.16). Written only by the api. One row
-- per (project, protected variable name); each version is an immutable Kubernetes Secret
-- `prs-<sha256(slug)[:8]>-<sha256(name)[:6]>-v<N>`, as preset_secrets is for presets. Every preset
-- of the project receives the current, not retired rows at session create (no opt-out column).
-- Numbered 0042: 0038 is change_proposals and 0041 is claimed by T-72; the migrator keys on a set.
-- Idempotent like 0037.
CREATE TABLE IF NOT EXISTS project_secrets (
    project text NOT NULL REFERENCES projects (slug),
    name text NOT NULL,
    version integer NOT NULL CHECK (version >= 1),
    -- versions up to this one are known deleted (bounds the delete calls of a collection pass)
    swept integer NOT NULL DEFAULT 0,
    retired_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (project, name)
);

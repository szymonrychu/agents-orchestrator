-- Central secrets vault (T-72 S2). Additive part: presets.vault_reattach lists the names of the
-- protected env entries a preset still holds, which a user re-attaches as vault references. The
-- destructive statements (removing protected entries from presets/sessions spec env, converting
-- sessions.secret_versions, dropping preset_secrets, change_proposal_secrets and
-- change_proposal_items.secret) are appended to this same file later (task O10). Idempotent,
-- one transaction; only names are read, never a value.
ALTER TABLE presets ADD COLUMN IF NOT EXISTS vault_reattach text[] NOT NULL DEFAULT '{}';

UPDATE presets p
SET vault_reattach = (
    SELECT coalesce(array_agg(n ORDER BY n), '{}')
    FROM (
        SELECT unnest(p.vault_reattach) AS n
        UNION
        SELECT e.key
        FROM jsonb_each(CASE WHEN jsonb_typeof(p.spec -> 'env') = 'object'
                             THEN p.spec -> 'env' ELSE '{}'::jsonb END) AS e
        WHERE jsonb_typeof(e.value) = 'object' AND e.value -> 'protected' = 'true'::jsonb
    ) names
)
WHERE jsonb_typeof(spec -> 'env') = 'object'
  AND EXISTS (
    SELECT 1
    FROM jsonb_each(CASE WHEN jsonb_typeof(spec -> 'env') = 'object'
                         THEN spec -> 'env' ELSE '{}'::jsonb END) AS e
    WHERE jsonb_typeof(e.value) = 'object' AND e.value -> 'protected' = 'true'::jsonb
  );

-- Names-only audit rows of vault reference attaches and detaches on presets (one writer: the
-- api). No foreign key: the rows outlive the preset.
CREATE TABLE IF NOT EXISTS preset_vault_events (
    id bigserial PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    kind text NOT NULL CHECK (kind IN ('preset_vault_attached', 'preset_vault_detached')),
    actor text NOT NULL,
    data jsonb NOT NULL
);

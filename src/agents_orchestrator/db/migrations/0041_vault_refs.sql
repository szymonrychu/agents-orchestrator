-- Central secrets vault (T-72 S2): protected env variables leave the orchestrator; a user
-- re-attaches each as a vault reference. Forward-only, one transaction, idempotent (a second run
-- changes nothing). Only names, flags and versions are read, never a value (there is none in
-- the database: protected values live in ps-* Kubernetes Secrets).
--
-- 1. presets.vault_reattach lists the names of the protected env entries a preset held.
-- 2. Refuses to run when an env entry carries a non-boolean `protected` flag (it could not tell
--    whether that entry is a secret).
-- 3. presets and sessions spec.env: protected entries are removed, the `protected` key leaves
--    every other entry. First, a legacy pin (a ps-* Secret name, str) in sessions.secret_versions
--    whose name is not a protected entry of that session's spec is dropped: a PATCH before this
--    migration that removed or unprotected the entry left it behind, inert in the old render. Every
--    other legacy pin stays and keeps rendering for a session created before this migration
--    (k8s/render.py).
-- 4. Open, rework and stale proposals whose current revision carries a secret item or a protected
--    declaration in an env item are withdrawn (status-guarded); `"protected": false` leaves the
--    revision bases and env items of every proposal, so an open one still applies.
-- 5. change_proposal_items.secret, change_proposal_secrets and preset_secrets are dropped. The
--    ps-* Secrets stay in Kubernetes for the user's delete once the old sessions ended (MEMORY).
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

-- 2. -----------------------------------------------------------------------------------------
-- An entry object's `protected` flag: true only for the JSON boolean true, never an error on a
-- scalar, an array or a missing key.
CREATE OR REPLACE FUNCTION pg_temp.orch_0041_protected(v jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN jsonb_typeof(v) = 'object'
                THEN coalesce(v -> 'protected' = 'true'::jsonb, false)
                ELSE false END
$$;

-- An env entry without its `protected` key (any other shape as it is).
CREATE OR REPLACE FUNCTION pg_temp.orch_0041_entry(v jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN jsonb_typeof(v) = 'object' AND v ? 'protected' THEN v - 'protected'
                ELSE v END
$$;

-- An env map without its protected entries and without `protected` keys; a non-object as it is.
CREATE OR REPLACE FUNCTION pg_temp.orch_0041_env(env jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN jsonb_typeof(env) = 'object' THEN (
        SELECT coalesce(jsonb_object_agg(k, pg_temp.orch_0041_entry(v)), '{}'::jsonb)
        FROM jsonb_each(CASE WHEN jsonb_typeof(env) = 'object' THEN env ELSE '{}'::jsonb END)
            AS t(k, v)
        WHERE NOT pg_temp.orch_0041_protected(v))
    ELSE env END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM (SELECT spec -> 'env' AS env FROM presets
              UNION ALL SELECT spec -> 'env' FROM sessions) AS s,
             jsonb_each(CASE WHEN jsonb_typeof(s.env) = 'object' THEN s.env
                             ELSE '{}'::jsonb END) AS t(k, v)
        WHERE CASE WHEN jsonb_typeof(v) = 'object' AND v ? 'protected'
                   THEN jsonb_typeof(v -> 'protected') <> 'boolean'
                   ELSE false END
    ) THEN
        RAISE EXCEPTION '0041: an env entry has a non-boolean protected flag; fix it first';
    END IF;
END
$$;

-- 3. -----------------------------------------------------------------------------------------
-- The legacy pins of a session that still name a protected entry of its spec; any other pin (a
-- vault pin object) as it is; a non-object as it is.
CREATE OR REPLACE FUNCTION pg_temp.orch_0041_pins(pins jsonb, env jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN jsonb_typeof(pins) = 'object' THEN (
        SELECT coalesce(jsonb_object_agg(k, v), '{}'::jsonb)
        FROM jsonb_each(CASE WHEN jsonb_typeof(pins) = 'object' THEN pins ELSE '{}'::jsonb END)
            AS t(k, v)
        WHERE jsonb_typeof(v) <> 'string'
           OR pg_temp.orch_0041_protected(
                CASE WHEN jsonb_typeof(env) = 'object' THEN env -> k END))
    ELSE pins END
$$;

-- Only before the strip below: once protected entries are gone (preset_secrets is dropped in the
-- same transaction), a second run must keep the remaining legacy pins.
DO $$
BEGIN
    IF to_regclass('preset_secrets') IS NOT NULL THEN
        UPDATE sessions
        SET secret_versions = pg_temp.orch_0041_pins(secret_versions, spec -> 'env')
        WHERE jsonb_typeof(secret_versions) = 'object'
          AND pg_temp.orch_0041_pins(secret_versions, spec -> 'env')
              IS DISTINCT FROM secret_versions;
    END IF;
END
$$;

UPDATE presets SET spec = jsonb_set(spec, '{env}', pg_temp.orch_0041_env(spec -> 'env'))
WHERE jsonb_typeof(spec -> 'env') = 'object'
  AND pg_temp.orch_0041_env(spec -> 'env') IS DISTINCT FROM spec -> 'env';

UPDATE sessions SET spec = jsonb_set(spec, '{env}', pg_temp.orch_0041_env(spec -> 'env'))
WHERE jsonb_typeof(spec -> 'env') = 'object'
  AND pg_temp.orch_0041_env(spec -> 'env') IS DISTINCT FROM spec -> 'env';

-- 4. -----------------------------------------------------------------------------------------
-- The `secret` column is gone on a second run, so its test is built only while it exists.
DO $$
DECLARE
    secret_item text := CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'change_proposal_items' AND column_name = 'secret'
    ) THEN 'i.secret IS NOT NULL' ELSE 'false' END;
BEGIN
    EXECUTE format($q$
        WITH hit AS (
            SELECT p.id
            FROM change_proposals p
            WHERE p.status IN ('open', 'rework', 'stale')
              AND EXISTS (
                SELECT 1 FROM change_proposal_items i
                WHERE i.proposal_id = p.id AND i.revision = p.revision
                  AND (%s
                       OR (i.path LIKE 'env.%%'
                           AND (pg_temp.orch_0041_protected(i.before)
                                OR pg_temp.orch_0041_protected(i.after)))))
        ), closed AS (
            UPDATE change_proposals p
            SET status = 'withdrawn', updated_at = now(), closed_at = now()
            FROM hit
            WHERE p.id = hit.id AND p.status IN ('open', 'rework', 'stale')
            RETURNING p.id, p.revision
        )
        UPDATE change_proposal_revisions r SET outcome = 'withdrawn'
        FROM closed
        WHERE r.proposal_id = closed.id AND r.revision = closed.revision AND r.outcome IS NULL
    $q$, secret_item);
END
$$;

UPDATE change_proposal_items
SET before = CASE WHEN pg_temp.orch_0041_protected(before) THEN before
                  ELSE pg_temp.orch_0041_entry(before) END,
    after = CASE WHEN pg_temp.orch_0041_protected(after) THEN after
                 ELSE pg_temp.orch_0041_entry(after) END
WHERE path LIKE 'env.%'
  AND ((NOT pg_temp.orch_0041_protected(before)
        AND pg_temp.orch_0041_entry(before) IS DISTINCT FROM before)
       OR (NOT pg_temp.orch_0041_protected(after)
           AND pg_temp.orch_0041_entry(after) IS DISTINCT FROM after));

-- A base keeps its protected entries (history); the others lose the `protected` key.
CREATE OR REPLACE FUNCTION pg_temp.orch_0041_base_env(env jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN jsonb_typeof(env) = 'object' THEN (
        SELECT coalesce(jsonb_object_agg(
            k, CASE WHEN pg_temp.orch_0041_protected(v) THEN v
                    ELSE pg_temp.orch_0041_entry(v) END), '{}'::jsonb)
        FROM jsonb_each(CASE WHEN jsonb_typeof(env) = 'object' THEN env ELSE '{}'::jsonb END)
            AS t(k, v))
    ELSE env END
$$;

UPDATE change_proposal_revisions r
SET base = jsonb_set(r.base, '{env}', pg_temp.orch_0041_base_env(r.base -> 'env'))
FROM change_proposals p
WHERE p.id = r.proposal_id AND p.kind IN ('preset', 'session')
  AND jsonb_typeof(r.base -> 'env') = 'object'
  AND pg_temp.orch_0041_base_env(r.base -> 'env') IS DISTINCT FROM r.base -> 'env';

-- 5. -----------------------------------------------------------------------------------------
ALTER TABLE change_proposal_items DROP COLUMN IF EXISTS secret;
DROP TABLE IF EXISTS change_proposal_secrets;
DROP TABLE IF EXISTS preset_secrets;

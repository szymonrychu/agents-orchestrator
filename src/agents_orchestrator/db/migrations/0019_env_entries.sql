-- spec.env became a map NAME -> {value, protected, expand} (GitLab CI variables model). Every
-- stored plaintext string value becomes {"value": v, "protected": false, "expand": false}, in the
-- spec JSON of sessions and presets. Idempotent: an entry that is already an object is kept, and
-- a spec with no string env value matches no row.
CREATE FUNCTION pg_temp.orch_env_entries(env jsonb) RETURNS jsonb LANGUAGE sql AS $$
    SELECT jsonb_object_agg(
        k,
        CASE WHEN jsonb_typeof(v) = 'string'
             THEN jsonb_build_object('value', v, 'protected', false, 'expand', false)
             ELSE v END)
    FROM jsonb_each(env) AS t(k, v)
$$;

UPDATE sessions SET spec = jsonb_set(spec, '{env}', pg_temp.orch_env_entries(spec -> 'env'))
WHERE EXISTS (
    SELECT 1 FROM jsonb_each(CASE WHEN jsonb_typeof(spec -> 'env') = 'object'
                                  THEN spec -> 'env' ELSE '{}'::jsonb END) AS t(k, v)
    WHERE jsonb_typeof(v) = 'string');

UPDATE presets SET spec = jsonb_set(spec, '{env}', pg_temp.orch_env_entries(spec -> 'env'))
WHERE EXISTS (
    SELECT 1 FROM jsonb_each(CASE WHEN jsonb_typeof(spec -> 'env') = 'object'
                                  THEN spec -> 'env' ELSE '{}'::jsonb END) AS t(k, v)
    WHERE jsonb_typeof(v) = 'string');

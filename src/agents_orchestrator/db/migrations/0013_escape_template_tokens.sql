-- Spec text fields became Jinja templates (init scripts, wrapper system_append, restart prompt,
-- auto_compact instructions). Text stored before that may hold literal `{{`, `{%` or `{#` (awk,
-- Go templates, wrapper `{{name}}` prompts) that would no longer render. Rewrite each token to
-- its escaped form (`{{ '{{' }}`) in presets.spec and sessions.spec. Idempotent: an already
-- escaped token is protected by a placeholder first, so a second run changes nothing.
-- Never aborts: a value whose escaped form would exceed its field limit (lim, NULL for none) is
-- left unchanged with a NOTICE, so it fails validation visibly at use instead of blocking startup.
CREATE FUNCTION pg_temp.orch_esc(t text, lim int DEFAULT NULL) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    orig text := t;
BEGIN
    t := replace(t, '{{ ''{{'' }}', chr(1) || 'A');
    t := replace(t, '{{ ''{%'' }}', chr(1) || 'B');
    t := replace(t, '{{ ''{#'' }}', chr(1) || 'C');
    t := replace(t, '{{', chr(2) || 'A');
    t := replace(t, '{%', chr(2) || 'B');
    t := replace(t, '{#', chr(2) || 'C');
    t := replace(t, chr(1) || 'A', '{{ ''{{'' }}');
    t := replace(t, chr(1) || 'B', '{{ ''{%'' }}');
    t := replace(t, chr(1) || 'C', '{{ ''{#'' }}');
    t := replace(t, chr(2) || 'A', '{{ ''{{'' }}');
    t := replace(t, chr(2) || 'B', '{{ ''{%'' }}');
    t := replace(t, chr(2) || 'C', '{{ ''{#'' }}');
    IF lim IS NOT NULL AND length(t) > lim AND length(orig) <= lim THEN
        RAISE NOTICE 'migration 0013: skipped a value, escaping would exceed % characters', lim;
        RETURN orig;
    END IF;
    RETURN t;
END $$;

CREATE FUNCTION pg_temp.orch_esc_spec(spec jsonb) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
    p text;
    v jsonb;
    lim int;
BEGIN
    IF jsonb_typeof(spec -> 'init_scripts') = 'array' THEN
        spec := jsonb_set(spec, '{init_scripts}', (
            SELECT coalesce(jsonb_agg(
                CASE WHEN jsonb_typeof(e) = 'string' THEN to_jsonb(pg_temp.orch_esc(e #>> '{}', 16384)) ELSE e END
                ORDER BY o), '[]'::jsonb)
            FROM jsonb_array_elements(spec -> 'init_scripts') WITH ORDINALITY AS t(e, o)));
    END IF;
    FOREACH p SLICE 0 IN ARRAY ARRAY[
        'wrapper.system_append',
        'wrapper.restart_continue_prompt',
        'wrapper.auto_compact.instructions'
    ] LOOP
        lim := CASE p WHEN 'wrapper.system_append' THEN 16384
                      WHEN 'wrapper.auto_compact.instructions' THEN 2000 END;
        v := spec #> string_to_array(p, '.');
        IF jsonb_typeof(v) = 'string' THEN
            spec := jsonb_set(spec, string_to_array(p, '.'), to_jsonb(pg_temp.orch_esc(v #>> '{}', lim)));
        END IF;
    END LOOP;
    RETURN spec;
END $$;

UPDATE presets SET spec = pg_temp.orch_esc_spec(spec) WHERE spec::text ~ '\{[{%#]';
UPDATE sessions SET spec = pg_temp.orch_esc_spec(spec) WHERE spec::text ~ '\{[{%#]';

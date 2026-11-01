-- spec.cache.key became two Jinja templates, restore_key and save_key. A stored single key k
-- becomes restore_key = save_key = k (same archive restored and overwritten as before), in the
-- spec JSON of sessions and presets. A cache without a key (derived from the preset at session
-- create) and a spec without a cache are left untouched. Idempotent: once `key` is gone no row
-- matches. A null `key` is dropped.
UPDATE sessions
SET spec = jsonb_set(
        jsonb_set(spec #- '{cache,key}', '{cache,restore_key}', spec #> '{cache,key}'),
        '{cache,save_key}', spec #> '{cache,key}')
WHERE jsonb_typeof(spec #> '{cache,key}') = 'string';

UPDATE sessions SET spec = spec #- '{cache,key}'
WHERE jsonb_typeof(spec #> '{cache,key}') = 'null';

UPDATE presets
SET spec = jsonb_set(
        jsonb_set(spec #- '{cache,key}', '{cache,restore_key}', spec #> '{cache,key}'),
        '{cache,save_key}', spec #> '{cache,key}')
WHERE jsonb_typeof(spec #> '{cache,key}') = 'string';

UPDATE presets SET spec = spec #- '{cache,key}'
WHERE jsonb_typeof(spec #> '{cache,key}') = 'null';

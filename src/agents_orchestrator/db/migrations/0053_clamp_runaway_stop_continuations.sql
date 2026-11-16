-- The lower bound of `runaway.max_stop_continuations` rose from 1 to 5 (wrapper 0.19.0 refuses a
-- guard cap not above pressure.l3_stop_blocks + 1 while pressure is on). Stored values of 1 to 4
-- would fail SessionSpec validation on every read, so they are raised to 5. null, absent, 0
-- (off) and values of 5 or more are untouched. Idempotent, forward only. The runaway block lives
-- in sessions.spec, presets.spec, the proposal revision base (kinds preset and session) and the
-- proposal items of path 'runaway' (before and after).
UPDATE sessions SET spec = jsonb_set(spec, '{runaway,max_stop_continuations}', '5'::jsonb)
WHERE CASE WHEN jsonb_typeof(spec #> '{runaway,max_stop_continuations}') = 'number'
           THEN (spec #>> '{runaway,max_stop_continuations}')::numeric BETWEEN 1 AND 4
           ELSE false END;

UPDATE presets SET spec = jsonb_set(spec, '{runaway,max_stop_continuations}', '5'::jsonb)
WHERE CASE WHEN jsonb_typeof(spec #> '{runaway,max_stop_continuations}') = 'number'
           THEN (spec #>> '{runaway,max_stop_continuations}')::numeric BETWEEN 1 AND 4
           ELSE false END;

UPDATE change_proposal_revisions
SET base = jsonb_set(base, '{runaway,max_stop_continuations}', '5'::jsonb)
WHERE CASE WHEN jsonb_typeof(base #> '{runaway,max_stop_continuations}') = 'number'
           THEN (base #>> '{runaway,max_stop_continuations}')::numeric BETWEEN 1 AND 4
           ELSE false END;

UPDATE change_proposal_items
SET before = jsonb_set(before, '{max_stop_continuations}', '5'::jsonb)
WHERE path = 'runaway'
  AND CASE WHEN jsonb_typeof(before #> '{max_stop_continuations}') = 'number'
           THEN (before #>> '{max_stop_continuations}')::numeric BETWEEN 1 AND 4
           ELSE false END;

UPDATE change_proposal_items
SET after = jsonb_set(after, '{max_stop_continuations}', '5'::jsonb)
WHERE path = 'runaway'
  AND CASE WHEN jsonb_typeof(after #> '{max_stop_continuations}') = 'number'
           THEN (after #>> '{max_stop_continuations}')::numeric BETWEEN 1 AND 4
           ELSE false END;

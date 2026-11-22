-- spec.retrospective is gone (S-83 step 4, no alias): strip the key from stored rows. Idempotent.
UPDATE sessions SET spec = spec - 'retrospective' WHERE spec ? 'retrospective';
UPDATE presets SET spec = spec - 'retrospective' WHERE spec ? 'retrospective';
-- Proposals: the revision base (a spec) and the item before/after values, at the top level and
-- under a nested `spec` key; an item whose path is the removed field goes with it.
DELETE FROM change_proposal_items WHERE path = 'retrospective';
UPDATE change_proposal_revisions SET base = base - 'retrospective'
WHERE base ? 'retrospective';
UPDATE change_proposal_revisions
SET base = jsonb_set(base, '{spec}', (base -> 'spec') - 'retrospective')
WHERE jsonb_typeof(base -> 'spec') = 'object' AND (base -> 'spec') ? 'retrospective';
UPDATE change_proposal_items SET before = before - 'retrospective'
WHERE jsonb_typeof(before) = 'object' AND before ? 'retrospective';
UPDATE change_proposal_items
SET before = jsonb_set(before, '{spec}', (before -> 'spec') - 'retrospective')
WHERE jsonb_typeof(before) = 'object' AND jsonb_typeof(before -> 'spec') = 'object'
  AND (before -> 'spec') ? 'retrospective';
UPDATE change_proposal_items SET after = after - 'retrospective'
WHERE jsonb_typeof(after) = 'object' AND after ? 'retrospective';
UPDATE change_proposal_items
SET after = jsonb_set(after, '{spec}', (after -> 'spec') - 'retrospective')
WHERE jsonb_typeof(after) = 'object' AND jsonb_typeof(after -> 'spec') = 'object'
  AND (after -> 'spec') ? 'retrospective';

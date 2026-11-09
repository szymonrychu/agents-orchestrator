-- The `project:` label prefix is reserved for labels the orchestrator derives itself; remove any
-- stored label that starts with it from presets and sessions (order of the others is kept).
UPDATE presets
SET labels = ARRAY(
    SELECT l FROM unnest(labels) WITH ORDINALITY AS t(l, i) WHERE l NOT LIKE 'project:%' ORDER BY i
)
WHERE EXISTS (SELECT 1 FROM unnest(labels) AS l WHERE l LIKE 'project:%');
UPDATE sessions
SET labels = ARRAY(
    SELECT l FROM unnest(labels) WITH ORDINALITY AS t(l, i) WHERE l NOT LIKE 'project:%' ORDER BY i
)
WHERE EXISTS (SELECT 1 FROM unnest(labels) AS l WHERE l LIKE 'project:%');

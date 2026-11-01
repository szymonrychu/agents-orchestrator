-- Per agent attention (the wrapper is waiting on a person) and bootstrap progress, both written
-- only by the ingester. `waiting_reason` is the wrapper's reason (permission, dialog, question);
-- `bootstrap_step` is the latest bootstrap step object and `bootstrap_failure` a failed one.
ALTER TABLE agent_status
    ADD COLUMN waiting_reason text,
    ADD COLUMN waiting_since timestamptz,
    ADD COLUMN bootstrap_step jsonb,
    ADD COLUMN bootstrap_failure jsonb;

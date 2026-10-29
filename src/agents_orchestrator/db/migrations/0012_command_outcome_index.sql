-- Command outcome (D19): the message view looks up, at query time, the latest `command_completed`
-- or `command_failed` event of the message's agent whose `data.record.id` is the message's
-- `wrapper_command_id`. This partial index is keyed by that lookup's two equality predicates, over
-- those two event types only. The ingester stays the only writer of `events`. Building it briefly
-- blocks event inserts (a migration runs in a transaction, so not CONCURRENTLY). Idempotent.
CREATE INDEX IF NOT EXISTS events_command_record_id ON events (agent_id, (data -> 'record' ->> 'id'))
    WHERE type IN ('command_completed', 'command_failed');

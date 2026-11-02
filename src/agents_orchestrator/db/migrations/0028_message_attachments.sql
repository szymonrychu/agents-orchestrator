-- Image and file attachments of a message (a Boox sketch PNG reaching an agent session).
-- Writer: the api, inside enqueue_message, in the same transaction as the message row.
-- Readers: the dispatcher (to send the bytes to the wrapper) and the message views (metadata).
-- Rows go with their message. Additive and idempotent like 0006; PG16-compatible.
CREATE TABLE IF NOT EXISTS message_attachments (
    message_id bigint NOT NULL REFERENCES messages (id) ON DELETE CASCADE,
    position int NOT NULL,
    name text NOT NULL,
    mime text NOT NULL,
    size int NOT NULL,
    data bytea NOT NULL,
    PRIMARY KEY (message_id, position),
    UNIQUE (message_id, name)
);

-- Attachment bytes are cleared (set NULL) when the owning message becomes sent or cancelled;
-- the metadata row (name, mime, size) stays so the message views still list it.
-- A failed message keeps its bytes because Retry re-queues it and the dispatcher re-reads them.
-- Writer: the api and the dispatcher (queue.py), in the same transaction as the status change.
-- Idempotent: DROP NOT NULL on an already nullable column is a no-op; PG16-compatible.
ALTER TABLE message_attachments ALTER COLUMN data DROP NOT NULL;

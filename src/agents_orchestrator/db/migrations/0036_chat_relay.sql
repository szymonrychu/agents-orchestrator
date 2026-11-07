-- Chat relay marks on session messages: a message the api enqueued as a relay notice for a chat
-- room reply. The api is the only writer. A non-null `chat_relay_send` marks a relay notice
-- (the send id it answers); `chat_relay_room` is the room it came from. Both stay NULL on every
-- other message. The index serves the "did sender message receiver recently" lookup. Idempotent
-- like 0007.
ALTER TABLE messages ADD COLUMN IF NOT EXISTS chat_relay_send text;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS chat_relay_room text;
CREATE INDEX IF NOT EXISTS messages_sender_receiver_created
    ON messages (sender_session_id, session_id, created_at);

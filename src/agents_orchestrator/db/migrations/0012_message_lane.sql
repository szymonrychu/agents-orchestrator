-- Delivery lane of a message: idle (wait for the agent to be free), regular (type into the running
-- turn) or priority (interrupt, then start a turn). Existing rows and omitted lanes are regular.
-- The api is the only writer and sets it once, at insert. Additive and idempotent like 0006.
ALTER TABLE messages ADD COLUMN IF NOT EXISTS lane text NOT NULL DEFAULT 'regular';
ALTER TABLE messages DROP CONSTRAINT IF EXISTS messages_lane_check;
ALTER TABLE messages ADD CONSTRAINT messages_lane_check CHECK (lane IN ('idle', 'regular', 'priority'));

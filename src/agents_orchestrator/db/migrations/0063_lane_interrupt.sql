-- Delivery lanes are regular, interrupt and idle: the priority lane is renamed interrupt.
-- Rows (queued or history) are rewritten; the api is the only writer of the column.
ALTER TABLE messages DROP CONSTRAINT IF EXISTS messages_lane_check;
UPDATE messages SET lane = 'interrupt' WHERE lane = 'priority';
ALTER TABLE messages ADD CONSTRAINT messages_lane_check CHECK (lane IN ('idle', 'regular', 'interrupt'));

-- The superleader session that created a session through the admin routes (superleader-crud
-- O7). NULL: a user, a trigger service or a session from before the column. No foreign key: the
-- record outlives the creator. Additive and idempotent like 0006.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS created_by_session uuid;

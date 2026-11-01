-- The last restart an agent of the session asked for on its own session (the self-restart
-- cooldown, claimed atomically by the api). NULL: never. Additive and idempotent like 0006.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS last_self_restart_at timestamptz;

-- volume owner attempts (S-77 fix, volume root ownership): how many owner Jobs of a volume failed.
-- It names the next owner Job (`vol-<id>-owner-<n>`, stable across restarts) and bounds the
-- retries: at the maximum a claim is refused. Idempotent, forward only.
ALTER TABLE volumes ADD COLUMN IF NOT EXISTS owner_attempts integer NOT NULL DEFAULT 0;

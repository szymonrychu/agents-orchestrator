-- volume backup dirty mark (S-77, decision 858): when a rw claim with backup_restore last started
-- or stopped writing (claim, attachment change, release); NULL once a successful save (saved or
-- save_unchanged) started after it, a failed save keeps it. The last release saves while it is
-- set, whatever the last claim's own setting, so a backup-on writer that ends before a read-only
-- mounter still gets saved. Backfill: volumes with such a claim still unreleased or released after
-- the start of the newest successful save (any, when never saved) start dirty.
-- Idempotent, forward only.
ALTER TABLE volumes ADD COLUMN IF NOT EXISTS backup_dirty_at timestamptz;
UPDATE volumes v SET backup_dirty_at = now() WHERE backup_dirty_at IS NULL AND EXISTS (
    SELECT 1 FROM volume_mounts m WHERE m.volume_id = v.id AND m.mode = 'rw' AND m.backup_restore
    AND (m.released_at IS NULL OR m.released_at > COALESCE((
        SELECT max(s.at) FROM volume_events s WHERE s.volume_id = v.id AND s.kind = 'save_started'
        AND s.id < (SELECT max(f.id) FROM volume_events f WHERE f.volume_id = v.id
            AND f.kind IN ('saved', 'save_unchanged'))
    ), '-infinity'))
);

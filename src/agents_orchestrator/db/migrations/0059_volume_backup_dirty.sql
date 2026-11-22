-- volume backup dirty flag (S-77, decision 858): a rw claim with backup_restore was taken since
-- the last successful save. The last release saves when it is set, whatever the last claim's own
-- setting, so a backup-on writer that ends before a read-only mounter still gets saved. Set by
-- such a claim, cleared by a successful save (saved or save_unchanged) once no such claim is
-- still unreleased; a failed save keeps it. Volumes that hold such a claim now start dirty.
-- Idempotent, forward only.
ALTER TABLE volumes ADD COLUMN IF NOT EXISTS backup_dirty boolean NOT NULL DEFAULT false;
UPDATE volumes v SET backup_dirty = true WHERE EXISTS (
    SELECT 1 FROM volume_mounts m WHERE m.volume_id = v.id AND m.released_at IS NULL
    AND m.mode = 'rw' AND m.backup_restore
);

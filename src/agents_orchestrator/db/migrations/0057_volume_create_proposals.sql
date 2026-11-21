-- volume_create proposals (S-77 Q10): a superleader asks the user to create a volume; the user's
-- approval creates it. The kind check is 0052's list plus volume_create. Idempotent, forward only.
ALTER TABLE change_proposals DROP CONSTRAINT IF EXISTS change_proposals_kind_check;
ALTER TABLE change_proposals ADD CONSTRAINT change_proposals_kind_check
    CHECK (kind IN (
        'preset', 'schedule', 'session', 'session_projects', 'preset_projects',
        'preset_manageable', 'volume_create'));

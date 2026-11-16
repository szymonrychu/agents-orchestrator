-- preset_manageable proposals (S-66, T-489): a superleader proposes turning
-- `superleader_manageable` on for an unmanageable preset; a human approves. The kind check is
-- 0045's list plus preset_manageable. Idempotent, forward only. (0051 is the cooperative
-- handoff migration of PR #146.)
ALTER TABLE change_proposals DROP CONSTRAINT IF EXISTS change_proposals_kind_check;
ALTER TABLE change_proposals ADD CONSTRAINT change_proposals_kind_check
    CHECK (kind IN (
        'preset', 'schedule', 'session', 'session_projects', 'preset_projects',
        'preset_manageable'));

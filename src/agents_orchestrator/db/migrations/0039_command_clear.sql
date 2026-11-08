-- Command kind `clear` (voluntary-handover Q13): after a voluntary leadership handover the
-- dispatcher sends `/clear` to the ex-leader (claude-code-wrapper >= 0.10.0, client-only
-- command). The CHECK was created inline in 0001_init.sql under the default name. Idempotent.
ALTER TABLE commands DROP CONSTRAINT IF EXISTS commands_kind_check;
ALTER TABLE commands ADD CONSTRAINT commands_kind_check
    CHECK (kind IN ('interrupt', 'compact', 'restart', 'rebootstrap', 'clear'));

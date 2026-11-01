-- Per agent permission-mode state reported by the wrapper (0.7.1), written only by the ingester:
-- `permission_mode_state` is "unavailable" while the wrapper found the requested mode (auto)
-- unusable on its screen, `permission_mode_reason` the wrapper's reason. Both null otherwise.
ALTER TABLE agent_status
    ADD COLUMN IF NOT EXISTS permission_mode_state text,
    ADD COLUMN IF NOT EXISTS permission_mode_reason text;

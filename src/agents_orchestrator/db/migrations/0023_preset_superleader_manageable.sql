-- Whether the superleader may manage a preset (preset-superleader-switch). The main-port API
-- sets it; the superleader port lists, reads and changes only presets where it is true. Existing
-- presets stay unmanageable. Additive and idempotent like 0006.
ALTER TABLE presets ADD COLUMN IF NOT EXISTS superleader_manageable boolean NOT NULL DEFAULT false;

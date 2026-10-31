-- carries_start_prompt: the message that carries the preset start prompt (api sets it once, at
-- insert). The prompt counts as delivered while such a row is neither failed nor cancelled.
-- busy_requeues: 409 booting/busy requeues of a regular or priority message, the exponent of its
-- backoff (the dispatcher is the only writer). Additive and idempotent like 0006.
ALTER TABLE messages ADD COLUMN IF NOT EXISTS carries_start_prompt boolean NOT NULL DEFAULT false;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS busy_requeues integer NOT NULL DEFAULT 0;

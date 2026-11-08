-- Why an agent's wrapper session failed to start: {step, message, log_tail}, written only by the
-- reconciler's session bootstrapper before it clears a failed session (or when a create fails),
-- and cleared when the agent holds a session again. 0037 is reserved for projects-s1; 0040 is
-- the next number free on origin/main and every origin branch.
ALTER TABLE agents ADD COLUMN start_failure jsonb;

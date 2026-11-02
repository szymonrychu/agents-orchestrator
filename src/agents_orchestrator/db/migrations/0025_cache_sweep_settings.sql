-- Runtime-editable archive sweep policy and the "run sweep now" request. One row each, id fixed
-- at 1. The api is the only writer of both tables; the reconciler only reads them. An absent
-- cache_sweep_settings row means the ORCH_CACHE_* environment values apply. The bounds are the
-- SWEEP_*_MAX constants of settings.py (an unbounded retention overflows the planner's timedelta,
-- an unbounded integer overflows the column).
CREATE TABLE cache_sweep_settings (
    id smallint PRIMARY KEY CHECK (id = 1),
    enabled boolean NOT NULL,
    dry_run boolean NOT NULL,
    retention_days integer NOT NULL CHECK (retention_days BETWEEN 1 AND 3650),
    keep_newest integer NOT NULL CHECK (keep_newest BETWEEN 1 AND 1000),
    interval_seconds integer NOT NULL CHECK (interval_seconds BETWEEN 60 AND 2592000),
    grace_seconds integer NOT NULL CHECK (grace_seconds BETWEEN 0 AND 604800),
    updated_at timestamptz NOT NULL
);

CREATE TABLE cache_sweep_requests (
    id smallint PRIMARY KEY CHECK (id = 1),
    requested_at timestamptz NOT NULL
);

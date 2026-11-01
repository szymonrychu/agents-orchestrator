-- The last cache cleanup sweep, for the api (a separate process from the reconciler). One row,
-- id fixed at 1; the reconciler is its only writer, the api only reads it.
CREATE TABLE cache_sweeps (
    id smallint PRIMARY KEY CHECK (id = 1),
    finished_at timestamptz NOT NULL,
    result jsonb NOT NULL
);

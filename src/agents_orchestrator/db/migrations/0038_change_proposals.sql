-- Change proposals (preset-change-proposals, T-18). Written only by the api; idempotent.
-- A proposal is one requested change to a preset, schedule or session spec. Its status moves
-- only through status-guarded single UPDATEs (db/proposals.py). Revisions are the Q7 history:
-- a row is never rewritten except its review stamp and outcome. Items are one changed path per
-- revision. Secret reservations hold staged Secret version numbers, never a value.
CREATE TABLE IF NOT EXISTS change_proposals (
    id bigserial PRIMARY KEY,
    kind text NOT NULL CHECK (kind IN ('preset', 'schedule', 'session')),
    target_id text NOT NULL,
    target_name text NOT NULL,
    -- no FK: a proposal outlives its proposer session
    proposer_session uuid NOT NULL,
    proposer_slug text NOT NULL,
    proposer_superleader boolean NOT NULL,
    client_key text,
    status text NOT NULL DEFAULT 'open' CHECK (status IN (
        'open', 'rework', 'stale', 'applied', 'denied', 'withdrawn', 'expired', 'target_gone')),
    revision int NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    closed_at timestamptz,
    CONSTRAINT change_proposals_client_key UNIQUE (proposer_session, client_key)
);
CREATE INDEX IF NOT EXISTS change_proposals_status_expires
    ON change_proposals (status, expires_at);
CREATE INDEX IF NOT EXISTS change_proposals_active_target
    ON change_proposals (kind, target_id) WHERE status IN ('open', 'rework', 'stale');

CREATE TABLE IF NOT EXISTS change_proposal_revisions (
    proposal_id bigint NOT NULL REFERENCES change_proposals ON DELETE CASCADE,
    revision int NOT NULL,
    reason text NOT NULL CHECK (char_length(reason) BETWEEN 1 AND 4000),
    base jsonb NOT NULL,
    fingerprint text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    reviewed_at timestamptz,
    outcome text CHECK (outcome IN (
        'applied', 'denied', 'rework', 'stale', 'revised', 'withdrawn', 'expired', 'target_gone')),
    PRIMARY KEY (proposal_id, revision)
);

CREATE TABLE IF NOT EXISTS change_proposal_items (
    proposal_id bigint NOT NULL,
    revision int NOT NULL,
    path text NOT NULL,
    before jsonb,
    after jsonb,
    security boolean NOT NULL,
    "grant" boolean NOT NULL,
    secret text CHECK (secret IN ('set', 'changed')),
    decision text NOT NULL DEFAULT 'pending'
        CHECK (decision IN ('pending', 'approved', 'rework', 'denied')),
    note text,
    PRIMARY KEY (proposal_id, revision, path),
    FOREIGN KEY (proposal_id, revision)
        REFERENCES change_proposal_revisions ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS change_proposal_secrets (
    preset_id uuid NOT NULL,
    name text NOT NULL,
    version int NOT NULL,
    proposal_id bigint NOT NULL,
    revision int NOT NULL,
    state text NOT NULL CHECK (state IN ('staged', 'bound')),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (preset_id, name, version)
);

-- Notice kinds: the union of every kind on main (0030 inactivity, 0035 handover) plus proposal.
-- Whichever of T-18, T-19 and T-54 merges second carries the union.
ALTER TABLE messages DROP CONSTRAINT IF EXISTS messages_notice_check;
ALTER TABLE messages ADD CONSTRAINT messages_notice_check
    CHECK (notice IN ('inactivity_reminder', 'inactivity_close', 'handover', 'proposal'));

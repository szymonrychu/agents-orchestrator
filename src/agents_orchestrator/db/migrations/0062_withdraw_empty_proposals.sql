-- 0060 deleted the `retrospective` items of preset and session proposals; an active proposal whose
-- current revision held only that item was left with nothing to review or apply. Withdraw it like
-- the proposer's own withdraw (status-guarded, the revision's first outcome kept). Idempotent.
WITH emptied AS (
    UPDATE change_proposals p SET status = 'withdrawn', updated_at = now(), closed_at = now()
    WHERE p.kind IN ('preset', 'session') AND p.status IN ('open', 'rework', 'stale')
      AND NOT EXISTS (
          SELECT 1 FROM change_proposal_items i
          WHERE i.proposal_id = p.id AND i.revision = p.revision)
    RETURNING p.id, p.revision
)
UPDATE change_proposal_revisions r SET outcome = 'withdrawn'
FROM emptied e
WHERE r.proposal_id = e.id AND r.revision = e.revision AND r.outcome IS NULL;

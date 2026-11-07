"""Change proposals repository: proposals, their revisions and items. The api is the only writer.

Every status change goes through `transition`, one status-guarded UPDATE
(`WHERE id = %s AND status = ANY(%s)`) that raises `NotInState` when no row matched, as the queue
tables do. Times come from the database clock (`now()`).

Transactions: each writing function runs in `conn.transaction()`. On an idle connection that is
its own transaction; inside a caller's open transaction it is a savepoint and the caller commits
(create, revise and review also write Secret reservations in that same transaction).

No value of a protected variable is ever stored here: an `env.NAME` item carries the redacted
declaration and `secret` ('set' or 'changed') only.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from agents_orchestrator.db.queue import _all, _one

ACTIVE = ("open", "rework", "stale")
TERMINAL = ("applied", "denied", "withdrawn", "expired", "target_gone")


class ProposalExists(Exception):
    """The proposer already filed a proposal with this client_key (an idempotent replay)."""

    def __init__(self, proposal_id: int) -> None:
        super().__init__(proposal_id)
        self.proposal_id = proposal_id


class NotInState(Exception):
    """A guarded transition matched no row: the proposal is gone or not in an expected state."""


class UnknownItem(Exception):
    """A review decision names a path that is not an item of that revision."""


@dataclass(frozen=True)
class Item:
    path: str
    before: Any
    after: Any
    security: bool
    grant: bool
    secret: str | None
    decision: str = "pending"
    note: str | None = None


@dataclass(frozen=True)
class Revision:
    revision: int
    reason: str
    base: dict[str, Any]
    fingerprint: str
    created_at: datetime
    reviewed_at: datetime | None
    outcome: str | None
    items: list[Item]


@dataclass(frozen=True)
class Proposal:
    id: int
    kind: str
    target_id: str
    target_name: str
    proposer_session: uuid.UUID
    proposer_slug: str
    proposer_superleader: bool
    client_key: str | None
    status: str
    revision: int
    created_at: datetime
    updated_at: datetime
    expires_at: datetime
    closed_at: datetime | None
    revisions: list[Revision]


def _json(value: Any) -> Jsonb | None:
    """None is SQL NULL (the path is absent before, or removed after), not JSON null."""
    return None if value is None else Jsonb(value)


async def _write_revision(
    conn: psycopg.AsyncConnection,
    proposal_id: int,
    revision: int,
    reason: str,
    base: dict[str, Any],
    fingerprint: str,
    items: Sequence[Item],
) -> None:
    await conn.execute(
        "INSERT INTO change_proposal_revisions (proposal_id, revision, reason, base, fingerprint)"
        " VALUES (%s, %s, %s, %s, %s)",
        (proposal_id, revision, reason, Jsonb(base), fingerprint),
    )
    async with conn.cursor() as cur:
        await cur.executemany(
            "INSERT INTO change_proposal_items (proposal_id, revision, path, before, after,"
            ' security, "grant", secret, decision, note)'
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    proposal_id,
                    revision,
                    i.path,
                    _json(i.before),
                    _json(i.after),
                    i.security,
                    i.grant,
                    i.secret,
                    i.decision,
                    i.note,
                )
                for i in items
            ],
        )


async def insert(
    conn: psycopg.AsyncConnection,
    *,
    kind: str,
    target_id: str,
    target_name: str,
    proposer_session: uuid.UUID,
    proposer_slug: str,
    proposer_superleader: bool,
    client_key: str | None,
    reason: str,
    base: dict[str, Any],
    fingerprint: str,
    items: Sequence[Item],
    ttl_seconds: int,
) -> int:
    """File a proposal at revision 1, status open; a client_key replay raises ProposalExists."""
    try:
        async with conn.transaction():
            row = await _one(
                conn,
                "INSERT INTO change_proposals (kind, target_id, target_name, proposer_session,"
                " proposer_slug, proposer_superleader, client_key, expires_at)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, now() + make_interval(secs => %s))"
                " RETURNING id",
                (
                    kind,
                    target_id,
                    target_name,
                    proposer_session,
                    proposer_slug,
                    proposer_superleader,
                    client_key,
                    ttl_seconds,
                ),
            )
            assert row is not None
            await _write_revision(conn, row["id"], 1, reason, base, fingerprint, items)
    except psycopg.errors.UniqueViolation as e:
        if client_key is None:
            raise
        first = await by_client_key(conn, proposer_session, client_key)
        if first is None:
            raise
        raise ProposalExists(first) from e
    return row["id"]


async def add_revision(
    conn: psycopg.AsyncConnection,
    proposal_id: int,
    *,
    from_states: Sequence[str],
    reason: str,
    base: dict[str, Any],
    fingerprint: str,
    items: Sequence[Item],
    ttl_seconds: int,
) -> int:
    """Make a new revision the one under review: status open, revision+1, expiry reset.

    The previous revision keeps an outcome already set (rework, stale) and otherwise becomes
    'revised'; its items stay. Returns the new revision number.
    """
    async with conn.transaction():
        row = await _one(
            conn,
            "UPDATE change_proposals SET status = 'open', revision = revision + 1,"
            " updated_at = now(), expires_at = now() + make_interval(secs => %s)"
            " WHERE id = %s AND status = ANY(%s) RETURNING revision",
            (ttl_seconds, proposal_id, list(from_states)),
        )
        if row is None:
            raise NotInState(proposal_id)
        revision: int = row["revision"]
        await conn.execute(
            "UPDATE change_proposal_revisions SET outcome = 'revised'"
            " WHERE proposal_id = %s AND revision = %s AND outcome IS NULL",
            (proposal_id, revision - 1),
        )
        await _write_revision(conn, proposal_id, revision, reason, base, fingerprint, items)
    return revision


async def transition(
    conn: psycopg.AsyncConnection,
    proposal_id: int,
    *,
    from_states: Sequence[str],
    to: str,
    outcome: str | None = None,
) -> None:
    """Move the status from one of from_states to `to`; raise NotInState when no row matched.

    A terminal `to` stamps closed_at. `outcome`, when given, is written on the current revision.
    """
    async with conn.transaction():
        row = await _one(
            conn,
            "UPDATE change_proposals SET status = %(to)s, updated_at = now(),"
            " closed_at = CASE WHEN %(to)s = ANY(%(terminal)s) THEN now() END"
            " WHERE id = %(id)s AND status = ANY(%(from)s) RETURNING revision",
            {"to": to, "terminal": list(TERMINAL), "id": proposal_id, "from": list(from_states)},
        )
        if row is None:
            raise NotInState(proposal_id)
        if outcome is not None:
            await conn.execute(
                "UPDATE change_proposal_revisions SET outcome = %s"
                " WHERE proposal_id = %s AND revision = %s",
                (outcome, proposal_id, row["revision"]),
            )


async def record_review(
    conn: psycopg.AsyncConnection,
    proposal_id: int,
    revision: int,
    decisions: dict[str, tuple[str, str | None]],
) -> None:
    """Write each path's (decision, note) on that revision only and stamp its reviewed_at.

    A path that is not an item of the revision raises UnknownItem and writes nothing.
    """
    async with conn.transaction():
        for path, (decision, note) in decisions.items():
            cur = await conn.execute(
                "UPDATE change_proposal_items SET decision = %s, note = %s"
                " WHERE proposal_id = %s AND revision = %s AND path = %s",
                (decision, note, proposal_id, revision, path),
            )
            if cur.rowcount != 1:
                raise UnknownItem(path)
        await conn.execute(
            "UPDATE change_proposal_revisions SET reviewed_at = now()"
            " WHERE proposal_id = %s AND revision = %s",
            (proposal_id, revision),
        )


def _item(row: dict[str, Any]) -> Item:
    return Item(
        path=row["path"],
        before=row["before"],
        after=row["after"],
        security=row["security"],
        grant=row["grant"],
        secret=row["secret"],
        decision=row["decision"],
        note=row["note"],
    )


async def _load(
    conn: psycopg.AsyncConnection, rows: list[dict[str, Any]], *, current_only: bool
) -> list[Proposal]:
    """Attach revisions (all, or only the current one) with their items to proposal rows."""
    if not rows:
        return []
    ids = [r["id"] for r in rows]
    only = " AND r.revision = p.revision" if current_only else ""
    revs = await _all(
        conn,
        "SELECT r.* FROM change_proposal_revisions r"
        " JOIN change_proposals p ON p.id = r.proposal_id"
        f" WHERE r.proposal_id = ANY(%s){only} ORDER BY r.proposal_id, r.revision",
        (ids,),
    )
    items = await _all(
        conn,
        "SELECT i.* FROM change_proposal_items i JOIN change_proposals p ON p.id = i.proposal_id"
        " JOIN change_proposal_revisions r"
        " ON (r.proposal_id, r.revision) = (i.proposal_id, i.revision)"
        f" WHERE i.proposal_id = ANY(%s){only} ORDER BY i.proposal_id, i.revision, i.path",
        (ids,),
    )
    by_rev: dict[tuple[int, int], list[Item]] = {}
    for i in items:
        by_rev.setdefault((i["proposal_id"], i["revision"]), []).append(_item(i))
    by_proposal: dict[int, list[Revision]] = {}
    for r in revs:
        by_proposal.setdefault(r["proposal_id"], []).append(
            Revision(
                revision=r["revision"],
                reason=r["reason"],
                base=r["base"],
                fingerprint=r["fingerprint"],
                created_at=r["created_at"],
                reviewed_at=r["reviewed_at"],
                outcome=r["outcome"],
                items=by_rev.get((r["proposal_id"], r["revision"]), []),
            )
        )
    return [Proposal(**row, revisions=by_proposal.get(row["id"], [])) for row in rows]


async def lock(conn: psycopg.AsyncConnection, proposal_id: int) -> Proposal | None:
    """SELECT ... FOR UPDATE inside the caller's transaction; the current revision only."""
    rows = await _all(
        conn, "SELECT * FROM change_proposals WHERE id = %s FOR UPDATE", (proposal_id,)
    )
    found = await _load(conn, rows, current_only=True)
    return found[0] if found else None


async def get(
    conn: psycopg.AsyncConnection, proposal_id: int, proposer_session: uuid.UUID | None = None
) -> Proposal | None:
    """The proposal with its whole revision history; with proposer_session, only that one's."""
    rows = await _all(
        conn,
        "SELECT * FROM change_proposals WHERE id = %s"
        " AND (%s::uuid IS NULL OR proposer_session = %s)",
        (proposal_id, proposer_session, proposer_session),
    )
    found = await _load(conn, rows, current_only=False)
    return found[0] if found else None


async def list_for(
    conn: psycopg.AsyncConnection,
    *,
    proposer_session: uuid.UUID | None,
    statuses: Sequence[str],
    limit: int,
) -> list[Proposal]:
    """Newest first, in `statuses`, of one proposer or (None) of everyone; current revision only."""
    rows = await _all(
        conn,
        "SELECT * FROM change_proposals WHERE status = ANY(%s)"
        " AND (%s::uuid IS NULL OR proposer_session = %s) ORDER BY id DESC LIMIT %s",
        (list(statuses), proposer_session, proposer_session, limit),
    )
    return await _load(conn, rows, current_only=True)


async def count_active(conn: psycopg.AsyncConnection, proposer_session: uuid.UUID) -> int:
    row = await _one(
        conn,
        "SELECT count(*) AS n FROM change_proposals"
        " WHERE proposer_session = %s AND status = ANY(%s)",
        (proposer_session, list(ACTIVE)),
    )
    assert row is not None
    return int(row["n"])


async def by_client_key(
    conn: psycopg.AsyncConnection, proposer_session: uuid.UUID, client_key: str
) -> int | None:
    row = await _one(
        conn,
        "SELECT id FROM change_proposals WHERE proposer_session = %s AND client_key = %s",
        (proposer_session, client_key),
    )
    return None if row is None else int(row["id"])


async def active_on_target(
    conn: psycopg.AsyncConnection, kind: str, target_id: str, exclude: int | None = None
) -> list[int]:
    rows = await _all(
        conn,
        "SELECT id FROM change_proposals WHERE kind = %s AND target_id = %s"
        " AND status = ANY(%s) AND id IS DISTINCT FROM %s ORDER BY id",
        (kind, target_id, list(ACTIVE), exclude),
    )
    return [r["id"] for r in rows]


async def due(conn: psycopg.AsyncConnection, now: datetime) -> list[int]:
    """Active proposals whose expires_at has passed `now`."""
    rows = await _all(
        conn,
        "SELECT id FROM change_proposals WHERE status = ANY(%s) AND expires_at <= %s ORDER BY id",
        (list(ACTIVE), now),
    )
    return [r["id"] for r in rows]

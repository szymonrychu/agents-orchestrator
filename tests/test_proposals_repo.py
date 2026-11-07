"""db.proposals: change proposals, revisions and items with status-guarded transitions."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import psycopg
import psycopg.errors
import pytest

from agents_orchestrator.db import proposals
from agents_orchestrator.db.migrate import MIGRATIONS_DIR, run_migrations

MIGRATION = MIGRATIONS_DIR / "0038_change_proposals.sql"
TTL = 7 * 24 * 3600


def _item(path, after=1, *, security=False, grant=False, secret=None):
    return proposals.Item(
        path=path, before=None, after=after, security=security, grant=grant, secret=secret
    )


async def _insert(conn, session=None, *, client_key=None, items=None, target="t1", ttl=TTL):
    return await proposals.insert(
        conn,
        kind="preset",
        target_id=target,
        target_name="Preset one",
        proposer_session=session or uuid.uuid4(),
        proposer_slug="lead",
        proposer_superleader=True,
        client_key=client_key,
        reason="because",
        base={"replicas": 1},
        fingerprint="fp1",
        items=items or [_item("replicas", 2)],
        ttl_seconds=ttl,
    )


async def _status(conn, pid):
    cur = await conn.execute("SELECT status FROM change_proposals WHERE id = %s", (pid,))
    return (await cur.fetchone())[0]


def test_migration_applies_twice(dsn):
    assert run_migrations(dsn) == []
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute(MIGRATION.read_text())  # second run of the file: a no-op
        tables = {
            r[0]
            for r in c.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'change_%'"
            )
        }
        indexes = {
            r[0]
            for r in c.execute(
                "SELECT indexname FROM pg_indexes WHERE tablename = 'change_proposals'"
            )
        }
    assert tables == {
        "change_proposals",
        "change_proposal_revisions",
        "change_proposal_items",
        "change_proposal_secrets",
    }
    assert {"change_proposals_status_expires", "change_proposals_active_target"} <= indexes


async def test_insert_with_items_and_read_back(pool):
    session = uuid.uuid4()
    items = [
        _item("replicas", 2),
        _item("kube_cluster_access", True, security=True, grant=True),
        _item("env.TOKEN", {"protected": True, "expand": False}, security=True, secret="set"),
    ]
    async with pool.connection() as conn:
        pid = await _insert(conn, session, client_key="k1", items=items)
        got = await proposals.get(conn, pid)
    assert got is not None
    assert (got.kind, got.target_id, got.target_name) == ("preset", "t1", "Preset one")
    assert (got.proposer_session, got.proposer_slug, got.proposer_superleader) == (
        session,
        "lead",
        True,
    )
    assert (got.client_key, got.status, got.revision, got.closed_at) == ("k1", "open", 1, None)
    assert timedelta(days=7) - timedelta(seconds=5) < got.expires_at - got.created_at
    assert got.expires_at - got.created_at <= timedelta(days=7) + timedelta(seconds=5)
    [rev] = got.revisions
    assert (rev.revision, rev.reason, rev.base, rev.fingerprint) == (
        1,
        "because",
        {"replicas": 1},
        "fp1",
    )
    assert (rev.reviewed_at, rev.outcome) == (None, None)
    assert sorted(rev.items, key=lambda i: i.path) == sorted(items, key=lambda i: i.path)
    assert all(i.decision == "pending" and i.note is None for i in rev.items)


async def test_get_scoped_to_proposer(pool):
    session = uuid.uuid4()
    async with pool.connection() as conn:
        pid = await _insert(conn, session)
        assert (await proposals.get(conn, pid, session)).id == pid
        assert await proposals.get(conn, pid, uuid.uuid4()) is None
        assert await proposals.get(conn, pid + 1000) is None


async def test_client_key_replay_raises_with_first_id(pool):
    session = uuid.uuid4()
    async with pool.connection() as conn:
        first = await _insert(conn, session, client_key="same")
        with pytest.raises(proposals.ProposalExists) as exc:
            await _insert(conn, session, client_key="same")
        assert exc.value.proposal_id == first
        assert await proposals.by_client_key(conn, session, "same") == first
        assert await proposals.by_client_key(conn, session, "other") is None
        # another proposer, or no key, never collides
        await _insert(conn, uuid.uuid4(), client_key="same")
        await _insert(conn, session)
        await _insert(conn, session)
        cur = await conn.execute("SELECT count(*) FROM change_proposals")
        assert (await cur.fetchone())[0] == 4


async def test_transition_from_wrong_state_raises_and_changes_nothing(pool):
    async with pool.connection() as conn:
        pid = await _insert(conn)
        await proposals.transition(conn, pid, from_states=("open",), to="denied", outcome="denied")
        before = await proposals.get(conn, pid)
        with pytest.raises(proposals.NotInState):
            await proposals.transition(conn, pid, from_states=proposals.ACTIVE, to="withdrawn")
        with pytest.raises(proposals.NotInState):
            await proposals.transition(conn, pid + 1000, from_states=proposals.ACTIVE, to="stale")
        after = await proposals.get(conn, pid)
    assert after == before
    assert after.status == "denied"
    assert after.closed_at is not None
    assert after.revisions[0].outcome == "denied"


async def test_transition_between_active_states_keeps_closed_at_null(pool):
    async with pool.connection() as conn:
        pid = await _insert(conn)
        await proposals.transition(conn, pid, from_states=("open",), to="stale", outcome="stale")
        got = await proposals.get(conn, pid)
    assert (got.status, got.closed_at, got.revisions[0].outcome) == ("stale", None, "stale")
    assert got.updated_at >= got.created_at


async def test_concurrent_transitions_exactly_one_wins(pool):
    async with pool.connection() as setup:
        pid = await _insert(setup)

    first_done, release = asyncio.Event(), asyncio.Event()

    async def first():
        async with pool.connection() as conn, conn.transaction():
            await proposals.transition(conn, pid, from_states=("open",), to="applied")
            first_done.set()
            await release.wait()
        return "applied"

    async def second():
        await first_done.wait()
        async with pool.connection() as conn, conn.transaction():
            await proposals.transition(conn, pid, from_states=("open",), to="withdrawn")
        return "withdrawn"

    t1 = asyncio.create_task(first())
    t2 = asyncio.create_task(second())
    await first_done.wait()
    await asyncio.sleep(0.3)  # the second UPDATE is now blocked on the row lock
    assert not t2.done()
    release.set()
    results = await asyncio.gather(t1, t2, return_exceptions=True)
    assert results[0] == "applied"
    assert isinstance(results[1], proposals.NotInState)
    async with pool.connection() as conn:
        assert await _status(conn, pid) == "applied"


async def test_add_revision_bumps_and_keeps_history(pool):
    async with pool.connection() as conn:
        pid = await _insert(conn, ttl=60)
        first = await proposals.get(conn, pid)
        rev = await proposals.add_revision(
            conn,
            pid,
            from_states=proposals.ACTIVE,
            reason="second try",
            base={"replicas": 2},
            fingerprint="fp2",
            items=[_item("model", "opus")],
            ttl_seconds=TTL,
        )
        got = await proposals.get(conn, pid)
    assert rev == 2
    assert (got.status, got.revision) == ("open", 2)
    assert got.expires_at > first.expires_at + timedelta(days=6)
    old, new = got.revisions
    assert (old.revision, old.outcome, [i.path for i in old.items]) == (1, "revised", ["replicas"])
    assert (new.revision, new.outcome, new.reason, new.base, new.fingerprint) == (
        2,
        None,
        "second try",
        {"replicas": 2},
        "fp2",
    )
    assert [i.path for i in new.items] == ["model"]


async def test_add_revision_keeps_an_outcome_already_set(pool):
    async with pool.connection() as conn:
        pid = await _insert(conn)
        await proposals.transition(conn, pid, from_states=("open",), to="rework", outcome="rework")
        await proposals.add_revision(
            conn,
            pid,
            from_states=proposals.ACTIVE,
            reason="fixed",
            base={},
            fingerprint="fp2",
            items=[_item("model", "opus")],
            ttl_seconds=TTL,
        )
        got = await proposals.get(conn, pid)
    assert [r.outcome for r in got.revisions] == ["rework", None]
    assert got.status == "open"


async def test_add_revision_from_wrong_state_raises_and_changes_nothing(pool):
    async with pool.connection() as conn:
        pid = await _insert(conn)
        await proposals.transition(conn, pid, from_states=("open",), to="withdrawn")
        before = await proposals.get(conn, pid)
        with pytest.raises(proposals.NotInState):
            await proposals.add_revision(
                conn,
                pid,
                from_states=proposals.ACTIVE,
                reason="late",
                base={},
                fingerprint="fp2",
                items=[_item("model", "opus")],
                ttl_seconds=TTL,
            )
        assert await proposals.get(conn, pid) == before


async def test_lock_returns_current_revision_only(pool):
    async with pool.connection() as conn:
        pid = await _insert(conn)
        await proposals.add_revision(
            conn,
            pid,
            from_states=proposals.ACTIVE,
            reason="r2",
            base={},
            fingerprint="fp2",
            items=[_item("model", "opus")],
            ttl_seconds=TTL,
        )
        async with conn.transaction():
            locked = await proposals.lock(conn, pid)
        assert await proposals.lock(conn, pid + 1000) is None
    assert locked.revision == 2
    assert [(r.revision, [i.path for i in r.items]) for r in locked.revisions] == [(2, ["model"])]


async def test_lock_blocks_a_concurrent_transition(pool):
    async with pool.connection() as setup:
        pid = await _insert(setup)
    locked, release = asyncio.Event(), asyncio.Event()

    async def holder():
        async with pool.connection() as conn, conn.transaction():
            await proposals.lock(conn, pid)
            locked.set()
            await release.wait()
            await proposals.transition(conn, pid, from_states=("open",), to="applied")

    async def other():
        await locked.wait()
        async with pool.connection() as conn, conn.transaction():
            await proposals.transition(conn, pid, from_states=("open",), to="withdrawn")

    t1, t2 = asyncio.create_task(holder()), asyncio.create_task(other())
    await locked.wait()
    await asyncio.sleep(0.3)
    assert not t2.done()
    release.set()
    r1, r2 = await asyncio.gather(t1, t2, return_exceptions=True)
    assert r1 is None
    assert isinstance(r2, proposals.NotInState)


async def test_record_review_writes_only_the_given_revision(pool):
    async with pool.connection() as conn:
        pid = await _insert(conn, items=[_item("replicas", 2), _item("model", "x")])
        await proposals.add_revision(
            conn,
            pid,
            from_states=proposals.ACTIVE,
            reason="r2",
            base={},
            fingerprint="fp2",
            items=[_item("replicas", 3), _item("model", "y")],
            ttl_seconds=TTL,
        )
        await proposals.record_review(
            conn, pid, 2, {"replicas": ("approved", None), "model": ("rework", "use opus")}
        )
        with pytest.raises(proposals.UnknownItem):
            await proposals.record_review(conn, pid, 2, {"nope": ("denied", None)})
        got = await proposals.get(conn, pid)
    r1, r2 = got.revisions
    assert {(i.path, i.decision, i.note) for i in r1.items} == {
        ("replicas", "pending", None),
        ("model", "pending", None),
    }
    assert r1.reviewed_at is None
    assert {(i.path, i.decision, i.note) for i in r2.items} == {
        ("replicas", "approved", None),
        ("model", "rework", "use opus"),
    }
    assert r2.reviewed_at is not None


async def test_count_active_counts_open_rework_and_stale_only(pool):
    session = uuid.uuid4()
    async with pool.connection() as conn:
        ids = [await _insert(conn, session) for _ in range(8)]
        await _insert(conn)  # another proposer
        for pid, to in zip(ids[1:], ("rework", "stale", *proposals.TERMINAL), strict=True):
            await proposals.transition(conn, pid, from_states=("open",), to=to)
        assert await proposals.count_active(conn, session) == 3
        assert await proposals.count_active(conn, uuid.uuid4()) == 0


async def test_list_for_filters_by_proposer_and_status(pool):
    session = uuid.uuid4()
    async with pool.connection() as conn:
        a = await _insert(conn, session)
        b = await _insert(conn, session)
        c = await _insert(conn)
        await proposals.transition(conn, b, from_states=("open",), to="denied")
        mine = await proposals.list_for(
            conn, proposer_session=session, statuses=proposals.ACTIVE, limit=10
        )
        everyone = await proposals.list_for(
            conn, proposer_session=None, statuses=(*proposals.ACTIVE, "denied"), limit=10
        )
        limited = await proposals.list_for(
            conn, proposer_session=None, statuses=(*proposals.ACTIVE, "denied"), limit=2
        )
    assert [p.id for p in mine] == [a]
    assert [p.id for p in everyone] == [c, b, a]
    assert [p.id for p in limited] == [c, b]
    assert [r.revision for r in mine[0].revisions] == [1]
    assert [i.path for i in mine[0].revisions[0].items] == ["replicas"]


async def test_active_on_target(pool):
    async with pool.connection() as conn:
        a = await _insert(conn, target="t1")
        b = await _insert(conn, target="t1")
        done = await _insert(conn, target="t1")
        await _insert(conn, target="t2")
        await proposals.transition(conn, done, from_states=("open",), to="applied")
        assert await proposals.active_on_target(conn, "preset", "t1") == [a, b]
        assert await proposals.active_on_target(conn, "preset", "t1", exclude=a) == [b]
        assert await proposals.active_on_target(conn, "schedule", "t1") == []


async def test_due_returns_only_active_past_expiry(pool):
    async with pool.connection() as conn:
        past = await _insert(conn)
        stale_past = await _insert(conn)
        closed_past = await _insert(conn)
        await _insert(conn)  # not yet due
        await proposals.transition(conn, stale_past, from_states=("open",), to="stale")
        await proposals.transition(conn, closed_past, from_states=("open",), to="withdrawn")
        await conn.execute(
            "UPDATE change_proposals SET expires_at = now() - interval '1 minute'"
            " WHERE id = ANY(%s)",
            ([past, stale_past, closed_past],),
        )
        now = datetime.now(UTC)
        assert await proposals.due(conn, now) == [past, stale_past]
        assert await proposals.due(conn, now - timedelta(hours=1)) == []


async def test_notice_check_accepts_proposal_and_handover_refuses_unknown(dsn):
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute("INSERT INTO sessions (id, slug, spec) VALUES (gen_random_uuid(), 's', '{}')")
        for kind in ("inactivity_reminder", "inactivity_close", "handover", "proposal"):
            c.execute(
                "INSERT INTO messages (session_id, text, notice) SELECT id, 't', %s FROM sessions",
                (kind,),
            )
        with pytest.raises(psycopg.errors.CheckViolation):
            c.execute(
                "INSERT INTO messages (session_id, text, notice) SELECT id, 't', 'x' FROM sessions"
            )


async def test_checks_refuse_unknown_kind_status_decision_and_secret(pool):
    async with pool.connection() as conn:
        pid = await _insert(conn)
        for stmt in (
            "UPDATE change_proposals SET kind = 'other' WHERE id = %s",
            "UPDATE change_proposals SET status = 'other' WHERE id = %s",
            "UPDATE change_proposal_revisions SET outcome = 'other' WHERE proposal_id = %s",
            "UPDATE change_proposal_items SET decision = 'other' WHERE proposal_id = %s",
            "UPDATE change_proposal_items SET secret = 'other' WHERE proposal_id = %s",
            "UPDATE change_proposal_revisions SET reason = '' WHERE proposal_id = %s",
        ):
            with pytest.raises(psycopg.errors.CheckViolation):
                async with conn.transaction():
                    await conn.execute(stmt, (pid,))
        with pytest.raises(psycopg.errors.CheckViolation):
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO change_proposal_secrets"
                    " (preset_id, name, version, proposal_id, revision, state)"
                    " VALUES (gen_random_uuid(), 'X', 1, %s, 1, 'other')",
                    (pid,),
                )


async def test_deleting_a_proposal_cascades_to_revisions_and_items(pool):
    async with pool.connection() as conn:
        pid = await _insert(conn, items=[_item("replicas", 2), _item("model", "x")])
        await proposals.add_revision(
            conn,
            pid,
            from_states=proposals.ACTIVE,
            reason="r2",
            base={},
            fingerprint="fp2",
            items=[_item("model", "y")],
            ttl_seconds=TTL,
        )
        await conn.execute("DELETE FROM change_proposals WHERE id = %s", (pid,))
        for table in ("change_proposal_revisions", "change_proposal_items"):
            cur = await conn.execute(f"SELECT count(*) FROM {table} WHERE proposal_id = %s", (pid,))
            assert (await cur.fetchone())[0] == 0
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO change_proposal_items"
                    ' (proposal_id, revision, path, security, "grant")'
                    " VALUES (%s, 1, 'x', false, false)",
                    (pid,),
                )


async def test_transition_keeps_the_first_revision_outcome(pool):
    async with pool.connection() as conn:
        pid = await _insert(conn)
        await proposals.transition(conn, pid, from_states=("open",), to="stale", outcome="stale")
        await proposals.transition(conn, pid, from_states=("stale",), to="rework", outcome="rework")
        got = await proposals.get(conn, pid)
    assert got.status == "rework"
    assert got.revisions[0].outcome == "stale"


async def test_record_review_refuses_a_closed_or_old_revision_and_empty_review(pool):
    async with pool.connection() as conn:
        pid = await _insert(conn)
        await proposals.record_review(conn, pid, 1, {})
        assert (await proposals.get(conn, pid)).revisions[0].reviewed_at is None
        with pytest.raises(proposals.NotInState):
            await proposals.record_review(conn, pid, 2, {"replicas": ("approved", None)})
        await proposals.transition(conn, pid, from_states=("open",), to="denied")
        with pytest.raises(proposals.NotInState):
            await proposals.record_review(conn, pid, 1, {"replicas": ("approved", None)})
        got = await proposals.get(conn, pid)
    assert got.revisions[0].reviewed_at is None
    assert got.revisions[0].items[0].decision == "pending"


async def test_concurrent_client_key_replay_makes_one_row(pool):
    session = uuid.uuid4()

    async def file():
        async with pool.connection() as conn:
            return await _insert(conn, session, client_key="race")

    r1, r2 = await asyncio.gather(file(), file(), return_exceptions=True)
    results = [r1, r2]
    assert sum(isinstance(r, int) for r in results) == 1
    loser = next(r for r in results if not isinstance(r, int))
    winner = next(r for r in results if isinstance(r, int))
    assert isinstance(loser, proposals.ProposalExists)
    assert loser.proposal_id == winner
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT count(*) FROM change_proposals WHERE client_key = 'race'")
        assert (await cur.fetchone())[0] == 1

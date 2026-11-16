"""db.handoffs: the agent_handoffs rows and the agent_limits start-context record (Postgres)."""

from datetime import UTC, datetime

import psycopg.errors
import pytest
from factories import insert_agent, insert_session

from agents_orchestrator.db import handoffs, queue


async def _agent(pool):
    session = await insert_session(pool)
    agent = await insert_agent(pool, session["id"])
    return session["id"], agent["id"]


async def test_one_open_row_per_agent(pool):
    session_id, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        first = await handoffs.insert(conn, session_id, agent_id, "strong", "pressuring")
    async with pool.connection() as conn:
        assert (await handoffs.open_row(conn, agent_id)) == first
        with pytest.raises(psycopg.errors.UniqueViolation):
            await handoffs.insert(conn, session_id, agent_id, "voluntary", "submitted")
    async with pool.connection() as conn:
        assert await handoffs.close(conn, first.id, "done", "x")
        assert await handoffs.open_row(conn, agent_id) is None
        second = await handoffs.insert(conn, session_id, agent_id, "voluntary", "submitted")
    assert second.id != first.id
    assert second.phase == "submitted"


async def test_advance_is_status_guarded(pool):
    session_id, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        row = await handoffs.insert(conn, session_id, agent_id, "strong", "pressuring")
        assert not await handoffs.advance(conn, row.id, ["submitted"], "moving", handoff_text="t")
        after = await handoffs.open_row(conn, agent_id)
        assert after == row
        assert await handoffs.advance(
            conn, row.id, ["pressuring", "submitted"], "moving", leadership_request_id=7
        )
        after = await handoffs.open_row(conn, agent_id)
    assert after.phase == "moving"
    assert after.leadership_request_id == 7


async def test_close_sets_reason_and_time(pool):
    session_id, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        row = await handoffs.insert(conn, session_id, agent_id, "shutdown", "handed")
        assert await handoffs.close(conn, row.id, "cancelled", "agent_gone")
        assert not await handoffs.close(conn, row.id, "done", "again")
        cur = await conn.execute(
            "SELECT phase, close_reason, closed_at FROM agent_handoffs WHERE id = %s", (row.id,)
        )
        phase, reason, closed_at = await cur.fetchone()
    assert (phase, reason) == ("cancelled", "agent_gone")
    assert closed_at is not None


async def test_limits_start_upsert(pool):
    _, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        assert await handoffs.limits_start(conn, agent_id) is None
        await handoffs.set_limits_start(conn, agent_id, 1234)
        assert await handoffs.limits_start(conn, agent_id) == 1234
        await handoffs.set_limits_start(conn, agent_id, 99)
        assert await handoffs.limits_start(conn, agent_id) == 99
        await handoffs.set_limits_start(conn, agent_id, None)
        assert await handoffs.limits_start(conn, agent_id) is None


async def test_advance_never_closes_or_reopens(pool):
    session_id, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        row = await handoffs.insert(conn, session_id, agent_id, "strong", "pressuring")
        with pytest.raises(ValueError):
            await handoffs.advance(conn, row.id, ["pressuring"], "done")
        assert await handoffs.close(conn, row.id, "cancelled", "compacted")
        with pytest.raises(ValueError):
            await handoffs.advance(conn, row.id, ["cancelled"], "pressuring")
        cur = await conn.execute("SELECT phase FROM agent_handoffs WHERE id = %s", (row.id,))
        assert (await cur.fetchone())[0] == "cancelled"


async def test_start_clear_counts_attempts_in_the_update(pool):
    session_id, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        row = await handoffs.insert(conn, session_id, agent_id, "voluntary", "handed")
        assert await handoffs.start_clear(conn, row.id, ["handed"], 11)
        assert not await handoffs.start_clear(conn, row.id, ["handed"], 12)
        assert await handoffs.start_clear(conn, row.id, ["clearing"], 13)
        after = await handoffs.open_row(conn, agent_id)
    assert (after.phase, after.clear_command_id, after.clear_attempts) == ("clearing", 13, 2)


async def test_open_pressure_one_open_row(pool):
    session_id, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        row = await handoffs.open_pressure(conn, session_id, agent_id, 1, "work")
    assert (row.trigger, row.phase, row.level, row.profile) == (
        "pressure",
        "pressuring",
        1,
        "work",
    )
    assert (row.rejects, row.reject_reasons, row.gate_blocks) == (0, [], 0)
    async with pool.connection() as conn:
        with pytest.raises(psycopg.errors.UniqueViolation):
            await handoffs.open_pressure(conn, session_id, agent_id, 1, "chat")
    async with pool.connection() as conn:
        assert await handoffs.close(conn, row.id, "done", "x")
        again = await handoffs.open_pressure(conn, session_id, agent_id, 2, "chat", "voluntary")
    assert (again.trigger, again.profile) == ("voluntary", "chat")


async def test_set_level_only_rises_and_records_l2_tokens(pool):
    session_id, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        row = await handoffs.open_pressure(conn, session_id, agent_id, 1, "work")
        assert await handoffs.set_level(conn, row.id, 2, 150_000)
        after = await handoffs.open_row(conn, agent_id)
        assert (after.level, after.l2_tokens_at) == (2, 150_000)
        assert not await handoffs.set_level(conn, row.id, 2, 160_000)
        assert not await handoffs.set_level(conn, row.id, 1, 100)
        assert await handoffs.set_level(conn, row.id, 3, 170_000)
        after = await handoffs.open_row(conn, agent_id)
    assert (after.level, after.l2_tokens_at) == (3, 150_000)


async def test_set_level_below_two_leaves_l2_tokens_empty(pool):
    session_id, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        row = await handoffs.open_pressure(conn, session_id, agent_id, 0, "work")
        assert await handoffs.set_level(conn, row.id, 1, 90_000)
        assert (await handoffs.open_row(conn, agent_id)).l2_tokens_at is None


async def test_submit_status_guarded(pool):
    session_id, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        row = await handoffs.open_pressure(conn, session_id, agent_id, 2, "work")
        assert await handoffs.submit(conn, row.id, {"goal": "g"}, {"bg": []}, 123_000)
        after = await handoffs.open_row(conn, agent_id)
        assert (after.phase, after.handoff, after.inflight, after.submit_tokens) == (
            "submitted",
            {"goal": "g"},
            {"bg": []},
            123_000,
        )
        assert after.submitted_at is not None
        # a resubmit while submitted replaces it; a row past that is refused
        assert await handoffs.submit(conn, row.id, {"goal": "g2"}, {}, None)
        assert await handoffs.advance(conn, row.id, ["submitted"], "moving")
        assert not await handoffs.submit(conn, row.id, {"goal": "g3"}, {}, None)
        assert (await handoffs.open_row(conn, agent_id)).handoff == {"goal": "g2"}
        with pytest.raises(ValueError):
            await handoffs.submit(conn, row.id, {}, {}, None, from_phases=("done",))


async def test_record_reject_counts_and_reasons(pool):
    session_id, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        row = await handoffs.open_pressure(conn, session_id, agent_id, 1, "chat")
        assert await handoffs.record_reject(conn, row.id, ["too_short"]) == 1
        assert await handoffs.record_reject(conn, row.id, ["no_goal", "no_next"]) == 2
        after = await handoffs.open_row(conn, agent_id)
        assert await handoffs.close(conn, row.id, "cancelled", "x")
        assert await handoffs.record_reject(conn, row.id, ["late"]) == 0
    assert (after.rejects, after.reject_reasons) == (2, ["too_short", "no_goal", "no_next"])


async def test_record_gate_block(pool):
    session_id, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        row = await handoffs.open_pressure(conn, session_id, agent_id, 3, "work")
        await handoffs.record_gate_block(conn, row.id, {"subagents": 1})
        await handoffs.record_gate_block(conn, row.id, {"subagents": 2})
        after = await handoffs.open_row(conn, agent_id)
    assert (after.gate_blocks, after.inflight) == (2, {"subagents": 2})


async def test_mark_escalated_once(pool):
    session_id, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        row = await handoffs.open_pressure(conn, session_id, agent_id, 3, "work")
        assert await handoffs.mark_escalated(conn, row.id)
        assert not await handoffs.mark_escalated(conn, row.id)
        assert (await handoffs.open_row(conn, agent_id)).escalated_at is not None
        assert await handoffs.mark_stuck_reported(conn, row.id)
        assert not await handoffs.mark_stuck_reported(conn, row.id)


async def test_mark_nudged_per_level(pool):
    session_id, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        row = await handoffs.open_pressure(conn, session_id, agent_id, 1, "work")
        assert await handoffs.mark_nudged(conn, row.id, 1)
        assert not await handoffs.mark_nudged(conn, row.id, 1)
        assert not await handoffs.mark_nudged(conn, row.id, 0)
        assert await handoffs.mark_nudged(conn, row.id, 3)
        assert (await handoffs.open_row(conn, agent_id)).nudged_level == 3


async def _progress(conn, row_id):
    cur = await conn.execute("SELECT progress_at FROM agent_handoffs WHERE id = %s", (row_id,))
    return (await cur.fetchone())[0]


async def test_progress_at_touched_on_update(pool):
    session_id, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        row = await handoffs.open_pressure(conn, session_id, agent_id, 1, "work")
        steps = [
            lambda: handoffs.set_level(conn, row.id, 2, 1),
            lambda: handoffs.record_reject(conn, row.id, ["r"]),
            lambda: handoffs.record_gate_block(conn, row.id, {}),
            lambda: handoffs.mark_nudged(conn, row.id, 1),
            lambda: handoffs.mark_escalated(conn, row.id),
            lambda: handoffs.submit(conn, row.id, {}, {}, None),
            lambda: handoffs.advance(conn, row.id, ["submitted"], "moving"),
            lambda: handoffs.start_clear(conn, row.id, ["moving"], 5),
        ]
        old = datetime(2000, 1, 1, tzinfo=UTC)
        for step in steps:
            await conn.execute("UPDATE agent_handoffs SET progress_at = %s", (old,))
            await step()
            assert await _progress(conn, row.id) > old
        assert await handoffs.mark_stuck_reported(conn, row.id)
        stamped = await _progress(conn, row.id)
        assert await handoffs.close(conn, row.id, "done", "x")
        assert await _progress(conn, row.id) >= stamped


async def test_mark_stuck_reported_leaves_progress_alone(pool):
    session_id, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        row = await handoffs.open_pressure(conn, session_id, agent_id, 3, "work")
        old = datetime(2000, 1, 1, tzinfo=UTC)
        await conn.execute("UPDATE agent_handoffs SET progress_at = %s", (old,))
        assert await handoffs.mark_stuck_reported(conn, row.id)
        assert await _progress(conn, row.id) == old


async def test_take_undelivered_returns_handoff_and_text_of_newest(pool):
    session_id, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        successor = await insert_agent(pool, session_id)
        row = await handoffs.open_pressure(conn, session_id, agent_id, 2, "work")
        assert await handoffs.submit(conn, row.id, {"goal": "g"}, {}, None)
        assert await handoffs.advance(
            conn, row.id, ["submitted"], "discussing", successor_agent_id=successor["id"]
        )
        assert await handoffs.take_undelivered(conn, agent_id, successor["id"], None) == (
            {"goal": "g"},
            None,
        )
        assert await handoffs.take_undelivered(conn, agent_id, successor["id"], None) == (
            None,
            None,
        )


async def test_seen_submit_guards_advance_and_start_clear(pool):
    session_id, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        row = await handoffs.open_pressure(conn, session_id, agent_id, 1, "work")
        assert await handoffs.submit(conn, row.id, {"doing": "x"}, {"waiting_on": []}, None)
        seen = (await handoffs.open_row(conn, agent_id)).submitted_at
    async with pool.connection() as conn:  # a resubmit lands in its own transaction
        assert await handoffs.submit(conn, row.id, {"doing": "y"}, {"waiting_on": ["t"]}, None)
    async with pool.connection() as conn:
        assert not await handoffs.advance(
            conn, row.id, ["submitted"], "moving", seen_submit=seen, leadership_request_id=1
        )
        assert not await handoffs.start_clear(conn, row.id, ["submitted"], 1, seen_submit=seen)
        latest = (await handoffs.open_row(conn, agent_id)).submitted_at
        assert latest != seen
        assert await handoffs.start_clear(conn, row.id, ["submitted"], 1, seen_submit=latest)


async def _two_agents(pool):
    session = await insert_session(pool)
    a = await insert_agent(pool, session["id"], ordinal=0)
    b = await insert_agent(pool, session["id"], ordinal=1)
    return session["id"], a["id"], b["id"]


async def test_own_limit_is_the_rows_own_request_id(pool):
    # only the row's own limit request (by id) carries a moving row to the discussion; another
    # limit request applied for the same agent is a foreign move and closes it not_leader
    session_id, a, b = await _two_agents(pool)
    async with pool.connection() as conn:
        row = await handoffs.insert(conn, session_id, a, "pressure", "moving")
        assert await handoffs.advance(conn, row.id, ["moving"], "moving", leadership_request_id=7)
        await handoffs.on_leader_moved(
            conn, session_id, a, b, {"id": 8, "kind": "handover", "reason": "limit"}
        )
        assert await handoffs.open_row(conn, a) is None
        (closed,) = await (
            await conn.execute(
                "SELECT phase, close_reason, discuss_since FROM agent_handoffs WHERE id = %s",
                (row.id,),
            )
        ).fetchall()
    assert closed == ("cancelled", "not_leader", None)
    async with pool.connection() as conn:
        row = await handoffs.insert(conn, session_id, a, "pressure", "moving")
        assert await handoffs.advance(conn, row.id, ["moving"], "moving", leadership_request_id=9)
        await handoffs.on_leader_moved(
            conn, session_id, a, b, {"id": 9, "kind": "handover", "reason": "limit"}
        )
        after = await handoffs.open_row(conn, a)
    assert (after.phase, after.successor_agent_id) == ("discussing", b)


@pytest.mark.parametrize("phase", ["discussing", "moving"])
async def test_successor_leading_again_closes_its_own_row(pool, phase):
    # the ex-leader becomes leader again: its own discussing (or moving) row closes cancelled
    # leader_again in the move's transaction, so it is never cleared as an ex-leader and can
    # open its own pressure cycle
    session_id, a, b = await _two_agents(pool)
    async with pool.connection() as conn:
        own = await handoffs.insert(conn, session_id, a, "pressure", phase)
        await handoffs.on_leader_moved(conn, session_id, b, a, {"kind": "user"})
        assert await handoffs.open_row(conn, a) is None
        (closed,) = await (
            await conn.execute(
                "SELECT phase, close_reason FROM agent_handoffs WHERE id = %s", (own.id,)
            )
        ).fetchall()
        assert closed == ("cancelled", "leader_again")
        fresh = await handoffs.open_pressure(conn, session_id, a, 1, "work")
    assert fresh is not None and fresh.phase == "pressuring"


@pytest.mark.parametrize("phase", ["clearing", "returning", "handed"])
async def test_successor_leading_again_leaves_other_phases(pool, phase):
    session_id, a, b = await _two_agents(pool)
    async with pool.connection() as conn:
        trigger = "shutdown" if phase == "handed" else "pressure"
        await handoffs.insert(conn, session_id, a, trigger, phase)
        await handoffs.on_leader_moved(conn, session_id, b, a, {"kind": "user"})
        assert (await handoffs.open_row(conn, a)).phase == phase


async def _clearing_ex_leader(conn, session_id, agent_id, *, running: bool):
    """An ex-leader's discussion row in `clearing` with its clear command queued (or running)."""
    row = await handoffs.insert(conn, session_id, agent_id, "pressure", "discussing")
    now = datetime.now(UTC)
    assert await handoffs.advance(conn, row.id, ["discussing"], "discussing", discuss_since=now)
    command_id = await queue.enqueue_command(conn, agent_id, "clear")
    assert await handoffs.start_clear(conn, row.id, ["discussing"], command_id)
    if running:
        assert (await queue.claim_command(conn, ["clear"])).id == command_id
    return row, command_id


async def _status(conn, sql, params):
    return (await (await conn.execute(sql, params)).fetchone())[0]


async def test_successor_leading_again_cancels_its_queued_clear(pool):
    # an ex-leader whose clear is still queued leads again: the clear is cancelled and the row
    # closes cancelled leader_again in the move's transaction, so the successor's handoff it
    # now holds is never wiped
    session_id, a, b = await _two_agents(pool)
    async with pool.connection() as conn:
        row, command_id = await _clearing_ex_leader(conn, session_id, a, running=False)
        await handoffs.on_leader_moved(conn, session_id, b, a, {"kind": "user"})
        assert await handoffs.open_row(conn, a) is None
        sql = "SELECT phase, close_reason FROM agent_handoffs WHERE id = %s"
        closed = await (await conn.execute(sql, (row.id,))).fetchone()
        assert closed == ("cancelled", "leader_again")
        cmd = "SELECT status FROM commands WHERE id = %s"
        assert await _status(conn, cmd, (command_id,)) == "cancelled"


async def test_successor_leading_again_leaves_a_running_clear(pool):
    # a clear already running is left alone: its row stays `clearing`
    session_id, a, b = await _two_agents(pool)
    async with pool.connection() as conn:
        row, command_id = await _clearing_ex_leader(conn, session_id, a, running=True)
        await handoffs.on_leader_moved(conn, session_id, b, a, {"kind": "user"})
        assert (await handoffs.open_row(conn, a)).phase == "clearing"
        cmd = "SELECT status FROM commands WHERE id = %s"
        assert await _status(conn, cmd, (command_id,)) == "running"


async def test_cancel_queued_command_is_status_guarded(pool):
    session_id, a = await _agent(pool)
    async with pool.connection() as conn:
        queued = await queue.enqueue_command(conn, a, "clear")
        assert await queue.cancel_queued_command(conn, queued)
        assert not await queue.cancel_queued_command(conn, queued)  # already cancelled
        running = await queue.enqueue_command(conn, a, "clear")
        assert (await queue.claim_command(conn, ["clear"])).id == running
        assert not await queue.cancel_queued_command(conn, running)

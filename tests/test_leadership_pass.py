"""Leadership pass: election, epoch-fenced requests, lost-leader fallback, one lock, NOTIFY."""

from __future__ import annotations

import asyncio
import json
import re
import time
import uuid
from pathlib import Path

import psycopg
from factories import (
    insert_agent,
    insert_message,
    insert_session,
    set_agent_status,
)
from psycopg.rows import dict_row

from agents_orchestrator.db import leadership
from agents_orchestrator.db.queue import _CLAIM_LOCK_NAMESPACE
from agents_orchestrator.dispatcher import Dispatcher
from agents_orchestrator.dispatcher import hive as hive_module
from agents_orchestrator.dispatcher.loop import CHANNELS
from agents_orchestrator.settings import Settings

README = Path(__file__).resolve().parents[1] / "README.md"


# ---------------------------------------------------------------- helpers


async def hive(pool, n: int = 3, *, ready: bool = True, max_replicas: int = 4) -> tuple:
    """A hive session with `n` agents, each ready (or not) with a session, free `ordinal` s ago."""
    c = await insert_session(pool, spec={"replicas": n, "max_replicas": max_replicas})
    agents = []
    for i in range(n):
        a = await insert_agent(pool, c["id"], ordinal=i, ready=ready, wrapper_session_id=f"s{i}")
        await set_agent_status(pool, a["id"], state="idle")
        agents.append(a)
    return c, agents


async def run(pool, sql: str, params: tuple = ()) -> None:
    async with pool.connection() as conn:
        await conn.execute(sql, params)


async def rows(pool, sql: str, params: tuple = ()) -> list[dict]:
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, params)
        return await cur.fetchall()


async def leader_row(pool, session_id) -> dict | None:
    got = await rows(pool, "SELECT * FROM session_leaders WHERE session_id = %s", (session_id,))
    return got[0] if got else None


async def set_leader(pool, session_id, agent_id, epoch: int = 3) -> None:
    await run(
        pool,
        "INSERT INTO session_leaders (session_id, agent_id, epoch, since, reason)"
        " VALUES (%s, %s, %s, now(), 'initial')",
        (session_id, agent_id, epoch),
    )


async def free_ago(pool, agent_id, seconds: float, state: str = "idle") -> None:
    await run(
        pool,
        "UPDATE agent_status SET state = %s, free_since = now() - make_interval(secs => %s)"
        " WHERE agent_id = %s",
        (state, seconds, agent_id),
    )


async def request(pool, session_id, kind, frm=None, to=None, reason="full", epoch=None) -> int:
    async with pool.connection() as conn:
        return await leadership.insert_request(conn, session_id, kind, frm, to, reason, epoch)


async def decided(pool, request_id: int) -> tuple:
    r = (
        await rows(
            pool,
            "SELECT status, reject_reason, decided_at FROM leadership_requests WHERE id = %s",
            (request_id,),
        )
    )[0]
    assert r["decided_at"] is not None
    return (r["status"], r["reject_reason"])


async def lpass(pool, session_id):
    async with pool.connection() as conn:
        return await leadership.leadership_pass(conn, session_id)


class Notes:
    """Collect `leader_changed` payloads on a dedicated LISTEN connection."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.got: list[dict] = []

    async def __aenter__(self) -> Notes:
        self.conn = await psycopg.AsyncConnection.connect(self.dsn, autocommit=True)
        await self.conn.execute("LISTEN leader_changed")
        return self

    async def drain(self, timeout: float = 0.5) -> list[dict]:
        async for n in self.conn.notifies(timeout=timeout):
            self.got.append(json.loads(n.payload))
        return self.got

    async def __aexit__(self, *exc) -> None:
        await self.conn.close()


# ---------------------------------------------------------------- AC1, AC11


async def test_initial_leader(pool):
    c, (a0, a1, a2) = await hive(pool)
    change = await lpass(pool, c["id"])
    assert change is not None
    assert (change.session_id, change.agent_id, change.epoch, change.reason) == (
        c["id"],
        a0["id"],
        1,
        "initial",
    )
    row = await leader_row(pool, c["id"])
    assert (row["agent_id"], row["epoch"], row["reason"]) == (a0["id"], 1, "initial")
    assert row["since"] is not None and row["unready_since"] is None
    assert "feed_cursor" not in row
    # a second pass changes nothing
    assert await lpass(pool, c["id"]) is None
    assert (await leader_row(pool, c["id"]))["epoch"] == 1


async def test_initial_leader_needs_ready_agent_with_session(pool):
    c, (a0, a1, _a2) = await hive(pool, ready=False)
    assert await lpass(pool, c["id"]) is None
    assert await leader_row(pool, c["id"]) is None
    await run(
        pool, "UPDATE agents SET ready = true, wrapper_session_id = NULL WHERE id = %s", (a0["id"],)
    )
    assert await lpass(pool, c["id"]) is None  # ready but no session yet
    await run(pool, "UPDATE agents SET ready = true WHERE id = %s", (a1["id"],))
    change = await lpass(pool, c["id"])
    assert change is not None and change.agent_id == a1["id"]


async def test_poll_driven_election_lowest_ordinal(pool):
    c, (a0, a1, a2) = await hive(pool, ready=False)
    # ordinal 2 then ordinal 1 become ready between two passes: the next pass (the poll)
    # elects the lowest ordinal ready, not the first ready event
    await run(pool, "UPDATE agents SET ready = true WHERE id = %s", (a2["id"],))
    await run(pool, "UPDATE agents SET ready = true WHERE id = %s", (a1["id"],))
    change = await lpass(pool, c["id"])
    assert change is not None and change.agent_id == a1["id"]
    text = README.read_text()
    m = re.search(r"^### Leadership\n(.*?)(?=^## |^### )", text, re.S | re.M)
    assert m, "README has no ### Leadership section"
    section = " ".join(m.group(1).split())
    assert "poll-driven" in section
    assert "ORCH_DISPATCH_POLL_SECONDS" in section
    assert "lowest-ordinal ready agent" in section
    assert "within the poll window" in section
    for word in ("leader_changed", "leadership_requested", "LEADER_GRACE_SECONDS"):
        assert word in section, word


# ---------------------------------------------------------------- AC2, AC3, AC4


async def test_handover_named(pool):
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    rid = await request(pool, c["id"], "handover", a0["id"], a2["id"], "context full", 3)
    change = await lpass(pool, c["id"])
    assert change is not None
    assert (change.agent_id, change.epoch, change.reason) == (
        a2["id"],
        4,
        "handover: context full",
    )
    assert await decided(pool, rid) == ("applied", None)
    row = await leader_row(pool, c["id"])
    assert (row["agent_id"], row["epoch"], row["reason"]) == (
        a2["id"],
        4,
        "handover: context full",
    )


async def status(pool, agent_id, state: str, ctx: float | None, *, bg: bool = False) -> None:
    """Set an agent's state and context %; `bg` gives it a running background subagent."""
    await run(
        pool,
        "UPDATE agent_status SET state = %s, context_pct = %s,"
        " background_subagents = CASE WHEN %s"
        "  THEN jsonb_build_object('sub', now()::text) ELSE '{}'::jsonb END"
        " WHERE agent_id = %s",
        (state, ctx, bg, agent_id),
    )


async def test_successor_prefers_idle_low_context(pool):
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await status(pool, a0["id"], "idle", 5)  # the leader itself is never picked
    await status(pool, a1["id"], "idle", 40)
    await status(pool, a2["id"], "idle", 10)
    await free_ago(pool, a1["id"], 900)  # free the longest no longer decides
    rid = await request(pool, c["id"], "handover", a0["id"], None, "done", 3)
    change = await lpass(pool, c["id"])
    assert change is not None and (change.agent_id, change.epoch) == (a2["id"], 4)
    assert await decided(pool, rid) == ("applied", None)


async def test_successor_ranks_background_busy_last(pool):
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await status(pool, a1["id"], "idle", 40)
    await status(pool, a2["id"], "idle", 10, bg=True)
    await free_ago(pool, a2["id"], 900)
    await request(pool, c["id"], "handover", a0["id"], None, "done", 3)
    change = await lpass(pool, c["id"])
    assert change is not None and change.agent_id == a1["id"]


async def test_successor_falls_back_above_threshold(pool):
    # default compaction 95: threshold 80. Every peer above it: the lowest context is picked.
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a2["id"], epoch=3)
    await status(pool, a0["id"], "idle", 90)
    await status(pool, a1["id"], "idle", 85)
    await shrink(pool, c["id"], 2)
    change = await lpass(pool, c["id"])
    assert change is not None and (change.agent_id, change.reason) == (a1["id"], "scale_down")

    # below the threshold beats idle: a busy agent at 40 before an idle one at 85
    c, (b0, b1, b2) = await hive(pool)
    await set_leader(pool, c["id"], b2["id"], epoch=3)
    await status(pool, b0["id"], "idle", 85)
    await status(pool, b1["id"], "busy", 40)
    await shrink(pool, c["id"], 2)
    change = await lpass(pool, c["id"])
    assert change is not None and change.agent_id == b1["id"]

    # the threshold follows the policy and the spec: compaction 50 makes it 35, so both
    # are above it and the idle one wins
    half = leadership.PassPolicy(default_auto_compact_pct=50)
    for policy, spec_pct in ((half, None), (None, 50)):
        c, (d0, d1, d2) = await hive(pool)
        if spec_pct is not None:
            await run(
                pool,
                "UPDATE sessions SET spec = spec || jsonb_build_object('auto_compact_pct', %s::int)"
                " WHERE id = %s",
                (spec_pct, c["id"]),
            )
        await set_leader(pool, c["id"], d2["id"], epoch=3)
        await status(pool, d0["id"], "idle", 85)
        await status(pool, d1["id"], "busy", 40)
        await shrink(pool, c["id"], 2)
        async with pool.connection() as conn:
            if policy is None:
                change = await leadership.leadership_pass(conn, c["id"])
            else:
                change = await leadership.leadership_pass(conn, c["id"], policy)
        assert change is not None and change.agent_id == d0["id"]


async def test_successor_needs_token_headroom(pool):
    # compaction 25: hard limit 20% of a 1M window = 200k, less 50k headroom = 150k
    c, (a0, a1, a2) = await hive(pool)
    await run(
        pool,
        "UPDATE sessions SET spec = spec || jsonb_build_object('auto_compact_pct', 25)"
        " WHERE id = %s",
        (c["id"],),
    )
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await status(pool, a1["id"], "idle", 18)
    await status(pool, a2["id"], "busy", 6)
    await run(
        pool,
        "UPDATE agent_status SET context_window_tokens = 1000000, context_used_tokens ="
        " CASE agent_id WHEN %s THEN 180000 ELSE 60000 END WHERE agent_id IN (%s, %s)",
        (a1["id"], a1["id"], a2["id"]),
    )
    await request(pool, c["id"], "handover", a0["id"], None, "done", 3)
    change = await lpass(pool, c["id"])
    assert change is not None and change.agent_id == a2["id"]


async def test_headroom_percent_fallback(pool):
    # tokens unknown: hard 20% less 10 pct of headroom leaves 10%
    c, (a0, a1, a2) = await hive(pool)
    await run(
        pool,
        "UPDATE sessions SET spec = spec || jsonb_build_object('auto_compact_pct', 25)"
        " WHERE id = %s",
        (c["id"],),
    )
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await status(pool, a1["id"], "idle", 16)
    await status(pool, a2["id"], "busy", 5)
    await request(pool, c["id"], "handover", a0["id"], None, "done", 3)
    change = await lpass(pool, c["id"])
    assert change is not None and change.agent_id == a2["id"]


async def set_pending(pool, session_id, *ordinals: int) -> None:
    await run(
        pool,
        "UPDATE sessions SET status = %s WHERE id = %s",
        (json.dumps({"rollout": {"pending": list(ordinals)}}), session_id),
    )


async def test_rollout_pending_ranked_last(pool):
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await status(pool, a1["id"], "idle", 2)
    await status(pool, a2["id"], "idle", 8)
    await set_pending(pool, c["id"], 1)
    await request(pool, c["id"], "handover", a0["id"], None, "done", 3)
    change = await lpass(pool, c["id"])
    assert change is not None and change.agent_id == a2["id"]

    # every candidate pending: a successor is still picked, lowest context first
    c, (b0, b1, b2) = await hive(pool)
    await set_leader(pool, c["id"], b0["id"], epoch=3)
    await status(pool, b1["id"], "idle", 8)
    await status(pool, b2["id"], "idle", 2)
    await set_pending(pool, c["id"], 1, 2)
    await request(pool, c["id"], "handover", b0["id"], None, "done", 3)
    change = await lpass(pool, c["id"])
    assert change is not None and change.agent_id == b2["id"]


async def test_set_leader_records_start_and_previous_since(pool):
    c, (a0, a1, _a2) = await hive(pool)
    await run(
        pool,
        "UPDATE agent_status SET context_used_tokens = CASE agent_id WHEN %s THEN 11000"
        " ELSE 42000 END WHERE agent_id IN (%s, %s)",
        (a0["id"], a0["id"], a1["id"]),
    )
    await lpass(pool, c["id"])
    row = await leader_row(pool, c["id"])
    assert (row["agent_id"], row["start_context_tokens"], row["previous_since"]) == (
        a0["id"],
        11000,
        None,
    )
    first_since = row["since"]
    await request(pool, c["id"], "handover", a0["id"], a1["id"], "x", 1)
    await lpass(pool, c["id"])
    row = await leader_row(pool, c["id"])
    assert row["start_context_tokens"] == 42000 and row["previous_since"] == first_since


async def test_leader_lost_and_scale_down_use_successor(pool):
    c, (a0, a1, a2, a3) = await hive(pool, 4)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await status(pool, a1["id"], "busy", 5)
    await status(pool, a2["id"], "idle", 40)
    await status(pool, a3["id"], "idle", 10)
    await free_ago(pool, a2["id"], 900)
    await run(pool, "UPDATE agents SET gone_at = now() WHERE id = %s", (a0["id"],))
    change = await lpass(pool, c["id"])
    assert change is not None and (change.agent_id, change.reason) == (a3["id"], "leader_lost")

    await shrink(pool, c["id"], 3)  # a3 leaves
    change = await lpass(pool, c["id"])
    assert change is not None and (change.agent_id, change.reason) == (a2["id"], "scale_down")


async def test_set_leader_records_previous(pool):
    c, (a0, a1, _a2) = await hive(pool)
    change = await lpass(pool, c["id"])
    assert change is not None and change.previous_agent_id is None
    assert (await leader_row(pool, c["id"]))["previous_agent_id"] is None
    await request(pool, c["id"], "handover", a0["id"], a1["id"], "x", 1)
    change = await lpass(pool, c["id"])
    assert change is not None and change.previous_agent_id == a0["id"]
    row = await leader_row(pool, c["id"])
    assert (row["agent_id"], row["previous_agent_id"]) == (a1["id"], a0["id"])


async def test_note_candidates_equal_pass_pick(pool):
    """Ping-pong regression: the note's first candidate is the agent the pass would pick."""
    # (state, context %, background) for the leader then three peers; threshold 80
    mixes = [
        [("idle", 90, False), ("idle", 40, False), ("idle", 10, False), ("busy", 5, False)],
        [("idle", 90, False), ("idle", 40, False), ("idle", 10, True), ("busy", 20, False)],
        [("busy", 85, False), ("busy", 30, False), ("idle", None, False), ("idle", 70, False)],
        [("idle", 95, False), ("idle", 85, False), ("busy", 50, False), ("idle", 79, True)],
        [("idle", 82, False), ("busy", None, False), ("busy", 60, True), ("busy", 12, False)],
    ]
    for mix in mixes:
        c, agents = await hive(pool, 4)
        await set_leader(pool, c["id"], agents[0]["id"])
        for agent, (state, ctx, bg) in zip(agents, mix, strict=True):
            await status(pool, agent["id"], state, ctx, bg=bg)
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    hive_module._CANDIDATES,
                    {"session": c["id"], "leader": agents[0]["id"], "compact": 95},
                )
                note = await cur.fetchall()
            pick = await leadership._successor(
                conn, c["id"], agents[0]["id"], leadership.PassPolicy()
            )
        assert note, mix
        by_pod = {a["pod_name"]: a["id"] for a in agents}
        assert by_pod[note[0]["pod_name"]] == pick, mix


async def test_handover_rejections(pool):
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    not_leader = await request(pool, c["id"], "handover", a1["id"], a2["id"], "x", 3)
    stale = await request(pool, c["id"], "handover", a0["id"], a2["id"], "x", 2)
    await run(pool, "UPDATE agents SET ready = false WHERE id = %s", (a1["id"],))
    unready = await request(pool, c["id"], "handover", a0["id"], a1["id"], "x", 3)
    to_self = await request(pool, c["id"], "handover", a0["id"], a0["id"], "x", 3)
    await run(pool, "UPDATE agents SET gone_at = now() WHERE id = %s", (a2["id"],))
    gone = await request(pool, c["id"], "handover", a0["id"], a2["id"], "x", 3)
    other_c, (stranger,) = await hive(pool, 1)
    foreign = await request(pool, c["id"], "handover", a0["id"], stranger["id"], "x", 3)
    no_peers = await request(pool, c["id"], "handover", a0["id"], None, "x", 3)
    assert await lpass(pool, c["id"]) is None
    assert await decided(pool, not_leader) == ("rejected", "not_leader")
    assert await decided(pool, stale) == ("rejected", "stale_epoch")
    assert await decided(pool, unready) == ("rejected", "target_not_ready")
    assert await decided(pool, to_self) == ("rejected", "target_not_ready")
    assert await decided(pool, gone) == ("rejected", "target_not_ready")
    assert await decided(pool, foreign) == ("rejected", "target_not_ready")
    assert await decided(pool, no_peers) == ("rejected", "no_peers")
    row = await leader_row(pool, c["id"])
    assert (row["agent_id"], row["epoch"]) == (a0["id"], 3)


async def test_epoch_rechecked_at_apply_time(pool):
    """Two handovers queued at the same epoch: the first applies, the second is now stale."""
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    first = await request(pool, c["id"], "handover", a0["id"], a1["id"], "x", 3)
    # queued by the old leader before the first one was applied
    second = await request(pool, c["id"], "handover", a0["id"], a2["id"], "x", 3)
    # queued by the new leader at the new epoch, decided in the same pass
    third = await request(pool, c["id"], "handover", a1["id"], a2["id"], "y", 4)
    change = await lpass(pool, c["id"])
    assert await decided(pool, first) == ("applied", None)
    assert await decided(pool, second) == ("rejected", "not_leader")
    assert await decided(pool, third) == ("applied", None)
    assert change is not None and (change.agent_id, change.epoch) == (a2["id"], 5)


# ---------------------------------------------------------------- AC5


async def test_user_override(pool):
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    # a user request carries no epoch and is never fenced on it
    rid = await request(pool, c["id"], "user", None, a1["id"], "user", None)
    change = await lpass(pool, c["id"])
    assert change is not None and (change.agent_id, change.epoch, change.reason) == (
        a1["id"],
        4,
        "user",
    )
    assert await decided(pool, rid) == ("applied", None)
    # the target already leads: applied, no epoch bump, no change
    again = await request(pool, c["id"], "user", None, a1["id"], "user", None)
    assert await lpass(pool, c["id"]) is None
    assert await decided(pool, again) == ("applied", None)
    row = await leader_row(pool, c["id"])
    assert (row["agent_id"], row["epoch"], row["reason"]) == (a1["id"], 4, "user")
    # an unready or gone target is rejected
    await run(pool, "UPDATE agents SET ready = false WHERE id = %s", (a2["id"],))
    unready = await request(pool, c["id"], "user", None, a2["id"], "user", None)
    await run(pool, "UPDATE agents SET ready = true, gone_at = now() WHERE id = %s", (a2["id"],))
    gone = await request(pool, c["id"], "user", None, a2["id"], "user", None)
    assert await lpass(pool, c["id"]) is None
    assert await decided(pool, unready) == ("rejected", "target_not_ready")
    assert await decided(pool, gone) == ("rejected", "target_not_ready")
    assert (await leader_row(pool, c["id"]))["epoch"] == 4


# ---------------------------------------------------------------- AC6, AC10


async def test_lost_leader_fallback(pool):
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await status(pool, a1["id"], "idle", 10)
    await status(pool, a2["id"], "idle", 40)
    # gone: replaced at once by the successor (lowest context)
    await run(pool, "UPDATE agents SET gone_at = now() WHERE id = %s", (a0["id"],))
    change = await lpass(pool, c["id"])
    assert change is not None
    assert (change.agent_id, change.epoch, change.reason) == (a1["id"], 4, "leader_lost")
    # unready: first sight records unready_since, no takeover inside the grace
    await run(pool, "UPDATE agents SET ready = false WHERE id = %s", (a1["id"],))
    assert await lpass(pool, c["id"]) is None
    row = await leader_row(pool, c["id"])
    assert row["unready_since"] is not None and (row["agent_id"], row["epoch"]) == (a1["id"], 4)
    # past the grace: the successor among the ready agents takes over
    await run(
        pool,
        "UPDATE session_leaders SET unready_since = now() - make_interval(secs => %s)"
        " WHERE session_id = %s",
        (leadership.LEADER_GRACE_SECONDS + 1, c["id"]),
    )
    change = await lpass(pool, c["id"])
    assert change is not None
    assert (change.agent_id, change.epoch, change.reason) == (a2["id"], 5, "leader_lost")
    row = await leader_row(pool, c["id"])
    assert row["unready_since"] is None
    # with no ready agent left, the row stays
    await run(pool, "UPDATE agents SET ready = false WHERE session_id = %s", (c["id"],))
    await run(
        pool,
        "UPDATE session_leaders SET unready_since = now() - interval '1 hour'"
        " WHERE session_id = %s",
        (c["id"],),
    )
    assert await lpass(pool, c["id"]) is None
    row = await leader_row(pool, c["id"])
    assert (row["agent_id"], row["epoch"]) == (a2["id"], 5)


async def test_unready_since_cleared_when_ready(pool):
    assert leadership.LEADER_GRACE_SECONDS == 30
    c, (a0, a1, _a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=1)

    async def unready_for(seconds: int) -> None:
        await run(pool, "UPDATE agents SET ready = false WHERE id = %s", (a0["id"],))
        assert await lpass(pool, c["id"]) is None  # first sight
        await run(
            pool,
            "UPDATE session_leaders SET unready_since = unready_since"
            " - make_interval(secs => %s) WHERE session_id = %s",
            (seconds, c["id"]),
        )
        assert await lpass(pool, c["id"]) is None  # still inside the grace

    await unready_for(20)
    await run(pool, "UPDATE agents SET ready = true WHERE id = %s", (a0["id"],))
    async with pool.connection() as conn:
        assert c["id"] in await leadership.leadership_candidates(conn)  # the clear is due
    assert await lpass(pool, c["id"]) is None
    assert (await leader_row(pool, c["id"]))["unready_since"] is None
    await unready_for(20)
    row = await leader_row(pool, c["id"])
    assert (row["agent_id"], row["epoch"]) == (a0["id"], 1)


# ---------------------------------------------------------------- AC7


async def test_concurrent_passes_single_change(pool, dsn):
    c, (a0, a1, _a2) = await hive(pool)
    # a pass that cannot take the session lock is skipped
    async with await psycopg.AsyncConnection.connect(dsn) as holder:
        await holder.execute(
            "SELECT pg_advisory_xact_lock(%s, hashtext(%s::text))",
            (_CLAIM_LOCK_NAMESPACE, c["id"]),
        )
        assert await lpass(pool, c["id"]) is None
        assert await leader_row(pool, c["id"]) is None
        await holder.rollback()

    async with Notes(dsn) as notes:
        conns = [await psycopg.AsyncConnection.connect(dsn) for _ in range(4)]
        try:
            got = await asyncio.gather(*(leadership.leadership_pass(k, c["id"]) for k in conns))
            assert sum(g is not None for g in got) == 1
            rid = await request(pool, c["id"], "handover", a0["id"], None, "x", 1)
            for _ in range(3):
                got = await asyncio.gather(*(leadership.leadership_pass(k, c["id"]) for k in conns))
                assert sum(g is not None for g in got) <= 1
        finally:
            for k in conns:
                await k.close()
        payloads = await notes.drain()
    assert await decided(pool, rid) == ("applied", None)
    row = await leader_row(pool, c["id"])
    assert row["epoch"] == 2
    assert [p["epoch"] for p in payloads] == [1, 2]


# ---------------------------------------------------------------- AC8


async def test_alone_untouched_and_notify(pool, dsn):
    alone = await insert_session(pool, spec={"replicas": 1})
    solo = await insert_agent(pool, alone["id"], ordinal=0, ready=True, wrapper_session_id="s0")
    capped = await insert_session(pool, spec={"replicas": 1, "max_replicas": 1})
    await insert_agent(pool, capped["id"], ordinal=0, ready=True, wrapper_session_id="s0")
    rid = await request(pool, alone["id"], "user", None, solo["id"], "user", None)
    for cl in (alone, capped):
        assert await lpass(pool, cl["id"]) is None
        assert await leader_row(pool, cl["id"]) is None
    assert (await rows(pool, "SELECT status FROM leadership_requests WHERE id = %s", (rid,)))[0][
        "status"
    ] == "queued"
    async with pool.connection() as conn:
        candidates = await leadership.leadership_candidates(conn)
    assert alone["id"] not in candidates and capped["id"] not in candidates

    c, (a0, a1, a2) = await hive(pool)
    async with Notes(dsn) as notes:
        await lpass(pool, c["id"])  # initial
        await request(pool, c["id"], "handover", a0["id"], a1["id"], "x", 1)
        await request(pool, c["id"], "user", None, a2["id"], "user", None)
        await lpass(pool, c["id"])  # handover then user, two changes in one pass
        await run(pool, "UPDATE agents SET gone_at = now() WHERE id = %s", (a2["id"],))
        await lpass(pool, c["id"])  # leader lost
        payloads = await notes.drain()
    assert payloads == [
        {"session_id": str(c["id"]), "agent_id": str(a0["id"]), "epoch": 1, "reason": "initial"},
        {
            "session_id": str(c["id"]),
            "agent_id": str(a1["id"]),
            "epoch": 2,
            "reason": "handover: x",
        },
        {"session_id": str(c["id"]), "agent_id": str(a2["id"]), "epoch": 3, "reason": "user"},
        {
            "session_id": str(c["id"]),
            "agent_id": str(a0["id"]),
            "epoch": 4,
            "reason": "leader_lost",
        },
    ]


# ---------------------------------------------------------------- dispatcher wiring


def make_settings(dsn: str, **kw) -> Settings:
    return Settings(database_url=dsn, master_key="k" * 32, **kw)


async def test_candidates(pool):
    c_new, _ = await hive(pool)  # no leader row yet, a ready agent
    c_idle, (b0, _b1, _b2) = await hive(pool)  # a healthy leader, nothing queued
    await set_leader(pool, c_idle["id"], b0["id"])
    c_req, (r0, r1, _r2) = await hive(pool)
    await set_leader(pool, c_req["id"], r0["id"])
    await request(pool, c_req["id"], "user", None, r1["id"])
    c_gone, (g0, _g1, _g2) = await hive(pool)
    await set_leader(pool, c_gone["id"], g0["id"])
    await run(pool, "UPDATE agents SET gone_at = now() WHERE id = %s", (g0["id"],))
    c_unready, (u0, _u1, _u2) = await hive(pool)
    await set_leader(pool, c_unready["id"], u0["id"])
    await run(pool, "UPDATE agents SET ready = false WHERE id = %s", (u0["id"],))
    c_booting, _ = await hive(pool, ready=False)  # no leader row and nobody ready
    async with pool.connection() as conn:
        got = set(await leadership.leadership_candidates(conn))
    assert got == {c_new["id"], c_req["id"], c_gone["id"], c_unready["id"]}
    assert c_idle["id"] not in got and c_booting["id"] not in got


async def test_drain_runs_the_pass(pool, dsn):
    c, (a0, a1, _a2) = await hive(pool)
    d = Dispatcher(pool, make_settings(dsn))
    try:
        await d.drain()
        assert (await leader_row(pool, c["id"]))["agent_id"] == a0["id"]
        rid = await request(pool, c["id"], "handover", a0["id"], a1["id"], "x", 1)
        await d.drain()
        assert await decided(pool, rid) == ("applied", None)
        assert (await leader_row(pool, c["id"]))["agent_id"] == a1["id"]
    finally:
        await d.aclose()


async def test_leadership_requested_wakes_dispatcher(pool, dsn):
    assert leadership.CHANNEL in CHANNELS
    c, (a0, a1, _a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=1)
    d = Dispatcher(pool, make_settings(dsn, dispatch_poll_seconds=60))
    stop = asyncio.Event()
    task = asyncio.create_task(d.run(stop))
    try:
        await asyncio.sleep(0.5)  # initial drain done, listening
        rid = await request(pool, c["id"], "user", None, a1["id"])
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if (await leader_row(pool, c["id"]))["agent_id"] == a1["id"]:
                break
            await asyncio.sleep(0.05)
        assert await decided(pool, rid) == ("applied", None)
    finally:
        stop.set()
        await asyncio.wait_for(task, 5)


async def test_unknown_session_is_a_no_op(pool):
    assert await lpass(pool, uuid.uuid4()) is None


# ---------------------------------------------------------------- long-term leader


async def age_term(pool, session_id, seconds: int) -> None:
    await run(
        pool,
        "UPDATE session_leaders SET since = now() - make_interval(secs => %s)"
        " WHERE session_id = %s",
        (seconds, session_id),
    )


async def test_long_term_leader_is_never_moved(pool):
    c, (a0, _a1, _a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"])
    await age_term(pool, c["id"], 10 * 3600)
    before = await leader_row(pool, c["id"])
    async with pool.connection() as conn:
        assert c["id"] not in set(await leadership.leadership_candidates(conn))
    assert await lpass(pool, c["id"]) is None
    assert await leader_row(pool, c["id"]) == before


# ---------------------------------------------------------------- auto handover


async def compacted(pool, agent_id, *, after_term: bool = True) -> None:
    """Stamp `agent_status.compaction_at` 5 s after (or before) any term that began just now."""
    await run(
        pool,
        "UPDATE agent_status SET compaction_at = now() + make_interval(secs => %s)"
        " WHERE agent_id = %s",
        (5 if after_term else -5, agent_id),
    )


async def shrink(pool, session_id, replicas: int) -> None:
    await run(
        pool,
        "UPDATE sessions SET spec = jsonb_set(spec, '{replicas}', to_jsonb(%s::int)) WHERE id = %s",
        (replicas, session_id),
    )


async def test_compacted_leader_keeps_leading(pool):
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await free_ago(pool, a1["id"], 100)  # an idle peer that could take over
    await free_ago(pool, a2["id"], 10)
    await compacted(pool, a0["id"])
    async with pool.connection() as conn:
        assert c["id"] not in await leadership.leadership_candidates(conn)
    assert await lpass(pool, c["id"]) is None
    row = await leader_row(pool, c["id"])
    assert (row["agent_id"], row["epoch"], row["reason"]) == (a0["id"], 3, "initial")


async def test_scale_down_hands_over_leader(pool):
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a2["id"], epoch=3)
    await free_ago(pool, a0["id"], 100)
    await free_ago(pool, a1["id"], 10)
    await free_ago(pool, a2["id"], 0, state="busy")  # busy: no turn boundary needed
    await insert_message(pool, c["id"], status="dispatching", agent_id=a2["id"])
    await shrink(pool, c["id"], 2)
    change = await lpass(pool, c["id"])
    assert change is not None
    assert (change.agent_id, change.epoch, change.reason) == (a0["id"], 4, "scale_down")
    assert await lpass(pool, c["id"]) is None


async def test_scale_down_unready_leaving_leader_is_scale_down(pool):
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a2["id"], epoch=3)
    await shrink(pool, c["id"], 2)
    await run(pool, "UPDATE agents SET ready = false WHERE id = %s", (a2["id"],))
    await run(
        pool,
        "UPDATE session_leaders SET unready_since = now() - interval '1 hour'"
        " WHERE session_id = %s",
        (c["id"],),
    )
    change = await lpass(pool, c["id"])
    assert change is not None
    assert (change.epoch, change.reason) == (4, "scale_down")
    assert change.agent_id in (a0["id"], a1["id"])


async def test_scale_down_without_staying_agent_retries(pool):
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a2["id"], epoch=3)
    await shrink(pool, c["id"], 2)
    await run(pool, "UPDATE agents SET ready = false WHERE id IN (%s, %s)", (a0["id"], a1["id"]))
    async with pool.connection() as conn:
        assert c["id"] in await leadership.leadership_candidates(conn)
    assert await lpass(pool, c["id"]) is None
    row = await leader_row(pool, c["id"])
    assert (row["agent_id"], row["epoch"]) == (a2["id"], 3)
    await run(pool, "UPDATE agents SET ready = true WHERE id = %s", (a1["id"],))
    change = await lpass(pool, c["id"])
    assert change is not None
    assert (change.agent_id, change.epoch, change.reason) == (a1["id"], 4, "scale_down")


async def test_picks_skip_leaving_agents(pool):
    # the initial election
    c0 = await insert_session(pool, spec={"replicas": 2, "max_replicas": 4})
    for i, ready in enumerate((False, False, True)):
        a = await insert_agent(pool, c0["id"], ordinal=i, ready=ready, wrapper_session_id=f"s{i}")
        await set_agent_status(pool, a["id"], state="idle")
    assert await lpass(pool, c0["id"]) is None
    assert await leader_row(pool, c0["id"]) is None

    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await status(pool, a1["id"], "idle", 50)
    await status(pool, a2["id"], "idle", 5)  # the successor, but leaving and still ready
    await shrink(pool, c["id"], 2)
    # named and user targets that leave are rejected
    named = await request(pool, c["id"], "handover", a0["id"], a2["id"], "full", 3)
    user = await request(pool, c["id"], "user", None, a2["id"], "user", None)
    assert await lpass(pool, c["id"]) is None
    assert await decided(pool, named) == ("rejected", "target_not_ready")
    assert await decided(pool, user) == ("rejected", "target_not_ready")
    # a handover without a target skips the leaving agent
    await request(pool, c["id"], "handover", a0["id"], None, "full", 3)
    change = await lpass(pool, c["id"])
    assert change is not None and change.agent_id == a1["id"]
    # so does the lost-leader pick
    await run(pool, "UPDATE agents SET gone_at = now() WHERE id = %s", (a1["id"],))
    change = await lpass(pool, c["id"])
    assert change is not None
    assert (change.agent_id, change.reason) == (a0["id"], "leader_lost")


async def test_auto_handover_candidates(pool):
    c_ok, (o0, _o1, _o2) = await hive(pool)  # healthy, uncompacted, staying
    await set_leader(pool, c_ok["id"], o0["id"])
    c_comp, (p0, _p1, _p2) = await hive(pool)  # compacted: an advisory only
    await set_leader(pool, c_comp["id"], p0["id"])
    await compacted(pool, p0["id"])
    c_leave, (q0, _q1, q2) = await hive(pool)
    await set_leader(pool, c_leave["id"], q2["id"])
    await shrink(pool, c_leave["id"], 2)
    async with pool.connection() as conn:
        got = set(await leadership.leadership_candidates(conn))
    assert c_leave["id"] in got and c_comp["id"] not in got and c_ok["id"] not in got

    # an alone session is never a candidate
    c_alone = await insert_session(pool, spec={"replicas": 1, "max_replicas": 1})
    x0 = await insert_agent(pool, c_alone["id"], ordinal=0, ready=True, wrapper_session_id="s0")
    await set_agent_status(pool, x0["id"], state="idle")
    await compacted(pool, x0["id"])
    async with pool.connection() as conn:
        assert c_alone["id"] not in set(await leadership.leadership_candidates(conn))


async def test_steps_reread_leader_after_change(pool):
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await run(
        pool, "UPDATE session_leaders SET agent_id = %s WHERE session_id = %s", (a2["id"], c["id"])
    )
    await shrink(pool, c["id"], 2)
    change = await lpass(pool, c["id"])
    assert change is not None
    assert (change.epoch, change.reason) == (4, "scale_down")
    row = await leader_row(pool, c["id"])
    assert (row["epoch"], row["reason"]) == (4, "scale_down")
    assert row["agent_id"] in (a0["id"], a1["id"])


# ---------------------------------------------------------------- the true boundary


async def age_request(pool, request_id: int, seconds: int) -> None:
    await run(
        pool,
        "UPDATE leadership_requests SET created_at = now() - make_interval(secs => %s)"
        " WHERE id = %s",
        (seconds, request_id),
    )


async def test_agent_handover_waits_mid_turn(pool):
    c, (a0, a1, _) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await status(pool, a0["id"], "busy", 10)
    rid = await request(pool, c["id"], "handover", a0["id"], a1["id"], "context full", 3)
    assert await lpass(pool, c["id"]) is None
    assert (await rows(pool, "SELECT status FROM leadership_requests WHERE id = %s", (rid,)))[0][
        "status"
    ] == "queued"
    await status(pool, a0["id"], "idle", 10)
    change = await lpass(pool, c["id"])
    assert change is not None and change.agent_id == a1["id"]
    assert await decided(pool, rid) == ("applied", None)


async def test_agent_handover_waits_for_background(pool):
    c, (a0, a1, _) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await status(pool, a0["id"], "idle", 10, bg=True)
    rid = await request(pool, c["id"], "handover", a0["id"], a1["id"], "summary text", 3)
    assert await lpass(pool, c["id"]) is None
    assert (await rows(pool, "SELECT status FROM leadership_requests WHERE id = %s", (rid,)))[0][
        "status"
    ] == "queued"
    await status(pool, a0["id"], "idle", 10)
    change = await lpass(pool, c["id"])
    assert change is not None and change.agent_id == a1["id"]
    assert change.reason == "handover: summary text"


async def test_rollout_handover_ignores_background_when_overdue(pool):
    c, (a0, a1, _) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await status(pool, a0["id"], "idle", 10, bg=True)
    fresh = await request(pool, c["id"], "handover", a0["id"], a1["id"], "rollout", 3)
    assert await lpass(pool, c["id"]) is None
    await age_request(pool, fresh, 14400 + 60)
    # overdue only lifts the background wait, never the turn boundary
    await status(pool, a0["id"], "busy", 10, bg=True)
    assert await lpass(pool, c["id"]) is None
    await status(pool, a0["id"], "idle", 10, bg=True)
    change = await lpass(pool, c["id"])
    assert change is not None and change.agent_id == a1["id"]
    assert await decided(pool, fresh) == ("applied", None)


async def test_voluntary_handover_ignores_background_past_its_deadline(pool):
    # every handover has a deadline: a voluntary one stops waiting for background work once it
    # is queued longer than ORCH_HANDOVER_MAX_WAIT_SECONDS (the pass policy's max wait)
    c, (a0, a1, _) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await status(pool, a0["id"], "idle", 10, bg=True)
    rid = await request(pool, c["id"], "handover", a0["id"], a1["id"], "context", 3)
    assert leadership.DEFAULT_POLICY.handover_max_wait_seconds == 1800
    await age_request(pool, rid, 1800 - 60)
    assert await lpass(pool, c["id"]) is None
    assert (await rows(pool, "SELECT status FROM leadership_requests WHERE id = %s", (rid,)))[0][
        "status"
    ] == "queued"
    await age_request(pool, rid, 1800 + 60)
    change = await lpass(pool, c["id"])
    assert change is not None and change.agent_id == a1["id"]
    assert await decided(pool, rid) == ("applied", None)


async def test_voluntary_handover_expires_mid_turn_past_its_deadline(pool):
    c, (a0, a1, _) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await status(pool, a0["id"], "busy", 10)
    rid = await request(pool, c["id"], "handover", a0["id"], a1["id"], "context", 3)
    await age_request(pool, rid, 1800 - 60)
    assert await lpass(pool, c["id"]) is None  # inside its deadline it waits for the turn
    await age_request(pool, rid, 1800 + 60)
    assert await lpass(pool, c["id"]) is None  # never moved mid-turn
    assert await decided(pool, rid) == ("rejected", "expired")
    row = await leader_row(pool, c["id"])
    assert (row["agent_id"], row["epoch"]) == (a0["id"], 3)


async def test_handover_deadline_is_a_policy_setting(pool):
    c, (a0, a1, _) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await status(pool, a0["id"], "busy", 10)
    rid = await request(pool, c["id"], "handover", a0["id"], a1["id"], "context", 3)
    await age_request(pool, rid, 120)
    short = leadership.PassPolicy(handover_max_wait_seconds=60)
    async with pool.connection() as conn:
        assert await leadership.leadership_pass(conn, c["id"], short) is None
    assert await decided(pool, rid) == ("rejected", "expired")


async def test_rollout_handover_never_expires_mid_turn(pool):
    # the reconciler owns the rollout request: past its cap it waits for the turn only
    c, (a0, a1, _) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await status(pool, a0["id"], "busy", 10, bg=True)
    rid = await request(pool, c["id"], "handover", a0["id"], None, "rollout", 3)
    await age_request(pool, rid, 14400 + 60)
    assert await lpass(pool, c["id"]) is None
    got = await rows(pool, "SELECT status FROM leadership_requests WHERE id = %s", (rid,))
    assert got[0]["status"] == "queued"


async def test_user_make_leader_never_waits(pool):
    c, (a0, a1, _) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    await status(pool, a0["id"], "busy", 10, bg=True)
    rid = await request(pool, c["id"], "user", None, a1["id"], "user", None)
    change = await lpass(pool, c["id"])
    assert change is not None and change.agent_id == a1["id"]
    assert await decided(pool, rid) == ("applied", None)


# ---------------------------------------------------------------- Q13: ex-leader reset


async def commands_of(pool, agent_id) -> list[tuple[str, str]]:
    got = await rows(
        pool, "SELECT kind, status FROM commands WHERE agent_id = %s ORDER BY id", (agent_id,)
    )
    return [(r["kind"], r["status"]) for r in got]


async def test_voluntary_move_resets_ex_leader(pool):
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    # a named target
    await request(pool, c["id"], "handover", a0["id"], a1["id"], "summary", 3)
    change = await lpass(pool, c["id"])
    assert change is not None and change.agent_id == a1["id"]
    assert await commands_of(pool, a0["id"]) == [("clear", "queued")]
    assert await commands_of(pool, a1["id"]) == []
    # the pass picks the successor
    await request(pool, c["id"], "handover", a1["id"], None, "summary", 4)
    change = await lpass(pool, c["id"])
    assert change is not None and change.previous_agent_id == a1["id"]
    assert await commands_of(pool, a1["id"]) == [("clear", "queued")]
    # one clear per move, each for the agent that stopped leading
    assert len(await rows(pool, "SELECT id FROM commands")) == 2


async def test_forced_and_user_moves_do_not_reset(pool):
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a0["id"], epoch=3)
    # a rollout handover is forced by the reconciler, not chosen by the leader
    await request(pool, c["id"], "handover", a0["id"], a1["id"], "rollout", 3)
    assert (await lpass(pool, c["id"])).agent_id == a1["id"]
    # a user make-leader
    await request(pool, c["id"], "user", None, a2["id"], "user", None)
    assert (await lpass(pool, c["id"])).agent_id == a2["id"]
    # a lost leader
    await run(pool, "UPDATE agents SET gone_at = now() WHERE id = %s", (a2["id"],))
    assert (await lpass(pool, c["id"])).reason == "leader_lost"
    # a rejected handover moves nobody
    leader = await leader_row(pool, c["id"])
    rid = await request(pool, c["id"], "handover", leader["agent_id"], a2["id"], "summary", 0)
    assert await lpass(pool, c["id"]) is None
    assert await decided(pool, rid) == ("rejected", "stale_epoch")
    assert await rows(pool, "SELECT * FROM commands") == []


async def test_scale_down_does_not_reset(pool):
    c, (a0, a1, a2) = await hive(pool)
    await set_leader(pool, c["id"], a2["id"], epoch=3)
    await shrink(pool, c["id"], 2)
    assert (await lpass(pool, c["id"])).reason == "scale_down"
    assert await rows(pool, "SELECT * FROM commands") == []

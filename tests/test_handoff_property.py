"""Property test of the cooperative handoff (decision 716): over seeded random event orders, no
`clear` command and no `limit` leadership request for an agent ever exists without a gate-clear
accepted submit of the same cycle, and an ex-leader only after a move made by that submit's own
`limit` request (a rollout or user move never leads to a clear); no row stays open past a
compaction before `moving`; at most one open row per agent; no handoff notice or request
message is ever queued.

The pass runs after every event against Postgres; the wrapper's in-flight inventory is a stub
the events drive, and submits are written the way the submit route writes them.
"""

from __future__ import annotations

import random
import uuid

from factories import insert_agent, insert_event, insert_session, set_agent_status
from psycopg.rows import dict_row

from agents_orchestrator import handoff_gate
from agents_orchestrator.db import handoffs, leadership
from agents_orchestrator.dispatcher import handoff
from agents_orchestrator.settings import Settings
from agents_orchestrator.wrapper.models import Inflight

KEY = "k" * 32
RUNS = 500
EVENTS_PER_RUN = 10
WINDOW = 200000
GAUGES = {"low": 20000, "l2": 140000, "l3": 170000}
HANDOFF = {
    "doing": "a long enough account of the work that is being handed over here",
    "done": ["commit abc"],
    "next": ["open the PR"],
}
EVENTS = (
    "level_up",
    "level_down",
    "submit_invalid",
    "submit_blocked",
    "submit_clear",
    "bg_start",
    "bg_stop",
    "compaction",
    "session_cleared",
    "idle",
    "busy",
    "leader_moved",
    "rollout_moved",
    "request_rejected",
    "time_passes",
    "other_busy",
    "clear_runs",
    "clear_refused",
)


class Run:
    def __init__(self, pool, settings: Settings, rng: random.Random) -> None:
        self.pool, self.settings, self.rng = pool, settings, rng
        self.policy = handoff.HandoffPolicy.from_settings(settings)
        self.bg: dict[uuid.UUID, bool] = {}
        # per row id: whether its latest accepted submit had the in-flight gate clear
        self.clear_submit: dict[int, bool] = {}
        self.seen_commands: set[int] = set()
        self.seen_requests: set[int] = set()
        self.trace: list[str] = []

    async def q(self, sql: str, params: tuple = ()) -> list[dict]:
        async with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            return await cur.fetchall() if cur.description else []

    async def setup(self) -> None:
        self.hive = self.rng.random() < 0.5
        spec = {"replicas": 2, "max_replicas": 2} if self.hive else {}
        self.session = await insert_session(self.pool, spec=spec)
        self.agents = []
        for i in range(2 if self.hive else 1):
            a = await insert_agent(
                self.pool,
                self.session["id"],
                ordinal=i,
                ready=True,
                wrapper_session_id=f"w{i}",
                url="http://wrapper.stub",  # the stub `fetch` answers, nothing listens here
            )
            await self.gauge(a["id"], "low")
            self.bg[a["id"]] = False
            self.agents.append(a)
        if self.hive:
            async with self.pool.connection() as conn:
                await leadership.leadership_pass(conn, self.session["id"])
        await self.gauge(self.agents[0]["id"], "l2")

    async def gauge(self, agent_id, level: str) -> None:
        used = GAUGES[level]
        await set_agent_status(
            self.pool,
            agent_id,
            state="idle",
            context_used_tokens=used,
            context_window_tokens=WINDOW,
            context_pct=100 * used / WINDOW,
        )

    async def subject(self) -> uuid.UUID:
        if not self.hive:
            return self.agents[0]["id"]
        async with self.pool.connection() as conn:
            leader = await leadership.current_leader(conn, self.session["id"])
        return leader.agent_id if leader is not None else self.agents[0]["id"]

    def inflight(self, agent_id) -> Inflight:
        tasks = [{"id": "bf3a", "description": "CI watch"}] if self.bg.get(agent_id) else []
        return Inflight(background_tasks=tasks)

    async def fetch(self, session_id, url, wsid) -> Inflight | None:
        (got,) = await self.q(
            "SELECT id FROM agents WHERE wrapper_session_id = %s AND session_id = %s",
            (wsid, session_id),
        )
        return self.inflight(got["id"])

    async def submit(self, agent_id, *, valid: bool) -> None:
        """The submit route: reuse a pressuring/submitted row, open a voluntary one only for a
        text that passes, count a reject, store the handoff and the gate's verdict."""
        async with self.pool.connection() as conn:
            row = await handoffs.open_row(conn, agent_id)
            if row is not None and row.phase not in ("pressuring", "submitted"):
                return  # 409 handoff_open
            if not valid:
                if row is not None:
                    await handoffs.record_reject(conn, row.id, ["too_short"])
                return
            if row is None:
                row = await handoffs.open_pressure(
                    conn, self.session["id"], agent_id, 0, "work", "voluntary"
                )
            inflight = self.inflight(agent_id)
            gate = handoff_gate.evaluate(inflight, [])
            snap = {
                **inflight.model_dump(mode="json"),
                "waiting_on": gate.waiting_on,
                "abandoned": gate.abandoned,
            }
            if await handoffs.submit(conn, row.id, {**HANDOFF, "abandon": []}, snap, None):
                self.clear_submit[row.id] = gate.clear
                if not gate.clear:
                    await handoffs.record_gate_block(conn, row.id, snap)
                self.trace.append(f"submit row {row.id} clear={gate.clear}")

    async def event(self, name: str) -> None:
        self.trace.append(name)
        agent_id = await self.subject()
        sid = self.session["id"]
        if name == "level_up":
            await self.gauge(agent_id, self.rng.choice(("l2", "l3")))
        elif name == "level_down":
            await self.gauge(agent_id, "low")
        elif name == "submit_invalid":
            await self.submit(agent_id, valid=False)
        elif name == "submit_blocked":
            self.bg[agent_id] = True
            await self.submit(agent_id, valid=True)
        elif name == "submit_clear":
            await self.submit(agent_id, valid=True)  # clear only while nothing runs
        elif name == "bg_start":
            self.bg[agent_id] = True
        elif name == "bg_stop":
            self.bg[agent_id] = False
        elif name in ("compaction", "session_cleared"):
            kind = "compacted" if name == "compaction" else "session_cleared"
            await insert_event(self.pool, sid, agent_id, type=kind, data={})
            await self.gauge(agent_id, "low")
        elif name == "idle":
            await self.q(
                "UPDATE agent_status SET state = 'idle', current_turn_id = NULL"
                " WHERE agent_id = %s",
                (agent_id,),
            )
        elif name == "busy":
            await self.q(
                "UPDATE agent_status SET state = 'busy', current_turn_id = 't' WHERE agent_id = %s",
                (agent_id,),
            )
        elif name == "leader_moved" and self.hive:
            queued = await self.q(
                "SELECT id FROM leadership_requests WHERE session_id = %s AND status = 'queued'",
                (sid,),
            )
            if not queued:
                other = next(a["id"] for a in self.agents if a["id"] != agent_id)
                async with self.pool.connection() as conn:
                    await leadership.insert_request(conn, sid, "user", None, other, "test", None)
            async with self.pool.connection() as conn:
                await leadership.leadership_pass(conn, sid)
        elif name == "rollout_moved" and self.hive:
            # the reconciler's forced rollout move: never the row's own accepted submit
            other = next(a["id"] for a in self.agents if a["id"] != agent_id)
            async with self.pool.connection() as conn:
                leader = await leadership.current_leader(conn, sid)
                if leader is not None:
                    await leadership.insert_request(
                        conn, sid, "handover", leader.agent_id, other, "rollout", leader.epoch
                    )
            async with self.pool.connection() as conn:
                await leadership.leadership_pass(conn, sid)
        elif name == "other_busy" and self.hive:
            # the agent that does not lead (an ex-leader in discussion) keeps working
            other = next(a["id"] for a in self.agents if a["id"] != agent_id)
            await self.q(
                "UPDATE agent_status SET state = 'busy', current_turn_id = 't' WHERE agent_id = %s",
                (other,),
            )
        elif name == "time_passes":
            # past the discussion's quiet window and cap: a discussing ex-leader may be cleared
            await self.q(
                "UPDATE agent_handoffs SET discuss_since = discuss_since - interval '700 seconds'"
                " WHERE session_id = %s AND discuss_since IS NOT NULL",
                (sid,),
            )
            await self.q(
                "UPDATE sessions SET created_at = now() - interval '1 day',"
                " last_update_at = now() - interval '1 day' WHERE id = %s",
                (sid,),
            )
            await self.q(
                "UPDATE agent_status SET free_since = now() - interval '700 seconds'"
                " WHERE agent_id IN (SELECT id FROM agents WHERE session_id = %s)",
                (sid,),
            )
        elif name == "clear_runs":
            # the reconciler claims a queued clear of this session
            await self.q(
                "UPDATE commands SET status = 'running', started_at = now() WHERE id = ("
                " SELECT k.id FROM commands k JOIN agents a ON a.id = k.agent_id"
                " WHERE a.session_id = %s AND k.kind = 'clear' AND k.status = 'queued'"
                " ORDER BY k.id LIMIT 1)",
                (sid,),
            )
        elif name == "clear_refused":
            # the wrapper refuses a running clear while a subagent runs: the pass may retry
            await self.q(
                "UPDATE commands SET status = 'failed', finished_at = now(),"
                ' result = \'{"error": "409 subagents_running"}\'::jsonb'
                " WHERE status = 'running' AND kind = 'clear' AND agent_id IN"
                " (SELECT id FROM agents WHERE session_id = %s)",
                (sid,),
            )
        elif name == "request_rejected":
            await self.q(
                "UPDATE leadership_requests SET status = 'rejected', reject_reason = 'test',"
                " decided_at = now() WHERE session_id = %s AND status = 'queued'",
                (sid,),
            )

    async def step(self) -> None:
        async with self.pool.connection() as conn:
            out = await handoff.handoff_pass(
                conn, self.session["id"], self.policy, self.settings, self.fetch
            )
        self.trace.append(f"pass {out}")
        await self.check()

    def fail(self, what: str) -> str:
        return what + "\n" + "\n".join(self.trace)

    async def check(self) -> None:
        sid = self.session["id"]
        for cmd in await self.q(
            "SELECT k.id, k.agent_id FROM commands k JOIN agents a ON a.id = k.agent_id"
            " WHERE a.session_id = %s AND k.kind = 'clear'",
            (sid,),
        ):
            if cmd["id"] in self.seen_commands:
                continue
            self.seen_commands.add(cmd["id"])
            (row,) = await self.q(
                "SELECT h.id, h.discuss_since, r.status AS moved FROM agent_handoffs h"
                " LEFT JOIN leadership_requests r ON r.id = h.leadership_request_id"
                " WHERE h.clear_command_id = %s",
                (cmd["id"],),
            )
            assert self.clear_submit.get(row["id"]) is True, self.fail(f"clear {cmd}")
            if row["discuss_since"] is not None:
                # an ex-leader is cleared only after a move by its own limit request, and
                # never while it leads again (the 2-agent ping-pong)
                assert row["moved"] == "applied", self.fail(f"clear after a foreign move {cmd}")
                assert cmd["agent_id"] != await self.subject(), self.fail(f"leader cleared {cmd}")
            assert not self.bg[cmd["agent_id"]], self.fail(f"clear with work in flight {cmd}")
        for req in await self.q(
            "SELECT id, from_agent_id FROM leadership_requests"
            " WHERE session_id = %s AND reason = 'limit'",
            (sid,),
        ):
            if req["id"] in self.seen_requests:
                continue
            self.seen_requests.add(req["id"])
            (row,) = await self.q(
                "SELECT id FROM agent_handoffs WHERE leadership_request_id = %s", (req["id"],)
            )
            assert self.clear_submit.get(row["id"]) is True, self.fail(f"limit {req}")
            assert not self.bg[req["from_agent_id"]], self.fail(f"limit with work {req}")
        stale = await self.q(
            "SELECT h.id FROM agent_handoffs h WHERE h.session_id = %s"
            " AND h.phase IN ('pressuring', 'submitted') AND EXISTS (SELECT 1 FROM events e"
            " WHERE e.agent_id = h.agent_id AND e.type = 'compacted'"
            " AND e.created_at >= h.created_at)",
            (sid,),
        )
        assert stale == [], self.fail(f"open past a compaction {stale}")
        many = await self.q(
            "SELECT agent_id FROM agent_handoffs WHERE session_id = %s"
            " AND phase = ANY(%s) GROUP BY agent_id HAVING count(*) > 1",
            (sid, list(handoffs.OPEN_PHASES)),
        )
        assert many == [], self.fail("two open rows")
        if self.hive:
            leading = await self.q(
                "SELECT h.id FROM agent_handoffs h JOIN session_leaders l"
                " ON l.session_id = h.session_id AND l.agent_id = h.agent_id"
                " WHERE h.session_id = %s AND h.phase = 'discussing'",
                (sid,),
            )
            assert leading == [], self.fail(f"the leader discusses its own move {leading}")
            # an ex-leader leading again never keeps a queued clear (it would wipe the
            # successor's handoff it now holds); a running one is left to finish
            queued = await self.q(
                "SELECT h.id FROM agent_handoffs h JOIN session_leaders l"
                " ON l.session_id = h.session_id AND l.agent_id = h.agent_id"
                " JOIN commands k ON k.id = h.clear_command_id"
                " WHERE h.session_id = %s AND h.phase = 'clearing'"
                " AND h.discuss_since IS NOT NULL AND k.status = 'queued'",
                (sid,),
            )
            assert queued == [], self.fail(f"a queued clear of the leader {queued}")
        removed = await self.q(
            "SELECT id FROM messages WHERE session_id = %s"
            " AND notice IN ('handoff_notice', 'handoff_request')",
            (sid,),
        )
        assert removed == [], self.fail("a removed notice kind was queued")


async def test_no_clear_without_accepted_submit(pool, dsn):
    settings = Settings(database_url=dsn, master_key=KEY)
    rng = random.Random(716)
    proceeded = ping_pong = clearing_again = 0
    for _ in range(RUNS):
        run = Run(pool, settings, rng)
        await run.setup()
        await run.step()
        for _ in range(EVENTS_PER_RUN):
            await run.event(rng.choice(EVENTS))
            await run.step()
        proceeded += len(run.seen_commands) + len(run.seen_requests)
        ping_pong += len(
            await run.q(
                "SELECT id FROM agent_handoffs WHERE session_id = %s"
                " AND close_reason = 'leader_again'",
                (run.session["id"],),
            )
        )
        clearing_again += len(
            await run.q(
                "SELECT id FROM agent_handoffs WHERE session_id = %s"
                " AND close_reason = 'leader_again' AND clear_command_id IS NOT NULL",
                (run.session["id"],),
            )
        )
    assert proceeded > 0  # the property is not vacuous: some cycles did clear or move
    assert ping_pong > 0  # some ex-leaders led again (the 2-agent ping-pong)
    assert clearing_again > 0  # some led again with their clear queued or refused

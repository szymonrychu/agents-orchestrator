"""POST /agent/v1/handoff: quality check, in-flight gate and the handoff row (T-423 step 10)."""

from __future__ import annotations

import asyncio

import pytest
from test_api_agent import IP0, IP1, client_for, headers, make_settings
from test_api_leadership import SOLO_IP, hive, solo_session

from agents_orchestrator.api import workspace
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import handoffs
from agents_orchestrator.wrapper.models import Inflight

TEXT = "Implementing the submit route for the cooperative handoff and its quality check. " * 5
GOOD = {
    "doing": TEXT,
    "done": ["wrote the storage layer and its tests"],
    "next": ["wire the dispatcher pass"],
}


@pytest.fixture
def inflight(monkeypatch):
    """Answer for the wrapper's in-flight inventory (None: unreachable)."""
    box = {"value": Inflight()}

    async def fake(app, settings, agent):
        return box["value"]

    monkeypatch.setattr(workspace, "fetch_inflight", fake)
    return box


async def submit(dsn, pool, c, ip=SOLO_IP, **body):
    body = {**GOOD, **body}
    async with client_for(create_app(make_settings(dsn), pool), ip) as http:
        return await http.post("/agent/v1/handoff", json=body, headers=headers(c["id"]))


async def rows(pool, agent_id):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT trigger, phase, rejects, gate_blocks, profile, handoff IS NOT NULL"
            " FROM agent_handoffs WHERE agent_id = %s ORDER BY id",
            (agent_id,),
        )
        return await cur.fetchall()


async def open_row(pool, c, a, phase="pressuring", level=1):
    async with pool.connection() as conn:
        row = await handoffs.open_pressure(conn, c["id"], a["id"], level, "work")
        if phase != "pressuring":
            await handoffs.advance(conn, row.id, ["pressuring"], phase)
        return row


async def test_submit_rejected_422_counts_reject(pool, dsn, inflight):
    c, a0 = await solo_session(pool)
    await open_row(pool, c, a0)
    r = await submit(dsn, pool, c, doing="\U0001f44d", done=[], next=[])
    assert r.status_code == 422, r.text
    err = r.json()["error"]
    assert err["code"] == "handoff_rejected" and err["rejects"] == 1
    assert {"missing:done", "missing:next", "too_short", "not_text"} <= set(err["missing"])
    assert "too short" in err["message"] and "done" in err["message"]
    r = await submit(dsn, pool, c, doing="x", done=[], next=[])
    assert r.json()["error"]["rejects"] == 2
    assert await rows(pool, a0["id"]) == [("pressure", "pressuring", 2, 0, "work", False)]


async def test_submit_blocked_409_in_flight_row_submitted(pool, dsn, inflight):
    c, a0 = await solo_session(pool)
    await open_row(pool, c, a0)
    inflight["value"] = Inflight(
        background_tasks=[{"id": "bf3a", "description": "CI watch"}],
        worktrees=[{"path": "/workdir/code/web", "dirty": 2}],
    )
    r = await submit(dsn, pool, c, in_flight=["bf3a CI watch on the PR"])
    assert r.status_code == 409, r.text
    err = r.json()["error"]
    assert err["code"] == "in_flight"
    assert err["waiting_on"] == ["task:bf3a (CI watch)", "worktree:/workdir/code/web dirty 2"]
    assert await rows(pool, a0["id"]) == [("pressure", "submitted", 0, 1, "work", True)]


async def test_submit_409_in_flight_lists_scratch(pool, dsn, inflight):
    c, a0 = await solo_session(pool)
    await open_row(pool, c, a0)
    inflight["value"] = Inflight(scratch=[{"path": "/tmp/claude-1/notes.md"}])
    r = await submit(dsn, pool, c, results=["notes in /tmp/claude-1/notes.md"])
    err = r.json()["error"]
    assert (r.status_code, err["code"]) == (409, "in_flight")
    assert err["waiting_on"] == ["scratch:/tmp/claude-1/notes.md (pod-local, not persisted)"]
    assert ".scratchpad/<name>/" in err["message"] and "abandon" in err["message"]


async def test_submit_abandon_clears_the_gate(pool, dsn, inflight):
    c, a0 = await solo_session(pool)
    await open_row(pool, c, a0)
    inflight["value"] = Inflight(scratch=[{"path": "/tmp/claude-1/notes.md"}])
    r = await submit(
        dsn, pool, c, abandon=[{"item": "/tmp/claude-1", "why": "throwaway notes, not needed"}]
    )
    assert r.status_code == 202, r.text
    async with pool.connection() as conn:
        row = await handoffs.open_row(conn, a0["id"])
    assert row.handoff["abandoned"][0]["why"] == "throwaway notes, not needed"


async def test_submit_accepted_202(pool, dsn, inflight):
    c, a0 = await solo_session(pool)
    row = await open_row(pool, c, a0)
    r = await submit(dsn, pool, c, handled=[])
    assert r.status_code == 202, r.text
    body = r.json()
    assert (body["handoff_id"], body["phase"]) == (row.id, "submitted")
    assert "clears" in body["applies"] or "cleared" in body["applies"]
    assert await rows(pool, a0["id"]) == [("pressure", "submitted", 0, 0, "work", True)]
    async with pool.connection() as conn:
        stored = (await handoffs.open_row(conn, a0["id"])).handoff
    assert stored["doing"] == TEXT and stored["next"] == ["wire the dispatcher pass"]
    assert "epoch" not in stored and "handled" not in stored


async def test_wrapper_unreachable_blocks(pool, dsn, inflight):
    c, a0 = await solo_session(pool)
    await open_row(pool, c, a0)
    inflight["value"] = None
    r = await submit(dsn, pool, c)
    assert (r.status_code, r.json()["error"]["code"]) == (409, "in_flight")
    assert r.json()["error"]["waiting_on"] == ["inflight:unknown (wrapper unreachable)"]


async def test_member_refused(pool, dsn, inflight):
    c, a0, a1 = await hive(pool)
    r = await submit(dsn, pool, c, ip=IP1, epoch=3)
    assert (r.status_code, r.json()["error"]["code"]) == (409, "not_leader")
    assert await rows(pool, a1["id"]) == []


async def test_leader_needs_epoch(pool, dsn, inflight):
    c, a0, _ = await hive(pool)
    r = await submit(dsn, pool, c, ip=IP0)
    assert (r.status_code, r.json()["error"]["code"]) == (422, "epoch_required")
    r = await submit(dsn, pool, c, ip=IP0, epoch=2)
    assert (r.status_code, r.json()["error"]["code"]) == (409, "stale_epoch")
    r = await submit(dsn, pool, c, ip=IP0, epoch=3, to="nope-9")
    assert (r.status_code, r.json()["error"]["code"]) == (404, "not_found")
    assert await rows(pool, a0["id"]) == []
    r = await submit(dsn, pool, c, ip=IP0, epoch=3, to="agent-1")
    assert r.status_code == 202, r.text
    async with pool.connection() as conn:
        assert (await handoffs.open_row(conn, a0["id"])).handoff["to"] == "agent-1"


async def test_voluntary_below_l1_opens_row(pool, dsn, inflight):
    c, a0 = await solo_session(pool)
    assert await rows(pool, a0["id"]) == []
    r = await submit(dsn, pool, c)
    assert r.status_code == 202, r.text
    assert await rows(pool, a0["id"]) == [("voluntary", "submitted", 0, 0, "work", True)]


async def test_resubmit_after_blocker_clears(pool, dsn, inflight):
    c, a0 = await solo_session(pool)
    await open_row(pool, c, a0)
    inflight["value"] = Inflight(subagents=1)
    r = await submit(dsn, pool, c, in_flight=["one subagent reviewing"])
    assert r.status_code == 409
    inflight["value"] = Inflight()
    r = await submit(dsn, pool, c, results=["the review found nothing"])
    assert r.status_code == 202, r.text
    async with pool.connection() as conn:
        row = await handoffs.open_row(conn, a0["id"])
    assert (row.phase, row.gate_blocks) == ("submitted", 1)
    assert row.handoff["results"] == ["the review found nothing"]


@pytest.mark.parametrize("phase", ["moving", "clearing", "discussing", "returning", "handed"])
async def test_submit_while_moving_409(pool, dsn, inflight, phase):
    c, a0 = await solo_session(pool)
    await open_row(pool, c, a0, phase=phase)
    r = await submit(dsn, pool, c)
    assert (r.status_code, r.json()["error"]["code"]) == (409, "handoff_open"), phase


async def test_concurrent_submits_one_row(pool, dsn, inflight):
    c, a0 = await solo_session(pool)
    async with client_for(create_app(make_settings(dsn), pool), SOLO_IP) as http:
        answers = await asyncio.gather(
            *[http.post("/agent/v1/handoff", json=GOOD, headers=headers(c["id"])) for _ in range(2)]
        )
    assert [r.status_code for r in answers] == [202, 202], [r.text for r in answers]
    assert answers[0].json()["handoff_id"] == answers[1].json()["handoff_id"]
    assert len(await rows(pool, a0["id"])) == 1


async def test_submit_marks_messages_handled(pool, dsn, inflight):
    c, a0 = await solo_session(pool)
    r = await submit(dsn, pool, c, handled=[999999])
    assert r.status_code == 202 and r.json()["handled"] == []


async def test_extra_field_422(pool, dsn, inflight):
    c, _ = await solo_session(pool)
    r = await submit(dsn, pool, c, bogus=1)
    assert r.status_code == 422
    r = await submit(dsn, pool, c, abandon=[{"item": "x", "why": "short"}])
    assert r.status_code == 422
    r = await submit(dsn, pool, c, next=["x"] * 51)
    assert r.status_code == 422


async def test_inflight_none_without_wrapper_session_and_on_errors(pool, dsn, monkeypatch):

    from fastapi import FastAPI

    from agents_orchestrator.db import agents as agents_repo
    from agents_orchestrator.wrapper import WrapperClient, WrapperUnreachable

    c, a0 = await solo_session(pool)
    async with pool.connection() as conn:
        agent = await agents_repo.get_agent(conn, a0["id"])
    settings = make_settings(dsn)
    app = FastAPI()
    assert (
        await workspace.fetch_inflight(app, settings, agent) is None
    )  # no url, no wrapper session
    from dataclasses import replace

    agent = replace(agent, url="http://10.0.0.9:8000", wrapper_session_id="w1")

    async def boom(self, session_id, timeout=15.0):
        raise WrapperUnreachable("down")

    monkeypatch.setattr(WrapperClient, "get_inflight", boom)
    assert await workspace.fetch_inflight(app, settings, agent) is None

    async def ok(self, session_id, timeout=15.0):
        return Inflight(subagents=1)

    monkeypatch.setattr(WrapperClient, "get_inflight", ok)
    assert (await workspace.fetch_inflight(app, settings, agent)).subagents == 1


async def test_rejected_voluntary_submit_leaves_no_open_row(pool, dsn, inflight):
    c, a0 = await solo_session(pool)
    r = await submit(dsn, pool, c, doing="x", done=[], next=[])
    assert r.status_code == 422, r.text
    assert r.json()["error"]["code"] == "handoff_rejected"
    assert await rows(pool, a0["id"]) == []
    async with pool.connection() as conn:
        assert await handoffs.open_row(conn, a0["id"]) is None
    r = await submit(dsn, pool, c)
    assert r.status_code == 202, r.text
    assert await rows(pool, a0["id"]) == [("voluntary", "submitted", 0, 0, "work", True)]


async def test_existing_row_without_profile_runs_the_detector(pool, dsn, inflight, monkeypatch):
    from agents_orchestrator import handoff_profile

    calls = []

    async def fake(conn, session, agent_id, since):
        calls.append(agent_id)
        return "chat"

    monkeypatch.setattr(handoff_profile, "detect", fake)
    c, a0 = await solo_session(pool)
    row = await open_row(pool, c, a0)
    async with pool.connection() as conn:
        await conn.execute("UPDATE agent_handoffs SET profile = NULL WHERE id = %s", (row.id,))
    r = await submit(dsn, pool, c)  # the detected chat profile asks for chat fields
    assert r.status_code == 422, r.text
    assert {"missing:open_requests", "missing:results"} <= set(r.json()["error"]["missing"])
    assert calls == [a0["id"]]

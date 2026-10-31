"""Message, command and check routes (D4, D11, D24)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from factories import (
    insert_agent,
    insert_check,
    insert_command,
    insert_message,
    insert_session,
    insert_turn,
)
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app


@pytest_asyncio.fixture
async def http(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as c:
        yield c


async def _count(pool, table: str) -> int:
    async with pool.connection() as conn:
        cur = await conn.execute(f"SELECT count(*) FROM {table}")  # noqa: S608
        return (await cur.fetchone())[0]


async def test_post_message_queues_202(http, pool):
    cl = await insert_session(pool)
    r = await http.post(f"/api/v1/sessions/{cl['id']}/messages", json={"text": "hi"})
    assert r.status_code == 202, r.text
    body = r.json()
    assert body["status"] == "queued"
    got = (await http.get(f"/api/v1/messages/{body['message_id']}")).json()
    assert got["status"] == "queued"
    assert got["text"] == "hi"
    assert got["turn"] is None
    ag = await insert_agent(pool, cl["id"])
    r = await http.post(
        f"/api/v1/sessions/{cl['id']}/messages",
        json={"text": "t", "target_agent_id": str(ag["id"])},
    )
    assert r.status_code == 202
    got = (await http.get(f"/api/v1/messages/{r.json()['message_id']}")).json()
    assert got["target_agent_id"] == str(ag["id"])


async def test_post_message_validation(http, pool):
    cl = await insert_session(pool)
    other = await insert_session(pool)
    foreign = await insert_agent(pool, other["id"])
    url = f"/api/v1/sessions/{cl['id']}/messages"
    assert_error(await http.post(url, json={"text": ""}), 422, "invalid_request")
    assert_error(await http.post(url, json={}), 422, "invalid_request")
    r = await http.post(url, json={"text": "x", "target_agent_id": str(foreign["id"])})
    assert_error(r, 422, "invalid_request")
    r = await http.post(url, json={"text": "x", "target_agent_id": str(uuid.uuid4())})
    assert_error(r, 422, "invalid_request")
    assert_error(
        await http.post(f"/api/v1/sessions/{uuid.uuid4()}/messages", json={"text": "x"}),
        404,
        "not_found",
    )
    dying = await insert_session(pool, desired="deleting")
    r = await http.post(f"/api/v1/sessions/{dying['id']}/messages", json={"text": "x"})
    assert_error(r, 409, "session_deleting")
    assert await _count(pool, "messages") == 0


async def test_post_message_to_gone_target_rejected(http, pool):
    cl = await insert_session(pool)
    ag = await insert_agent(pool, cl["id"])
    async with pool.connection() as conn:
        await conn.execute("UPDATE agents SET gone_at = now() WHERE id = %s", (ag["id"],))
    r = await http.post(
        f"/api/v1/sessions/{cl['id']}/messages",
        json={"text": "x", "target_agent_id": str(ag["id"])},
    )
    assert_error(r, 422, "target_gone")
    assert await _count(pool, "messages") == 0


async def test_get_message_with_turn(http, pool):
    cl = await insert_session(pool)
    ag = await insert_agent(pool, cl["id"])
    done = datetime.now(UTC)
    t = await insert_turn(
        pool,
        ag["id"],
        status="completed",
        final_text="answer",
        completed_at=done,
        usage={"input_tokens": 7, "output_tokens": 3},
    )
    m = await insert_message(pool, cl["id"], status="sent", agent_id=ag["id"], turn_id=t["turn_id"])
    got = (await http.get(f"/api/v1/messages/{m['id']}")).json()
    assert got["status"] == "sent"
    assert got["agent_id"] == str(ag["id"])
    assert got["turn"]["status"] == "completed"
    assert got["turn"]["final_text"] == "answer"
    assert got["turn"]["usage"] == {"input_tokens": 7, "output_tokens": 3}
    assert got["turn"]["started_at"] is not None
    assert got["turn"]["completed_at"] is not None
    assert_error(await http.get("/api/v1/messages/999999"), 404, "not_found")


async def test_cancel_only_queued(http, pool):
    cl = await insert_session(pool)
    q = await insert_message(pool, cl["id"])
    r = await http.delete(f"/api/v1/messages/{q['id']}")
    assert r.status_code == 204
    assert (await http.get(f"/api/v1/messages/{q['id']}")).json()["status"] == "cancelled"
    assert_error(await http.delete(f"/api/v1/messages/{q['id']}"), 409, "not_queued")
    for st in ("dispatching", "verifying", "sent", "failed"):
        m = await insert_message(pool, cl["id"], status=st)
        assert_error(await http.delete(f"/api/v1/messages/{m['id']}"), 409, "not_queued")
    assert_error(await http.delete("/api/v1/messages/999999"), 404, "not_found")


async def test_retry_only_failed(http, pool):
    cl = await insert_session(pool)
    f = await insert_message(pool, cl["id"], status="failed", attempts=5, error="unavailable")
    r = await http.post(f"/api/v1/messages/{f['id']}/retry")
    assert r.status_code == 202, r.text
    got = (await http.get(f"/api/v1/messages/{f['id']}")).json()
    assert got["status"] == "queued"
    assert got["attempts"] == 0
    assert got["error"] is None
    assert_error(await http.post(f"/api/v1/messages/{f['id']}/retry"), 409, "not_failed")
    for st in ("sent", "cancelled", "verifying"):
        m = await insert_message(pool, cl["id"], status=st)
        assert_error(await http.post(f"/api/v1/messages/{m['id']}/retry"), 409, "not_failed")
    assert_error(await http.post("/api/v1/messages/999999/retry"), 404, "not_found")


async def test_retry_allowed_for_ambiguous_failure(http, pool):
    cl = await insert_session(pool)
    m = await insert_message(
        pool,
        cl["id"],
        status="failed",
        error="ambiguous",
        attempts=1,
        ambiguous_at=datetime.now(UTC),
    )
    got = (await http.get(f"/api/v1/messages/{m['id']}")).json()
    assert got["status"] == "failed"
    assert got["error"] == "ambiguous"
    r = await http.post(f"/api/v1/messages/{m['id']}/retry")
    assert r.status_code == 202
    got = (await http.get(f"/api/v1/messages/{m['id']}")).json()
    assert got["status"] == "queued"
    assert got["error"] is None


async def test_message_view_retry_fields(http, pool):
    cl = await insert_session(pool, spec={"max_attempts": 7})
    when = datetime.now(UTC) + timedelta(minutes=5)
    m = await insert_message(pool, cl["id"], attempts=2, not_before=when, error="unavailable")
    got = (await http.get(f"/api/v1/messages/{m['id']}")).json()
    assert got["attempts"] == 2
    assert got["max_attempts"] == 7
    assert datetime.fromisoformat(got["next_attempt_at"]) == when
    assert got["error"] == "unavailable"
    v = await insert_message(pool, cl["id"], status="verifying")
    assert (await http.get(f"/api/v1/messages/{v['id']}")).json()["status"] == "verifying"
    plain = await insert_session(pool)
    p = await insert_message(pool, plain["id"])
    assert (await http.get(f"/api/v1/messages/{p['id']}")).json()["max_attempts"] == 5


async def test_list_messages_status_filter(http, pool):
    cl = await insert_session(pool)
    other = await insert_session(pool)
    a = await insert_message(pool, cl["id"], status="queued")
    b = await insert_message(pool, cl["id"], status="failed")
    c = await insert_message(pool, cl["id"], status="sent")
    await insert_message(pool, other["id"], status="queued")
    url = f"/api/v1/sessions/{cl['id']}/messages"
    ids = [m["id"] for m in (await http.get(url)).json()]
    assert ids == [c["id"], b["id"], a["id"]]
    ids = [m["id"] for m in (await http.get(url, params={"status": "queued,failed"})).json()]
    assert ids == [b["id"], a["id"]]
    ids = [m["id"] for m in (await http.get(url, params={"limit": 1})).json()]
    assert ids == [c["id"]]
    assert_error(await http.get(url, params={"status": "bogus"}), 422, "invalid_request")
    assert_error(await http.get(f"/api/v1/sessions/{uuid.uuid4()}/messages"), 404, "not_found")


@pytest.mark.parametrize("kind", ["interrupt", "compact", "restart"])
async def test_agent_commands_queue_202(http, pool, kind):
    cl = await insert_session(pool)
    ag = await insert_agent(pool, cl["id"])
    r = await http.post(f"/api/v1/agents/{ag['id']}/{kind}")
    assert r.status_code == 202, r.text
    cid = r.json()["command_id"]
    got = (await http.get(f"/api/v1/commands/{cid}")).json()
    assert got["kind"] == kind
    assert got["status"] == "queued"
    assert got["agent_id"] == str(ag["id"])
    done = await insert_command(pool, ag["id"], status="done", result={"ok": True})
    assert (await http.get(f"/api/v1/commands/{done['id']}")).json()["result"] == {"ok": True}
    assert_error(await http.post(f"/api/v1/agents/{uuid.uuid4()}/{kind}"), 404, "not_found")
    assert_error(await http.get("/api/v1/commands/999999"), 404, "not_found")
    async with pool.connection() as conn:
        await conn.execute("UPDATE agents SET gone_at = now() WHERE id = %s", (ag["id"],))
    assert_error(await http.post(f"/api/v1/agents/{ag['id']}/{kind}"), 409, "agent_gone")


async def test_checks_enqueue_and_get(http, pool):
    cl = await insert_session(pool)
    r = await http.post(f"/api/v1/sessions/{cl['id']}/checks", json={"command": "make test"})
    assert r.status_code == 202, r.text
    got = (await http.get(f"/api/v1/checks/{r.json()['check_id']}")).json()
    assert got["status"] == "queued"
    assert got["command"] == "make test"
    assert got["exit_code"] is None
    done = await insert_check(pool, cl["id"], status="failed", exit_code=2, log_tail="boom")
    got = (await http.get(f"/api/v1/checks/{done['id']}")).json()
    assert (got["status"], got["exit_code"], got["log_tail"]) == ("failed", 2, "boom")
    assert_error(await http.get("/api/v1/checks/999999"), 404, "not_found")
    assert_error(
        await http.post(f"/api/v1/sessions/{cl['id']}/checks", json={"command": ""}),
        422,
        "invalid_request",
    )
    assert_error(
        await http.post(f"/api/v1/sessions/{uuid.uuid4()}/checks", json={"command": "x"}),
        404,
        "not_found",
    )
    dying = await insert_session(pool, desired="deleting")
    assert_error(
        await http.post(f"/api/v1/sessions/{dying['id']}/checks", json={"command": "x"}),
        409,
        "session_deleting",
    )


async def test_post_message_lane_defaults_to_regular_and_round_trips(http, pool):
    cl = await insert_session(pool)
    url = f"/api/v1/sessions/{cl['id']}/messages"
    default = (await http.post(url, json={"text": "a"})).json()["message_id"]
    assert (await http.get(f"/api/v1/messages/{default}")).json()["lane"] == "regular"
    for lane in ("idle", "regular", "priority"):
        r = await http.post(url, json={"text": lane, "lane": lane})
        assert r.status_code == 202, r.text
        got = (await http.get(f"/api/v1/messages/{r.json()['message_id']}")).json()
        assert got["lane"] == lane
    listed = (await http.get(url)).json()
    assert {m["lane"] for m in listed} == {"idle", "regular", "priority"}


async def test_post_message_rejects_unknown_lane(http, pool):
    cl = await insert_session(pool)
    r = await http.post(
        f"/api/v1/sessions/{cl['id']}/messages", json={"text": "x", "lane": "urgent"}
    )
    assert r.status_code == 422
    assert await _count(pool, "messages") == 0

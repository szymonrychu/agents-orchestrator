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


# ---------------------------------------------------------------- start prompt first (T-63)
# Session create queues the start prompt as its own first message; a post is never prefixed.


async def _texts(http, session_id) -> list[str]:
    rows = (await http.get(f"/api/v1/sessions/{session_id}/messages")).json()
    return [m["text"] for m in sorted(rows, key=lambda m: m["id"])]


async def _created(http, start_prompt: str) -> dict:
    """A session created through the API, so the start prompt is queued as at runtime."""
    r = await http.post(
        "/api/v1/sessions", json={"slug": "sp", "spec": {"start_prompt": start_prompt}}
    )
    assert r.status_code == 201, r.text
    return r.json()


async def test_first_post_is_its_own_message_after_the_start_prompt(http, pool):
    cl = await _created(http, "You are on {{ session_slug }}.")
    r = await http.post(f"/api/v1/sessions/{cl['id']}/messages", json={"text": "fix the bug"})
    assert r.status_code == 202, r.text
    assert await _texts(http, cl["id"]) == ["You are on sp.", "fix the bug"]


async def test_posts_are_never_prefixed_with_the_start_prompt(http, pool):
    cl = await _created(http, "ctx")
    url = f"/api/v1/sessions/{cl['id']}/messages"
    await http.post(url, json={"text": "one"})
    await http.post(url, json={"text": "two"})
    assert await _texts(http, cl["id"]) == ["ctx", "one", "two"]
    plain = await insert_session(pool, spec={"start_prompt": "ctx"})  # no carrier: none added
    await http.post(f"/api/v1/sessions/{plain['id']}/messages", json={"text": "again"})
    assert await _texts(http, plain["id"]) == ["again"]


async def test_no_start_prompt_sends_the_message_unchanged(http, pool):
    cl = await insert_session(pool)
    await http.post(f"/api/v1/sessions/{cl['id']}/messages", json={"text": "plain"})
    assert await _texts(http, cl["id"]) == ["plain"]


async def test_concurrent_first_posts_leave_one_start_prompt_first(http, pool):
    import asyncio

    cl = await _created(http, "ctx")
    url = f"/api/v1/sessions/{cl['id']}/messages"
    await asyncio.gather(*(http.post(url, json={"text": f"m{i}"}) for i in range(4)))
    texts = await _texts(http, cl["id"])
    assert texts[0] == "ctx"
    assert sorted(texts[1:]) == ["m0", "m1", "m2", "m3"]


async def test_peer_message_arriving_first_still_comes_after_the_start_prompt(http, pool):
    # Replaces the D6 test that pinned the peer message as turn 1 (reversed by the user, T-63).
    cl = await _created(http, "ctx")
    await insert_message(pool, cl["id"], text="from a peer")  # no carries_start_prompt
    await http.post(f"/api/v1/sessions/{cl['id']}/messages", json={"text": "user"})
    assert await _texts(http, cl["id"]) == ["ctx", "from a peer", "user"]


async def test_failed_or_cancelled_start_prompt_is_not_handed_to_the_next_post(http, pool):
    cl = await _created(http, "ctx")
    url = f"/api/v1/sessions/{cl['id']}/messages"
    async with pool.connection() as conn:
        await conn.execute("UPDATE messages SET status='failed' WHERE session_id=%s", (cl["id"],))
    second = (await http.post(url, json={"text": "b"})).json()["message_id"]
    assert (await http.get(f"/api/v1/messages/{second}")).json()["text"] == "b"
    assert await _texts(http, cl["id"]) == ["ctx", "b"]


async def test_message_view_notice(http, pool):
    cl = await insert_session(pool)
    notice = await insert_message(pool, cl["id"], text="remind", notice="inactivity_reminder")
    plain = await insert_message(pool, cl["id"], text="hi")
    got = (await http.get(f"/api/v1/messages/{notice['id']}")).json()
    assert got["notice"] == "inactivity_reminder"
    assert (await http.get(f"/api/v1/messages/{plain['id']}")).json()["notice"] is None
    listed = (await http.get(f"/api/v1/sessions/{cl['id']}/messages")).json()
    assert {m["id"]: m["notice"] for m in listed} == {
        notice["id"]: "inactivity_reminder",
        plain["id"]: None,
    }


# ---- attachments ----

_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16


def _att(name="s.png", mime="image/png", data=_PNG, **kw):
    import base64

    return {"name": name, "mime": mime, "data_b64": base64.b64encode(data).decode(), **kw}


async def test_post_message_with_png_attachment(http, pool):
    cl = await insert_session(pool)
    r = await http.post(
        f"/api/v1/sessions/{cl['id']}/messages", json={"text": "see", "attachments": [_att()]}
    )
    assert r.status_code == 202, r.text
    mid = r.json()["message_id"]
    want = [
        {
            "name": "s.png",
            "mime": "image/png",
            "size": len(_PNG),
            "path": f".attachments/orch-{mid}/s.png",
        }
    ]
    one = await http.get(f"/api/v1/messages/{mid}")
    assert one.json()["attachments"] == want
    assert "data_b64" not in one.text
    lst = await http.get(f"/api/v1/sessions/{cl['id']}/messages")
    assert lst.json()[0]["attachments"] == want
    assert "data_b64" not in lst.text


async def test_message_without_attachments_lists_empty(http, pool):
    cl = await insert_session(pool)
    r = await http.post(f"/api/v1/sessions/{cl['id']}/messages", json={"text": "x"})
    got = (await http.get(f"/api/v1/messages/{r.json()['message_id']}")).json()
    assert got["attachments"] == []


async def _post_att(http, pool, *items):
    cl = await insert_session(pool)
    return await http.post(
        f"/api/v1/sessions/{cl['id']}/messages", json={"text": "x", "attachments": list(items)}
    )


async def test_attachment_mime_not_allowed_422(http, pool):
    r = await _post_att(http, pool, _att("a.gif", "image/gif", b"GIF89a"))
    assert_error(r, 422, "invalid_attachment")
    assert await _count(pool, "messages") == 0


@pytest.mark.parametrize(
    ("mime", "data"),
    [
        ("image/png", b'{"a": 1}'),
        ("image/jpeg", _PNG),
        ("application/json", b"{"),
        pytest.param("application/json", b"[" * 200000, id="json-deeply-nested"),
        ("text/plain", b"\xff\xfe"),
    ],
)
async def test_attachment_content_mismatch_422(http, pool, mime, data):
    r = await _post_att(http, pool, _att("f.bin", mime, data))
    assert_error(r, 422, "invalid_attachment")
    assert await _count(pool, "messages") == 0


async def test_valid_jpeg_json_and_text_accepted(http, pool):
    r = await _post_att(
        http,
        pool,
        _att("a.jpg", "image/jpeg", b"\xff\xd8\xff\xe0abc"),
        _att("b.json", "application/json", b'{"a": 1}'),
        _att("c.txt", "text/plain", "caf\u00e9".encode()),
    )
    assert r.status_code == 202, r.text


async def test_attachment_bad_base64_422(http, pool):
    r = await _post_att(http, pool, {"name": "a.png", "mime": "image/png", "data_b64": "@@@@"})
    assert_error(r, 422, "invalid_attachment")
    r = await _post_att(http, pool, {"name": "a.png", "mime": "image/png", "data_b64": "aGk=\n"})
    assert_error(r, 422, "invalid_attachment")


async def test_attachment_zero_bytes_422(http, pool):
    assert_error(
        await _post_att(http, pool, _att("a.txt", "text/plain", b"")), 422, "invalid_attachment"
    )


async def test_attachment_duplicate_name_422(http, pool):
    r = await _post_att(http, pool, _att("a.png"), _att("a.png"))
    assert_error(r, 422, "invalid_attachment")


@pytest.mark.parametrize("name", ["../x", "a/b", ".gitignore", "x\x00y", "a" * 129, ""])
async def test_attachment_bad_name_422(http, pool, name):
    assert_error(await _post_att(http, pool, _att(name)), 422, "invalid_attachment")


async def test_attachment_caps_413(pool, dsn):
    app = create_app(
        make_settings(
            dsn,
            attachments_max_bytes=100,
            attachments_max_total_bytes=150,
            attachments_max_count=2,
        ),
        pool,
    )
    async with client_for(app) as http:
        txt = "text/plain"
        assert_error(
            await _post_att(http, pool, *[_att(f"{i}.txt", txt, b"a") for i in range(3)]),
            413,
            "too_large",
        )
        assert_error(await _post_att(http, pool, _att("a.txt", txt, b"a" * 101)), 413, "too_large")
        assert_error(
            await _post_att(
                http, pool, _att("a.txt", txt, b"a" * 100), _att("b.txt", txt, b"b" * 60)
            ),
            413,
            "too_large",
        )
        # over-long data_b64 is refused before decoding (invalid base64 would be 422 otherwise)
        long = {"name": "a.txt", "mime": txt, "data_b64": "!" * 1000}
        assert_error(await _post_att(http, pool, long), 413, "too_large")
    assert await _count(pool, "messages") == 0


async def test_start_prompt_session_post_keeps_attachments_unprefixed(http, pool):
    cl = await _created(http, "ctx")
    r = await http.post(
        f"/api/v1/sessions/{cl['id']}/messages", json={"text": "go", "attachments": [_att()]}
    )
    assert r.status_code == 202, r.text
    got = (await http.get(f"/api/v1/messages/{r.json()['message_id']}")).json()
    assert got["text"] == "go"
    assert [a["name"] for a in got["attachments"]] == ["s.png"]
    assert await _count(pool, "message_attachments") == 1


async def _last_user_message_at(pool, session_id):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT last_user_message_at FROM sessions WHERE id = %s", (session_id,)
        )
        return (await cur.fetchone())[0]


async def _view_last_user_message_at(http, session_id):
    return (await http.get(f"/api/v1/sessions/{session_id}")).json()["last_user_message_at"]


async def test_post_message_main_port_marks_last_user_message_at(http, pool):
    cl = await insert_session(pool)
    assert await _view_last_user_message_at(http, cl["id"]) is None
    r = await http.post(f"/api/v1/sessions/{cl['id']}/messages", json={"text": "hi"})
    assert r.status_code == 202, r.text
    row = await _last_user_message_at(pool, cl["id"])
    assert abs(datetime.now(UTC) - row) < timedelta(seconds=10)
    view = await _view_last_user_message_at(http, cl["id"])
    assert datetime.fromisoformat(view) == row


async def test_post_message_from_user_false_leaves_last_user_message_at(http, pool):
    cl = await insert_session(pool)
    r = await http.post(
        f"/api/v1/sessions/{cl['id']}/messages", json={"text": "hi", "from_user": False}
    )
    assert r.status_code == 202, r.text
    assert await _last_user_message_at(pool, cl["id"]) is None
    assert await _view_last_user_message_at(http, cl["id"]) is None


async def test_refused_post_leaves_last_user_message_at(http, pool):
    gone = await insert_session(pool, desired="deleting")
    r = await http.post(f"/api/v1/sessions/{gone['id']}/messages", json={"text": "hi"})
    assert_error(r, 409, "session_deleting")
    assert await _last_user_message_at(pool, gone["id"]) is None
    cl = await insert_session(pool)
    other = await insert_session(pool)
    foreign = await insert_agent(pool, other["id"])
    r = await http.post(
        f"/api/v1/sessions/{cl['id']}/messages",
        json={"text": "hi", "target_agent_id": str(foreign["id"])},
    )
    assert_error(r, 422, "invalid_request")
    assert await _last_user_message_at(pool, cl["id"]) is None


async def test_retry_does_not_touch_last_user_message_at(http, pool):
    cl = await insert_session(pool)
    past = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET last_user_message_at = %s WHERE id = %s", (past, cl["id"])
        )
    f = await insert_message(pool, cl["id"], status="failed", attempts=5, error="unavailable")
    r = await http.post(f"/api/v1/messages/{f['id']}/retry")
    assert r.status_code == 202, r.text
    assert await _last_user_message_at(pool, cl["id"]) == past


async def test_post_message_rejects_non_bool_from_user(http, pool):
    cl = await insert_session(pool)
    r = await http.post(
        f"/api/v1/sessions/{cl['id']}/messages", json={"text": "hi", "from_user": "yes"}
    )
    assert_error(r, 422, "invalid_request")

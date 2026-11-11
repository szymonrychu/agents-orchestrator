"""Project room agent routes (T-117): POST and GET /agent/v1/project/messages. Leaders post to
their project's room, the post fans out as an untargeted session message to the leader of every
other active, non-superleader session of the project; leaders (and superleaders, naming the
project) read the room."""

from __future__ import annotations

from typing import Any

from factories import insert_session, set_agent_status
from psycopg.rows import dict_row
from test_agent_session_messaging import Sess, _app, _delivered, _exec, _rows, hive
from test_api_agent import client_for, headers
from test_api_sessions import assert_error
from test_queue_leader_routing import _agent, _changed

from agents_orchestrator.db import queue as q
from agents_orchestrator.db import sessions as sessions_repo

PATH = "/agent/v1/project/messages"


async def _room(pool) -> dict[str, Sess]:
    """Project `a` with hives A, B, C and the superleader S; project `b` with hive D."""
    await _exec(pool, "INSERT INTO projects (slug, name) VALUES ('a', 'A'), ('b', 'B')")
    return {
        "A": await hive(pool, "a-alpha", project="a"),
        "B": await hive(pool, "a-bravo", project="a"),
        "C": await hive(pool, "a-charlie", project="a"),
        "S": await hive(pool, "s-boss", project="a", superleader=True),
        "D": await hive(pool, "d-delta", project="b"),
    }


async def _post(app, caller: Sess, text: str = "hello room", ip: str | None = None, **extra):
    async with client_for(app, ip or caller.leader_ip) as http:
        return await http.post(PATH, json={"text": text, **extra}, headers=headers(caller.id))


async def _read(app, caller: Sess, ip: str | None = None, **params):
    async with client_for(app, ip or caller.leader_ip) as http:
        return await http.get(PATH, params=params, headers=headers(caller.id))


async def _query(pool, query: str, params: tuple = ()) -> list[dict[str, Any]]:
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        return list(await cur.fetchall())


async def _posts(pool) -> list[dict[str, Any]]:
    return await _query(pool, "SELECT * FROM project_posts ORDER BY id")


async def test_leader_post_fans_out_to_other_project_leaders(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn)
    resp = await _post(app, r["A"])
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["delivered"] == sorted([r["B"].slug, r["C"].slug])
    assert body["skipped"] == [] and body["handled"] == []
    rows = await _rows(pool)
    assert sorted(row["session_id"] for row in rows) == sorted([r["B"].id, r["C"].id])
    for row in rows:
        assert row["project_post_id"] == body["post_id"]
        assert row["sender_slug"] == r["A"].slug
        assert row["sender_session_id"] == r["A"].id
        assert row["lane"] == "regular"
        assert row["text"] == "hello room"
    [post] = await _posts(pool)
    assert post["id"] == body["post_id"]
    assert (post["project"], post["sender_kind"], post["sender_slug"]) == ("a", "leader", "a-alpha")
    assert post["recipients"] == body["delivered"]
    assert post["addressed"] == []


async def test_to_narrows_push(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn)
    resp = await _post(app, r["A"], to=[r["B"].slug])
    assert resp.status_code == 202, resp.text
    assert resp.json()["delivered"] == [r["B"].slug]
    [post] = await _posts(pool)
    assert post["addressed"] == [r["B"].slug]
    assert post["recipients"] == [r["B"].slug]
    assert [row["session_id"] for row in await _rows(pool)] == [r["B"].id]


async def test_to_outside_project_refused(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn)
    resp = await _post(app, r["A"], to=[r["D"].slug])
    assert_error(resp, 422, "not_in_project")
    assert r["D"].slug in resp.json()["error"]["message"]
    assert_error(await _post(app, r["A"], to=[r["S"].slug]), 422, "not_in_project")
    assert await _posts(pool) == []
    assert await _rows(pool) == []


async def test_worker_cannot_post_or_read(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn)
    assert_error(await _post(app, r["A"], ip=r["A"].member_ip), 403, "not_leader")
    assert_error(await _read(app, r["A"], ip=r["A"].member_ip), 403, "not_leader")
    assert await _posts(pool) == []


async def test_foreign_project_forbidden_for_leader(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn)
    assert_error(await _post(app, r["A"], project="b"), 403, "project_forbidden")
    assert_error(await _read(app, r["A"], project="b"), 403, "project_forbidden")
    # naming its own project is allowed
    assert (await _post(app, r["A"], project="a")).status_code == 202
    assert (await _read(app, r["A"], project="a")).status_code == 200


async def test_superleader_needs_project_and_is_audited(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn)
    assert_error(await _post(app, r["S"]), 422, "project_required")
    assert_error(await _read(app, r["S"]), 422, "project_required")
    resp = await _post(app, r["S"], project="a")
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["delivered"] == sorted([r["A"].slug, r["B"].slug, r["C"].slug])
    [post] = await _posts(pool)
    assert post["sender_kind"] == "superleader"
    events = await _query(
        pool, "SELECT * FROM events WHERE type = 'admin_call' AND session_id = %s", (r["S"].id,)
    )
    assert len(events) == 1
    assert events[0]["data"] == {
        "method": "POST",
        "path": PATH,
        "status": 202,
        "session": r["S"].slug,
        "project": "a",
        "post_id": body["post_id"],
    }
    # a leader's post is not audited
    assert (await _post(app, r["A"])).status_code == 202
    assert len(await _query(pool, "SELECT * FROM events WHERE type = 'admin_call'")) == 1


async def test_superleader_unknown_project(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn)
    assert_error(await _post(app, r["S"], project="nope"), 404, "unknown_project")
    assert_error(await _read(app, r["S"], project="nope"), 404, "unknown_project")


async def test_superleader_reads_any_project(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn)
    assert (await _post(app, r["D"], "in b")).status_code == 202
    resp = await _read(app, r["S"], project="b")
    assert resp.status_code == 200, resp.text
    assert [p["text"] for p in resp.json()["posts"]] == ["in b"]


async def test_archived_project(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn)
    assert (await _post(app, r["A"], "before")).status_code == 202
    await _exec(pool, "UPDATE projects SET archived_at = now() WHERE slug = 'a'")
    assert_error(await _post(app, r["A"]), 409, "project_archived")
    resp = await _read(app, r["A"])
    assert resp.status_code == 200, resp.text
    assert [p["text"] for p in resp.json()["posts"]] == ["before"]


async def test_hop_limit_across_room_replies(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn)
    await _delivered(pool, r["A"], r["B"], hop=8, turn="turn-8")
    await set_agent_status(pool, r["A"].leader_id, state="busy", current_turn_id="turn-8")
    assert_error(await _post(app, r["A"]), 409, "hop_limit")
    assert await _posts(pool) == []
    await _delivered(pool, r["A"], r["B"], hop=3, turn="turn-3")
    await set_agent_status(pool, r["A"].leader_id, state="busy", current_turn_id="turn-3")
    resp = await _post(app, r["A"])
    assert resp.status_code == 202, resp.text
    fanned = await _rows(pool, "project_post_id = %s", (resp.json()["post_id"],))
    assert len(fanned) == 2
    assert {row["hop"] for row in fanned} == {4}


async def test_rate_limited_counts_post_once(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn, session_message_rate_per_minute=2)
    for _ in range(2):
        resp = await _post(app, r["A"])
        assert resp.status_code == 202, resp.text
        assert len(resp.json()["delivered"]) == 2
    assert_error(await _post(app, r["A"]), 429, "rate_limited")
    assert len(await _posts(pool)) == 2


async def test_rate_counts_direct_session_messages(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn, session_message_rate_per_minute=2)
    async with client_for(app, r["A"].leader_ip) as http:
        sent = await http.post(
            f"/agent/v1/sessions/{r['B'].slug}/messages",
            json={"text": "direct"},
            headers=headers(r["A"].id),
        )
    assert sent.status_code == 202, sent.text
    assert (await _post(app, r["A"])).status_code == 202
    assert_error(await _post(app, r["A"]), 429, "rate_limited")


async def test_project_budget_per_hour(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn, project_chat_posts_per_hour=2)
    assert (await _post(app, r["A"])).status_code == 202
    assert (await _post(app, r["B"])).status_code == 202
    assert_error(await _post(app, r["C"]), 429, "project_budget")
    # another project has its own budget
    assert (await _post(app, r["D"])).status_code == 202


async def test_too_many_recipients(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn, project_chat_max_recipients=1)
    resp = await _post(app, r["A"])
    assert_error(resp, 409, "too_many_recipients")
    assert "to" in resp.json()["error"]["message"]
    assert await _posts(pool) == []
    narrowed = await _post(app, r["A"], to=[r["B"].slug])
    assert narrowed.status_code == 202, narrowed.text
    assert narrowed.json()["delivered"] == [r["B"].slug]


async def test_fanout_skips_deleting_and_full_queue(pool, dsn, monkeypatch):
    """C holds `session_message_queued_max` queued session messages; B turns `deleting` between
    the recipients read and its enqueue (simulated: its enqueue raises). Both are skipped, the
    post still goes to the others."""
    r = await _room(pool)
    e = await hive(pool, "a-echo", project="a")
    app = _app(pool, dsn, session_message_queued_max=1)
    full = await _rows(pool)
    assert full == []
    await _exec(
        pool,
        "INSERT INTO messages (session_id, text, sender_session_id, sender_slug, hop)"
        " VALUES (%s, 'waiting', %s, %s, 1)",
        (r["C"].id, r["D"].id, r["D"].slug),
    )
    real = q.enqueue_message

    async def enqueue(conn, session_id, *args, **kwargs):
        if session_id == r["B"].id:
            raise q.SessionBeingDeleted(str(session_id))
        return await real(conn, session_id, *args, **kwargs)

    monkeypatch.setattr(q, "enqueue_message", enqueue)
    resp = await _post(app, r["A"])
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["delivered"] == [e.slug]
    assert body["skipped"] == [
        {"slug": r["B"].slug, "reason": "session_deleting"},
        {"slug": r["C"].slug, "reason": "queue_full"},
    ]
    [post] = await _posts(pool)
    assert post["recipients"] == [e.slug]
    fanned = await _rows(pool, "project_post_id IS NOT NULL")
    assert [row["session_id"] for row in fanned] == [e.id]


async def test_handled_marks_incoming(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn)
    first = await _post(app, r["A"], to=[r["B"].slug])
    assert first.status_code == 202, first.text
    [row] = await _rows(pool, "session_id = %s", (r["B"].id,))
    await _exec(pool, "UPDATE messages SET status = 'sent' WHERE id = %s", (row["id"],))
    resp = await _post(app, r["B"], "on it", handled=[row["id"]], reply_to=first.json()["post_id"])
    assert resp.status_code == 202, resp.text
    assert resp.json()["handled"] == [row["id"]]
    posts = await _posts(pool)
    assert posts[-1]["reply_to"] == first.json()["post_id"]


async def test_invalid_reply_to(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn)
    other = await _post(app, r["D"], "in b")
    assert other.status_code == 202, other.text
    assert_error(
        await _post(app, r["A"], reply_to=other.json()["post_id"]), 422, "invalid_reply_to"
    )
    assert_error(await _post(app, r["A"], reply_to=999999), 422, "invalid_reply_to")
    assert len(await _posts(pool)) == 1
    assert (await _post(app, r["A"], reply_to=0)).status_code == 422


async def test_post_stamps_last_update(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn)
    assert (await _post(app, r["A"])).status_code == 202
    [row] = await _query(pool, "SELECT last_update_at FROM sessions WHERE id = %s", (r["A"].id,))
    assert row["last_update_at"] is not None


async def test_session_send_budget_unaffected_by_room(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn, session_message_pair_max_per_hour=3)
    for _ in range(3):
        assert (await _post(app, r["A"], to=[r["B"].slug])).status_code == 202
    async with client_for(app, r["A"].leader_ip) as http:
        sent = await http.post(
            f"/agent/v1/sessions/{r['B'].slug}/messages",
            json={"text": "direct"},
            headers=headers(r["A"].id),
        )
    assert sent.status_code == 202, sent.text


async def test_moved_session_posts_to_new_project(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn)
    async with pool.connection() as conn:
        await sessions_repo.set_project(conn, r["A"].id, "b")
    resp = await _post(app, r["A"])
    assert resp.status_code == 202, resp.text
    assert resp.json()["delivered"] == [r["D"].slug]
    [post] = await _posts(pool)
    assert post["project"] == "b"
    assert (await _read(app, r["A"])).json()["posts"][0]["id"] == post["id"]


async def test_read_paging_cursor_first(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn, session_message_rate_per_minute=100)
    for n in range(25):
        assert (await _post(app, r["A"], f"post {n}")).status_code == 202
    first = await _read(app, r["B"])
    assert first.status_code == 200, first.text
    body = first.json()
    assert list(body) == ["next_before_id", "posts"]
    posts = body["posts"]
    assert len(posts) == 20
    ids = [p["id"] for p in posts]
    assert ids == sorted(ids, reverse=True)
    assert body["next_before_id"] == ids[-1]
    assert posts[0]["text"] == "post 24"
    assert set(posts[0]) == {
        "id",
        "from",
        "sender_kind",
        "sender_session_id",
        "at",
        "text",
        "reply_to",
        "to",
    }
    assert posts[0]["from"] == r["A"].slug
    assert posts[0]["sender_session_id"] == str(r["A"].id)
    assert posts[0]["sender_kind"] == "leader"
    second = (await _read(app, r["B"], before_id=body["next_before_id"])).json()
    assert len(second["posts"]) == 5
    assert second["next_before_id"] is None
    assert second["posts"][-1]["text"] == "post 0"
    newer = (await _read(app, r["B"], after_id=ids[2], limit=5)).json()
    assert [p["id"] for p in newer["posts"]] == [ids[1], ids[0]]
    assert newer["next_before_id"] is None
    assert_error(await _read(app, r["B"], before_id=ids[0], after_id=ids[1]), 422, "invalid_cursor")


async def test_post_row_is_untargeted(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn)
    assert (await _post(app, r["A"])).status_code == 202
    rows = await _rows(pool, "project_post_id IS NOT NULL")
    assert rows and all(row["target_agent_id"] is None for row in rows)


async def test_post_held_during_handover_and_claimed_by_successor(pool, dsn):
    """A recipient hive with a leader change whose handover notice is not queued yet holds the
    fan-out row; once the notice epoch is recorded the successor claims it."""
    await _exec(pool, "INSERT INTO projects (slug, name) VALUES ('a', 'A')")
    sender = await hive(pool, "a-alpha", project="a")
    target = await insert_session(
        pool, slug="a-bravo", spec={"replicas": 2, "max_replicas": 2}, project="a"
    )
    old = await _agent(pool, target["id"], free_ago=100, ordinal=0)
    successor = await _agent(pool, target["id"], free_ago=1, ordinal=1)
    await _changed(pool, target["id"], successor["id"], old["id"])
    app = _app(pool, dsn)
    resp = await _post(app, sender)
    assert resp.status_code == 202, resp.text
    assert resp.json()["delivered"] == ["a-bravo"]
    [row] = await _rows(pool, "project_post_id IS NOT NULL")
    async with pool.connection() as conn:
        assert await q.claim_and_pick(conn) is None
    await _exec(
        pool, "UPDATE sessions SET handover_notice_epoch = 2 WHERE id = %s", (target["id"],)
    )
    async with pool.connection() as conn:
        claim = await q.claim_and_pick(conn)
    assert claim is not None
    assert (claim.message_id, claim.agent_id) == (row["id"], successor["id"])


async def test_text_and_list_bounds(pool, dsn):
    r = await _room(pool)
    app = _app(pool, dsn)
    assert (await _post(app, r["A"], "x" * 4001)).status_code == 422
    assert (await _post(app, r["A"], "")).status_code == 422
    assert (await _post(app, r["A"], to=[f"s{n}" for n in range(21)])).status_code == 422
    assert (await _post(app, r["A"], handled=list(range(1, 52)))).status_code == 422
    assert (await _post(app, r["A"], extra="x")).status_code == 422
    assert (await _read(app, r["A"], limit=101)).status_code == 422
    assert (await _read(app, r["A"], limit=0)).status_code == 422
    assert await _posts(pool) == []
    assert (await _post(app, r["A"], "x" * 4000)).status_code == 202

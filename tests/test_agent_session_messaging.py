"""Cross-session messaging agent routes (D6, D7, D10): GET /agent/v1/sessions and
POST /agent/v1/sessions/{slug}/messages. A superleader session (`session_messaging` true) lists
and messages every other session; every other session lists and messages only superleaders."""

from __future__ import annotations

import itertools
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
from factories import insert_agent, insert_message, insert_session, set_agent_status
from psycopg.rows import dict_row
from test_api_agent import AGENT_PORT, client_for, headers, make_settings, set_ip, token
from test_api_sessions import assert_error

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import queue

_ips = itertools.count(1)


def _ip() -> str:
    n = next(_ips)
    return f"10.9.{n // 250 + 1}.{n % 250 + 1}"


@dataclass
class Sess:
    row: dict[str, Any]
    leader_ip: str
    member_ip: str | None
    leader_id: Any

    @property
    def id(self) -> Any:
        return self.row["id"]

    @property
    def slug(self) -> str:
        return self.row["slug"]


def _spec(base: dict, superleader: bool) -> dict:
    return {**base, "session_messaging": True} if superleader else base


async def _exec(pool, query: str, params: tuple = ()) -> None:
    async with pool.connection() as conn:
        await conn.execute(query, params)


async def alone(pool, slug: str, *, superleader: bool = False, desired: str = "active") -> Sess:
    row = await insert_session(
        pool, slug=slug, spec=_spec({"replicas": 1}, superleader), desired=desired
    )
    a0 = await insert_agent(pool, row["id"], ordinal=0, ready=True)
    ip = _ip()
    await set_ip(pool, a0["id"], ip)
    return Sess(row, ip, None, a0["id"])


async def hive(
    pool, slug: str, *, superleader: bool = False, leader: bool = True, desired: str = "active"
) -> Sess:
    spec = _spec({"replicas": 2, "max_replicas": 2}, superleader)
    row = await insert_session(pool, slug=slug, spec=spec, desired=desired)
    a0 = await insert_agent(pool, row["id"], ordinal=0, ready=True)
    a1 = await insert_agent(pool, row["id"], ordinal=1, ready=True)
    ip0, ip1 = _ip(), _ip()
    await set_ip(pool, a0["id"], ip0)
    await set_ip(pool, a1["id"], ip1)
    if leader:
        await _exec(
            pool,
            "INSERT INTO session_leaders (session_id, agent_id, epoch, since, reason)"
            " VALUES (%s, %s, 1, now(), 'start')",
            (row["id"], a0["id"]),
        )
    return Sess(row, ip0, ip1, a0["id"])


async def _rows(pool, where: str = "TRUE", params: tuple = ()) -> list[dict]:
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(f"SELECT * FROM messages WHERE {where} ORDER BY id", params)
        return list(await cur.fetchall())


def _app(pool, dsn, **overrides):
    settings = make_settings(dsn)
    if overrides:
        settings = settings.model_copy(update=overrides)
    return create_app(settings, pool)


async def _list(app, caller: Sess, ip: str | None = None):
    async with client_for(app, ip or caller.leader_ip) as http:
        return await http.get("/agent/v1/sessions", headers=headers(caller.id))


async def _send(app, caller: Sess, slug: str, text: str = "hello", ip: str | None = None):
    async with client_for(app, ip or caller.leader_ip) as http:
        return await http.post(
            f"/agent/v1/sessions/{slug}/messages", json={"text": text}, headers=headers(caller.id)
        )


def _entry(sess: Sess, *, superleader: bool, leader_present: bool, preset: str | None = None):
    return {
        "slug": sess.slug,
        "preset_name": preset,
        "superleader": superleader,
        "leader_present": leader_present,
    }


async def test_superleader_lists_all_sessions(pool, dsn):
    boss = await hive(pool, "m-boss", superleader=True)
    other_boss = await alone(pool, "m-a-other-boss", superleader=True)
    plain_hive = await hive(pool, "m-c-hive")
    leaderless = await hive(pool, "m-d-leaderless", leader=False)
    gone_leader = await hive(pool, "m-e-gone-leader")
    await _exec(pool, "UPDATE agents SET gone_at = now() WHERE id = %s", (gone_leader.leader_id,))
    plain_alone = await alone(pool, "m-b-alone")
    gone_alone = await alone(pool, "m-f-gone-alone")
    await _exec(pool, "UPDATE agents SET gone_at = now() WHERE id = %s", (gone_alone.leader_id,))
    await hive(pool, "m-g-deleting", desired="deleting")
    await alone(pool, "m-h-deleting-boss", superleader=True, desired="deleting")
    await _exec(
        pool, "UPDATE sessions SET preset_name = 'reviewer' WHERE id = %s", (plain_alone.id,)
    )
    app = _app(pool, dsn)

    expected = [
        _entry(other_boss, superleader=True, leader_present=True),
        _entry(plain_alone, superleader=False, leader_present=True, preset="reviewer"),
        _entry(plain_hive, superleader=False, leader_present=True),
        _entry(leaderless, superleader=False, leader_present=False),
        _entry(gone_leader, superleader=False, leader_present=False),
        _entry(gone_alone, superleader=False, leader_present=False),
    ]
    r = await _list(app, boss)
    assert r.status_code == 200, r.text
    assert r.json() == {"sessions": expected}
    # listing is read only: any agent of the session may list
    member = await _list(app, boss, boss.member_ip)
    assert member.status_code == 200, member.text
    assert member.json() == {"sessions": expected}


async def test_plain_session_lists_only_superleaders(pool, dsn):
    app = _app(pool, dsn)
    lonely = await alone(pool, "p-lonely")
    lonely_hive = await hive(pool, "p-lonely-hive")
    for caller in (lonely, lonely_hive):
        r = await _list(app, caller)
        assert r.status_code == 200, r.text
        assert r.json() == {"sessions": []}

    boss_b = await alone(pool, "p-b-boss", superleader=True)
    boss_a = await hive(pool, "p-a-boss", superleader=True)
    await hive(pool, "p-c-deleting-boss", superleader=True, desired="deleting")
    expected = {
        "sessions": [
            _entry(boss_a, superleader=True, leader_present=True),
            _entry(boss_b, superleader=True, leader_present=True),
        ]
    }
    for caller, ip in (
        (lonely, lonely.leader_ip),
        (lonely_hive, lonely_hive.leader_ip),
        (lonely_hive, lonely_hive.member_ip),
    ):
        r = await _list(app, caller, ip)
        assert r.status_code == 200, r.text
        assert r.json() == expected


async def test_send_queues_with_sender(pool, dsn):
    app = _app(pool, dsn)
    boss = await hive(pool, "s-boss", superleader=True)
    boss_alone = await alone(pool, "s-boss-alone", superleader=True)
    plain_alone = await alone(pool, "s-alone")
    plain_hive = await hive(pool, "s-hive")
    sends = [
        (boss, plain_alone),
        (boss, plain_hive),
        (plain_hive, boss),
        (plain_alone, boss),
        (boss, boss_alone),
        (boss_alone, boss),
    ]
    for caller, target in sends:
        r = await _send(app, caller, target.slug, f"from {caller.slug}")
        assert r.status_code == 202, r.text
        body = r.json()
        assert set(body) == {"message_id"}
        (row,) = await _rows(pool, "id = %s", (body["message_id"],))
        assert row["session_id"] == target.id
        assert row["text"] == f"from {caller.slug}"
        assert row["sender_session_id"] == caller.id
        assert row["sender_slug"] == caller.slug
        assert row["hop"] == 1
        assert row["target_agent_id"] is None
        assert row["status"] == "queued"
    assert len(await _rows(pool)) == len(sends)


async def test_send_refusals_for_sender(pool, dsn):
    app = _app(pool, dsn)
    boss = await hive(pool, "f-boss", superleader=True)
    plain = await hive(pool, "f-plain")
    leaderless = await hive(pool, "f-leaderless", leader=False)
    lone = await alone(pool, "f-lone")
    for caller, target, ip in (
        (boss, lone, boss.member_ip),
        (plain, boss, plain.member_ip),
        (leaderless, boss, leaderless.leader_ip),
        # the first failing check answers: not_leader before not_found and self_send
        (boss, "f-nobody", boss.member_ip),
        (boss, boss, boss.member_ip),
    ):
        slug = target if isinstance(target, str) else target.slug
        assert_error(await _send(app, caller, slug, ip=ip), 403, "not_leader")
    assert await _rows(pool) == []


async def test_send_refusals_for_target(pool, dsn):
    app = _app(pool, dsn)
    boss = await hive(pool, "t-boss", superleader=True)
    plain = await hive(pool, "t-plain")
    lone = await alone(pool, "t-lone")
    deleting = await alone(pool, "t-deleting", desired="deleting")
    deleting_boss = await alone(pool, "t-deleting-boss", superleader=True, desired="deleting")

    assert_error(await _send(app, boss, "t-nobody"), 404, "not_found")
    assert_error(await _send(app, plain, "t-nobody"), 404, "not_found")
    assert_error(await _send(app, boss, boss.slug), 422, "self_send")
    assert_error(await _send(app, plain, plain.slug), 422, "self_send")
    assert_error(await _send(app, plain, lone.slug), 403, "target_not_superleader")
    assert_error(await _send(app, lone, plain.slug), 403, "target_not_superleader")
    # target_not_superleader comes before session_deleting
    assert_error(await _send(app, plain, deleting.slug), 403, "target_not_superleader")
    assert_error(await _send(app, boss, deleting.slug), 409, "session_deleting")
    assert_error(await _send(app, plain, deleting_boss.slug), 409, "session_deleting")
    assert_error(await _send(app, boss, plain.slug, ""), 422, "invalid_request")
    assert_error(await _send(app, boss, plain.slug, "x" * 16001), 422, "invalid_request")
    async with client_for(app, boss.leader_ip) as http:
        r = await http.post(
            f"/agent/v1/sessions/{plain.slug}/messages",
            json={"text": "hi", "hop": 0},
            headers=headers(boss.id),
        )
    assert_error(r, 422, "invalid_request")
    assert await _rows(pool) == []
    # the bounds themselves are accepted
    assert (await _send(app, boss, plain.slug, "x")).status_code == 202
    assert (await _send(app, boss, plain.slug, "x" * 16000)).status_code == 202


async def _delivered(pool, receiver: Sess, sender: Sess, *, hop: int, turn: str) -> None:
    """A cross-session message of `hop` delivered into `turn` of the receiver's leader."""
    row = await insert_message(
        pool, receiver.id, status="sent", agent_id=receiver.leader_id, turn_id=turn
    )
    await _exec(
        pool,
        "UPDATE messages SET sender_session_id = %s, sender_slug = %s, hop = %s WHERE id = %s",
        (sender.id, sender.slug, hop, row["id"]),
    )


async def test_hop_counter(pool, dsn):
    app = _app(pool, dsn, session_message_max_hops=3)
    boss = await hive(pool, "h-boss", superleader=True)
    plain = await alone(pool, "h-plain")
    other = await alone(pool, "h-other")

    async def hop_of(resp) -> int:
        assert resp.status_code == 202, resp.text
        (row,) = await _rows(pool, "id = %s", (resp.json()["message_id"],))
        return row["hop"]

    # no current turn: hop 1
    await set_agent_status(pool, plain.leader_id, state="busy", current_turn_id=None)
    assert await hop_of(await _send(app, plain, boss.slug)) == 1

    # the current turn delivered a hop-2 message: the reply is hop 3
    await _delivered(pool, plain, boss, hop=2, turn="turn-a")
    await set_agent_status(pool, plain.leader_id, state="busy", current_turn_id="turn-a")
    assert await hop_of(await _send(app, plain, boss.slug)) == 3

    # a hop-3 message in the current turn: hop 4 exceeds the maximum of 3
    await _delivered(pool, boss, plain, hop=3, turn="turn-b")
    await set_agent_status(pool, boss.leader_id, state="busy", current_turn_id="turn-b")
    before = len(await _rows(pool))
    assert_error(await _send(app, boss, plain.slug), 409, "hop_limit")
    assert_error(await _send(app, boss, other.slug), 409, "hop_limit")
    assert len(await _rows(pool)) == before

    # a turn that delivered a message without a sender starts a fresh chain
    await insert_message(pool, boss.id, status="sent", agent_id=boss.leader_id, turn_id="turn-c")
    await set_agent_status(pool, boss.leader_id, state="busy", current_turn_id="turn-c")
    assert await hop_of(await _send(app, boss, other.slug)) == 1

    # a cross-session message delivered in an earlier turn does not carry over
    await set_agent_status(pool, boss.leader_id, state="busy", current_turn_id="turn-d")
    assert await hop_of(await _send(app, boss, other.slug)) == 1


async def test_rate_limit_and_queue_cap(pool, dsn):
    app = _app(pool, dsn, session_message_rate_per_minute=3)
    boss = await hive(pool, "r-boss", superleader=True)
    targets = [await alone(pool, f"r-target-{i}") for i in range(4)]
    # a send older than 60 s does not count
    old = await _send(app, boss, targets[0].slug)
    assert old.status_code == 202, old.text
    await _exec(
        pool,
        "UPDATE messages SET created_at = now() - interval '61 seconds' WHERE id = %s",
        (old.json()["message_id"],),
    )
    for t in targets[:3]:
        assert (await _send(app, boss, t.slug)).status_code == 202
    # rows of any status count toward the rate
    await _exec(
        pool, "UPDATE messages SET status = 'sent' WHERE sender_session_id = %s", (boss.id,)
    )
    before = len(await _rows(pool))
    assert_error(await _send(app, boss, targets[3].slug), 429, "rate_limited")
    assert len(await _rows(pool)) == before

    app = _app(pool, dsn, session_message_queued_max=2)
    inbox = await alone(pool, "q-inbox", superleader=True)
    senders = [await alone(pool, f"q-sender-{i}") for i in range(3)]
    # plain user messages and non-queued cross-session messages do not count
    await insert_message(pool, inbox.id)
    await insert_message(pool, inbox.id)
    await _delivered(pool, inbox, senders[0], hop=1, turn="q-turn")
    for s in senders[:2]:
        assert (await _send(app, s, inbox.slug)).status_code == 202
    before = len(await _rows(pool))
    assert_error(await _send(app, senders[2], inbox.slug), 409, "target_queue_full")
    assert len(await _rows(pool)) == before


async def test_pair_budget_stops_cross_turn_ping_pong(pool, dsn):
    app = _app(pool, dsn, session_message_pair_max_per_hour=3)
    boss = await hive(pool, "b-boss", superleader=True)
    peer = await alone(pool, "b-peer")
    third = await alone(pool, "b-third")
    # every send is made from a turn that delivered no cross-session message: hop 1
    stale = await _send(app, boss, peer.slug)
    assert stale.status_code == 202, stale.text
    await _exec(
        pool,
        "UPDATE messages SET created_at = %s WHERE id = %s",
        (datetime(2020, 1, 1, tzinfo=UTC), stale.json()["message_id"]),
    )
    for caller, target in ((boss, peer), (peer, boss), (boss, peer)):
        r = await _send(app, caller, target.slug)
        assert r.status_code == 202, r.text
    assert all(row["hop"] == 1 for row in await _rows(pool))
    before = len(await _rows(pool))
    assert_error(await _send(app, boss, peer.slug), 429, "pair_budget")
    assert_error(await _send(app, peer, boss.slug), 429, "pair_budget")
    assert len(await _rows(pool)) == before
    r = await _send(app, boss, third.slug)
    assert r.status_code == 202, r.text
    r = await _send(app, third, boss.slug)
    assert r.status_code == 202, r.text


async def test_session_tools_parity(pool, dsn):
    """The released any-mcp orchestrator plugin drives sessions_list and session_send against
    the real agent routes."""
    from any_mcp.testing import PluginHarness
    from any_mcp_tatara_orchestrator import OrchestratorPlugin

    boss = await hive(pool, "x-boss", superleader=True)
    plain = await alone(pool, "x-plain")
    app = _app(pool, dsn)
    config = {
        "base_url": f"http://orch:{AGENT_PORT}",
        "token": token(plain.id),
        "session": str(plain.id),
        "agent": "a0",
        "slug": plain.slug,
    }
    transport = httpx.ASGITransport(app=app, client=(plain.leader_ip, 4321))
    async with PluginHarness(OrchestratorPlugin, config=config, transport=transport) as h:
        listed = await h.call("sessions_list")
        sent = await h.call("session_send", {"slug": boss.slug, "text": "from the plugin"})
    assert not listed.is_error, listed.text
    assert listed.structured == (await _list(app, plain)).json()
    assert listed.structured == {"sessions": [_entry(boss, superleader=True, leader_present=True)]}
    assert not sent.is_error, sent.text
    assert set(sent.structured) == {"message_id"}
    (row,) = await _rows(pool)
    assert row["id"] == sent.structured["message_id"]
    assert row["session_id"] == boss.id
    assert row["text"] == "from the plugin"
    assert row["sender_session_id"] == plain.id
    assert row["status"] == "queued"


async def test_session_send_slash_is_command_kind(pool, dsn):
    app = _app(pool, dsn)
    boss = await hive(pool, "c-boss", superleader=True)
    plain = await alone(pool, "c-plain")
    r = await _send(app, boss, plain.slug, "/compact")
    assert r.status_code == 202, r.text
    (row,) = await _rows(pool, "id = %s", (r.json()["message_id"],))
    assert row["kind"] == "command"
    assert row["text"] == "/compact"
    assert row["session_id"] == plain.id
    assert row["sender_session_id"] == boss.id
    assert row["sender_slug"] == boss.slug
    assert row["hop"] == 1
    # plain prose and a path-like text from a superleader stay turns
    for text in ("compact", "/etc/hosts"):
        r = await _send(app, boss, plain.slug, text)
        assert r.status_code == 202, r.text
        (row,) = await _rows(pool, "id = %s", (r.json()["message_id"],))
        assert row["kind"] == "turn"


async def test_session_send_slash_from_non_superleader_stays_turn(pool, dsn):
    app = _app(pool, dsn)
    boss = await alone(pool, "n-boss", superleader=True)
    worker = await hive(pool, "n-worker")
    lone = await alone(pool, "n-lone")
    for caller in (worker, lone):
        r = await _send(app, caller, boss.slug, "/compact")
        assert r.status_code == 202, r.text
        (row,) = await _rows(pool, "id = %s", (r.json()["message_id"],))
        assert row["kind"] == "turn"
        assert row["text"] == "/compact"
        assert row["sender_session_id"] == caller.id
        assert row["session_id"] == boss.id


async def test_session_send_slash_keeps_refusals(pool, dsn):
    app = _app(pool, dsn, session_message_rate_per_minute=2)
    boss = await hive(pool, "k-boss", superleader=True)
    plain = await hive(pool, "k-plain")
    other = await alone(pool, "k-other")
    # not_leader comes before not_found and the kind decision
    assert_error(
        await _send(app, boss, other.slug, "/compact", ip=boss.member_ip), 403, "not_leader"
    )
    assert_error(await _send(app, plain, other.slug, "/compact"), 403, "target_not_superleader")
    assert await _rows(pool) == []
    for _ in range(2):
        assert (await _send(app, boss, other.slug, "/compact")).status_code == 202
    assert_error(await _send(app, boss, other.slug, "/compact"), 429, "rate_limited")
    assert len(await _rows(pool)) == 2
    assert {r["kind"] for r in await _rows(pool)} == {"command"}


async def test_promoted_command_turn_keeps_hop_count(pool, dsn):
    boss = await hive(pool, "p-boss", superleader=True)
    target = await alone(pool, "p-target")
    sent = await _send(_app(pool, dsn, session_message_max_hops=3), boss, target.slug, "/x")
    assert sent.status_code == 202, sent.text
    message_id = sent.json()["message_id"]
    # the dispatcher sent it to the target's leader, which promoted it to the turn "turn-p"
    await _exec(
        pool,
        "UPDATE messages SET status = 'sent', agent_id = %s, wrapper_command_id = 'cmd_p'"
        " WHERE id = %s",
        (target.leader_id, message_id),
    )
    async with pool.connection() as conn:
        assert await queue.link_command_turn(conn, "cmd_p", "turn-p")
    await set_agent_status(pool, target.leader_id, state="busy", current_turn_id="turn-p")

    reply = await _send(_app(pool, dsn, session_message_max_hops=3), target, boss.slug)
    assert reply.status_code == 202, reply.text
    (row,) = await _rows(pool, "id = %s", (reply.json()["message_id"],))
    assert row["hop"] == 2

    before = len(await _rows(pool))
    strict = _app(pool, dsn, session_message_max_hops=1)
    assert_error(await _send(strict, target, boss.slug), 409, "hop_limit")
    assert len(await _rows(pool)) == before

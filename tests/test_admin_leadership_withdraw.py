"""POST /agent/v1/admin/sessions/{slug}/leadership/withdraw: a pending handover request is
withdrawn by the asking session's leader or by a superleader, with status `rejected` and reason
`withdrawn`; the leadership pass never applies it afterwards."""

from __future__ import annotations

import pytest
from factories import attach_preset
from fakes.fake_kube import FakeKube
from psycopg.rows import dict_row
from test_admin_session_patch import audits
from test_agent_session_messaging import Sess, alone, hive
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import leadership

S = f"{ADMIN_PREFIX}/sessions"


@pytest.fixture
def app(pool, dsn):
    return create_app(make_settings(dsn), pool, kube=FakeKube(namespace="agents"))


async def withdraw(app, caller: Sess, slug: str, *, ip: str | None = None, **body):
    async with client_for(app, ip or caller.leader_ip) as http:
        return await http.post(
            f"{S}/{slug}/leadership/withdraw", json=body, headers=headers(caller.id)
        )


async def queue(pool, target: Sess, kind: str = "handover", reason: str = "ctx") -> int:
    frm = target.leader_id if kind == "handover" else None
    epoch = 1 if kind == "handover" else None
    async with pool.connection() as conn:
        return await leadership.insert_request(
            conn, target.id, kind, frm, None if kind == "handover" else frm, reason, epoch
        )


async def state(pool, request_id: int) -> tuple:
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT status, reject_reason, decided_at IS NOT NULL AS decided"
            " FROM leadership_requests WHERE id = %s",
            (request_id,),
        )
        r = await cur.fetchone()
    return (r["status"], r["reject_reason"], r["decided"])


async def test_leader_withdraws_its_pending_handover(app, pool):
    me = await hive(pool, "lw-leader")
    rid = await queue(pool, me)
    r = await withdraw(app, me, me.slug, request_id=rid)
    assert r.status_code == 200, r.text
    assert r.json() == {"withdrawn": [rid]}
    assert await state(pool, rid) == ("rejected", "withdrawn", True)
    # the pass never applies it afterwards
    async with pool.connection() as conn:
        assert await leadership.leadership_pass(conn, me.id) is None
    assert await state(pool, rid) == ("rejected", "withdrawn", True)
    rows = await audits(pool, me.id)
    assert rows[-1]["status"] == 200 and rows[-1]["path"] == f"{S}/{me.slug}/leadership/withdraw"


async def test_without_an_id_every_voluntary_handover_is_withdrawn(app, pool):
    # spec changed by review ruling (T-83 I2): the reconciler's `rollout` request is not the
    # leader's to withdraw, or a leader could defer a rollout forever (each re-queue restarts
    # its cap); it stays queued while every voluntary handover is withdrawn
    me = await hive(pool, "lw-all")
    first = await queue(pool, me)
    rollout = await queue(pool, me, reason="rollout")
    third = await queue(pool, me, reason="other")
    r = await withdraw(app, me, me.slug)
    assert r.status_code == 200, r.text
    assert r.json() == {"withdrawn": [first, third]}
    assert await state(pool, rollout) == ("queued", None, False)
    again = await withdraw(app, me, me.slug)
    assert again.status_code == 200 and again.json() == {"withdrawn": []}


async def test_a_rollout_request_is_refused(app, pool):
    # spec changed by review ruling (T-83 I2): withdrawing the reconciler's request by its id
    # is a 409, and the request stays queued
    me = await hive(pool, "lw-rollout")
    rid = await queue(pool, me, reason="rollout")
    r = await withdraw(app, me, me.slug, request_id=rid)
    assert_error(r, 409, "rollout_request")
    assert await state(pool, rid) == ("queued", None, False)


async def test_a_voluntary_request_beside_a_rollout_one_is_withdrawn(app, pool):
    me = await hive(pool, "lw-beside")
    rollout = await queue(pool, me, reason="rollout")
    rid = await queue(pool, me, reason="context")
    r = await withdraw(app, me, me.slug, request_id=rid)
    assert r.status_code == 200, r.text
    assert r.json() == {"withdrawn": [rid]}
    assert await state(pool, rid) == ("rejected", "withdrawn", True)
    assert (await state(pool, rollout))[0] == "queued"


async def test_a_decided_request_is_not_pending(app, pool):
    me = await hive(pool, "lw-decided")
    rid = await queue(pool, me)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE leadership_requests SET status = 'applied', decided_at = now() WHERE id = %s",
            (rid,),
        )
    r = await withdraw(app, me, me.slug, request_id=rid)
    assert_error(r, 409, "not_pending")
    assert await state(pool, rid) == ("applied", None, True)


async def test_a_user_request_or_another_sessions_request_is_not_found(app, pool):
    me = await hive(pool, "lw-own")
    other = await hive(pool, "lw-other")
    foreign = await queue(pool, other)
    user = await queue(pool, me, kind="user")
    for rid in (foreign, user, 999999):
        r = await withdraw(app, me, me.slug, request_id=rid)
        assert_error(r, 404, "not_found")
    assert (await state(pool, foreign))[0] == "queued"


async def test_a_member_is_refused(app, pool):
    me = await hive(pool, "lw-member")
    rid = await queue(pool, me)
    r = await withdraw(app, me, me.slug, ip=me.member_ip, request_id=rid)
    assert_error(r, 403, "not_leader")
    assert (await state(pool, rid))[0] == "queued"


async def test_a_regular_leader_is_refused_on_another_session(app, pool):
    me = await hive(pool, "lw-regular")
    other = await hive(pool, "lw-regular-other")
    await attach_preset(pool, other.id)
    rid = await queue(pool, other)
    r = await withdraw(app, me, other.slug, request_id=rid)
    assert_error(r, 403, "not_superleader")
    assert (await state(pool, rid))[0] == "queued"


async def test_a_superleader_withdraws_in_a_manageable_session(app, pool):
    boss = await alone(pool, "lw-boss", superleader=True)
    target = await hive(pool, "lw-target")
    await attach_preset(pool, target.id)
    rid = await queue(pool, target)
    r = await withdraw(app, boss, target.slug, request_id=rid)
    assert r.status_code == 200, r.text
    assert r.json() == {"withdrawn": [rid]}
    assert await state(pool, rid) == ("rejected", "withdrawn", True)


async def test_a_superleader_outside_its_remit_gets_404(app, pool):
    boss = await alone(pool, "lw-boss-hidden", superleader=True)
    target = await hive(pool, "lw-hidden")
    await attach_preset(pool, target.id, manageable=False)
    rid = await queue(pool, target)
    r = await withdraw(app, boss, target.slug, request_id=rid)
    assert_error(r, 404, "not_found")
    assert (await state(pool, rid))[0] == "queued"


async def test_invalid_body_422(app, pool):
    me = await hive(pool, "lw-invalid")
    r = await withdraw(app, me, me.slug, request_id="x", extra=1)
    assert_error(r, 422, "invalid_request")

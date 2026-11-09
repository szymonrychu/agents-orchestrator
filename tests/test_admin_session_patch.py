"""PATCH (and GET) /agent/v1/admin/sessions/{slug} over HTTP: the session editor gate (a
superleader on any slug it may manage, any other session's leader on its own slug only), the
service behind it, and the admin audit of every admitted call."""

from __future__ import annotations

import pytest
from factories import attach_preset
from fakes.fake_kube import FakeKube
from psycopg.rows import dict_row
from test_admin_gate import mark_trigger
from test_agent_session_messaging import Sess, alone, hive
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error
from test_session_spec_service import LIMITS, _row, _seed

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app

S = f"{ADMIN_PREFIX}/sessions"
PEERS_OFF = {"spec": {"peers": False}}


@pytest.fixture
def settings(dsn):
    return make_settings(dsn).model_copy(update={"docker_image": "docker:27-dind"})


@pytest.fixture
def app(pool, settings):
    return create_app(settings, pool, kube=FakeKube(namespace="agents"))


async def call(app, caller: Sess, method: str, slug: str, *, ip: str | None = None, **kw):
    async with client_for(app, ip or caller.leader_ip) as http:
        return await http.request(method, f"{S}/{slug}", headers=headers(caller.id), **kw)


async def audits(pool, session_id) -> list[dict]:
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT data FROM events WHERE session_id = %s AND type = 'admin_call' ORDER BY id",
            (session_id,),
        )
        return [r["data"] for r in await cur.fetchall()]


async def _patched(pool, app, settings, caller: Sess, target: Sess, **kw) -> None:
    """A 200 that bumps the target's generation once and records the changed paths."""
    await _seed(pool, settings, target, {})
    before = await _row(pool, target.id)
    r = await call(app, caller, "PATCH", target.slug, json=PEERS_OFF, **kw)
    assert r.status_code == 200, r.text
    assert r.json()["slug"] == target.slug and r.json()["spec"]["peers"] is False
    after = await _row(pool, target.id)
    assert after["generation"] == before["generation"] + 1
    rows = await audits(pool, caller.id)
    assert rows[-1]["status"] == 200 and rows[-1]["fields"] == ["peers"]
    assert rows[-1]["method"] == "PATCH" and rows[-1]["path"] == f"{S}/{target.slug}"
    assert rows[-1]["session"] == caller.slug


# ---------------------------------------------------------------- the gate


async def test_no_auth_401(app, pool):
    me = await alone(pool, "sp-noauth")
    async with client_for(app, me.leader_ip) as http:
        r = await http.patch(f"{S}/{me.slug}", json=PEERS_OFF)
        bad = await http.patch(
            f"{S}/{me.slug}", json=PEERS_OFF, headers=headers(me.id, "wrong-token")
        )
    assert_error(r, 401, "unauthorized")
    assert_error(bad, 401, "unauthorized")
    assert await audits(pool, me.id) == []


async def test_wrong_pod_ip_403(app, pool):
    me = await alone(pool, "sp-wrong-ip")
    r = await call(app, me, "PATCH", me.slug, ip="10.200.0.1", json=PEERS_OFF)
    assert_error(r, 403, "forbidden")
    assert await audits(pool, me.id) == []


async def test_regular_alone_agent_on_its_own_slug(app, pool, settings):
    me = await alone(pool, "sp-alone")
    await _patched(pool, app, settings, me, me)


async def test_regular_hive_leader_on_its_own_slug(app, pool, settings):
    me = await hive(pool, "sp-hive")
    await _patched(pool, app, settings, me, me)


async def test_regular_hive_non_leader_403(app, pool, settings):
    me = await hive(pool, "sp-hive-member")
    await _seed(pool, settings, me, {})
    before = await _row(pool, me.id)
    r = await call(app, me, "PATCH", me.slug, ip=me.member_ip, json=PEERS_OFF)
    assert_error(r, 403, "not_leader")
    assert await _row(pool, me.id) == before
    assert await audits(pool, me.id) == []


async def test_regular_leader_on_another_slug_403_same_as_unknown(app, pool, settings):
    """Another session's slug and an unknown slug answer alike: the gate refuses before any
    lookup, so a regular leader learns nothing about other sessions."""
    me = await hive(pool, "sp-regular")
    other = await alone(pool, "sp-regular-other")
    await _seed(pool, settings, other, {})
    await attach_preset(pool, other.id)
    before = await _row(pool, other.id)
    existing = await call(app, me, "PATCH", other.slug, json=PEERS_OFF)
    unknown = await call(app, me, "PATCH", "sp-nobody", json=PEERS_OFF)
    assert_error(existing, 403, "not_superleader")
    assert_error(unknown, 403, "not_superleader")
    assert existing.json() == unknown.json()
    assert await _row(pool, other.id) == before
    assert await audits(pool, me.id) == []


async def test_superleader_on_its_own_slug(app, pool, settings):
    boss = await alone(pool, "sp-boss-own", superleader=True)
    await _patched(pool, app, settings, boss, boss)


async def test_superleader_non_leader_member_on_its_own_slug(app, pool, settings):
    boss = await hive(pool, "sp-boss-member", superleader=True)
    await _patched(pool, app, settings, boss, boss, ip=boss.member_ip)


async def test_superleader_on_a_manageable_session(app, pool, settings):
    boss = await alone(pool, "sp-boss-mng", superleader=True)
    target = await alone(pool, "sp-mng-target")
    await attach_preset(pool, target.id)
    await _patched(pool, app, settings, boss, target)


async def test_trigger_created_superleader_is_admitted(app, pool, settings):
    boss = await alone(pool, "sp-boss-trig", superleader=True)
    await mark_trigger(pool, boss.id)
    target = await alone(pool, "sp-trig-target")
    await attach_preset(pool, target.id)
    await _patched(pool, app, settings, boss, target)


@pytest.mark.parametrize("kind", ["hidden", "presetless", "unknown"])
async def test_superleader_on_a_session_outside_its_remit_404(app, pool, settings, kind):
    boss = await alone(pool, f"sp-boss-{kind}", superleader=True)
    slug = "sp-nobody"
    if kind != "unknown":
        target = await alone(pool, f"sp-{kind}-target")
        await _seed(pool, settings, target, {})
        if kind == "hidden":
            await attach_preset(pool, target.id, manageable=False)
        slug = target.slug
        before = await _row(pool, target.id)
    r = await call(app, boss, "PATCH", slug, json=PEERS_OFF)
    assert_error(r, 404, "not_found")
    if kind != "unknown":
        assert await _row(pool, target.id) == before
    rows = await audits(pool, boss.id)
    assert [(a["status"], "fields" in a) for a in rows] == [(404, False)]


# ---------------------------------------------------------------- refusals are audited


async def test_guard_refusal_is_audited_with_fields(app, pool, settings):
    me = await alone(pool, "sp-guard")
    await _seed(pool, settings, me, LIMITS)
    before = await _row(pool, me.id)
    r = await call(app, me, "PATCH", me.slug, json={"spec": {"usage_quota": None}})
    assert_error(r, 403, "human_only_field")
    assert r.json()["error"]["fields"] == ["usage_quota.five_hour_pct"]
    assert r.json()["error"]["proposable"] is True  # the leader's tighten rule (Q3 A, F8)
    assert await _row(pool, me.id) == before
    rows = await audits(pool, me.id)
    assert [(a["status"], a["fields"]) for a in rows] == [(403, ["usage_quota.five_hour_pct"])]


async def test_fixed_field_refusal_is_audited_with_fields(app, pool, settings):
    me = await hive(pool, "sp-fixed")
    await _seed(pool, settings, me, {})
    r = await call(
        app, me, "PATCH", me.slug, json={"spec": {"pvc_size": "5Gi", "workdir": "other"}}
    )
    assert_error(r, 422, "session_fixed_field")
    assert r.json()["error"]["fields"] == ["pvc_size", "workdir"]
    assert "proposable" not in r.json()["error"]  # a fixed field is never a proposal item
    rows = await audits(pool, me.id)
    assert [(a["status"], a["fields"]) for a in rows] == [(422, ["pvc_size", "workdir"])]


async def test_protected_flag_is_422_and_never_echoed(app, pool, settings):
    """O10: `protected` is an unknown env field; the value next to it is never echoed."""
    me = await alone(pool, "sp-unset")
    await _seed(pool, settings, me, {})
    canary = "CANARY-sp-protected-flag"
    body = {"spec": {"env": {"B": {"value": canary, "protected": True}}}}
    r = await call(app, me, "PATCH", me.slug, json=body)
    assert_error(r, 422, "invalid_request")
    assert canary not in r.text
    rows = await audits(pool, me.id)
    assert [a["status"] for a in rows] == [422] and canary not in str(rows)


async def test_body_validation_422_is_audited(app, pool, settings):
    me = await alone(pool, "sp-badbody")
    await _seed(pool, settings, me, {})
    r = await call(app, me, "PATCH", me.slug, json={"spec": {}, "extra": 1})
    assert r.status_code == 422, r.text
    assert [a["status"] for a in await audits(pool, me.id)] == [422]


# ---------------------------------------------------------------- GET by a regular leader


async def test_regular_leader_reads_its_own_session(app, pool, settings):
    me = await hive(pool, "sp-get-own")
    pins = {"L": "ps-00000000-0a0b0c-v1"}  # a legacy pin is never shown
    await _seed(pool, settings, me, {"env": {"K": {"value": "v"}}}, versions=pins)
    r = await call(app, me, "GET", me.slug)
    assert r.status_code == 200, r.text
    assert r.json()["slug"] == me.slug
    assert r.json()["spec"]["env"] == {"K": {"value": "v", "expand": False}}
    assert "ps-00000000" not in r.text
    assert [a["status"] for a in await audits(pool, me.id)] == [200]


async def test_regular_leader_cannot_read_another_session(app, pool):
    me = await alone(pool, "sp-get-mine")
    other = await alone(pool, "sp-get-other")
    await attach_preset(pool, other.id)
    assert_error(await call(app, me, "GET", other.slug), 403, "not_superleader")
    assert_error(await call(app, me, "GET", "sp-get-nobody"), 403, "not_superleader")
    assert await audits(pool, me.id) == []


async def test_regular_hive_non_leader_cannot_read_its_own_session(app, pool, settings):
    """The gate's not_leader rule is the only guard on GET: a member that is not the leader reads
    nothing, even its own session, and no admin_call row is written."""
    me = await hive(pool, "sp-get-member")
    await _seed(pool, settings, me, {})
    assert_error(await call(app, me, "GET", me.slug, ip=me.member_ip), 403, "not_leader")
    assert (await call(app, me, "GET", me.slug)).status_code == 200  # the leader still reads it
    assert [a["status"] for a in await audits(pool, me.id)] == [200]

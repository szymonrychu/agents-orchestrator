"""PATCH /agent/v1/admin/presets/{ref} with superleader_manageable false: the superleader gives a
preset back to the user (never the other direction), refused 409 open_proposals while a preset,
grant or schedule proposal on it is active (open, rework, stale)."""

from __future__ import annotations

import pytest
from fakes.fake_kube import FakeKube
from test_admin_audit_detail import admin_rows
from test_admin_presets import M, main_call
from test_agent_session_messaging import alone
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import proposals as proposals_repo

A = f"{ADMIN_PREFIX}/presets"
FLIP = {"superleader_manageable": False}


@pytest.fixture
async def boss(pool):
    return await alone(pool, "flip-boss", superleader=True)


@pytest.fixture
def call(pool, dsn, boss):
    app = create_app(make_settings(dsn), pool, kube=FakeKube(namespace="agents"))

    async def go(method, path, **kw):
        async with client_for(app, boss.leader_ip) as http:
            return await http.request(method, path, headers=headers(boss.id), **kw)

    go.app = app
    return go


@pytest.fixture
async def p(call):
    r = await call("POST", A, json={"name": "flipme", "spec": {"env": {"PLAIN": {"value": "v"}}}})
    assert r.status_code == 201, r.text
    return r.json()


async def file_preset_proposal(call, target):
    r = await call(
        "POST",
        f"{ADMIN_PREFIX}/proposals",
        json={
            "kind": "preset",
            "target": target["id"],
            "patch": {"env": {"PLAIN": {"value": "changed"}}},
            "reason": "r",
        },
    )
    assert r.status_code == 201, r.text
    return r.json()["id"]


async def set_status(pool, proposal_id, status):
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE change_proposals SET status = %s WHERE id = %s", (status, proposal_id)
        )


async def status_of(pool, proposal_id):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT status FROM change_proposals WHERE id = %s", (proposal_id,)
        )
        return (await cur.fetchone())[0]


async def file_schedule_proposal(pool, boss, base_preset, item_after=None):
    items = []
    if item_after is not None:
        items = [proposals_repo.Item("preset_id", base_preset, item_after, True, False)]
    async with pool.connection() as conn:
        return await proposals_repo.insert(
            conn,
            kind="schedule",
            target_id="sched-1",
            target_name="sched",
            proposer_session=boss.id,
            proposer_slug=boss.slug,
            proposer_superleader=True,
            client_key=None,
            reason="r",
            base={"preset_id": base_preset, "enabled": True},
            fingerprint="f",
            items=items,
            ttl_seconds=3600,
        )


async def flag(call, target):
    return (await call("GET", f"{A}/{target['id']}")).json()["superleader_manageable"]


async def test_patch_false_flips_and_preset_becomes_unwritable(call, p, pool):
    r = await call("PATCH", f"{A}/{p['id']}", json=FLIP)
    assert r.status_code == 200, r.text
    assert r.json()["superleader_manageable"] is False
    assert_error(await call("PATCH", f"{A}/{p['id']}", json={"name": "x"}), 403, "not_manageable")
    got = await call("GET", f"{A}/{p['id']}")
    assert got.status_code == 200 and got.json()["superleader_manageable"] is False
    rows = [r for r in await admin_rows(pool) if "superleader_manageable" in r.get("fields", [])]
    assert len(rows) == 1


async def test_patch_false_with_other_fields(call, p):
    r = await call("PATCH", f"{A}/{p['id']}", json={**FLIP, "labels": ["a"]})
    assert r.status_code == 200, r.text
    assert r.json()["superleader_manageable"] is False and r.json()["labels"] == ["a"]


async def test_patch_true_422(call, p):
    r = await call("PATCH", f"{A}/{p['id']}", json={"superleader_manageable": True})
    assert r.status_code == 422, r.text
    assert await flag(call, p) is True


async def test_copy_with_false_is_still_422(call, p):
    r = await call("POST", f"{A}/{p['id']}/copy", json={"superleader_manageable": False})
    assert r.status_code == 422, r.text


async def test_flip_refused_while_preset_proposal_open(call, p, pool):
    pid = await file_preset_proposal(call, p)
    r = await call("PATCH", f"{A}/{p['id']}", json=FLIP)
    assert_error(r, 409, "open_proposals")
    assert r.json()["error"]["proposals"] == [pid]
    assert await flag(call, p) is True
    assert await status_of(pool, pid) == "open"
    assert (await main_call(call.app, "GET", f"{M}/{p['id']}")).json()["superleader_manageable"]


@pytest.mark.parametrize("status", ["rework", "stale"])
async def test_flip_refused_for_rework_and_stale(call, p, pool, status):
    pid = await file_preset_proposal(call, p)
    await set_status(pool, pid, status)
    assert_error(await call("PATCH", f"{A}/{p['id']}", json=FLIP), 409, "open_proposals")
    assert await flag(call, p) is True


async def test_flip_refused_while_project_grant_open(call, p, pool):
    async with pool.connection() as conn:
        await conn.execute("INSERT INTO projects (slug, name) VALUES ('gp', 'GP')")
    r = await call("POST", f"{A}/{p['id']}/projects", json={"project": "gp", "reason": "r"})
    assert r.status_code == 202, r.text
    gid = r.json()["id"]
    r = await call("PATCH", f"{A}/{p['id']}", json=FLIP)
    assert_error(r, 409, "open_proposals")
    assert r.json()["error"]["proposals"] == [gid]


async def test_flip_refused_while_schedule_proposal_names_preset(call, p, pool, boss):
    sid = await file_schedule_proposal(pool, boss, p["id"])
    r = await call("PATCH", f"{A}/{p['id']}", json=FLIP)
    assert_error(r, 409, "open_proposals")
    assert r.json()["error"]["proposals"] == [sid]


async def test_flip_refused_while_schedule_item_proposes_preset(call, p, pool, boss):
    other = "00000000-0000-0000-0000-000000000001"
    sid = await file_schedule_proposal(pool, boss, other, item_after=p["id"])
    r = await call("PATCH", f"{A}/{p['id']}", json=FLIP)
    assert_error(r, 409, "open_proposals")
    assert r.json()["error"]["proposals"] == [sid]


async def test_flip_ignores_schedule_proposal_naming_other_preset(call, p, pool, boss):
    other = "00000000-0000-0000-0000-000000000001"
    await file_schedule_proposal(pool, boss, other, item_after=other)
    assert (await call("PATCH", f"{A}/{p['id']}", json=FLIP)).status_code == 200


async def test_flip_ignores_a_revision_that_no_longer_names_the_preset(call, p, pool, boss):
    """Only the proposal as it stands counts: an older revision naming the preset is history."""
    other = "00000000-0000-0000-0000-000000000001"
    sid = await file_schedule_proposal(pool, boss, p["id"])
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO change_proposal_revisions (proposal_id, revision, reason, base,"
            " fingerprint) VALUES (%s, 2, 'r', %s::jsonb, 'g')",
            (sid, f'{{"preset_id": "{other}"}}'),
        )
        await conn.execute("UPDATE change_proposals SET revision = 2 WHERE id = %s", (sid,))
    assert (await call("PATCH", f"{A}/{p['id']}", json=FLIP)).status_code == 200


@pytest.mark.parametrize("status", ["applied", "withdrawn", "denied", "expired", "target_gone"])
async def test_flip_allowed_once_proposals_closed(call, p, pool, status):
    pid = await file_preset_proposal(call, p)
    await set_status(pool, pid, status)
    assert (await call("PATCH", f"{A}/{p['id']}", json=FLIP)).status_code == 200
    assert await status_of(pool, pid) == status


async def test_flip_ignores_other_targets(call, p, pool, boss):
    r = await call("POST", A, json={"name": "other", "spec": {}})
    await file_preset_proposal(call, r.json())
    async with pool.connection() as conn:
        await proposals_repo.insert(
            conn,
            kind="session",
            target_id=str(boss.id),
            target_name=boss.slug,
            proposer_session=boss.id,
            proposer_slug=boss.slug,
            proposer_superleader=True,
            client_key=None,
            reason="r",
            base={"preset_id": p["id"]},
            fingerprint="f",
            items=[],
            ttl_seconds=3600,
        )
    assert (await call("PATCH", f"{A}/{p['id']}", json=FLIP)).status_code == 200

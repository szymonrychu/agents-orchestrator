"""preset_manageable proposals (S-66, T-489, user answer Q2 B): a superleader asks the user to make
an unmanageable preset superleader-manageable through
`POST /agent/v1/admin/presets/{ref}/manageable`; the human approves or denies it on the main
port. A direct PATCH with true stays 422."""

from __future__ import annotations

import pytest
from psycopg.rows import dict_row
from test_agent_session_messaging import alone
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error
from test_api_sessions import client_for as main_client

from agents_orchestrator.api import proposals
from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import presets as presets_repo


@pytest.fixture
def app(pool, dsn):
    return create_app(make_settings(dsn), pool)


@pytest.fixture
async def boss(pool):
    return await alone(pool, "man-boss", superleader=True)


@pytest.fixture
async def preset(pool) -> presets_repo.Preset:
    async with pool.connection() as conn:
        await presets_repo.create(conn, "man-keep", {}, False)  # never the last preset
        return await presets_repo.create(conn, "man-target", {}, False)


async def _agent(app, who, method, path, **kw):
    async with client_for(app, who.leader_ip) as http:
        return await http.request(method, f"{ADMIN_PREFIX}{path}", headers=headers(who.id), **kw)


async def _request(app, who, ref, **extra):
    body = {"reason": "S-66: the hive needs to edit it", **extra}
    return await _agent(app, who, "POST", f"/presets/{ref}/manageable", json=body)


async def _main(app, method, path, **kw):
    async with main_client(app) as http:
        return await http.request(method, f"/api/v1{path}", **kw)


async def _full(app, r):
    """The full proposal view behind an agent-port ack, read on the human route."""
    async with main_client(app) as http:
        return (await http.get(f"/api/v1/proposals/{r.json()['id']}")).json()


async def _review(app, proposal, decision="approve"):
    items = [{"path": i["path"], "decision": decision} for i in proposal["revisions"][0]["items"]]
    return await _main(
        app,
        "POST",
        f"/proposals/{proposal['id']}/review",
        json={"revision": proposal["revision"], "items": items},
    )


async def _manageable(pool, preset_id) -> bool:
    async with pool.connection() as conn:
        found = await presets_repo.get(conn, preset_id)
    assert found is not None
    return found.superleader_manageable


async def _status(app, proposal_id) -> str:
    return (await _main(app, "GET", f"/proposals/{proposal_id}")).json()["status"]


async def _notices(pool, session_id) -> list[str]:
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT text FROM messages WHERE session_id = %s AND notice = 'proposal' ORDER BY id",
            (session_id,),
        )
        return [r["text"] for r in await cur.fetchall()]


def test_fingerprint_is_the_canonical_flag():
    import hashlib
    import json

    doc = json.dumps({"superleader_manageable": False}, sort_keys=True, separators=(",", ":"))
    assert proposals.fingerprint_manageable(False) == hashlib.sha256(doc.encode()).hexdigest()
    assert proposals.fingerprint_manageable(True) != proposals.fingerprint_manageable(False)
    assert proposals.MANAGEABLE_KIND == "preset_manageable"


async def test_request_files_a_proposal_and_replays(app, boss, preset):
    r = await _request(app, boss, preset.name, client_key="k1")
    assert r.status_code == 202, r.text
    proposal = await _full(app, r)
    assert (proposal["kind"], proposal["target_id"], proposal["target_name"]) == (
        "preset_manageable",
        str(preset.id),
        preset.name,
    )
    assert proposal["status"] == "open" and proposal["stale"] is False
    [item] = proposal["revisions"][0]["items"]
    assert {k: item[k] for k in ("path", "before", "after", "security", "grant", "decision")} == {
        "path": "superleader_manageable",
        "before": False,
        "after": True,
        "security": True,
        "grant": True,
        "decision": "pending",
    }
    again = await _request(app, boss, str(preset.id), client_key="k1")
    assert again.status_code == 200, again.text
    assert again.json()["id"] == proposal["id"]


async def test_request_refusals(pool, app, boss, preset):
    async with pool.connection() as conn:
        open_one = await presets_repo.create(conn, "man-open", {}, True)
    assert_error(await _request(app, boss, open_one.name), 409, "already_manageable")
    assert_error(await _request(app, boss, "no-such-preset"), 404, "not_found")
    plain = await alone(pool, "man-plain")
    assert_error(await _request(app, plain, preset.name), 403, "not_superleader")
    r = await _request(app, boss, preset.name, superleader_manageable=True)
    assert r.status_code == 422, r.text
    r = await _agent(app, boss, "POST", f"/presets/{preset.name}/manageable", json={})
    assert r.status_code == 422, r.text


async def test_request_is_audited_with_the_field_name(pool, app, boss, preset):
    await _request(app, boss, preset.name)
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT data FROM events WHERE session_id = %s AND type = 'admin_call'", (boss.id,)
        )
        [row] = await cur.fetchall()
    assert row["data"]["status"] == 202
    assert row["data"]["fields"] == ["superleader_manageable"]


async def test_proposer_sees_it_although_the_preset_is_unmanageable(app, boss, preset):
    proposal = await _full(app, await _request(app, boss, preset.name))
    r = await _agent(app, boss, "GET", "/proposals")
    assert [p["id"] for p in r.json()] == [proposal["id"]]
    r = await _agent(app, boss, "GET", f"/proposals/{proposal['id']}")
    assert r.status_code == 200 and r.json()["kind"] == "preset_manageable"
    assert r.json()["stale"] is False


async def test_not_revised_but_withdrawn(app, boss, preset):
    proposal = await _full(app, await _request(app, boss, preset.name))
    r = await _agent(app, boss, "POST", f"/proposals/{proposal['id']}/revise", json={"patch": {}})
    assert_error(r, 422, "not_revisable")
    r = await _agent(app, boss, "POST", f"/proposals/{proposal['id']}/withdraw")
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "withdrawn"


async def test_approve_sets_the_column_and_notifies(pool, app, boss, preset):
    proposal = await _full(app, await _request(app, boss, preset.name))
    assert await _manageable(pool, preset.id) is False
    r = await _review(app, proposal)
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "applied"
    assert await _manageable(pool, preset.id) is True
    [notice] = await _notices(pool, boss.id)
    assert f"P-{proposal['id']}" in notice and "superleader_manageable" in notice
    r = await _agent(app, boss, "PATCH", f"/presets/{preset.id}", json={"labels": ["now-mine"]})
    assert r.status_code == 200, r.text


async def test_deny_leaves_the_column(pool, app, boss, preset):
    proposal = await _full(app, await _request(app, boss, preset.name))
    r = await _review(app, proposal, "deny")
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "denied"
    assert await _manageable(pool, preset.id) is False
    assert await _notices(pool, boss.id)


async def test_human_flip_makes_the_review_stale(pool, app, boss, preset):
    proposal = await _full(app, await _request(app, boss, preset.name))
    r = await _main(
        app,
        "PUT",
        f"/presets/{preset.id}",
        json={"name": preset.name, "spec": {}, "superleader_manageable": True},
    )
    assert r.status_code == 200, r.text
    got = await _agent(app, boss, "GET", f"/proposals/{proposal['id']}")
    assert got.json()["stale"] is True
    assert_error(await _review(app, proposal), 409, "proposal_stale")
    assert await _status(app, proposal["id"]) == "stale"


async def test_deleted_preset_makes_the_review_target_gone(app, boss, preset):
    proposal = await _full(app, await _request(app, boss, preset.name))
    r = await _main(app, "DELETE", f"/presets/{preset.id}")
    assert r.status_code == 204, r.text
    assert_error(await _review(app, proposal), 409, "target_gone")
    assert await _status(app, proposal["id"]) == "target_gone"


async def test_direct_patch_true_stays_422(pool, app, boss):
    async with pool.connection() as conn:
        mine = await presets_repo.create(conn, "man-mine", {}, True)
    r = await _agent(
        app, boss, "PATCH", f"/presets/{mine.id}", json={"superleader_manageable": True}
    )
    assert r.status_code == 422, r.text


async def test_approving_one_request_marks_the_other_stale(pool, app, boss, preset):
    other = await alone(pool, "man-boss-2", superleader=True)
    first = await _full(app, await _request(app, boss, preset.name))
    second = await _full(app, await _request(app, other, preset.name, client_key="second"))
    assert first["id"] != second["id"]
    r = await _review(app, first)
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "applied"
    assert await _status(app, second["id"]) == "stale"

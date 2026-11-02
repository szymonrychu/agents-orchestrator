"""Session labels: snapshot of the preset's at creation, PATCH replaces, list filter."""

from __future__ import annotations

import pytest_asyncio
from factories import insert_session
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app

S = "/api/v1/sessions"
P = "/api/v1/presets"


@pytest_asyncio.fixture
async def http(pool, dsn):
    async with client_for(create_app(make_settings(dsn), pool)) as c:
        yield c


async def _preset(http, name, labels):
    r = await http.post(P, json={"name": name, "labels": labels})
    assert r.status_code == 201, r.text
    return r.json()["id"]


async def test_session_inherits_preset_labels_snapshot(http):
    pid = await _preset(http, "inh", ["team-a", "bot"])
    r = await http.post(S, json={"preset_id": pid})
    assert r.status_code == 201, r.text
    assert r.json()["labels"] == ["team-a", "bot"]
    assert (await http.get(f"{S}/{r.json()['id']}")).json()["labels"] == ["team-a", "bot"]


async def test_preset_edit_does_not_change_existing_session_labels(http):
    pid = await _preset(http, "snap", ["old"])
    sid = (await http.post(S, json={"preset_id": pid})).json()["id"]
    r = await http.put(f"{P}/{pid}", json={"name": "snap", "spec": {}, "labels": ["new"]})
    assert r.json()["labels"] == ["new"]
    assert (await http.get(f"{S}/{sid}")).json()["labels"] == ["old"]
    assert (await http.post(S, json={"preset_id": pid})).json()["labels"] == ["new"]


async def test_session_without_preset_has_no_labels(http):
    r = await http.post(S, json={"slug": "nopreset", "spec": {}})
    assert r.status_code == 201, r.text
    assert r.json()["labels"] == []


async def test_patch_session_labels_replaces(http):
    pid = await _preset(http, "pat", ["a", "b"])
    sid = (await http.post(S, json={"preset_id": pid})).json()["id"]
    r = await http.patch(f"{S}/{sid}/labels", json={"labels": [" C ", "d", "c"]})
    assert r.status_code == 200, r.text
    assert r.json()["labels"] == ["c", "d"]
    assert (await http.get(f"{S}/{sid}")).json()["labels"] == ["c", "d"]
    r = await http.patch(f"{S}/{sid}/labels", json={"labels": []})
    assert r.json()["labels"] == []


async def test_patch_session_labels_errors(http):
    sid = (await http.post(S, json={"preset_id": await _preset(http, "e", [])})).json()["id"]
    assert_error(
        await http.patch(f"{S}/{sid}/labels", json={"labels": ["no good"]}), 422, "invalid_request"
    )
    missing = "00000000-0000-0000-0000-0000000000aa"
    assert_error(await http.patch(f"{S}/{missing}/labels", json={"labels": []}), 404, "not_found")


async def test_sessions_list_label_filter_any_match(http, pool):
    a = await insert_session(pool, slug="la")
    b = await insert_session(pool, slug="lb")
    await insert_session(pool, slug="lc")
    for sid, labels in ((a["id"], ["x"]), (b["id"], ["y", "z"])):
        r = await http.patch(f"{S}/{sid}/labels", json={"labels": labels})
        assert r.status_code == 200, r.text

    def slugs(r):
        assert r.status_code == 200, r.text
        return sorted(s["slug"] for s in r.json())

    assert slugs(await http.get(S, params={"label": "x"})) == ["la"]
    assert slugs(await http.get(S, params=[("label", "x"), ("label", "z")])) == ["la", "lb"]
    assert slugs(await http.get(S, params={"label": " Y "})) == ["lb"]
    assert slugs(await http.get(S, params={"label": "nope"})) == []
    assert slugs(await http.get(S)) == ["la", "lb", "lc"]
    assert_error(await http.get(S, params={"label": "bad label"}), 422, "invalid_request")


async def test_sessions_list_label_filter_excludes_ended(http, pool):
    live = await insert_session(pool, slug="live")
    ended = await insert_session(pool, slug="ended", desired="deleting")
    for sid in (live["id"], ended["id"]):
        async with pool.connection() as conn:
            await conn.execute("UPDATE sessions SET labels = '{x}' WHERE id = %s", (sid,))
    got = await http.get(S, params={"label": "x"})
    assert [s["slug"] for s in got.json()] == ["live"]
    assert sorted(s["slug"] for s in (await http.get(S)).json()) == ["ended", "live"]

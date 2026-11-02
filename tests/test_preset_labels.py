"""Preset labels over the API: POST/PUT accept and return them, invalid ones are 422."""

from __future__ import annotations

import pytest_asyncio
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app

P = "/api/v1/presets"


@pytest_asyncio.fixture
async def http(pool, dsn):
    async with client_for(create_app(make_settings(dsn), pool)) as c:
        yield c


async def test_preset_put_post_roundtrip_labels(http):
    r = await http.post(P, json={"name": "lab", "labels": [" Alpha ", "beta", "ALPHA"]})
    assert r.status_code == 201, r.text
    assert r.json()["labels"] == ["alpha", "beta"]
    pid = r.json()["id"]
    assert (await http.get(f"{P}/{pid}")).json()["labels"] == ["alpha", "beta"]
    r = await http.put(f"{P}/{pid}", json={"name": "lab", "spec": {}, "labels": ["gamma"]})
    assert r.status_code == 200, r.text
    assert r.json()["labels"] == ["gamma"]
    r = await http.put(f"{P}/{pid}", json={"name": "lab", "spec": {}, "labels": []})
    assert r.json()["labels"] == []


async def test_preset_labels_default_empty_and_kept_when_omitted(http):
    r = await http.post(P, json={"name": "plain"})
    assert r.json()["labels"] == []
    pid = (await http.post(P, json={"name": "kept", "labels": ["x"]})).json()["id"]
    r = await http.put(f"{P}/{pid}", json={"name": "kept", "spec": {}})
    assert r.json()["labels"] == ["x"]


async def test_preset_copy_keeps_labels(http):
    pid = (await http.post(P, json={"name": "src", "labels": ["x", "y"]})).json()["id"]
    r = await http.post(f"{P}/{pid}/copy")
    assert r.status_code == 201 and r.json()["labels"] == ["x", "y"]


async def test_invalid_label_422(http):
    for bad in (["has space"], ["a" * 41], [""], [f"l{i}" for i in range(21)]):
        assert_error(
            await http.post(P, json={"name": "bad", "labels": bad}), 422, "invalid_request"
        )

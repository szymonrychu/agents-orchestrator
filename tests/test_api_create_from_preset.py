"""POST /sessions from a preset: slug generation, snapshot, revalidation and the raw spec path."""

from __future__ import annotations

import re
import uuid

import pytest
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api import sessions as api_sessions
from agents_orchestrator.api.app import create_app

DEFAULT_ID = "00000000-0000-0000-0000-000000000001"
C = "/api/v1/sessions"
P = "/api/v1/presets"


def app_client(pool, dsn, **kw):
    return client_for(create_app(make_settings(dsn, **kw), pool))


async def make_preset(http, name, spec=None):
    r = await http.post(P, json={"name": name, "spec": spec or {}})
    assert r.status_code == 201, r.text
    return r.json()


async def test_create_from_default_preset_generates_slug(pool, dsn):
    async with app_client(pool, dsn) as http:
        r = await http.post(C, json={"preset_id": DEFAULT_ID})
        assert r.status_code == 201, r.text
        body = r.json()
        assert re.fullmatch(r"default-[a-z]{4}", body["slug"])
        assert body["spec"]["replicas"] == 1
        assert body["spec"]["resources"] == {
            "requests": {"cpu": "500m", "memory": "1Gi"},
            "limits": {"cpu": "4", "memory": "8Gi"},
        }
        assert body["preset_id"] == DEFAULT_ID
        assert body["preset_name"] == "Default"
        assert body["generation"] == 1
        got = (await http.get(f"{C}/{body['id']}")).json()
        assert got["preset_id"] == DEFAULT_ID and got["preset_name"] == "Default"


async def test_explicit_slug_and_generated_retry(pool, dsn, monkeypatch):
    async with app_client(pool, dsn) as http:
        r = await http.post(C, json={"preset_id": DEFAULT_ID, "slug": "mine"})
        assert r.status_code == 201, r.text
        assert r.json()["slug"] == "mine"
        assert_error(
            await http.post(C, json={"preset_id": DEFAULT_ID, "slug": "mine"}), 409, "conflict"
        )
        assert_error(
            await http.post(C, json={"preset_id": DEFAULT_ID, "slug": "Bad_Slug"}),
            422,
            "invalid_request",
        )
        taken = iter(["default-aaaa", "default-aaaa", "default-bbbb"])
        monkeypatch.setattr(api_sessions, "generated_slug", lambda name: next(taken))
        first = await http.post(C, json={"preset_id": DEFAULT_ID, "slug": "default-aaaa"})
        assert first.status_code == 201
        retried = await http.post(C, json={"preset_id": DEFAULT_ID})
        assert retried.status_code == 201, retried.text
        assert retried.json()["slug"] == "default-bbbb"


async def test_generated_slug_exhausted_409(pool, dsn, monkeypatch):
    async with app_client(pool, dsn) as http:
        assert (
            await http.post(C, json={"preset_id": DEFAULT_ID, "slug": "default-aaaa"})
        ).status_code == 201
        calls = []

        def always_taken(name):
            calls.append(name)
            return "default-aaaa"

        monkeypatch.setattr(api_sessions, "generated_slug", always_taken)
        assert_error(await http.post(C, json={"preset_id": DEFAULT_ID}), 409, "conflict")
        assert len(calls) == 5
        assert len((await http.get(C)).json()) == 1


@pytest.mark.parametrize(
    "body",
    [
        {"preset_id": DEFAULT_ID, "spec": {}, "slug": "x"},
        {"slug": "x"},
        {},
        {"spec": {}},
        {"spec": {"replicas": 1}},
        {"preset_id": "not-a-uuid"},
        {"preset_id": DEFAULT_ID, "nope": 1},
    ],
)
async def test_exactly_one_of_spec_or_preset_id(pool, dsn, body):
    async with app_client(pool, dsn) as http:
        assert_error(await http.post(C, json=body), 422, "invalid_request")
        assert (await http.get(C)).json() == []


async def test_preset_revalidated_at_schedule(pool, dsn):
    async with app_client(pool, dsn, max_replicas=4) as http:
        preset = await make_preset(http, "wide", {"replicas": 3})
        assert (await http.post(C, json={"preset_id": preset["id"]})).status_code == 201
    async with app_client(pool, dsn, max_replicas=2) as http:
        r = await http.post(C, json={"preset_id": preset["id"], "slug": "wide-1"})
        assert_error(r, 422, "invalid_request")
        assert r.json()["error"]["message"].startswith("preset 'wide': ")
        assert len((await http.get(C)).json()) == 1


async def test_snapshot_survives_preset_edit_and_delete(pool, dsn):
    async with app_client(pool, dsn, max_replicas=4) as http:
        preset = await make_preset(http, "snap", {"replicas": 2, "model": "claude-opus-5-5"})
        made = (await http.post(C, json={"preset_id": preset["id"], "slug": "snapped"})).json()
        assert made["spec"]["replicas"] == 2 and made["preset_id"] == preset["id"]
        edit = await http.put(f"{P}/{preset['id']}", json={"name": "snap", "spec": {"replicas": 4}})
        assert edit.status_code == 200, edit.text
        after_edit = (await http.get(f"{C}/{made['id']}")).json()
        assert after_edit["spec"] == made["spec"]
        assert after_edit["generation"] == made["generation"]
        assert after_edit["preset_id"] == preset["id"]
        assert (await http.delete(f"{P}/{preset['id']}")).status_code == 204
        after_delete = (await http.get(f"{C}/{made['id']}")).json()
        assert after_delete["spec"] == made["spec"]
        assert after_delete["generation"] == made["generation"]
        assert after_delete["preset_id"] is None
        assert after_delete["preset_name"] == "snap"


async def test_unknown_preset_404(pool, dsn):
    async with app_client(pool, dsn) as http:
        r = await http.post(C, json={"preset_id": str(uuid.uuid4())})
        assert_error(r, 404, "not_found")
        assert r.json()["error"]["message"] == "preset not found"
        assert (await http.get(C)).json() == []


async def test_defaults_filled_at_schedule(pool, dsn):
    async with app_client(pool, dsn) as http:
        preset = await make_preset(http, "bare", {"resources": {"requests": {"cpu": "250m"}}})
        assert (await http.get(f"{P}/{preset['id']}")).json()["spec"] == {
            "resources": {"requests": {"cpu": "250m"}}
        }
        first = (await http.post(C, json={"preset_id": preset["id"]})).json()
        assert first["spec"]["resources"] == {
            "requests": {"cpu": "250m", "memory": "1Gi"},
            "limits": {"cpu": "4", "memory": "8Gi"},
        }
    async with app_client(pool, dsn, default_memory_request="2Gi", default_cpu_limit="2") as http:
        second = (await http.post(C, json={"preset_id": preset["id"]})).json()
        assert second["spec"]["resources"] == {
            "requests": {"cpu": "250m", "memory": "2Gi"},
            "limits": {"cpu": "2", "memory": "8Gi"},
        }
        # the earlier session keeps what it was created with
        again = (await http.get(f"{C}/{first['id']}")).json()
        assert again["spec"] == first["spec"]


async def test_replicas_filled_from_settings_at_schedule(pool, dsn):
    async with app_client(pool, dsn, default_replicas=3, max_replicas=4) as http:
        unset = await make_preset(http, "unset", {"model": "claude-opus-5-5"})
        got = await http.post(C, json={"preset_id": unset["id"]})
        assert got.status_code == 201, got.text
        assert got.json()["spec"]["replicas"] == 3
        default = (await http.post(C, json={"preset_id": DEFAULT_ID})).json()
        assert default["spec"]["replicas"] == 1
        raw = (await http.post(C, json={"slug": "raw", "spec": {}})).json()
        assert raw["spec"]["replicas"] == 1
        assert raw["preset_id"] is None and raw["preset_name"] is None


async def test_main_port_ignores_flag(pool, dsn):
    async with app_client(pool, dsn) as http:
        p = await make_preset(http, "unmanaged")
        assert p["superleader_manageable"] is False
        assert (await http.post(C, json={"preset_id": p["id"], "slug": "m-one"})).status_code == 201
        raw = await http.post(C, json={"slug": "m-two", "spec": {}})
        assert raw.status_code == 201, raw.text

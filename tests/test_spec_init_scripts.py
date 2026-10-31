"""SessionSpec.init_scripts: limits, hash, preset round trip and the create-from-preset snapshot."""

from __future__ import annotations

import hashlib
import json

import pytest
from pydantic import ValidationError
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app
from agents_orchestrator.spec import INIT_DIR, SessionSpec, init_scripts_hash

P = "/api/v1/presets"
C = "/api/v1/sessions"


def test_init_scripts_limits():
    assert SessionSpec().init_scripts == []
    assert len(SessionSpec(init_scripts=["x"] * 20).init_scripts) == 20
    assert len(SessionSpec(init_scripts=["a" * 16384]).init_scripts[0]) == 16384
    with pytest.raises(ValidationError, match=r"init_scripts\[1\]"):
        SessionSpec(init_scripts=["ok", "   "])
    with pytest.raises(ValidationError, match=r"init_scripts\[0\]"):
        SessionSpec(init_scripts=[""])
    with pytest.raises(ValidationError, match=r"init_scripts\[0\]"):
        SessionSpec(init_scripts=["a" * 16385])
    with pytest.raises(ValidationError, match="at most 20"):
        SessionSpec(init_scripts=["x"] * 21)
    with pytest.raises(ValidationError):
        SessionSpec(init_scripts=["x", 3])


def test_init_scripts_hash():
    assert INIT_DIR == ".agent-init"
    scripts = ["echo a", "echo b"]
    expected = hashlib.sha256(json.dumps(scripts, separators=(",", ":")).encode()).hexdigest()
    assert init_scripts_hash(scripts) == expected
    assert init_scripts_hash([]) == hashlib.sha256(b"[]").hexdigest()
    assert init_scripts_hash(["echo a", "echo c"]) != expected
    assert init_scripts_hash(["echo b", "echo a"]) != expected


async def test_preset_round_trips_init_scripts(pool, dsn):
    app = create_app(make_settings(dsn, max_replicas=4), pool)
    async with client_for(app) as http:
        r = await http.post(P, json={"name": "withinit", "spec": {"init_scripts": ["echo 1"]}})
        assert r.status_code == 201, r.text
        assert r.json()["spec"] == {"init_scripts": ["echo 1"]}
        pid = r.json()["id"]
        r = await http.put(
            f"{P}/{pid}", json={"name": "withinit", "spec": {"init_scripts": ["a", "b"]}}
        )
        assert r.status_code == 200, r.text
        assert r.json()["spec"] == {"init_scripts": ["a", "b"]}
        r = await http.post(P, json={"name": "plain", "spec": {}})
        assert "init_scripts" not in r.json()["spec"]
        for bad in ([""], ["x"] * 21, ["a" * 16385]):
            assert_error(
                await http.post(P, json={"name": "bad", "spec": {"init_scripts": bad}}),
                422,
                "invalid_request",
            )
        assert_error(
            await http.put(
                f"{P}/{pid}", json={"name": "withinit", "spec": {"init_scripts": [" "]}}
            ),
            422,
            "invalid_request",
        )


async def test_session_from_preset_snapshots_init_scripts(pool, dsn):
    app = create_app(make_settings(dsn, max_replicas=4), pool)
    async with client_for(app) as http:
        r = await http.post(P, json={"name": "snap", "spec": {"init_scripts": ["echo hi"]}})
        pid = r.json()["id"]
        r = await http.post(C, json={"preset_id": pid})
        assert r.status_code == 201, r.text
        assert r.json()["spec"]["init_scripts"] == ["echo hi"]


def test_init_scripts_hash_covers_template_and_rendered_bytes():
    templates = ["echo {{ env.A }}"]
    plain = ["echo a"]
    assert init_scripts_hash(plain, plain) == init_scripts_hash(plain)  # nothing templated
    one = init_scripts_hash(templates, ["echo 1"])
    assert one != init_scripts_hash(templates, ["echo 2"])  # env changed
    assert one != init_scripts_hash(["echo {{ env.B }}"], ["echo 1"])  # template changed
    assert one != init_scripts_hash(["echo 1"])

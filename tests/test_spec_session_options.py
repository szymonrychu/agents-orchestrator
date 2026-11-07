"""The session_messaging and kube_cluster_access spec options, the gate and the new settings.

Design D1 (sparse strict fields), D3 (setting gate and /meta), D4 and D10 (settings).
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import presets as repo
from agents_orchestrator.settings import Settings, SettingsError
from agents_orchestrator.spec import SessionSpec, SpecInvalid, validate_spec

C = "/api/v1/sessions"
P = "/api/v1/presets"
OPTIONS = ("session_messaging", "kube_cluster_access")
GATE = "Kubernetes access is not configured (ORCH_KUBE_ACCESS_SERVICE_ACCOUNT is unset)"


def app_client(pool, dsn, **kw):
    return client_for(create_app(make_settings(dsn, **kw), pool))


async def stored_spec(pool, session_id: str) -> dict:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT spec FROM sessions WHERE id = %s", (session_id,))
        row = await cur.fetchone()
    return row[0]


async def test_options_sparse_and_strict(pool, dsn):
    for option in OPTIONS:
        assert getattr(SessionSpec(), option) is None
        assert option not in SessionSpec().model_dump(exclude_none=True)
        assert option not in SessionSpec().model_dump(exclude_unset=True)
        assert SessionSpec.model_validate({option: False}).model_dump(exclude_none=True)[
            option
        ] is (False)
    async with app_client(pool, dsn, kube_access_service_account="sa") as http:
        for option in OPTIONS:
            for bad in ("yes", "true", 1, 0, 2.0):
                r = await http.post(C, json={"slug": "bad", "spec": {option: bad}})
                assert_error(r, 422, "invalid_request")
                assert option in r.json()["error"]["message"]
                r = await http.post(P, json={"name": "bad", "spec": {option: bad}})
                assert_error(r, 422, "invalid_request")
                assert option in r.json()["error"]["message"]

        plain = await http.post(C, json={"slug": "plain", "spec": {"replicas": 1}})
        assert plain.status_code == 201, plain.text
        row = await stored_spec(pool, plain.json()["id"])
        assert not set(OPTIONS) & set(row), row

        off = await http.post(
            C,
            json={
                "slug": "off",
                "spec": {"session_messaging": False, "kube_cluster_access": False},
            },
        )
        assert off.status_code == 201, off.text
        row = await stored_spec(pool, off.json()["id"])
        assert row["session_messaging"] is False and row["kube_cluster_access"] is False

        r = await http.post(P, json={"name": "plain", "spec": {"replicas": 2}})
        assert r.status_code == 201, r.text
        assert not set(OPTIONS) & set(r.json()["spec"])
        got = (await http.get(f"{P}/{r.json()['id']}")).json()
        assert not set(OPTIONS) & set(got["spec"])
        async with pool.connection() as conn:
            preset = await repo.get(conn, r.json()["id"])
        assert not set(OPTIONS) & set(preset.spec)


async def test_preset_carries_options_to_session(pool, dsn):
    async with app_client(pool, dsn, kube_access_service_account="sa") as http:
        spec = {"replicas": 2, "session_messaging": True, "kube_cluster_access": True}
        made = await http.post(P, json={"name": "both", "spec": spec})
        assert made.status_code == 201, made.text
        assert made.json()["spec"] == spec
        got = (await http.get(f"{P}/{made.json()['id']}")).json()
        assert got["spec"] == spec

        s = await http.post(C, json={"preset_id": made.json()["id"], "slug": "both-s"})
        assert s.status_code == 201, s.text
        assert s.json()["spec"]["session_messaging"] is True
        assert s.json()["spec"]["kube_cluster_access"] is True
        view = (await http.get(f"{C}/{s.json()['id']}")).json()
        assert view["spec"]["session_messaging"] is True
        assert view["spec"]["kube_cluster_access"] is True
        row = await stored_spec(pool, s.json()["id"])
        assert row["session_messaging"] is True and row["kube_cluster_access"] is True


async def test_kube_access_gated_by_setting(pool, dsn):
    off = make_settings(dsn)
    on = make_settings(dsn, kube_access_service_account="sa")
    spec = SessionSpec(kube_cluster_access=True)
    with pytest.raises(SpecInvalid) as e:
        validate_spec(spec, off)
    assert GATE in e.value.errors
    validate_spec(spec, on)
    validate_spec(SessionSpec(kube_cluster_access=False), off)
    validate_spec(SessionSpec(session_messaging=True), off)
    validate_spec(spec, off, stored=SessionSpec(kube_cluster_access=True))
    for stored in (SessionSpec(), SessionSpec(kube_cluster_access=False)):
        with pytest.raises(SpecInvalid):
            validate_spec(spec, off, stored=stored)

    async with pool.connection() as conn:
        kept = await repo.create(conn, "kept", {"kube_cluster_access": True})
        bare = await repo.create(conn, "bare", {})

    async with client_for(create_app(off, pool)) as http:
        for r in (
            await http.post(C, json={"slug": "ka", "spec": {"kube_cluster_access": True}}),
            await http.post(P, json={"name": "ka", "spec": {"kube_cluster_access": True}}),
            await http.put(
                f"{P}/{bare.id}", json={"name": "bare", "spec": {"kube_cluster_access": True}}
            ),
            await http.post(C, json={"preset_id": str(kept.id), "slug": "kept-s"}),
        ):
            assert_error(r, 422, "invalid_request")
            assert GATE in r.json()["error"]["message"]
        r = await http.put(
            f"{P}/{kept.id}",
            json={"name": "kept", "spec": {"kube_cluster_access": True, "replicas": 2}},
        )
        assert r.status_code == 200, r.text
        assert r.json()["spec"] == {"kube_cluster_access": True, "replicas": 2}
        r = await http.post(C, json={"slug": "ka-off", "spec": {"kube_cluster_access": False}})
        assert r.status_code == 201, r.text

    async with client_for(create_app(on, pool)) as http:
        r = await http.post(C, json={"slug": "ka", "spec": {"kube_cluster_access": True}})
        assert r.status_code == 201, r.text
        r = await http.post(P, json={"name": "ka", "spec": {"kube_cluster_access": True}})
        assert r.status_code == 201, r.text
        r = await http.put(
            f"{P}/{bare.id}", json={"name": "bare", "spec": {"kube_cluster_access": True}}
        )
        assert r.status_code == 200, r.text


def test_old_kube_access_field_is_refused():
    with pytest.raises(ValidationError):
        SessionSpec.model_validate({"kube_access": True})


async def test_meta_kube_cluster_access_available(pool, dsn):
    async with app_client(pool, dsn) as http:
        assert (await http.get("/api/v1/meta")).json()["kube_cluster_access"] == {
            "available": False
        }
    async with app_client(pool, dsn, kube_access_service_account="orch-kube-access") as http:
        assert (await http.get("/api/v1/meta")).json()["kube_cluster_access"] == {"available": True}
        assert "kube_access" not in (await http.get("/api/v1/meta")).json()


async def test_meta_kube_namespace_access(pool, dsn):
    async with app_client(pool, dsn) as http:
        assert (await http.get("/api/v1/meta")).json()["kube_namespace_access"] == {
            "available": False,
            "namespaces": [],
            "tiers": [],
        }
    async with app_client(
        pool,
        dsn,
        kube_namespace_access_namespaces="media,games",
        kube_namespace_access_tiers="read,exec",
        kube_namespace_access_role_prefix="o-ns-",
    ) as http:
        assert (await http.get("/api/v1/meta")).json()["kube_namespace_access"] == {
            "available": True,
            "namespaces": ["media", "games"],
            "tiers": ["read", "exec"],
        }


BOUNDED = (
    "session_message_rate_per_minute",
    "session_message_queued_max",
    "session_message_max_hops",
    "session_message_pair_max_per_hour",
)


def test_new_settings_defaults_and_bounds(monkeypatch):
    s = Settings(database_url="postgresql://x/y")
    assert s.kube_access_service_account == ""
    assert s.kube_access_available is False
    assert s.agent_service_account == "default"
    assert s.session_message_rate_per_minute == 10
    assert s.session_message_queued_max == 20
    assert s.session_message_max_hops == 8
    assert s.session_message_pair_max_per_hour == 30

    monkeypatch.setenv("ORCH_KUBE_ACCESS_SERVICE_ACCOUNT", "orch-kube-access")
    monkeypatch.setenv("ORCH_AGENT_SERVICE_ACCOUNT", "orch-agent")
    for name in BOUNDED:
        monkeypatch.setenv(f"ORCH_{name.upper()}", "1")
    s = Settings(database_url="postgresql://x/y")
    assert s.kube_access_service_account == "orch-kube-access"
    assert s.kube_access_available is True
    assert s.agent_service_account == "orch-agent"
    for name in BOUNDED:
        assert getattr(s, name) == 1

    for name in BOUNDED:
        monkeypatch.setenv(f"ORCH_{name.upper()}", "0")
    with pytest.raises(SettingsError) as e:
        Settings(database_url="postgresql://x/y")
    for name in BOUNDED:
        assert f"ORCH_{name.upper()}" in e.value.problems

"""O11: a canary readable value and canary vault error texts never leave the places allowed for
them. The readable value is pinned (sessions.secret_versions) and rendered into the pod env, and
nowhere else: no response body, event, log record or other row. The vault error canary appears
nowhere at all."""

from __future__ import annotations

import base64
import json
import logging
import uuid

import httpx
import pytest
from test_agent_session_messaging import alone
from test_api_agent import client_for as agent_client
from test_api_agent import headers
from test_api_agent import make_settings as agent_settings
from test_api_sessions import client_for

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import export
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.k8s import render

C = "/api/v1/sessions"
P = "/api/v1/presets"
A = f"{ADMIN_PREFIX}/presets"
V = f"{ADMIN_PREFIX}/vault"
PROTECTED = "11111111-1111-4111-8111-111111111111"
READABLE = "22222222-2222-4222-8222-222222222222"
FREE = "33333333-3333-4333-8333-333333333333"
CANARY_VALUE = "CANARY-READABLE-VALUE-7f3a"
CANARY_ERROR = "CANARY-VAULT-ERROR-TEXT-91c2"


class CanaryVault:
    """A vault whose readable entry holds CANARY_VALUE; in every failure mode its answer is
    laden with CANARY_ERROR (message, extra keys, raw body)."""

    def __init__(self):
        self.mode = "ok"

    def handler(self, req: httpx.Request) -> httpx.Response:
        if req.method == "DELETE":
            return httpx.Response(204)
        failure = self._failure()
        if failure is not None:
            return failure
        path = req.url.path
        if path.endswith("/resolve"):
            body = json.loads(req.content)
            names = {env: {"name": "entry"} for env in body["refs"]}
            return httpx.Response(
                200, json={"resolved": names, "unknown": [], "bad_env": [], "mismatched": []}
            )
        if path.endswith("/pins"):
            body = json.loads(req.content)
            pins, values = {}, {}
            for env, ref in body["refs"].items():
                if ref["entry"] == READABLE:
                    values[env] = CANARY_VALUE
                else:
                    pins[env] = {
                        "secret": f"vault-{ref['entry']}-v1",
                        "key": ref["env"],
                        "version": 1,
                    }
            return httpx.Response(200, json={"pins": pins, "values": values})
        return httpx.Response(
            200,
            json=[
                {
                    "id": FREE,
                    "name": "free",
                    "description": "unattached",
                    "labels": [],
                    "envs": [{"env": "HOST", "protected": False, "value": CANARY_VALUE}],
                    "attachments": [],
                }
            ],
        )

    def _failure(self) -> httpx.Response | None:
        if self.mode == "refuse":
            error = {
                "code": "vault_label_mismatch",
                "message": CANARY_ERROR,
                "envs": ["X"],
                "detail": CANARY_ERROR,
            }
            return httpx.Response(422, json={"error": error})
        if self.mode == "invalid":
            return httpx.Response(
                422, json={"error": {"code": "invalid_request", "message": CANARY_ERROR}}
            )
        if self.mode == "server":
            return httpx.Response(500, text=CANARY_ERROR)
        if self.mode == "nonjson":
            return httpx.Response(200, text=CANARY_ERROR)
        return None


def ref(entry, env):
    return {"vault": entry, "env": env}


async def table_texts(pool) -> list[tuple[str, str]]:
    """Every row of every table as text; the readable pin column is the one allowed place."""
    out: list[tuple[str, str]] = []
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
        )
        for (table,) in await cur.fetchall():
            expr = "to_jsonb(t) - 'secret_versions'" if table == "sessions" else "to_jsonb(t)"
            cur = await conn.execute(f'SELECT ({expr})::text FROM "{table}" t')
            out.extend((table, row[0]) for row in await cur.fetchall())
    return out


async def test_vault_canary_never_leaks(pool, dsn, caplog):
    caplog.set_level(logging.DEBUG)
    vault = CanaryVault()
    settings = agent_settings(dsn).model_copy(
        update={
            "vault_url": "http://vault",
            "vault_token": "t",
            "kube_cluster_access_enabled": True,
        }
    )
    app = create_app(settings, pool, vault_transport=httpx.MockTransport(vault.handler))
    boss = await alone(pool, "canary-boss", superleader=True)
    bodies: list[str] = []

    def seen(r: httpx.Response) -> httpx.Response:
        bodies.append(r.text)
        return r

    async def admin(method, path, **kw):
        async with agent_client(app, boss.leader_ip) as http:
            return seen(await http.request(method, path, headers=headers(boss.id), **kw))

    spec = {"env": {"X": ref(PROTECTED, "TOKEN"), "Y": ref(READABLE, "NOTE")}}
    async with client_for(app) as http:
        r = seen(
            await http.post(
                P,
                json={"name": "canary", "spec": spec, "superleader_manageable": True},
            )
        )
        assert r.status_code == 201, r.text
        preset_id = r.json()["id"]
        r = seen(await http.post(C, json={"preset_id": preset_id, "slug": "canary"}))
        assert r.status_code == 201, r.text
        session_id = r.json()["id"]
        seen(await http.get(f"{C}/{session_id}"))
        seen(await http.get(C))
        seen(await http.get(P))
        seen(await http.get(f"{P}/{preset_id}"))
        seen(await http.post(f"{P}/{preset_id}/copy", json={"name": "canary-copy"}))
        seen(await http.get(f"{C}/{session_id}/events"))

        # the readable value is in the rendered pod env and nowhere else (positive control)
        async with pool.connection() as conn:
            session = await sessions_repo.get(conn, uuid.UUID(session_id))
            lines = [ln async for ln in export.export_session_history(conn, session.id)]
        bodies.extend(lines)
        entries = {e["name"]: e for e in render.env_entries(session)}
        assert entries["Y"] == {"name": "Y", "value": CANARY_VALUE}
        assert "value" not in entries["X"] and "secretKeyRef" in json.dumps(entries["X"])

        # the superleader's views: an unattached entry's readable value stays hidden
        r = await admin("GET", V)
        assert r.status_code == 200, r.text
        assert r.json()["entries"][0]["envs"][0]["value"] is None
        r = await admin("PUT", f"{A}/canary/vault/{PROTECTED}", json={"envs": {"TOKEN": "Z"}})
        assert r.status_code == 200, r.text

        # project grants (T-120): views, proposals and notices carry names only
        r = seen(await http.post("/api/v1/projects", json={"slug": "canary-x", "name": "X"}))
        assert r.status_code == 201, r.text
        grant = {"project": "canary-x", "reason": "canary"}
        r = await admin("POST", f"{A}/canary/projects", json=grant)
        assert r.status_code == 202, r.text
        r = await admin("POST", f"{ADMIN_PREFIX}/sessions/canary/projects", json=grant)
        assert r.status_code == 202, r.text
        extra = {"extra_projects": ["canary-x"]}
        r = seen(await http.put(f"{C}/{session_id}/extra-projects", json=extra))
        assert r.status_code == 200, r.text
        r = seen(await http.put(f"{P}/{preset_id}/extra-projects", json=extra))
        assert r.status_code == 200, r.text
        r = await admin("DELETE", f"{ADMIN_PREFIX}/sessions/canary/projects/canary-x")
        assert r.status_code == 200, r.text
        r = await admin("DELETE", f"{A}/canary/projects/canary-x")
        assert r.status_code == 200, r.text
        seen(await http.get("/api/v1/proposals", params={"status": "all"}))

        # every failure mode of the vault, on every route that calls it
        for mode in ("refuse", "invalid", "server", "nonjson"):
            vault.mode = mode
            seen(await http.post(C, json={"preset_id": preset_id, "slug": f"fail-{mode}"}))
            seen(
                await http.post(
                    P,
                    json={"name": f"fail-{mode}", "spec": {"env": {"Q": ref(PROTECTED, "TOKEN")}}},
                )
            )
            seen(await http.put(f"{P}/{preset_id}", json={"name": "canary", "spec": spec}))
            await admin("PUT", f"{A}/canary/vault/{READABLE}", json={"envs": {"NOTE": "W"}})
            await admin("DELETE", f"{A}/canary/vault/{PROTECTED}")
            await admin("GET", V)
            await admin("GET", f"{A}/canary")
            await admin("POST", f"{ADMIN_PREFIX}/sessions", json={"preset_id": preset_id})

    forbidden = [CANARY_VALUE, CANARY_ERROR]
    forbidden += [base64.b64encode(x.encode()).decode().rstrip("=") for x in forbidden]
    rows = await table_texts(pool)
    for needle in forbidden:
        assert not [b for b in bodies if needle in b], f"{needle} in a response"
        assert needle not in caplog.text, f"{needle} in a log record"
        assert not [t for t, row in rows if needle in row], f"{needle} in a row"
    # the pinned readable value is stored in the one allowed column, the error text nowhere
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT coalesce(secret_versions::text, '') FROM sessions")
        pinned = " ".join(row[0] for row in await cur.fetchall())
        cur = await conn.execute("SELECT count(*) FROM events WHERE type = 'admin_call'")
        assert (await cur.fetchone())[0] >= 2
    assert CANARY_VALUE in pinned and CANARY_ERROR not in pinned


@pytest.mark.parametrize("mode", ["refuse", "invalid", "server", "nonjson"])
def test_failure_modes_are_laden(mode):
    """Guard: the fake really puts the canary on the wire in every failure mode."""
    vault = CanaryVault()
    vault.mode = mode
    r = vault._failure()
    assert r is not None and CANARY_ERROR in r.text


CANARY_TOKENS = {
    "taskboard_token": "CANARY-TASKBOARD-TOKEN-5b1d",
    "usage_token": "CANARY-USAGE-TOKEN-0e7c",
    "vault_token": "CANARY-VAULT-TOKEN-a42f",
}


async def test_superleader_reads_never_leak_tokens_or_values(pool, dsn, tmp_path, caplog):
    """T-120 slice 2: the new superleader reads (sessions list, meta, archives, activity, events,
    messages, locks, commands, vault list, usage relay) carry names and ids only. Canary service
    tokens, a readable vault value and an any-mcp config file that names a token env appear in
    no answer and no log record."""
    caplog.set_level(logging.DEBUG)
    config = tmp_path / "any-mcp.yaml"
    config.write_text("plugins:\n  - name: tracker\n    env:\n      TOKEN: ${TRACKER_TOKEN_ENV}\n")
    vault = CanaryVault()

    def usage(req: httpx.Request) -> httpx.Response:
        assert req.headers["authorization"] == f"Bearer {CANARY_TOKENS['usage_token']}"
        return httpx.Response(200, json={"items": [], "total": 0})

    settings = agent_settings(dsn).model_copy(
        update={
            "vault_url": "http://vault",
            "usage_url": "http://usage.test",
            "any_mcp_config_file": str(config),
            **CANARY_TOKENS,
        }
    )
    app = create_app(settings, pool, vault_transport=httpx.MockTransport(vault.handler))
    app.state.admin_http = httpx.AsyncClient(transport=httpx.MockTransport(usage))
    boss = await alone(pool, "canary-reader", superleader=True)
    peer = await alone(pool, "canary-peer")
    bodies: list[str] = []
    # The fixture has no api S3 pair, so the three archives reads answer 503 `unavailable`
    # (their answer is still scanned); every other read has a real backing and must answer 200.
    no_s3 = 503
    gets = {
        f"{ADMIN_PREFIX}/sessions": 200,
        f"{ADMIN_PREFIX}/meta": 200,
        f"{ADMIN_PREFIX}/archives": no_s3,
        f"{ADMIN_PREFIX}/archives/settings": 200,
        f"{ADMIN_PREFIX}/archives/cleanup/preview": no_s3,
        f"{ADMIN_PREFIX}/sessions/{peer.slug}/activity": 200,
        f"{ADMIN_PREFIX}/sessions/{peer.slug}/events": 200,
        f"{ADMIN_PREFIX}/sessions/{peer.slug}/messages": 200,
        f"{ADMIN_PREFIX}/sessions/{peer.slug}/locks": 200,
        f"{ADMIN_PREFIX}/sessions/{peer.slug}/commands": 200,
        f"{ADMIN_PREFIX}/usage/history": 200,
        f"{ADMIN_PREFIX}/usage/sessions": 200,
        V: 200,
    }
    async with agent_client(app, boss.leader_ip) as http:
        for path, expected in gets.items():
            r = await http.get(path, headers=headers(boss.id))
            assert r.status_code == expected, (path, r.status_code, r.text)
            bodies.append(r.text)
    forbidden = [CANARY_VALUE, CANARY_ERROR, *CANARY_TOKENS.values()]
    forbidden += [base64.b64encode(x.encode()).decode().rstrip("=") for x in forbidden]
    for needle in forbidden:
        assert not [b for b in bodies if needle in b], f"{needle} in a response"
        assert needle not in caplog.text, f"{needle} in a log record"

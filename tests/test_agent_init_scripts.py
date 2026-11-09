"""Agent API init scripts: GET|PUT /agent/v1/init-scripts and the orchestrator plugin parity."""

from __future__ import annotations

import asyncio
import uuid

import httpx
from factories import insert_agent, insert_session
from test_api_agent import AGENT_PORT, IP0, client_for, headers, make_settings, set_ip, token

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.spec import WORKDIR_ROOT, SessionSpec

URL = "/agent/v1/init-scripts"


async def preset_session(pool, scripts=None):
    """A session with one agent (IP0) created from a stored preset."""
    spec = {"replicas": 1, "init_scripts": scripts or ["echo old"]}
    async with pool.connection() as conn:
        preset = await presets_repo.create(conn, f"agent-p-{uuid.uuid4().hex[:8]}", spec)
    c = await insert_session(pool, spec=spec)
    async with pool.connection() as conn:
        await conn.execute("UPDATE sessions SET preset_id = %s WHERE id = %s", (preset.id, c["id"]))
    a = await insert_agent(pool, c["id"], ordinal=0, ready=True)
    await set_ip(pool, a["id"], IP0)
    return c, preset


async def stored(pool, session_id, preset_id):
    async with pool.connection() as conn:
        row = await (
            await conn.execute("SELECT spec, generation FROM sessions WHERE id = %s", (session_id,))
        ).fetchone()
        preset = await presets_repo.get(conn, preset_id)
    return row[0]["init_scripts"], row[1], preset.spec["init_scripts"]


async def test_agent_get_init_scripts(pool, dsn):
    c, preset = await preset_session(pool)
    async with client_for(create_app(make_settings(dsn), pool)) as http:
        r = await http.get(URL, headers=headers(c["id"]))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["session_id"] == str(c["id"])
    assert body["scripts"] == ["echo old"]
    assert body["preset_id"] == str(preset.id)
    assert body["status"] is None
    assert body["preset_updated"] is False
    base = f"{WORKDIR_ROOT}/{SessionSpec().workdir}/.agent-init"
    assert body["script_path"] == f"{base}/init.sh"
    assert body["log_path"] == f"{base}/init.log"


async def test_agent_put_writes_session_and_preset(pool, dsn):
    c, preset = await preset_session(pool)
    async with client_for(create_app(make_settings(dsn), pool)) as http:
        r = await http.put(URL, headers=headers(c["id"]), json={"scripts": ["echo a", "echo b"]})
        bad = await http.put(URL, headers=headers(c["id"]), json={"scripts": ["  "]})
    assert r.status_code == 200, r.text
    assert r.json()["preset_updated"] is True
    assert r.json()["scripts"] == ["echo a", "echo b"]
    assert bad.status_code == 422
    assert bad.json()["error"]["code"] == "invalid_request"
    session_scripts, generation, preset_scripts = await stored(pool, c["id"], preset.id)
    assert session_scripts == preset_scripts == ["echo a", "echo b"]
    assert generation == c["generation"]


async def test_agent_init_scripts_auth(pool, dsn):
    c, preset = await preset_session(pool)
    other, _ = await preset_session(pool)
    app = create_app(make_settings(dsn), pool)
    body = {"scripts": ["echo evil"]}
    async with client_for(app) as http:
        no_token = await http.put(URL, headers={"X-Orch-Session": str(c["id"])}, json=body)
        wrong = await http.put(URL, headers=headers(c["id"], token(other["id"])), json=body)
        get_no_token = await http.get(URL, headers={"X-Orch-Session": str(c["id"])})
    async with client_for(app, "10.9.9.9") as http:
        stranger = await http.put(URL, headers=headers(c["id"]), json=body)
        stranger_get = await http.get(URL, headers=headers(c["id"]))
    assert no_token.status_code == get_no_token.status_code == 401
    assert wrong.status_code == 401
    assert stranger.status_code == stranger_get.status_code == 403
    assert stranger.json()["error"]["code"] == "forbidden"
    assert (await stored(pool, c["id"], preset.id))[::2] == (["echo old"], ["echo old"])


async def test_agent_token_of_other_session_cannot_name_it(pool, dsn):
    """The session is the token's: another session's id with its own token never reaches c."""
    c, preset = await preset_session(pool)
    other = await insert_session(pool, spec={"replicas": 1})
    async with client_for(create_app(make_settings(dsn), pool)) as http:
        r = await http.put(
            URL, headers=headers(other["id"], token(other["id"])), json={"scripts": ["x"]}
        )
    assert r.status_code == 403
    assert (await stored(pool, c["id"], preset.id))[::2] == (["echo old"], ["echo old"])


async def test_plugin_parity(pool, dsn):
    from any_mcp.testing import PluginHarness
    from any_mcp_tatara_orchestrator import OrchestratorPlugin

    c, preset = await preset_session(pool)
    app = create_app(make_settings(dsn), pool)
    config = {
        "base_url": f"http://orch:{AGENT_PORT}",
        "token": token(c["id"]),
        "session": str(c["id"]),
        "agent": "a0",
        "slug": c["slug"],
    }
    transport = httpx.ASGITransport(app=app, client=(IP0, 4321))
    async with PluginHarness(OrchestratorPlugin, config=config, transport=transport) as h:
        tools = {
            t["name"].rsplit("_init", 1)[-1]: t
            for t in await h.list_tools()
            if "init_scripts" in t["name"]
        }
        got = await h.call("init_scripts_get")
        edit = await h.call("init_scripts_edit", {"scripts": ["echo plugin"]})
    assert ".agent-init/" in tools["_scripts_get"]["description"]
    assert "next session" in tools["_scripts_edit"]["description"]
    assert not got.is_error, got.text
    assert got.structured["scripts"] == ["echo old"]
    assert got.structured["script_path"].endswith("/.agent-init/init.sh")
    assert not edit.is_error, edit.text
    assert edit.structured["preset_updated"] is True
    assert (await stored(pool, c["id"], preset.id))[::2] == (["echo plugin"], ["echo plugin"])
    assert uuid.UUID(got.structured["session_id"]) == c["id"]


async def _move_preset(conn, preset_id, project: str) -> None:
    await conn.execute(
        "INSERT INTO projects (slug, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (project, project),
    )
    await conn.execute("UPDATE presets SET project = %s WHERE id = %s", (project, preset_id))


async def test_put_skips_preset_write_when_preset_in_other_project(pool, dsn):
    """Plan 3.3: the agent edit reaches the source preset only when the preset is in the
    session's project; otherwise only the session is written."""
    c, preset = await preset_session(pool)
    async with pool.connection() as conn:
        await _move_preset(conn, preset.id, "b")
    async with client_for(create_app(make_settings(dsn), pool)) as http:
        r = await http.put(URL, headers=headers(c["id"]), json={"scripts": ["echo new"]})
    assert r.status_code == 200, r.text
    assert r.json()["preset_updated"] is False
    assert r.json()["scripts"] == ["echo new"]
    session_scripts, _, preset_scripts = await stored(pool, c["id"], preset.id)
    assert (session_scripts, preset_scripts) == (["echo new"], ["echo old"])


async def test_put_skips_preset_write_when_preset_moved_before_lock(pool, dsn):
    """The project check runs under the preset row lock: a move committed while the edit waits
    on that lock is seen, and the preset is not written."""
    c, preset = await preset_session(pool)
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as http:
        async with pool.connection() as conn, conn.transaction():
            await _move_preset(conn, preset.id, "b")
            put = asyncio.create_task(
                http.put(URL, headers=headers(c["id"]), json={"scripts": ["echo new"]})
            )
            done, _ = await asyncio.wait({put}, timeout=0.5)
            assert not done
        r = await put
    assert r.status_code == 200, r.text
    assert r.json()["preset_updated"] is False
    session_scripts, _, preset_scripts = await stored(pool, c["id"], preset.id)
    assert (session_scripts, preset_scripts) == (["echo new"], ["echo old"])

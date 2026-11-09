"""Agent API identity: require_agent() and GET /agent/v1/me (D1, D22)."""

from __future__ import annotations

import uuid

import httpx
from factories import insert_agent, insert_session, set_agent_status

from agents_orchestrator.api.app import create_app
from agents_orchestrator.settings import Settings
from agents_orchestrator.tokens import derive_tokens

KEY = "k" * 32
AGENT_PORT = 8001
IP0, IP1 = "10.9.0.10", "10.9.0.11"


def make_settings(dsn: str) -> Settings:
    return Settings(
        database_url=dsn,
        allowed_cidrs="10.1.2.0/24",
        agent_image="img:1",
        master_key=KEY,
        agent_port=AGENT_PORT,
    )


def token(session_id) -> str:
    return derive_tokens(KEY.encode(), session_id).agent


def client_for(app, ip: str = IP0) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app, client=(ip, 4321))
    return httpx.AsyncClient(transport=transport, base_url=f"http://orch:{AGENT_PORT}")


def headers(session_id, tok: str | None = None) -> dict[str, str]:
    h = {"X-Orch-Session": str(session_id)}
    h["Authorization"] = f"Bearer {tok if tok is not None else token(session_id)}"
    return h


async def set_ip(pool, agent_id, ip):
    async with pool.connection() as conn:
        await conn.execute("UPDATE agents SET pod_ip = %s WHERE id = %s", (ip, agent_id))


async def hive(pool, replicas: int = 2):
    c = await insert_session(pool, spec={"replicas": replicas, "max_replicas": 4})
    a0 = await insert_agent(pool, c["id"], ordinal=0, ready=True)
    a1 = await insert_agent(pool, c["id"], ordinal=1, ready=False)
    await set_ip(pool, a0["id"], IP0)
    await set_ip(pool, a1["id"], IP1)
    return c, a0, a1


async def test_me_identity(pool, dsn):
    c, a0, a1 = await hive(pool)
    async with client_for(create_app(make_settings(dsn), pool), IP1) as http:
        r = await http.get("/agent/v1/me", headers=headers(c["id"]))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["session_id"] == str(c["id"])
    assert body["agent_id"] == str(a1["id"])
    assert body["pod_name"] == a1["pod_name"]
    assert body["ordinal"] == 1
    assert body["max_replicas"] == 4
    assert body["static"] is False
    assert body["leader"] is None
    assert body["epoch"] is None
    assert (body["project"], body["projects_scope"]) == ("default", "default")


async def test_token_rejections(pool, dsn):
    c, _, _ = await hive(pool)
    other = await insert_session(pool, spec={"replicas": 1})
    app = create_app(make_settings(dsn), pool)
    cases = [
        {},
        {"Authorization": f"Bearer {token(c['id'])}"},
        {"X-Orch-Session": str(c["id"])},
        headers(c["id"], "wrong"),
        headers(c["id"], token(other["id"])),
        {**headers(c["id"]), "Authorization": token(c["id"])},
        headers(uuid.uuid4()),
        {**headers(c["id"]), "X-Orch-Session": "not-a-uuid"},
    ]
    messages = set()
    async with client_for(app) as http:
        for h in cases:
            r = await http.get("/agent/v1/me", headers=h)
            assert r.status_code == 401, (h, r.text)
            err = r.json()["error"]
            assert err["code"] == "unauthorized"
            messages.add(err["message"])
    assert len(messages) == 1


async def test_old_cluster_header_refused(pool, dsn):
    """The pre-rename header is no alias: carrying only it is a missing X-Orch-Session."""
    c, _, _ = await hive(pool)
    old = {"Authorization": f"Bearer {token(c['id'])}", "X-Orch-Cluster": str(c["id"])}
    async with client_for(create_app(make_settings(dsn), pool), IP1) as http:
        refused = await http.get("/agent/v1/me", headers=old)
        missing = await http.get("/agent/v1/me", headers={"Authorization": old["Authorization"]})
        accepted = await http.get("/agent/v1/me", headers=headers(c["id"]))
    assert refused.status_code == missing.status_code == 401, refused.text
    assert refused.json() == missing.json()
    assert refused.json()["error"]["code"] == "unauthorized"
    assert accepted.status_code == 200, accepted.text
    assert accepted.json()["session_id"] == str(c["id"])


async def test_ip_binding(pool, dsn):
    c, a0, a1 = await hive(pool)
    app = create_app(make_settings(dsn), pool)
    async with client_for(app, "10.9.0.99") as http:
        r = await http.get("/agent/v1/me", headers=headers(c["id"]))
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden"
    async with client_for(app, IP0) as http:
        r = await http.get("/agent/v1/me", headers={**headers(c["id"]), "X-Forwarded-For": IP1})
        assert r.status_code == 200
        assert r.json()["agent_id"] == str(a0["id"])
    # a gone agent's former IP is refused
    async with pool.connection() as conn:
        await conn.execute("UPDATE agents SET gone_at = now() WHERE id = %s", (a0["id"],))
    async with client_for(app, IP0) as http:
        r = await http.get("/agent/v1/me", headers=headers(c["id"]))
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden"


async def test_me_leader_and_members(pool, dsn):
    c, a0, a1 = await hive(pool)
    await set_agent_status(pool, a0["id"], state="busy", context_pct=42.5)
    await set_agent_status(pool, a1["id"], state="booting")
    gone = await insert_agent(pool, c["id"], ordinal=2, ready=True)
    async with pool.connection() as conn:
        await conn.execute("UPDATE agents SET gone_at = now() WHERE id = %s", (gone["id"],))
    app = create_app(make_settings(dsn), pool)
    async with client_for(app, IP1) as http:
        body = (await http.get("/agent/v1/me", headers=headers(c["id"]))).json()
        assert body["leader"] is None and body["epoch"] is None
        async with pool.connection() as conn:
            await conn.execute(
                "INSERT INTO session_leaders (session_id, agent_id, epoch, since, reason)"
                " VALUES (%s, %s, 3, now(), 'test')",
                (c["id"], a0["id"]),
            )
        body = (await http.get("/agent/v1/me", headers=headers(c["id"]))).json()
    assert body["leader"] == a0["pod_name"]
    assert body["epoch"] == 3
    assert body["members"] == [
        {
            "pod_name": a0["pod_name"],
            "ready": True,
            "state": "busy",
            "context_pct": 42.5,
            "leader": True,
        },
        {
            "pod_name": a1["pod_name"],
            "ready": False,
            "state": "booting",
            "context_pct": None,
            "leader": False,
        },
    ]


async def _lead_and_message(pool, c, leader) -> int:
    """Make `leader` the leader and give the session one unhandled message from a peer."""
    peer = await insert_session(pool)
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO session_leaders (session_id, agent_id, epoch, since, reason)"
            " VALUES (%s, %s, 1, now(), 'test')",
            (c["id"], leader["id"]),
        )
        cur = await conn.execute(
            "INSERT INTO messages (session_id, text, status, lane, sender_session_id,"
            " sender_slug) VALUES (%s, 'please review', 'sent', 'regular', %s, 'peer-slug')"
            " RETURNING id",
            (c["id"], peer["id"]),
        )
        return (await cur.fetchone())[0]


async def test_me_leader_state_for_leader(pool, dsn):
    c, a0, _ = await hive(pool)
    message_id = await _lead_and_message(pool, c, a0)
    async with client_for(create_app(make_settings(dsn), pool), IP0) as http:
        r = await http.get("/agent/v1/me", headers=headers(c["id"]))
    assert r.status_code == 200, r.text
    state = r.json()["leader_state"]
    assert state.startswith("## State (generated")
    assert f"### message {message_id} from peer-slug" in state
    assert "please review" in state


async def test_me_leader_state_null_for_member(pool, dsn):
    c, a0, _ = await hive(pool)
    await _lead_and_message(pool, c, a0)
    async with client_for(create_app(make_settings(dsn), pool), IP1) as http:
        r = await http.get("/agent/v1/me", headers=headers(c["id"]))
    assert r.status_code == 200, r.text
    assert r.json()["leader_state"] is None


async def test_me_leader_state_null_without_a_leader(pool, dsn):
    c, _, _ = await hive(pool)
    async with client_for(create_app(make_settings(dsn), pool), IP0) as http:
        r = await http.get("/agent/v1/me", headers=headers(c["id"]))
    assert r.json()["leader_state"] is None


async def test_me_leader_state_taskboard_unconfigured(pool, dsn):
    c, a0, _ = await hive(pool)
    await _lead_and_message(pool, c, a0)
    settings = make_settings(dsn)
    assert not settings.taskboard_available
    async with client_for(create_app(settings, pool), IP0) as http:
        r = await http.get("/agent/v1/me", headers=headers(c["id"]))
    assert r.status_code == 200, r.text
    assert (
        "- taskboard unavailable: call orchestrator_task_list(waiting=true)"
        in r.json()["leader_state"]
    )

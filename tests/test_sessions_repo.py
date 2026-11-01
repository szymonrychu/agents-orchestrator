import inspect
import json
import uuid
from datetime import UTC, datetime

import psycopg
import pytest
from factories import (
    insert_agent,
    insert_check,
    insert_command,
    insert_event,
    insert_message,
    insert_turn,
)
from pydantic import ValidationError

from agents_orchestrator.db import sessions
from agents_orchestrator.db.sessions import SessionExists
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec, SpecInvalid, validate_slug, validate_spec


def _settings(**kw) -> Settings:
    kw.setdefault("database_url", "postgresql://x/y")
    kw.setdefault("agent_image", "img:1")
    return Settings(**kw)


async def test_create_sets_generation_1(pool):
    async with pool.connection() as conn:
        c = await sessions.create(conn, "alpha", SessionSpec(replicas=3))
        got = await sessions.get(conn, c.id)
        by_slug = await sessions.get_by_slug(conn, "alpha")
    assert (got.generation, got.observed_generation, got.desired) == (1, 0, "active")
    assert got.spec.replicas == 3 and by_slug.id == c.id
    assert await sessions.get_by_slug(conn, "nope") is None


async def test_create_records_the_creating_service(pool):
    async with pool.connection() as conn:
        user = await sessions.create(conn, "by-user", SessionSpec())
        svc = await sessions.create(
            conn, "by-svc", SessionSpec(), created_by_service="agents-webhook"
        )
        got = await sessions.get(conn, svc.id)
    assert user.created_by_service is None
    assert svc.created_by_service == got.created_by_service == "agents-webhook"


def test_slug_rules():
    for ok in ("a", "my-session", "a1", "x" * 40):
        validate_slug(ok)
    for bad in ("", "A", "-a", "a-", "a_b", "a.b", "x" * 41):
        with pytest.raises(ValueError):
            validate_slug(bad)


async def test_delete_marks_deleting(pool):
    async with pool.connection() as conn:
        c = await sessions.create(conn, "alpha", SessionSpec())
        d = await sessions.request_delete(conn, c.id)
        assert (d.desired, d.generation) == ("deleting", 2)
        again = await sessions.request_delete(conn, c.id)
        assert (again.desired, again.generation) == ("deleting", 2)
        listed = await sessions.list_sessions(conn)
        assert [x.slug for x in listed] == ["alpha"]
        s = await sessions.set_status(conn, c.id, {"phase": "x"}, 2)
        assert s.observed_generation == 2 and s.status == {"phase": "x"}


async def test_purge_cascades_history(pool):
    from factories import insert_session

    async with pool.connection() as conn:
        c = await sessions.create(conn, "alpha", SessionSpec())
    a = await insert_agent(pool, c.id)
    await insert_message(pool, c.id)
    await insert_event(pool, c.id, a["id"])
    await insert_turn(pool, a["id"])
    await insert_command(pool, a["id"])
    await insert_check(pool, c.id)
    other = await insert_session(pool)
    async with pool.connection() as conn:
        await sessions.purge(conn, c.id)
        assert await sessions.get(conn, c.id) is None
        for t in ("agents", "messages", "events", "turns", "commands", "checks"):
            cur = await conn.execute(f"SELECT count(*) FROM {t}")
            assert (await cur.fetchone())[0] == 0, t
        assert await sessions.get(conn, other["id"]) is not None


@pytest.mark.parametrize(
    "key", ["CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY", "lower", "1X", "A-B"]
)
def test_spec_rejects_secret_env(key):
    with pytest.raises(ValidationError):
        SessionSpec(env={key: {"value": "v"}})
    SessionSpec(env={"MY_FLAG": {"value": "v"}})


async def test_duplicate_slug(pool):
    async with pool.connection() as conn:
        await sessions.create(conn, "alpha", SessionSpec())
    async with pool.connection() as conn:
        with pytest.raises(SessionExists):
            await sessions.create(conn, "alpha", SessionSpec())


@pytest.mark.parametrize(
    "key",
    [
        "POD_NAME",
        "CCW_X",
        "CLAUDE_CODE_OAUTH_TOKEN",
        "ANTHROPIC_API_KEY",
        "HTTP_PROXY",
        "NO_PROXY",
        "OPENAI_BASE_URL",
    ],
)
def test_spec_rejects_reserved_env_keys(key):
    with pytest.raises(ValidationError):
        SessionSpec(env={key: {"value": "v"}})


def test_reserved_slugs_rejected():
    for bad in ("orchestrator", "orchestrator-x", "orchestrator-"):
        with pytest.raises(ValueError):
            validate_slug(bad)
    validate_slug("orchestrators")


def test_validate_spec_image_allowlist():
    s = _settings(agent_images="other:2")
    validate_spec(SessionSpec(), s)
    validate_spec(SessionSpec(image="img:1"), s)
    validate_spec(SessionSpec(image="other:2"), s)
    with pytest.raises(SpecInvalid) as e:
        validate_spec(SessionSpec(image="evil:3"), s)
    assert any("image" in m for m in e.value.errors)


def test_validate_spec_caps():
    s = _settings(max_replicas=4, max_pvc_size="10Gi")
    validate_spec(SessionSpec(replicas=4, pvc_size="10Gi"), s)
    validate_spec(SessionSpec(pvc_size="5120Mi"), s)
    with pytest.raises(SpecInvalid) as e:
        validate_spec(SessionSpec(replicas=5, pvc_size="11Gi"), s)
    # max_replicas defaults to replicas, so the replicas cap is reported for both fields
    assert len(e.value.errors) == 3
    assert sum("replicas" in m for m in e.value.errors) == 2
    with pytest.raises(SpecInvalid):
        validate_spec(SessionSpec(pvc_size="1Ti"), s)
    with pytest.raises(ValidationError):
        SessionSpec(pvc_size="lots")
    with pytest.raises(ValidationError):
        SessionSpec(replicas=17)


def test_validate_spec_any_mcp_config_schema():
    s = _settings()
    validate_spec(SessionSpec(any_mcp_config={"python_path": []}), s)
    with pytest.raises(SpecInvalid) as e:
        validate_spec(SessionSpec(any_mcp_config={"bogus_key": 1}), s)
    assert any("any_mcp_config" in m for m in e.value.errors)


async def test_count_active_counts_rows(pool):
    async with pool.connection() as conn:
        assert await sessions.count_active(conn) == 0
        a = await sessions.create(conn, "a", SessionSpec())
        await sessions.create(conn, "b", SessionSpec())
        await sessions.request_delete(conn, a.id)
        assert await sessions.count_active(conn) == 2
        await sessions.purge(conn, a.id)
        assert await sessions.count_active(conn) == 1


def test_spec_retry_default_and_discard_hook_url():
    assert SessionSpec().max_attempts == 5
    SessionSpec(discard_hook_url="https://h/x")
    SessionSpec(discard_hook_url="http://h/x")
    for bad in ("ftp://h/x", "file:///etc/passwd", "h/x", "http://"):
        with pytest.raises(ValidationError):
            SessionSpec(discard_hook_url=bad)
    for n in (0, 11):
        with pytest.raises(ValidationError):
            SessionSpec(max_attempts=n)
    with pytest.raises(ValidationError):
        SessionSpec(unknown=1)
    with pytest.raises(ValidationError):
        SessionSpec(auto_compact_pct=0)
    with pytest.raises(ValidationError):
        SessionSpec(workdir="Bad_Dir")
    validate_spec(SessionSpec(discard_hook_url="https://h/x"), _settings())


@pytest.mark.parametrize("bad", [" 10Gi", "10Gi ", "1Gi\n", "-1Gi", "0"])
def test_spec_rejects_malformed_pvc_size(bad):
    with pytest.raises(ValidationError):
        SessionSpec(pvc_size=bad)


def test_trailing_newline_rejected():
    with pytest.raises(ValueError):
        validate_slug("abc\n")
    with pytest.raises(ValidationError):
        SessionSpec(workdir="wd\n")
    with pytest.raises(ValidationError):
        SessionSpec(env={"FOO\n": {"value": "x"}})


def test_live_edit_code_retired():
    # names are split so the retired symbols never appear as a literal in the tree
    for name in ("update_" + "spec", "Session" + "Deleting"):
        assert not hasattr(sessions, name), name
    assert "stored" in inspect.signature(validate_spec).parameters


def test_validate_spec_stored_exception_kept():
    settings = _settings(models="sonnet,opus", efforts="low,high")
    stored = SessionSpec(model="legacy-model", effort="legacy-effort")
    # unchanged out-of-list values pass only against the stored spec
    validate_spec(stored, settings, stored)
    with pytest.raises(SpecInvalid):
        validate_spec(stored, settings)
    # a changed out-of-list value is rejected even with a stored spec
    with pytest.raises(SpecInvalid):
        validate_spec(SessionSpec(model="other-model", effort="legacy-effort"), settings, stored)
    with pytest.raises(SpecInvalid):
        validate_spec(SessionSpec(model="legacy-model", effort="other-effort"), settings, stored)


async def _spec_row(pool, session_id):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT spec, generation, observed_generation FROM sessions WHERE id = %s",
            (session_id,),
        )
        return await cur.fetchone()


async def _make_leader(pool, session_id, agent_id):
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO session_leaders (session_id, agent_id, epoch, since, reason)"
            " VALUES (%s, %s, 1, now(), 'test')",
            (session_id, agent_id),
        )


async def _scalable(pool, **spec):
    from factories import insert_session

    body = {"replicas": 2, "max_replicas": 4, **spec}
    return await insert_session(pool, spec=body, generation=3, observed_generation=3)


class _Listener:
    """A LISTEN connection on one channel; payloads() drains what arrived so far."""

    def __init__(self, dsn, channel="session_changed"):
        self.dsn = dsn
        self.channel = channel

    async def __aenter__(self):
        self.conn = await psycopg.AsyncConnection.connect(self.dsn, autocommit=True)
        await self.conn.execute(f"LISTEN {self.channel}")
        return self

    async def __aexit__(self, *exc):
        await self.conn.close()

    async def payloads(self, wait=0.5):
        got = []
        gen = self.conn.notifies(timeout=wait)
        async for n in gen:
            got.append(n.payload)
        return got


async def test_set_replicas_updates_spec_without_generation_bump(pool, dsn):
    c = await _scalable(pool, image="img:9")
    before = await _spec_row(pool, c["id"])
    async with _Listener(dsn) as lis, pool.connection() as conn:
        out = await sessions.set_replicas(conn, c["id"], 4)
        got = await lis.payloads()
    assert (out.session_id, out.replicas, out.previous_replicas, out.max_replicas) == (
        c["id"],
        4,
        2,
        4,
    )
    assert got == [json.dumps({"session_id": str(c["id"])})]
    spec, gen, obs = await _spec_row(pool, c["id"])
    assert (gen, obs) == (before[1], before[2])
    assert spec["replicas"] == 4
    assert {k: v for k, v in spec.items() if k != "replicas"} == {
        k: v for k, v in before[0].items() if k != "replicas"
    }
    async with pool.connection() as conn:
        assert (await sessions.get(conn, c["id"])).spec.replicas == 4


async def _refused(pool, session_id, replicas, **kw):
    async with pool.connection() as conn:
        with pytest.raises(sessions.ScaleRefused) as e:
            await sessions.set_replicas(conn, session_id, replicas, **kw)
    return e.value.code


async def test_set_replicas_refusals(pool):
    from factories import insert_session

    c = await _scalable(pool)
    before = await _spec_row(pool, c["id"])
    assert await _refused(pool, c["id"], 0) == "out_of_range"
    assert await _refused(pool, c["id"], 5) == "out_of_range"
    static = await insert_session(pool, spec={"replicas": 2, "static": True})
    assert await _refused(pool, static["id"], 1) == "static_session"
    dying = await insert_session(pool, spec={"replicas": 2, "max_replicas": 4}, desired="deleting")
    assert await _refused(pool, dying["id"], 3) == "session_deleting"
    assert await _spec_row(pool, c["id"]) == before
    assert (await _spec_row(pool, dying["id"]))[0]["replicas"] == 2
    assert (await _spec_row(pool, static["id"]))[0]["replicas"] == 2
    async with pool.connection() as conn:
        with pytest.raises(sessions.SessionNotFound):
            await sessions.set_replicas(conn, uuid.uuid4(), 2)


async def test_set_replicas_leader_rules(pool):
    c = await _scalable(pool, replicas=4)
    free = await _scalable(pool)
    async with pool.connection() as conn:
        out = await sessions.set_replicas(conn, free["id"], 1)
    assert out.replicas == 1
    zero = await insert_agent(pool, c["id"], ordinal=0)
    two = await insert_agent(pool, c["id"], ordinal=2, ready=True, wrapper_session_id="w2")
    await _make_leader(pool, c["id"], two["id"])
    assert await _refused(pool, c["id"], 3, caller_agent_id=zero["id"]) == "not_leader"
    before = await _spec_row(pool, c["id"])
    leader = await _leader_row(pool, c["id"])
    # Ordinal 0 stays at 2 replicas but can never take over: not ready, gone, or without a
    # wrapper session; so a scale-down below the leader is refused and nothing is written.
    ineligible = (
        (False, "w0", None),
        (True, "w0", datetime.now(UTC)),
        (True, None, None),
    )
    for ready, wrapper_session_id, gone_at in ineligible:
        await _set_agent(pool, zero["id"], ready, wrapper_session_id, gone_at)
        assert await _refused(pool, c["id"], 2, caller_agent_id=two["id"]) == "leader_would_leave"
        assert await _refused(pool, c["id"], 2) == "leader_would_leave"
    # A ready agent that is itself leaving (ordinal 3 >= 2) cannot take over either.
    await _set_agent(pool, zero["id"], False, "w0", None)
    await insert_agent(pool, c["id"], ordinal=3, ready=True, wrapper_session_id="w3")
    assert await _refused(pool, c["id"], 2) == "leader_would_leave"
    assert await _spec_row(pool, c["id"]) == before
    assert await _leader_row(pool, c["id"]) == leader
    async with pool.connection() as conn:
        out = await sessions.set_replicas(conn, c["id"], 3, caller_agent_id=two["id"])
    assert (out.replicas, out.previous_replicas) == (3, 4)


async def _set_agent(pool, agent_id, ready, wrapper_session_id, gone_at):
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE agents SET ready = %s, wrapper_session_id = %s, gone_at = %s WHERE id = %s",
            (ready, wrapper_session_id, gone_at, agent_id),
        )


async def _leader_row(pool, session_id):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT agent_id, epoch, since, reason FROM session_leaders WHERE session_id = %s",
            (session_id,),
        )
        return await cur.fetchone()


async def test_set_replicas_below_leader_with_staying_peer(pool, dsn):
    c = await _scalable(pool, replicas=4)
    await insert_agent(pool, c["id"], ordinal=0, ready=True, wrapper_session_id="w0")
    two = await insert_agent(pool, c["id"], ordinal=2, ready=True, wrapper_session_id="w2")
    await _make_leader(pool, c["id"], two["id"])
    leader = await _leader_row(pool, c["id"])
    async with (
        _Listener(dsn, "leadership_requested") as lis,
        _Listener(dsn) as changed,
        pool.connection() as conn,
    ):
        # 4 to 3 leaves the leader (ordinal 2) in place: no leadership wake.
        await sessions.set_replicas(conn, c["id"], 3, caller_agent_id=two["id"])
        assert await lis.payloads() == []
        # 3 to 2 removes the leader's ordinal: written, and the dispatcher is woken.
        out = await sessions.set_replicas(conn, c["id"], 2, caller_agent_id=two["id"])
        assert await lis.payloads() == [json.dumps({"session_id": str(c["id"])})]
        assert len(await changed.payloads()) == 2
    assert (out.replicas, out.previous_replicas) == (2, 3)
    assert (await _spec_row(pool, c["id"]))[0]["replicas"] == 2
    assert await _leader_row(pool, c["id"]) == leader


async def test_set_replicas_unchanged_is_noop(pool, dsn):
    c = await _scalable(pool)
    async with pool.connection() as conn:
        await conn.execute("UPDATE sessions SET status = '{}'::jsonb WHERE id = %s", (c["id"],))
    before = await _spec_row(pool, c["id"])
    async with _Listener(dsn) as lis, pool.connection() as conn:
        out = await sessions.set_replicas(conn, c["id"], 2)
        got = await lis.payloads()
    assert (out.replicas, out.previous_replicas, out.max_replicas) == (2, 2, 4)
    assert got == []
    assert await _spec_row(pool, c["id"]) == before

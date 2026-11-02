"""The pre-discard hook and the JSONL history export (D15)."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import pytest_asyncio
from factories import (
    insert_agent,
    insert_check,
    insert_command,
    insert_event,
    insert_message,
    insert_turn,
)
from fakes.fake_kube import FakeKube

from agents_orchestrator.db import sessions
from agents_orchestrator.db.export import export_session_history
from agents_orchestrator.reconciler import Reconciler
from agents_orchestrator.reconciler.discard_hook import run_discard_hook
from agents_orchestrator.settings import DiscardHook, Settings
from agents_orchestrator.spec import SessionSpec

IMAGE = "harbor.example/containers/claude-code-wrapper:ca2ddf3"


class Env:
    def __init__(self, pool, dsn: str, tmp_path, **settings) -> None:
        self.pool, self.tmp = pool, tmp_path
        settings.setdefault("discard_drain_seconds", 0)
        self.kube = FakeKube(namespace="agents")
        self.settings = Settings(
            database_url=dsn,
            master_key="k" * 32,
            agent_image=IMAGE,
            retry_backoff_base_seconds=30,
            retry_backoff_max_seconds=600,
            **settings,
        )
        self.rec = Reconciler(pool, self.kube, self.settings, wrapper_factory=None)

    async def session(self, slug: str = "alpha", **spec) -> sessions.Session:
        async with self.pool.connection() as conn:
            return await sessions.create(conn, slug, SessionSpec(**spec))

    async def get(self, session_id) -> sessions.Session | None:
        async with self.pool.connection() as conn:
            return await sessions.get(conn, session_id)

    async def discard(self, session_id) -> None:
        async with self.pool.connection() as conn:
            await sessions.request_delete(conn, session_id)

    async def export(self, session_id) -> list[dict]:
        async with self.pool.connection() as conn:
            return [json.loads(line) async for line in export_session_history(conn, session_id)]

    async def history_rows(self, session_id) -> int:
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                "SELECT (SELECT count(*) FROM agents WHERE session_id = %(c)s)"
                " + (SELECT count(*) FROM messages WHERE session_id = %(c)s)"
                " + (SELECT count(*) FROM events WHERE session_id = %(c)s)",
                {"c": session_id},
            )
            return (await cur.fetchone())[0]

    async def sql(self, query: str, params: tuple = ()) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(query, params)

    def command_hook(self, script: str = "") -> str:
        """A hook that saves stdin and the environment, then runs `script`."""
        out = self.tmp / "out"
        return (
            f'cat >> "{out}.jsonl"; echo "$ORCH_SESSION_ID $ORCH_SESSION_SLUG" >> "{out}.env"; '
            + script
        )

    def calls(self) -> int:
        path = self.tmp / "out.env"
        return len(path.read_text().splitlines()) if path.exists() else 0


@pytest_asyncio.fixture
async def env(pool, dsn, tmp_path) -> Env:
    return Env(pool, dsn, tmp_path)


async def populate(env: Env, session):
    """Two agents, with rows of every kind inserted out of id order where the kind allows."""
    a1 = await insert_agent(env.pool, session.id, ordinal=10, pod_name="p10")
    a2 = await insert_agent(env.pool, session.id, ordinal=11, pod_name="p11")
    for text in ("one", "two", "three"):
        await insert_message(env.pool, session.id, text=text)
    for agent in (a2, a1):
        await insert_event(env.pool, session.id, agent["id"], type="assistant_text")
        await insert_command(env.pool, agent["id"], kind="interrupt")
    await insert_event(env.pool, session.id, a1["id"], type="tool_call")
    await insert_check(env.pool, session.id, command="make a", status="passed")
    await insert_check(env.pool, session.id, command="make b", status="failed")
    await insert_turn(env.pool, a1["id"], turn_id="t-late")
    await insert_turn(env.pool, a1["id"], turn_id="t-early")
    await env.sql(
        "UPDATE turns SET started_at = now() - interval '1 hour' WHERE turn_id = 't-early'"
    )
    return a1, a2


# ---------------------------------------------------------------- the export


async def test_export_jsonl_kinds_and_order(env):
    c = await env.session("alpha")
    a1, a2 = await populate(env, c)
    other = await env.session("bravo")
    await insert_message(env.pool, other.id, text="not mine")

    async with env.pool.connection() as conn:
        lines = [line async for line in export_session_history(conn, c.id)]
    assert all(line.endswith("\n") and "\n" not in line[:-1] for line in lines)
    records = [json.loads(line) for line in lines]

    kinds = [r["kind"] for r in records]
    assert kinds[0] == "session" and kinds.count("session") == 1
    assert kinds[1:] == (
        ["agent"] * 2
        + ["message"] * 3
        + ["turn"] * 2
        + ["command"] * 2
        + ["check"] * 2
        + ["event"] * 3
    )
    session = records[0]
    assert session["slug"] == "alpha" and session["id"] == str(c.id)
    assert session["spec"] == c.spec.model_dump(mode="json", exclude_none=True)
    created = datetime.fromisoformat(session["created_at"])
    assert created.utcoffset() == timedelta(0) and created == c.created_at

    by_kind = {k: [r for r in records if r["kind"] == k] for k in set(kinds)}
    assert [r["id"] for r in by_kind["agent"]] == sorted(str(a["id"]) for a in (a1, a2))
    for kind in ("message", "command", "check", "event"):
        ids = [r["id"] for r in by_kind[kind]]
        assert ids == sorted(ids), kind
    assert [r["text"] for r in by_kind["message"]] == ["one", "two", "three"]
    assert [r["turn_id"] for r in by_kind["turn"]] == ["t-early", "t-late"]
    assert {r["session_id"] for r in by_kind["message"]} == {str(c.id)}
    for record in records:
        for key, value in record.items():
            if key.endswith("_at") and value is not None:
                assert datetime.fromisoformat(value).utcoffset() == timedelta(0)


async def test_export_session_record_carries_close_request(env):
    c = await env.session("alpha")
    async with env.pool.connection() as conn:
        await sessions.request_close(conn, c.id, "done")
    plain = await env.session("bravo")
    session = (await env.export(c.id))[0]
    assert session["kind"] == "session" and session["close_reason"] == "done"
    stored = (await env.get(c.id)).close_requested_at
    assert datetime.fromisoformat(session["close_requested_at"]) == stored
    untouched = (await env.export(plain.id))[0]
    assert untouched["close_reason"] is None and untouched["close_requested_at"] is None


async def test_export_streams_with_a_server_side_cursor(env):
    c = await env.session("alpha")
    await populate(env, c)
    seen: list[str] = []
    async with env.pool.connection() as conn:
        original = conn.cursor

        def spy(*args, **kwargs):
            seen.append(kwargs.get("name") or "")
            return original(*args, **kwargs)

        conn.cursor = spy  # type: ignore[method-assign]
        n = sum([1 async for _ in export_session_history(conn, c.id)])
    assert n > 10 and sorted(s for s in seen if s) == sorted(
        f"export_{k}" for k in ("agent", "message", "turn", "command", "check", "event")
    )


# ---------------------------------------------------------------- the hook runner


async def lines_of(env: Env, session):
    async with env.pool.connection() as conn:
        async for line in export_session_history(conn, session.id):
            yield line


async def test_command_hook_gets_export_then_discard_proceeds(pool, dsn, tmp_path):
    env = Env(pool, dsn, tmp_path)
    env.settings.discard_hook_command = env.command_hook()
    c = await env.session("alpha")
    await env.rec.reconcile_once()
    await populate(env, c)
    expected = await env.export(c.id)

    await env.discard(c.id)
    await env.rec.reconcile_once()

    sent = [json.loads(x) for x in (tmp_path / "out.jsonl").read_text().splitlines()]
    assert sent == expected
    assert (tmp_path / "out.env").read_text() == f"{c.id} alpha\n"
    assert await env.get(c.id) is None
    assert env.kube.names("StatefulSet") == []


async def test_webhook_hook_and_spec_precedence(env):
    c = await env.session("alpha", discard_hook_url="https://spec.example/hook")
    await populate(env, c)
    expected = await env.export(c.id)
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        await request.aread()
        requests.append(request)
        return httpx.Response(204)

    env.settings.discard_hook_url = "https://settings.example/hook"
    env.settings.discard_hook_command = ""
    env.rec.hook_transport = httpx.MockTransport(handler)
    await env.discard(c.id)
    await env.rec.reconcile_once()

    assert [str(r.url) for r in requests] == ["https://spec.example/hook"]
    req = requests[0]
    assert req.method == "POST"
    assert req.headers["content-type"] == "application/x-ndjson"
    assert req.headers["x-orch-session-id"] == str(c.id)
    assert req.headers["x-orch-session-slug"] == "alpha"
    assert [json.loads(x) for x in req.content.decode().splitlines()] == expected
    assert await env.get(c.id) is None

    # no spec URL: the settings webhook is used
    d = await env.session("bravo")
    requests.clear()
    await env.discard(d.id)
    await env.rec.reconcile_once()
    assert [str(r.url) for r in requests] == ["https://settings.example/hook"]


async def test_webhook_failures(env):
    c = await env.session("alpha")
    session = await env.get(c.id)

    async def run(handler) -> tuple[bool, str]:
        result = await run_discard_hook(
            DiscardHook("url", "http://hook.example/x"),
            session,
            lines_of(env, session),
            5,
            transport=httpx.MockTransport(handler),
        )
        return result.ok, result.reason

    ok, reason = await run(lambda r: httpx.Response(502))
    assert not ok and "502" in reason

    def boom(request):
        raise httpx.ConnectError("refused")

    ok, reason = await run(boom)
    assert not ok and "refused" in reason


async def test_command_hook_failure_carries_exit_code_and_stderr_tail(env):
    session = await env.get((await env.session("alpha")).id)
    noisy = (
        "i=0; while [ $i -lt 5000 ]; do echo xxxxxxxxxx >&2; i=$((i+1)); done; echo END >&2; exit 7"
    )
    result = await run_discard_hook(
        DiscardHook("command", noisy), session, lines_of(env, session), 20
    )
    assert not result.ok
    assert "exit 7" in result.reason and "END" in result.reason
    assert len(result.reason) < 4096


async def test_command_hook_timeout_kills_process_group(env, tmp_path):
    session = await env.get((await env.session("alpha")).id)
    marker = tmp_path / "child-survived"
    script = f"(sleep 2; touch {marker}) & sleep 30"
    result = await run_discard_hook(
        DiscardHook("command", script), session, lines_of(env, session), 0.5
    )
    assert not result.ok and "timed out" in result.reason
    await asyncio.sleep(2.5)
    assert not marker.exists()


# ---------------------------------------------------------------- the gate


async def test_failed_hook_blocks_deletion_and_retries(pool, dsn, tmp_path):
    env = Env(pool, dsn, tmp_path)
    env.settings.discard_hook_command = env.command_hook("echo boom >&2; exit 3")
    c = await env.session("alpha")
    await env.rec.reconcile_once()
    await populate(env, c)
    await env.discard(c.id)

    await env.rec.reconcile_once()
    got = await env.get(c.id)
    assert got is not None and got.desired == "deleting"
    assert (
        "exit 3" in got.status["discard_hook_failed"]
        and "boom" in got.status["discard_hook_failed"]
    )
    assert got.status["discard_hook_failures"] == 1
    assert "discard_hook_delivered" not in got.status
    assert env.kube.names("StatefulSet") and env.kube.names("PersistentVolumeClaim")
    assert await env.history_rows(c.id) > 0
    next_at = datetime.fromisoformat(got.status["discard_hook_next_at"])
    db_now = datetime.now(UTC)
    assert timedelta(seconds=20) < next_at - db_now <= timedelta(seconds=31)  # base 30 s
    assert env.calls() == 1

    await env.rec.reconcile_once()  # still backing off: the hook does not run
    assert env.calls() == 1

    await env.sql(
        "UPDATE sessions SET status = jsonb_set(status, '{discard_hook_next_at}',"
        " to_jsonb((now() - interval '1 second')::text)) WHERE id = %s",
        (c.id,),
    )
    await env.rec.reconcile_once()
    got = await env.get(c.id)
    assert env.calls() == 2 and got.status["discard_hook_failures"] == 2
    next_at = datetime.fromisoformat(got.status["discard_hook_next_at"])
    assert next_at - datetime.now(UTC) > timedelta(seconds=50)  # 60 s: doubled

    # a timed-out hook is a failure too, and still deletes nothing
    env.settings.discard_hook_command = "sleep 30"
    env.settings.discard_hook_timeout_seconds = 0.5
    await env.sql(
        "UPDATE sessions SET status = jsonb_set(status, '{discard_hook_next_at}',"
        " to_jsonb((now() - interval '1 second')::text)) WHERE id = %s",
        (c.id,),
    )
    await env.rec.reconcile_once()
    got = await env.get(c.id)
    assert "timed out" in got.status["discard_hook_failed"]
    assert got.status["discard_hook_failures"] == 3
    assert env.kube.names("StatefulSet")

    # the hook recovers: the discard completes
    env.settings.discard_hook_command = env.command_hook()
    await env.sql(
        "UPDATE sessions SET status = jsonb_set(status, '{discard_hook_next_at}',"
        " to_jsonb((now() - interval '1 second')::text)) WHERE id = %s",
        (c.id,),
    )
    await env.rec.reconcile_once()
    assert await env.get(c.id) is None


async def test_failing_hook_of_one_session_does_not_stop_others(pool, dsn, tmp_path):
    env = Env(pool, dsn, tmp_path)
    a = await env.session("alpha")
    b = await env.session("bravo", discard_hook_url="http://hook.example/x")
    c = await env.session("charlie")
    await env.rec.reconcile_once()
    env.settings.discard_hook_command = ""

    def handler(request):
        raise httpx.ConnectError("refused")

    env.rec.hook_transport = httpx.MockTransport(handler)
    for session in (a, b, c):
        await env.discard(session.id)
    await env.rec.reconcile_once()
    assert await env.get(a.id) is None and await env.get(c.id) is None
    held = await env.get(b.id)
    assert held is not None and "refused" in held.status["discard_hook_failed"]


async def test_delivered_hook_not_resent_and_history_deleted(pool, dsn, tmp_path):
    env = Env(pool, dsn, tmp_path)
    env.settings.discard_hook_command = env.command_hook()
    c = await env.session("alpha")
    await env.rec.reconcile_once()
    await populate(env, c)
    await env.discard(c.id)
    # a pass that delivers but cannot finish: an object lingers
    real_remaining = env.rec._remaining

    async def lingering(slug):
        return ["Pod/agent-alpha-0"]

    env.rec._remaining = lingering
    await env.rec.reconcile_once()
    got = await env.get(c.id)
    assert got is not None and env.calls() == 1
    delivered = datetime.fromisoformat(got.status["discard_hook_delivered"])
    assert abs(datetime.now(UTC) - delivered) < timedelta(minutes=1)
    assert "discard_hook_failed" not in got.status

    # a reconciler restart: a fresh Reconciler over the same database
    restarted = Reconciler(pool, env.kube, env.settings, wrapper_factory=None)
    restarted._remaining = real_remaining
    await restarted.reconcile_once()
    assert env.calls() == 1
    assert await env.get(c.id) is None
    assert await env.history_rows(c.id) == 0


async def test_no_hook_discards_directly(env):
    assert env.settings.discard_hook() is None
    c = await env.session("alpha")
    await env.rec.reconcile_once()
    await populate(env, c)
    env.rec.hook_transport = httpx.MockTransport(lambda r: pytest.fail("no export expected"))
    await env.discard(c.id)
    await env.rec.reconcile_once()
    assert await env.get(c.id) is None
    assert await env.history_rows(c.id) == 0
    assert env.kube.names("PersistentVolumeClaim") == []


async def _quiescing_env(pool, dsn, tmp_path, **settings):
    env = Env(pool, dsn, tmp_path, **settings)
    env.settings.discard_hook_command = env.command_hook()
    c = await env.session("alpha", replicas=2)
    await env.rec.reconcile_once()
    assert len(env.kube.names("Pod")) == 2
    a1 = await insert_agent(env.pool, c.id, ordinal=5, pod_name="p5")
    env.kube.controller_paused = True  # the pods linger after the scale-down, like a slow stop
    await env.discard(c.id)
    return env, c, a1


async def test_discard_quiesces_before_export(pool, dsn, tmp_path):
    env, c, _ = await _quiescing_env(pool, dsn, tmp_path)
    await env.rec.reconcile_once()
    sts = env.kube.applied("StatefulSet")[-1]
    assert sts.obj["spec"]["replicas"] == 0
    assert env.calls() == 0  # pods still running: nothing exported, nothing deleted
    got = await env.get(c.id)
    assert got is not None and "waiting for agent pods" in got.status["message"]
    assert env.kube.names("StatefulSet") and env.kube.names("PersistentVolumeClaim")

    for name in env.kube.names("Pod"):
        await env.kube.delete_pod("agents", name)
    await env.rec.reconcile_once()
    assert env.calls() == 1
    assert await env.get(c.id) is None


async def test_drain_grace_delays_export_and_timeout_proceeds(pool, dsn, tmp_path):
    env, c, _ = await _quiescing_env(pool, dsn, tmp_path, discard_drain_seconds=3600)
    for name in env.kube.names("Pod"):
        await env.kube.delete_pod("agents", name)
    await env.rec.reconcile_once()
    assert env.calls() == 0
    got = await env.get(c.id)
    assert "drain" in got.status["message"]

    # the bounded wait ends: the export goes ahead
    env.settings.discard_quiesce_timeout_seconds = 0
    await env.rec.reconcile_once()
    assert env.calls() == 1 and await env.get(c.id) is None


async def test_export_contains_in_flight_events(pool, dsn, tmp_path):
    env, c, a1 = await _quiescing_env(pool, dsn, tmp_path)
    await env.rec.reconcile_once()
    assert env.calls() == 0
    # a turn still in flight lands an event after the discard began, before the pods are gone
    await insert_event(env.pool, c.id, a1["id"], type="late_in_flight")
    for name in env.kube.names("Pod"):
        await env.kube.delete_pod("agents", name)
    await env.rec.reconcile_once()
    assert env.calls() == 1
    exported = (env.tmp / "out.jsonl").read_text()
    assert "late_in_flight" in exported
    assert await env.get(c.id) is None


async def test_stuck_pods_do_not_block_discard_past_timeout(pool, dsn, tmp_path, caplog):
    env, c, _ = await _quiescing_env(pool, dsn, tmp_path, discard_quiesce_timeout_seconds=0)
    assert env.kube.names("Pod")  # the pods never go away
    with caplog.at_level("WARNING"):
        await env.rec.reconcile_once()
    assert "still running" in caplog.text and "exporting anyway" in caplog.text
    assert env.calls() == 1
    assert await env.get(c.id) is None

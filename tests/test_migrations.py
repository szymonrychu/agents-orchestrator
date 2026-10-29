import asyncio
import json
import shutil
import threading
from datetime import UTC, datetime

import psycopg
import pytest
from factories import (
    insert_agent,
    insert_check,
    insert_command,
    insert_event,
    insert_message,
    insert_session,
    insert_turn,
    set_agent_status,
)

from agents_orchestrator.db.migrate import MIGRATIONS_DIR, run_migrations
from agents_orchestrator.db.pool import listen, notify

TABLES = {
    "sessions",
    "agents",
    "agent_status",
    "messages",
    "turns",
    "events",
    "commands",
    "checks",
    "presets",
    "schema_migrations",
}


def _columns(dsn: str, table: str) -> set[str]:
    with psycopg.connect(dsn) as c:
        rows = c.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name=%s", (table,)
        ).fetchall()
    return {r[0] for r in rows}


def test_fresh_database_gets_all_tables(empty_dsn):
    run_migrations(empty_dsn)
    with psycopg.connect(empty_dsn) as c:
        tables = {
            r[0]
            for r in c.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
            )
        }
        versions = [r[0] for r in c.execute("SELECT version FROM schema_migrations")]
    assert tables >= TABLES
    assert versions == [
        "0001", "0002", "0003", "0004", "0005", "0006", "0007", "0008", "0009", "0010", "0011",
        "0012",
    ]  # fmt: skip
    assert _command_outcome_index(empty_dsn) == _COMMAND_OUTCOME_INDEX
    sql = (MIGRATIONS_DIR / "0012_command_outcome_index.sql").read_text()
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        c.execute(sql)
        c.execute("DELETE FROM schema_migrations WHERE version = '0012'")
    assert run_migrations(empty_dsn) == ["0012"]
    assert run_migrations(empty_dsn) == []
    assert _command_outcome_index(empty_dsn) == _COMMAND_OUTCOME_INDEX


# the message view's command outcome lookup (D19): agent and record id, the two outcome types
_COMMAND_OUTCOME_INDEX = (
    "CREATE INDEX events_command_record_id ON public.events USING btree"
    " (agent_id, (((data -> 'record'::text) ->> 'id'::text)))"
    " WHERE (type = ANY (ARRAY['command_completed'::text, 'command_failed'::text]))"
)


def _command_outcome_index(dsn: str) -> str | None:
    with psycopg.connect(dsn) as c:
        row = c.execute(
            "SELECT indexdef FROM pg_indexes WHERE indexname = 'events_command_record_id'"
        ).fetchone()
    return row[0] if row else None


def test_presets_migration_on_existing_sessions(empty_dsn, tmp_path):
    shutil.copy(MIGRATIONS_DIR / "0001_init.sql", tmp_path / "0001_init.sql")
    assert run_migrations(empty_dsn, tmp_path) == ["0001"]
    spec = {"replicas": 3, "model": "opus"}
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        c.execute(
            "INSERT INTO clusters (id, slug, spec) VALUES (gen_random_uuid(), 'old', %s)",
            (json.dumps(spec),),
        )
    assert run_migrations(empty_dsn) == [
        "0002",
        "0003",
        "0004",
        "0005",
        "0006",
        "0007",
        "0008",
        "0009",
        "0010",
        "0011",
        "0012",
    ]
    with psycopg.connect(empty_dsn) as c:
        row = c.execute(
            "SELECT spec, preset_id, preset_name FROM sessions WHERE slug='old'"
        ).fetchall()
        default = c.execute("SELECT name, spec FROM presets WHERE is_default").fetchall()
    assert row == [({**spec, "max_replicas": 3}, None, None)]
    assert default == [("Default", {"replicas": 1})]


def test_rerun_is_noop(empty_dsn):
    run_migrations(empty_dsn)
    with psycopg.connect(empty_dsn) as c:
        before = c.execute("SELECT version, applied_at FROM schema_migrations").fetchall()
    assert run_migrations(empty_dsn) == []
    with psycopg.connect(empty_dsn) as c:
        after = c.execute("SELECT version, applied_at FROM schema_migrations").fetchall()
    assert before == after


def test_concurrent_migrate_serialises(empty_dsn):
    results: list = []
    errors: list = []

    def go():
        try:
            results.append(run_migrations(empty_dsn))
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=go) for _ in range(2)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert not errors
    assert sorted(len(r) for r in results) == [0, 12]
    with psycopg.connect(empty_dsn) as c:
        assert c.execute("SELECT count(*) FROM schema_migrations").fetchone()[0] == 12


async def test_events_unique_agent_session_seq(pool):
    cl = await insert_session(pool)
    ag = await insert_agent(pool, cl["id"])
    await insert_event(pool, cl["id"], ag["id"], wrapper_session_id="s", seq=1)
    with pytest.raises(psycopg.errors.UniqueViolation):
        await insert_event(pool, cl["id"], ag["id"], wrapper_session_id="s", seq=1)
    await insert_event(pool, cl["id"], ag["id"], wrapper_session_id="s2", seq=1)


async def test_session_delete_cascades(pool):
    cl = await insert_session(pool)
    ag = await insert_agent(pool, cl["id"])
    await set_agent_status(pool, ag["id"])
    await insert_message(pool, cl["id"])
    await insert_event(pool, cl["id"], ag["id"])
    await insert_turn(pool, ag["id"])
    await insert_command(pool, ag["id"])
    await insert_check(pool, cl["id"])
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM sessions WHERE id=%s", (cl["id"],))
        for t in ("agents", "agent_status", "messages", "events", "turns", "commands", "checks"):
            cur = await conn.execute(f"SELECT count(*) FROM {t}")
            assert (await cur.fetchone())[0] == 0, t


async def test_notify_listen_roundtrip(dsn, pool):
    stream = listen(dsn, ["orch_test"])
    got: list = []

    async def reader():
        async for item in stream:
            got.append(item)
            return

    task = asyncio.create_task(reader())
    for _ in range(50):
        await asyncio.sleep(0.1)
        async with pool.connection() as conn:
            await notify(conn, "orch_test", {"a": 1, "b": [2]})
        if got:
            break
    await asyncio.wait_for(task, 5)
    assert got[0] == ("orch_test", {"a": 1, "b": [2]})
    await stream.aclose()


async def test_listen_reconnects_after_connection_killed(dsn, pool):
    stream = listen(dsn, ["orch_rc"], initial_backoff=0.05, max_backoff=0.2)
    got: list = []

    async def reader():
        async for item in stream:
            got.append(item)
            if len(got) == 2:
                return

    async def wait_listener() -> None:
        for _ in range(100):
            async with pool.connection() as conn:
                cur = await conn.execute(
                    "SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'LISTEN%%'"
                    " AND pid <> pg_backend_pid() AND datname = current_database()"
                )
                if (await cur.fetchone())[0]:
                    return
            await asyncio.sleep(0.05)
        raise AssertionError("listener never connected")

    task = asyncio.create_task(reader())
    await wait_listener()
    async with pool.connection() as conn:
        await notify(conn, "orch_rc", {"n": 1})
    for _ in range(100):
        if got:
            break
        await asyncio.sleep(0.05)
    assert got == [("orch_rc", {"n": 1})]
    async with pool.connection() as conn:
        await conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
            " WHERE query LIKE 'LISTEN%%' AND pid <> pg_backend_pid()"
            " AND datname = current_database()"
        )
    for _ in range(100):
        await asyncio.sleep(0.1)
        async with pool.connection() as conn:
            await notify(conn, "orch_rc", {"n": 2})
        if len(got) == 2:
            break
    await asyncio.wait_for(task, 5)
    assert got[1] == ("orch_rc", {"n": 2})
    await stream.aclose()


async def test_dispatch_columns_and_rebootstrap_kind(pool):
    assert "last_dispatched_at" in _columns(pool.conninfo, "agents")
    assert "wrapper_idle_since" in _columns(pool.conninfo, "agent_status")
    assert "heartbeat_at" in _columns(pool.conninfo, "messages")
    cl = await insert_session(pool)
    ag = await insert_agent(pool, cl["id"])
    cmd = await insert_command(pool, ag["id"], kind="rebootstrap")
    assert cmd["kind"] == "rebootstrap"
    with pytest.raises(psycopg.errors.CheckViolation):
        await insert_command(pool, ag["id"], kind="bogus")


async def test_retry_columns_verifying_status_and_no_purge_columns(pool):
    cols = _columns(pool.conninfo, "messages")
    assert {"not_before", "ambiguous_at"} <= cols
    cl = await insert_session(pool)
    m = await insert_message(pool, cl["id"], status="verifying")
    assert m["status"] == "verifying"
    with pytest.raises(psycopg.errors.CheckViolation):
        await insert_message(pool, cl["id"], status="nope")
    ccols = _columns(pool.conninfo, "sessions")
    assert "deleted_at" not in ccols
    assert not any("purge" in c for c in ccols)


DEFAULT_PRESET_ID = "00000000-0000-0000-0000-000000000001"


def test_default_preset_seeded(dsn):
    with psycopg.connect(dsn) as c:
        rows = c.execute("SELECT id::text, name, spec, is_default FROM presets").fetchall()
        assert rows == [(DEFAULT_PRESET_ID, "Default", {"replicas": 1}, True)]
        with pytest.raises(psycopg.errors.UniqueViolation):
            c.execute("INSERT INTO presets (id, name) VALUES (gen_random_uuid(), 'default')")
    with psycopg.connect(dsn) as c, pytest.raises(psycopg.errors.UniqueViolation):
        c.execute(
            "INSERT INTO presets (id, name, is_default) VALUES (gen_random_uuid(), 'x', true)"
        )
    with psycopg.connect(dsn) as c, pytest.raises(psycopg.errors.CheckViolation):
        c.execute("INSERT INTO presets (id, name) VALUES (gen_random_uuid(), '')")
    with psycopg.connect(dsn) as c, pytest.raises(psycopg.errors.CheckViolation):
        c.execute("INSERT INTO presets (id, name) VALUES (gen_random_uuid(), %s)", ("x" * 41,))


def test_presets_migration_reapply_keeps_edited_default(empty_dsn, tmp_path):
    for name in ("0001_init.sql", "0002_presets.sql", "0003_hive.sql"):
        shutil.copy(MIGRATIONS_DIR / name, tmp_path / name)
    run_migrations(empty_dsn, tmp_path)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        c.execute("UPDATE presets SET spec = '{\"replicas\": 4}' WHERE is_default")
        c.execute("DELETE FROM schema_migrations WHERE version IN ('0002', '0003')")
    assert run_migrations(empty_dsn, tmp_path) == ["0002", "0003"]
    with psycopg.connect(empty_dsn) as c:
        assert c.execute("SELECT name, spec FROM presets").fetchall() == [
            ("Default", {"replicas": 4})
        ]
        assert c.execute("SELECT count(*) FROM schema_migrations").fetchone()[0] == 3


async def test_preset_delete_keeps_session_provenance(pool):
    cols = _columns(pool.conninfo, "sessions")
    assert {"preset_id", "preset_name"} <= cols
    cl = await insert_session(pool)
    async with pool.connection() as conn:
        assert await (
            await conn.execute(
                "SELECT preset_id, preset_name FROM sessions WHERE id=%s", (cl["id"],)
            )
        ).fetchone() == (None, None)
        await conn.execute(
            "INSERT INTO presets (id, name) VALUES (%s, 'Big')",
            ("00000000-0000-0000-0000-0000000000aa",),
        )
        await conn.execute(
            "UPDATE sessions SET preset_id=%s, preset_name='Big' WHERE id=%s",
            ("00000000-0000-0000-0000-0000000000aa", cl["id"]),
        )
        await conn.execute("DELETE FROM presets WHERE name='Big'")
        cur = await conn.execute(
            "SELECT preset_id, preset_name FROM sessions WHERE id=%s", (cl["id"],)
        )
        assert await cur.fetchone() == (None, "Big")


def _cols(c, table):
    return {
        r[0]: (r[1], r[2])
        for r in c.execute(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns"
            " WHERE table_name=%s",
            (table,),
        )
    }


def test_0003_hive_schema(empty_dsn, tmp_path):
    for path in sorted(MIGRATIONS_DIR.glob("000[1-3]_*.sql")):
        shutil.copy(path, tmp_path / path.name)
    assert run_migrations(empty_dsn, tmp_path) == ["0001", "0002", "0003"]
    with psycopg.connect(empty_dsn) as c:
        lr = _cols(c, "leadership_requests")
        cl = _cols(c, "cluster_leaders")
        fl = _cols(c, "file_locks")
        ag = _cols(c, "agents")
        idx = [
            r[0]
            for r in c.execute(
                "SELECT indexdef FROM pg_indexes WHERE tablename='leadership_requests'"
            )
        ]
        pk = [
            r[0]
            for r in c.execute(
                "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid=i.indrelid"
                " AND a.attnum = ANY(i.indkey) WHERE i.indrelid='file_locks'::regclass"
                " AND i.indisprimary ORDER BY a.attname"
            )
        ]
    assert set(lr) == {
        "id", "cluster_id", "kind", "from_agent_id", "to_agent_id", "reason", "epoch",
        "status", "reject_reason", "created_at", "decided_at",
    }  # fmt: skip
    assert lr["to_agent_id"][1] == "YES" and lr["epoch"][1] == "YES"
    assert lr["reason"][1] == "NO" and lr["status"][1] == "NO"
    assert set(cl) == {
        "cluster_id", "agent_id", "epoch", "since", "reason", "unready_since",
        "feed_cursor", "state_hash", "state_pushed_at",
    }  # fmt: skip
    assert cl["feed_cursor"] == ("bigint", "NO") and cl["state_hash"][1] == "YES"
    assert cl["epoch"] == ("integer", "NO")
    assert set(fl) == {"cluster_id", "path", "agent_id", "claimed_at"}
    assert pk == ["cluster_id", "path"]
    assert ag["pod_ip"] == ("text", "YES")
    assert any("(cluster_id, id)" in i and "queued" in i for i in idx)


def test_0003_idempotent(empty_dsn, tmp_path):
    for name in ("0001_init.sql", "0002_presets.sql", "0003_hive.sql"):
        shutil.copy(MIGRATIONS_DIR / name, tmp_path / name)
    run_migrations(empty_dsn, tmp_path)
    sql = (MIGRATIONS_DIR / "0003_hive.sql").read_text()
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        c.execute(sql)
        c.execute(sql)
        c.execute("DELETE FROM schema_migrations WHERE version = '0003'")
    assert run_migrations(empty_dsn, tmp_path) == ["0003"]
    assert run_migrations(empty_dsn, tmp_path) == []


async def test_0003_constraints(pool):
    cl = await insert_session(pool)
    ag = await insert_agent(pool, cl["id"])
    ins = (
        "INSERT INTO leadership_requests (session_id, kind, from_agent_id, status)"
        " VALUES (%s, %s, %s, %s)"
    )
    async with pool.connection() as conn:
        await conn.execute(ins, (cl["id"], "handover", ag["id"], "queued"))
        await conn.execute(ins, (cl["id"], "user", ag["id"], "applied"))
        await conn.execute(ins, (cl["id"], "user", ag["id"], "rejected"))
    for kind, status in (("bogus", "queued"), ("user", "bogus")):
        with pytest.raises(psycopg.errors.CheckViolation):
            async with pool.connection() as conn:
                await conn.execute(ins, (cl["id"], kind, ag["id"], status))


def test_0003_backfills_max_replicas(empty_dsn, tmp_path):
    shutil.copy(MIGRATIONS_DIR / "0001_init.sql", tmp_path / "0001_init.sql")
    shutil.copy(MIGRATIONS_DIR / "0002_presets.sql", tmp_path / "0002_presets.sql")
    assert run_migrations(empty_dsn, tmp_path) == ["0001", "0002"]
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        for slug, spec in (
            ("a", {"replicas": 3}),
            ("b", {"replicas": 2, "max_replicas": 5}),
            ("c", {"model": "opus"}),
        ):
            c.execute(
                "INSERT INTO clusters (id, slug, spec) VALUES (gen_random_uuid(), %s, %s)",
                (slug, json.dumps(spec)),
            )
    shutil.copy(MIGRATIONS_DIR / "0003_hive.sql", tmp_path / "0003_hive.sql")
    assert run_migrations(empty_dsn, tmp_path) == ["0003"]

    def specs():
        with psycopg.connect(empty_dsn) as c:
            return c.execute("SELECT slug, spec FROM clusters ORDER BY slug").fetchall()

    expected = [
        ("a", {"replicas": 3, "max_replicas": 3}),
        ("b", {"replicas": 2, "max_replicas": 5}),
        ("c", {"model": "opus"}),
    ]
    assert specs() == expected
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        c.execute("UPDATE clusters SET spec = jsonb_set(spec, '{replicas}', '1') WHERE slug='a'")
        c.execute("DELETE FROM schema_migrations WHERE version = '0003'")
    assert run_migrations(empty_dsn, tmp_path) == ["0003"]
    assert specs()[0] == ("a", {"replicas": 1, "max_replicas": 3})


def test_0004_renames_wrapper_session_columns(empty_dsn, tmp_path):
    for name in ("0001_init.sql", "0002_presets.sql", "0003_hive.sql"):
        shutil.copy(MIGRATIONS_DIR / name, tmp_path / name)
    assert run_migrations(empty_dsn, tmp_path) == ["0001", "0002", "0003"]
    created = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        unit = c.execute(
            "INSERT INTO clusters (id, slug, spec) VALUES (gen_random_uuid(), 'old', '{}')"
            " RETURNING id"
        ).fetchone()[0]
        agent = c.execute(
            "INSERT INTO agents (id, cluster_id, ordinal, pod_name, session_id, session_created_at)"
            " VALUES (gen_random_uuid(), %s, 0, 'old-0', 'ws-1', %s) RETURNING id",
            (unit, created),
        ).fetchone()[0]
        event = c.execute(
            "INSERT INTO events (cluster_id, agent_id, session_id, seq, type, data)"
            " VALUES (%s, %s, 'ws-1', 1, 'assistant_text', '{}') RETURNING id",
            (unit, agent),
        ).fetchone()[0]
    shutil.copy(MIGRATIONS_DIR / "0004_wrapper_session_id.sql", tmp_path)
    assert run_migrations(empty_dsn, tmp_path) == ["0004"]
    with psycopg.connect(empty_dsn) as c:
        assert c.execute(
            "SELECT wrapper_session_id, wrapper_session_created_at FROM agents WHERE id=%s",
            (agent,),
        ).fetchall() == [("ws-1", created)]
        assert c.execute(
            "SELECT id, agent_id, seq FROM events WHERE wrapper_session_id = 'ws-1'"
        ).fetchall() == [(event, agent, 1)]
        constraints = {
            r[0]
            for r in c.execute(
                "SELECT conname FROM pg_constraint WHERE conrelid IN"
                " ('agents'::regclass, 'events'::regclass)"
            )
        }
        indexes = {
            r[0]
            for r in c.execute(
                "SELECT indexname FROM pg_indexes WHERE tablename IN ('agents', 'events')"
            )
        }
    for table in ("agents", "events"):
        assert not {"session_id", "session_created_at"} & _columns(empty_dsn, table), table
    assert "events_agent_id_wrapper_session_id_seq_key" in constraints
    assert not any("session" in n and "wrapper_session" not in n for n in constraints | indexes)


def test_0003_feed_cursor_default(empty_dsn, tmp_path):
    for path in sorted(MIGRATIONS_DIR.glob("000[1-3]_*.sql")):
        shutil.copy(path, tmp_path / path.name)
    assert run_migrations(empty_dsn, tmp_path) == ["0001", "0002", "0003"]
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        session = c.execute(
            "INSERT INTO clusters (id, slug, spec) VALUES (gen_random_uuid(), 's', '{}')"
            " RETURNING id"
        ).fetchone()[0]
        agent = c.execute(
            "INSERT INTO agents (id, cluster_id, ordinal, pod_name)"
            " VALUES (gen_random_uuid(), %s, 0, 's-0') RETURNING id",
            (session,),
        ).fetchone()[0]
        assert c.execute("SELECT count(*) FROM cluster_leaders").fetchone()[0] == 0
        c.execute(
            "INSERT INTO cluster_leaders (cluster_id, agent_id, epoch, since, reason)"
            " VALUES (%s, %s, 1, now(), 'init')",
            (session, agent),
        )
        row = c.execute("SELECT feed_cursor, state_hash FROM cluster_leaders").fetchone()
    assert row == (0, None)


def test_0005_renames_clusters_to_sessions(empty_dsn, tmp_path):
    for path in sorted(MIGRATIONS_DIR.glob("000[1-4]_*.sql")):
        shutil.copy(path, tmp_path / path.name)
    assert run_migrations(empty_dsn, tmp_path) == ["0001", "0002", "0003", "0004"]
    preset = "00000000-0000-0000-0000-0000000000bb"
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        c.execute("INSERT INTO presets (id, name) VALUES (%s, 'Kept')", (preset,))
        unit = c.execute(
            "INSERT INTO clusters (id, slug, spec, preset_id, preset_name)"
            " VALUES (gen_random_uuid(), 'kept', '{\"replicas\": 1}', %s, 'Kept') RETURNING id",
            (preset,),
        ).fetchone()[0]
        agent = c.execute(
            "INSERT INTO agents (id, cluster_id, ordinal, pod_name, wrapper_session_id)"
            " VALUES (gen_random_uuid(), %s, 0, 'kept-0', 'ws-1') RETURNING id",
            (unit,),
        ).fetchone()[0]
        message = c.execute(
            "INSERT INTO messages (cluster_id, text) VALUES (%s, 'hi') RETURNING id", (unit,)
        ).fetchone()[0]
        event = c.execute(
            "INSERT INTO events (cluster_id, agent_id, wrapper_session_id, seq, type)"
            " VALUES (%s, %s, 'ws-1', 1, 'assistant_text') RETURNING id",
            (unit, agent),
        ).fetchone()[0]
        check = c.execute(
            "INSERT INTO checks (cluster_id, command) VALUES (%s, 'true') RETURNING id", (unit,)
        ).fetchone()[0]
        request = c.execute(
            "INSERT INTO leadership_requests (cluster_id, kind, from_agent_id)"
            " VALUES (%s, 'user', %s) RETURNING id",
            (unit, agent),
        ).fetchone()[0]
        c.execute(
            "INSERT INTO cluster_leaders (cluster_id, agent_id, epoch, since, reason)"
            " VALUES (%s, %s, 3, now(), 'init')",
            (unit, agent),
        )
        c.execute(
            "INSERT INTO file_locks (cluster_id, path, agent_id) VALUES (%s, 'a.py', %s)",
            (unit, agent),
        )
    assert run_migrations(empty_dsn) == [
        "0005", "0006", "0007", "0008", "0009", "0010", "0011", "0012",
    ]  # fmt: skip
    with psycopg.connect(empty_dsn) as c:
        assert c.execute(
            "SELECT id, slug, preset_id::text, preset_name FROM sessions"
        ).fetchall() == [(unit, "kept", preset, "Kept")]
        assert c.execute("SELECT id, session_id FROM agents").fetchall() == [(agent, unit)]
        assert c.execute("SELECT id, session_id FROM messages").fetchall() == [(message, unit)]
        assert c.execute("SELECT id, session_id FROM events").fetchall() == [(event, unit)]
        assert c.execute("SELECT id, session_id FROM checks").fetchall() == [(check, unit)]
        assert c.execute("SELECT id, session_id FROM leadership_requests").fetchall() == [
            (request, unit)
        ]
        assert c.execute("SELECT session_id, agent_id, epoch FROM session_leaders").fetchall() == [
            (unit, agent, 3)
        ]
        assert c.execute("SELECT session_id, path, agent_id FROM file_locks").fetchall() == [
            (unit, "a.py", agent)
        ]
        names = [
            r[0]
            for r in c.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
                " UNION ALL SELECT column_name FROM information_schema.columns"
                " WHERE table_schema='public'"
                " UNION ALL SELECT indexname FROM pg_indexes WHERE schemaname='public'"
                " UNION ALL SELECT conname FROM pg_constraint k JOIN pg_namespace n"
                " ON n.oid = k.connamespace WHERE n.nspname='public'"
                " UNION ALL SELECT relname FROM pg_class r JOIN pg_namespace n"
                " ON n.oid = r.relnamespace WHERE n.nspname='public'"
            )
        ]
        indexes = {r[0] for r in c.execute("SELECT indexname FROM pg_indexes")}
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            c.execute(
                "INSERT INTO messages (session_id, text) VALUES (gen_random_uuid(), 'orphan')"
            )
    assert [n for n in names if "cluster" in n] == []
    assert {"sessions", "session_leaders"} <= set(names)
    assert {"messages_session_status_id", "events_session_id_desc"} <= indexes


def test_0006_compaction_column(empty_dsn, tmp_path):
    for path in sorted(MIGRATIONS_DIR.glob("000[1-6]_*.sql")):
        shutil.copy(path, tmp_path / path.name)
    assert run_migrations(empty_dsn, tmp_path)[-1] == "0006"
    with psycopg.connect(empty_dsn) as c:
        assert _cols(c, "agent_status")["compaction_at"] == ("timestamp with time zone", "YES")
    sql = (MIGRATIONS_DIR / "0006_compaction.sql").read_text()
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        c.execute(sql)
        c.execute(sql)
        c.execute("DELETE FROM schema_migrations WHERE version = '0006'")
    assert run_migrations(empty_dsn, tmp_path) == ["0006"]
    assert run_migrations(empty_dsn, tmp_path) == []
    with psycopg.connect(empty_dsn) as c:
        assert _cols(c, "agent_status")["compaction_at"] == ("timestamp with time zone", "YES")


def test_0007_session_message_columns(empty_dsn, tmp_path):
    for path in sorted(MIGRATIONS_DIR.glob("000[1-6]_*.sql")):
        shutil.copy(path, tmp_path / path.name)
    assert run_migrations(empty_dsn, tmp_path)[-1] == "0006"
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        session = c.execute(
            "INSERT INTO sessions (id, slug, spec) VALUES (gen_random_uuid(), 'old', '{}')"
            " RETURNING id"
        ).fetchone()[0]
        old = c.execute(
            "INSERT INTO messages (session_id, text) VALUES (%s, 'before') RETURNING id",
            (session,),
        ).fetchone()[0]
    shutil.copy(MIGRATIONS_DIR / "0007_session_messages.sql", tmp_path)
    assert run_migrations(empty_dsn, tmp_path) == ["0007"]
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        cols = _cols(c, "messages")
        assert cols["sender_session_id"] == ("uuid", "YES")
        assert cols["sender_slug"] == ("text", "YES")
        assert cols["hop"] == ("integer", "NO")
        assert c.execute(
            "SELECT sender_session_id, sender_slug, hop FROM messages WHERE id=%s", (old,)
        ).fetchall() == [(None, None, 0)]
        assert any(
            "(sender_session_id, created_at)" in r[0]
            for r in c.execute("SELECT indexdef FROM pg_indexes WHERE tablename='messages'")
        )
        # No foreign key: a purged sender session leaves the message intact.
        sender = c.execute(
            "INSERT INTO sessions (id, slug, spec) VALUES (gen_random_uuid(), 'gone', '{}')"
            " RETURNING id"
        ).fetchone()[0]
        sent = c.execute(
            "INSERT INTO messages (session_id, text, sender_session_id, sender_slug, hop)"
            " VALUES (%s, 'hi', %s, 'gone', 1) RETURNING id",
            (session, sender),
        ).fetchone()[0]
        c.execute("DELETE FROM sessions WHERE id=%s", (sender,))
        assert c.execute(
            "SELECT sender_session_id, sender_slug, hop FROM messages WHERE id=%s", (sent,)
        ).fetchall() == [(sender, "gone", 1)]
    sql = (MIGRATIONS_DIR / "0007_session_messages.sql").read_text()
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        c.execute(sql)
        c.execute(sql)
        c.execute("DELETE FROM schema_migrations WHERE version = '0007'")
    assert run_migrations(empty_dsn, tmp_path) == ["0007"]
    assert run_migrations(empty_dsn, tmp_path) == []
    with psycopg.connect(empty_dsn) as c:
        assert c.execute("SELECT count(*) FROM messages").fetchone()[0] == 2
        assert _cols(c, "messages")["hop"] == ("integer", "NO")


def test_0008_wrapper_resumed_at_column(empty_dsn, tmp_path):
    for path in sorted(MIGRATIONS_DIR.glob("000[1-7]_*.sql")):
        shutil.copy(path, tmp_path / path.name)
    assert run_migrations(empty_dsn, tmp_path)[-1] == "0007"
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        session = c.execute(
            "INSERT INTO sessions (id, slug, spec) VALUES (gen_random_uuid(), 'old', '{}')"
            " RETURNING id"
        ).fetchone()[0]
        agent = c.execute(
            "INSERT INTO agents (id, session_id, ordinal, pod_name, wrapper_session_id)"
            " VALUES (gen_random_uuid(), %s, 0, 'agent-old-0', 'ses_1') RETURNING id",
            (session,),
        ).fetchone()[0]
    shutil.copy(MIGRATIONS_DIR / "0008_wrapper_resume.sql", tmp_path)
    assert run_migrations(empty_dsn, tmp_path) == ["0008"]
    with psycopg.connect(empty_dsn) as c:
        assert _cols(c, "agents")["wrapper_resumed_at"] == ("timestamp with time zone", "YES")
        assert c.execute(
            "SELECT wrapper_session_id, wrapper_resumed_at FROM agents WHERE id=%s", (agent,)
        ).fetchall() == [("ses_1", None)]
    sql = (MIGRATIONS_DIR / "0008_wrapper_resume.sql").read_text()
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        c.execute(sql)
        c.execute(sql)
        c.execute("DELETE FROM schema_migrations WHERE version = '0008'")
    assert run_migrations(empty_dsn, tmp_path) == ["0008"]
    assert run_migrations(empty_dsn, tmp_path) == []


def test_0009_created_by_service_column(empty_dsn, tmp_path):
    for path in sorted(MIGRATIONS_DIR.glob("000[1-8]_*.sql")):
        shutil.copy(path, tmp_path / path.name)
    assert run_migrations(empty_dsn, tmp_path)[-1] == "0008"
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        c.execute("INSERT INTO sessions (id, slug, spec) VALUES (gen_random_uuid(), 'old', '{}')")
    shutil.copy(MIGRATIONS_DIR / "0009_created_by_service.sql", tmp_path)
    assert run_migrations(empty_dsn, tmp_path) == ["0009"]
    with psycopg.connect(empty_dsn) as c:
        assert _cols(c, "sessions")["created_by_service"] == ("text", "YES")
        assert c.execute("SELECT created_by_service FROM sessions").fetchall() == [(None,)]
    sql = (MIGRATIONS_DIR / "0009_created_by_service.sql").read_text()
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        c.execute(sql)
        c.execute(sql)
        c.execute("DELETE FROM schema_migrations WHERE version = '0009'")
    assert run_migrations(empty_dsn, tmp_path) == ["0009"]
    assert run_migrations(empty_dsn, tmp_path) == []


def test_drop_feed_cursor_migration(empty_dsn, tmp_path):
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if path.name < "0010":
            shutil.copy(path, tmp_path / path.name)
    assert run_migrations(empty_dsn, tmp_path)[-1] == "0009"
    with psycopg.connect(empty_dsn) as c:
        assert _cols(c, "session_leaders")["feed_cursor"] == ("bigint", "NO")
    shutil.copy(MIGRATIONS_DIR / "0011_drop_feed_cursor.sql", tmp_path)
    assert run_migrations(empty_dsn, tmp_path) == ["0011"]
    with psycopg.connect(empty_dsn) as c:
        cols = _cols(c, "session_leaders")
    assert "feed_cursor" not in cols
    assert {"session_id", "agent_id", "epoch", "since", "reason", "unready_since"} <= set(cols)
    sql = (MIGRATIONS_DIR / "0011_drop_feed_cursor.sql").read_text()
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        c.execute(sql)
        c.execute(sql)
        c.execute("DELETE FROM schema_migrations WHERE version = '0011'")
    assert run_migrations(empty_dsn, tmp_path) == ["0011"]
    assert run_migrations(empty_dsn, tmp_path) == []


def test_0010_slash_message_columns(empty_dsn, tmp_path):
    for path in sorted(MIGRATIONS_DIR.glob("000[1-9]_*.sql")):
        shutil.copy(path, tmp_path / path.name)
    assert run_migrations(empty_dsn, tmp_path)[-1] == "0009"
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        session = c.execute(
            "INSERT INTO sessions (id, slug, spec) VALUES (gen_random_uuid(), 'old', '{}')"
            " RETURNING id"
        ).fetchone()[0]
        old = c.execute(
            "INSERT INTO messages (session_id, text) VALUES (%s, '/reload-plugins') RETURNING id",
            (session,),
        ).fetchone()[0]
    shutil.copy(MIGRATIONS_DIR / "0010_slash_messages.sql", tmp_path)
    assert run_migrations(empty_dsn, tmp_path) == ["0010"]
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        cols = _cols(c, "messages")
        assert cols["kind"] == ("text", "NO")
        assert cols["wrapper_command_id"] == ("text", "YES")
        assert c.execute(
            "SELECT text, kind, wrapper_command_id FROM messages WHERE id=%s", (old,)
        ).fetchall() == [("/reload-plugins", "turn", None)]
        c.execute(
            "INSERT INTO messages (session_id, text, kind) VALUES (%s, '/compact', 'command')",
            (session,),
        )
        with pytest.raises(psycopg.errors.CheckViolation):
            c.execute(
                "INSERT INTO messages (session_id, text, kind) VALUES (%s, 'x', 'other')",
                (session,),
            )
    sql = (MIGRATIONS_DIR / "0010_slash_messages.sql").read_text()
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        c.execute(sql)
        c.execute(sql)
        c.execute("DELETE FROM schema_migrations WHERE version = '0010'")
    assert run_migrations(empty_dsn, tmp_path) == ["0010"]
    assert run_migrations(empty_dsn, tmp_path) == []
    with psycopg.connect(empty_dsn) as c:
        assert c.execute("SELECT kind FROM messages ORDER BY id").fetchall() == [
            ("turn",),
            ("command",),
        ]
        assert (
            c.execute(
                "SELECT count(*) FROM pg_constraint WHERE conrelid = 'messages'::regclass"
                " AND contype = 'c' AND pg_get_constraintdef(oid) LIKE '%%kind%%'"
            ).fetchone()[0]
            == 1
        )

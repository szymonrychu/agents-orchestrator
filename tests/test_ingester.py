"""Ingester (D5): one SSE subscription per agent session, stored events and live status."""

from __future__ import annotations

import asyncio
import contextlib
import json
import uuid
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest
import pytest_asyncio
from factories import (
    insert_agent,
    insert_event,
    insert_message,
    insert_session,
    set_agent_status,
)
from fakes.fake_wrapper import FakeWrapper
from psycopg import sql
from psycopg.types.json import Jsonb

from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import queue, status
from agents_orchestrator.db.leadership import CHANNEL as LEADERSHIP_CHANNEL
from agents_orchestrator.ingester import Ingester
from agents_orchestrator.ingester.loop import map_state
from agents_orchestrator.reconciler.wrapper_sessions import WrapperSessionBootstrapper
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec
from agents_orchestrator.tokens import derive_tokens
from agents_orchestrator.wrapper import Snapshot, WrapperClient

MASTER_KEY = "k" * 40
OLD_SINCE = "2001-02-03T04:05:06+00:00"


# --------------------------------------------------------------------------- helpers


async def wait_until(pred: Callable[[], Any], timeout: float = 20.0, interval: float = 0.02):
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        result = pred()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return result
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(interval)


async def fetch(pool, query: str, params: tuple = ()) -> list[dict[str, Any]]:
    async with pool.connection() as conn:
        cur = await conn.execute(query, params)
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in await cur.fetchall()]


async def status_row(pool, agent_id: uuid.UUID) -> dict[str, Any] | None:
    rows = await fetch(pool, "SELECT * FROM agent_status WHERE agent_id=%s", (agent_id,))
    return rows[0] if rows else None


async def events_rows(pool, agent_id: uuid.UUID) -> list[dict[str, Any]]:
    return await fetch(pool, "SELECT * FROM events WHERE agent_id=%s ORDER BY id", (agent_id,))


async def db_now(pool) -> datetime:
    return (await fetch(pool, "SELECT now() AS n"))[0]["n"]


class Listener:
    """Collects NOTIFY payloads on a dedicated autocommit connection."""

    def __init__(self) -> None:
        self.got: list[tuple[str, dict[str, Any]]] = []

    def on(self, channel: str) -> list[dict[str, Any]]:
        return [p for c, p in self.got if c == channel]


@pytest_asyncio.fixture
async def listener(dsn: str) -> AsyncIterator[Listener]:
    lst = Listener()
    conn = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    for ch in ("events", "status", "agent_free", LEADERSHIP_CHANNEL):
        await conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(ch)))

    async def pump() -> None:
        async for n in conn.notifies():
            lst.got.append((n.channel, json.loads(n.payload)))

    task = asyncio.create_task(pump())
    try:
        yield lst
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
        await conn.close()


class RecordingClient(WrapperClient):
    """A real client that records the Last-Event-ID of every stream it opens and the state of
    every session snapshot it reads.

    `replay_from` holds Last-Event-IDs that the next streams use instead of the ingester's own,
    one per stream, as a wrapper replaying older events would.
    """

    def __init__(
        self,
        *a: Any,
        opened: list[Any],
        gets: list[str | None] | None = None,
        replay_all: bool = False,
        replay_from: list[int] | None = None,
        **kw: Any,
    ) -> None:
        super().__init__(*a, **kw)
        self._opened = opened
        self._gets = gets if gets is not None else []
        self._replay_all = replay_all
        self._replay_from = replay_from if replay_from is not None else []

    async def get_session(self, session_id: str) -> Snapshot:
        snap = await super().get_session(session_id)
        self._gets.append(snap.state)
        return snap

    async def events(self, wrapper_session_id: str, last_event_id: int | str | None = None):
        if self._replay_all:
            last_event_id = 0
        elif self._replay_from:
            last_event_id = self._replay_from.pop(0)
        self._opened.append(last_event_id)
        async for env in super().events(wrapper_session_id, last_event_id):
            yield env


class Harness:
    """A session, a fake wrapper with one session, an agent row and a running Ingester."""

    def __init__(self, pool, dsn: str) -> None:
        self.pool, self.dsn = pool, dsn
        self.opened: list[Any] = []
        self.gets: list[str | None] = []
        self.sleeps: list[float] = []
        self.replay_all = False
        self.replay_from: list[int] = []
        self.client_cls: type[RecordingClient] = RecordingClient
        self.ingester: Ingester | None = None
        self._task: asyncio.Task[None] | None = None
        self._stop: asyncio.Event | None = None

    async def setup(self, peers: dict[str, Any] | None = None) -> Harness:
        self.session = await insert_session(self.pool)
        self.session_id = self.session["id"]
        token = derive_tokens(MASTER_KEY.encode(), self.session_id).auth
        self.fake = FakeWrapper(token=token)
        if peers is not None:
            self.fake.peers = peers
        await self.fake.start()
        self.client = WrapperClient(self.fake.base_url, token)
        snap = await self.client.create_session("a0", "w")
        self.wrapper_session_id = snap.wrapper_session_id
        self.agent = await insert_agent(
            self.pool,
            self.session_id,
            url=self.fake.base_url,
            wrapper_session_id=self.wrapper_session_id,
            ready=True,
        )
        self.agent_id = self.agent["id"]
        return self

    def settings(self) -> Settings:
        return Settings(database_url=self.dsn, master_key=MASTER_KEY, ingest_refresh_seconds=0.05)

    def factory(self, url: str, session_id: uuid.UUID) -> WrapperClient:
        token = derive_tokens(MASTER_KEY.encode(), session_id).auth
        return self.client_cls(
            url,
            token,
            opened=self.opened,
            gets=self.gets,
            replay_all=self.replay_all,
            replay_from=self.replay_from,
        )

    async def fast_sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        await asyncio.sleep(0.02)

    async def start(self, *, factory: bool = True) -> Ingester:
        self.ingester = Ingester(
            self.pool,
            self.settings(),
            self.factory if factory else None,
            sleep=self.fast_sleep,
        )
        self._stop = asyncio.Event()
        self._task = asyncio.create_task(self.ingester.run(self._stop))
        return self.ingester

    async def stop(self) -> None:
        if self._task is not None and self._stop is not None:
            self._stop.set()
            await asyncio.wait_for(self._task, 20)
            self._task = None

    async def close(self) -> None:
        await self.stop()
        await self.client.aclose()
        await self.fake.stop()

    async def stored_through(self, seq: int) -> None:
        async def done() -> bool:
            row = await status_row(self.pool, self.agent_id)
            return row is not None and (row["last_seq"] or 0) >= seq

        await wait_until(done)

    def last_fake_seq(self) -> int:
        return self.fake.sessions[self.wrapper_session_id].events[-1]["seq"]

    async def state(self) -> str | None:
        row = await status_row(self.pool, self.agent_id)
        return row["state"] if row is not None else None

    def shut_down(self) -> int:
        """The wrapper's SIGTERM path: the session stops and `session_closed` ends its streams.

        Returns the seq of the `session_closed` event.
        """
        self.fake.set_session_state("stopped", has_transcript=True)
        self.fake.emit("session_closed", {})
        return self.last_fake_seq()

    async def resume(self) -> None:
        """The reconciler's successful resume: the wrapper continues seq, the row records it."""
        snap = await self.client.resume_session(self.wrapper_session_id)
        async with self.pool.connection() as conn:
            await agents_repo.set_wrapper_session(
                conn, self.agent_id, snap.wrapper_session_id, snap.created_at, resumed=True
            )

    def emit_at(self, type: str, ts: str, data: dict[str, Any] | None = None) -> None:
        """Emit an event stamped with the wrapper time `ts` instead of now.

        The fake serialises an envelope only when its stream next reads it, after an await, so
        rewriting the returned envelope here is what the ingester receives.
        """
        self.fake.emit(type, data)["ts"] = ts


@pytest_asyncio.fixture
async def harness(pool, dsn: str) -> AsyncIterator[Callable[..., Any]]:
    made: list[Harness] = []

    async def make(peers: dict[str, Any] | None = None) -> Harness:
        h = await Harness(pool, dsn).setup(peers)
        made.append(h)
        return h

    try:
        yield make
    finally:
        for h in made:
            await h.close()


# --------------------------------------------------------------------------- tests


def test_state_mapping() -> None:
    assert map_state("ready") == "idle"
    assert map_state("busy") == "busy"
    assert map_state("creating") == "booting"
    for s in ("stopped", "failed", "dead"):
        assert map_state(s) == "dead"
    assert map_state("something-new") == "unknown"


async def test_attach_seeds_status_from_snapshot(harness, pool) -> None:
    h = await harness()
    h.fake.set_context(used_tokens=50_000, window_tokens=200_000)
    idle_since = h.fake.sessions[h.wrapper_session_id].idle_since
    before = await db_now(pool)
    await h.start()
    await h.stored_through(h.last_fake_seq())

    row = await status_row(pool, h.agent_id)
    assert row["state"] == "idle"
    assert row["wrapper_idle_since"] == datetime.fromisoformat(idle_since)
    assert row["free_since"] is not None and row["free_since"] >= before
    assert row["context_used_tokens"] == 50_000
    assert row["context_window_tokens"] == 200_000
    assert row["context_pct"] == pytest.approx(25.0)
    assert h.opened[0] == 0  # a fresh session streams from the start

    # a null snapshot context clears the columns on the next attach
    await h.stop()
    h.fake.set_context()
    await h.start()
    await wait_until(lambda: len(h.opened) >= 2)

    async def cleared() -> bool:
        r = await status_row(pool, h.agent_id)
        return r["context_used_tokens"] is None

    await wait_until(cleared)
    row = await status_row(pool, h.agent_id)
    assert row["context_window_tokens"] is None and row["context_pct"] is None


async def test_attach_maps_creating_to_booting(harness, pool) -> None:
    h = await harness()
    # the log is already stored, so the stream replays nothing and the snapshot decides
    for env in h.fake.sessions[h.wrapper_session_id].events:
        await insert_event(
            pool,
            h.session_id,
            h.agent_id,
            wrapper_session_id=h.wrapper_session_id,
            seq=env["seq"],
            type=env["type"],
            data=env["data"],
        )
    await set_agent_status(pool, h.agent_id, state="idle", free_since=None)
    h.fake.sessions[h.wrapper_session_id].state = "creating"
    await h.start()

    async def booting() -> bool:
        r = await status_row(pool, h.agent_id)
        return r["state"] == "booting"

    await wait_until(booting)
    row = await status_row(pool, h.agent_id)
    assert row["free_since"] is None
    assert row["wrapper_idle_since"] is None
    assert row["last_seq"] == h.last_fake_seq()
    await wait_until(lambda: h.opened)
    assert h.opened == [h.last_fake_seq()]


async def test_events_stored_and_notified(harness, pool, listener) -> None:
    h = await harness()
    await h.start()
    await h.stored_through(h.last_fake_seq())
    h.fake.emit("assistant_text", {"text": "hello there"})
    await h.stored_through(h.last_fake_seq())

    rows = await events_rows(pool, h.agent_id)
    fake_events = h.fake.sessions[h.wrapper_session_id].events
    assert [r["seq"] for r in rows] == [e["seq"] for e in fake_events]
    assert [r["type"] for r in rows] == [e["type"] for e in fake_events]
    assert rows[-1]["data"] == {"text": "hello there"}
    assert all(
        r["session_id"] == h.session_id and r["wrapper_session_id"] == h.wrapper_session_id
        for r in rows
    )

    await wait_until(lambda: rows[-1]["id"] in [p["max_id"] for p in listener.on("events")])
    assert all(p["session_id"] == str(h.session_id) for p in listener.on("events"))


async def test_resume_uses_last_event_id_no_duplicates(harness, pool) -> None:
    h = await harness()
    await h.start()
    h.fake.emit("assistant_text", {"text": "one"})
    first_seq = h.last_fake_seq()
    await h.stored_through(first_seq)

    h.fake.drop_streams()
    await wait_until(lambda: len(h.opened) >= 2)
    h.fake.emit("assistant_text", {"text": "two"})
    await h.stored_through(h.last_fake_seq())

    assert h.opened[0] == 0
    assert h.opened[1] == first_seq

    # a fresh ingester process resumes from the stored last_seq too
    await h.stop()
    h.fake.emit("assistant_text", {"text": "three"})
    await h.start()
    await h.stored_through(h.last_fake_seq())
    assert h.opened[-1] == first_seq + 1

    rows = await events_rows(pool, h.agent_id)
    seqs = [r["seq"] for r in rows]
    assert seqs == sorted(set(seqs))
    assert seqs == [e["seq"] for e in h.fake.sessions[h.wrapper_session_id].events]


async def test_gap_row_on_seq_hole(harness, pool) -> None:
    h = await harness()
    await h.start()
    before = h.last_fake_seq()
    await h.stored_through(before)

    h.fake.skip_seq(3)
    h.fake.emit("assistant_text", {"text": "after the hole"})
    after = h.last_fake_seq()
    await h.stored_through(after)

    gaps = [r for r in await events_rows(pool, h.agent_id) if r["type"] == "gap"]
    assert len(gaps) == 1
    assert gaps[0]["data"] == {"after_seq": before, "before_seq": after}
    rows = await events_rows(pool, h.agent_id)
    # the gap row is written before the event that revealed it
    assert [r["type"] for r in rows][-2:] == ["gap", "assistant_text"]


async def test_idle_sets_free_since_and_notifies_agent_free(harness, pool, listener) -> None:
    h = await harness()
    await h.start()
    await h.stored_through(h.last_fake_seq())

    h.fake.emit("session_state", {"state": "busy", "since": OLD_SINCE})
    await h.stored_through(h.last_fake_seq())
    busy = await status_row(pool, h.agent_id)
    assert busy["state"] == "busy"
    assert busy["wrapper_idle_since"] is None
    listener.got.clear()

    before = await db_now(pool)
    h.fake.emit("session_state", {"state": "ready", "since": OLD_SINCE})
    await h.stored_through(h.last_fake_seq())
    after = await db_now(pool)

    row = await status_row(pool, h.agent_id)
    assert row["state"] == "idle"
    assert before <= row["free_since"] <= after  # the database clock
    assert row["free_since"] != datetime.fromisoformat(OLD_SINCE)
    assert row["wrapper_idle_since"] == datetime.fromisoformat(OLD_SINCE)

    want = {"session_id": str(h.session_id), "agent_id": str(h.agent_id)}
    await wait_until(lambda: listener.on("agent_free"))
    assert listener.on("agent_free") == [want]
    await wait_until(lambda: listener.on("status"))
    assert all(p["session_id"] == str(h.session_id) for p in listener.on("status"))


async def test_repeated_idle_does_not_move_free_since(harness, pool, listener) -> None:
    h = await harness()
    await h.start()
    await h.stored_through(h.last_fake_seq())
    first = await status_row(pool, h.agent_id)
    assert first["state"] == "idle"
    listener.got.clear()

    # a repeated idle event (e.g. a restart notice) while already idle
    h.fake.emit("session_state", {"state": "ready", "since": OLD_SINCE})
    await h.stored_through(h.last_fake_seq())
    row = await status_row(pool, h.agent_id)
    assert row["free_since"] == first["free_since"]
    assert row["wrapper_idle_since"] == first["wrapper_idle_since"]

    # a replay of the whole log (a fresh process whose stream starts at seq 0)
    await h.stop()
    h.replay_all = True
    await h.start()
    await wait_until(lambda: len(h.opened) >= 2)
    h.fake.emit("assistant_text", {"text": "marker"})
    await h.stored_through(h.last_fake_seq())
    row = await status_row(pool, h.agent_id)
    assert row["free_since"] == first["free_since"]
    assert listener.on("agent_free") == []

    # and the repository rule on its own
    async with pool.connection() as conn:
        change = await status.set_state(conn, h.agent_id, "idle", since=OLD_SINCE)
    assert not change.became_idle
    assert (await status_row(pool, h.agent_id))["free_since"] == first["free_since"]


async def test_context_event_updates_status(harness, pool, listener) -> None:
    h = await harness()
    await h.start()
    await h.stored_through(h.last_fake_seq())
    listener.got.clear()

    ctx = {
        "used_tokens": 120_000,
        "window_tokens": 200_000,
        "used_percentage": 60.0,
        "model": "claude-sonnet-4-5",
        "source": "statusline",
        "updated_at": datetime.now(UTC).isoformat(),
    }
    h.fake.emit("context", ctx)
    await h.stored_through(h.last_fake_seq())
    row = await status_row(pool, h.agent_id)
    assert (row["context_used_tokens"], row["context_window_tokens"]) == (120_000, 200_000)
    assert row["context_pct"] == pytest.approx(60.0)
    await wait_until(lambda: listener.on("status"))

    # compaction resets the gauge: the wrapper sends nulls
    h.fake.emit("context", {**dict.fromkeys(("used_tokens", "window_tokens", "used_percentage"))})
    await h.stored_through(h.last_fake_seq())
    row = await status_row(pool, h.agent_id)
    assert row["context_used_tokens"] is None
    assert row["context_window_tokens"] is None
    assert row["context_pct"] is None


T1 = "2026-10-20T10:00:00.000+00:00"
T2 = "2026-10-20T11:00:00.000+00:00"


async def compaction_at(pool, agent_id: uuid.UUID) -> datetime | None:
    return (await status_row(pool, agent_id))["compaction_at"]


async def test_compacting_sets_compaction_at(harness, pool, listener) -> None:
    h = await harness()
    await h.start()
    await h.stored_through(h.last_fake_seq())
    assert await compaction_at(pool, h.agent_id) is None
    listener.got.clear()

    h.emit_at("compacting", T1, {"trigger": "auto"})
    await h.stored_through(h.last_fake_seq())

    # the event's own time (the wrapper's clock), not the ingest time
    assert await compaction_at(pool, h.agent_id) == datetime.fromisoformat(T1)
    assert [r["type"] for r in await events_rows(pool, h.agent_id)][-1] == "compacting"
    want = {"session_id": str(h.session_id), "agent_id": str(h.agent_id)}
    await wait_until(lambda: want in listener.on("status"))
    await wait_until(lambda: listener.on(LEADERSHIP_CHANNEL))
    assert listener.on(LEADERSHIP_CHANNEL) == [{"session_id": str(h.session_id)}]


async def test_compacted_moves_compaction_at(harness, pool, listener) -> None:
    h = await harness()
    await h.start()
    await h.stored_through(h.last_fake_seq())

    h.emit_at("compacting", T1, {"trigger": "manual"})
    await h.stored_through(h.last_fake_seq())
    assert await compaction_at(pool, h.agent_id) == datetime.fromisoformat(T1)
    listener.got.clear()

    h.emit_at("compacted", T2, {"trigger": "manual"})
    await h.stored_through(h.last_fake_seq())
    assert await compaction_at(pool, h.agent_id) == datetime.fromisoformat(T2)
    await wait_until(lambda: listener.on(LEADERSHIP_CHANNEL))
    assert listener.on(LEADERSHIP_CHANNEL) == [{"session_id": str(h.session_id)}]

    # no other event type writes it, whatever its time
    later = "2026-10-20T12:00:00.000+00:00"
    listener.got.clear()
    h.emit_at("context", later, dict.fromkeys(("used_tokens", "window_tokens", "used_percentage")))
    h.emit_at("assistant_text", later, {"text": "after the compaction"})
    h.emit_at("user_message", later, {"turn_id": None, "origin": "command", "text": "/compact"})
    h.emit_at("session_state", later, {"state": "busy"})
    h.emit_at("session_state", later, {"state": "ready", "since": later})
    await h.stored_through(h.last_fake_seq())
    assert await compaction_at(pool, h.agent_id) == datetime.fromisoformat(T2)
    assert listener.on(LEADERSHIP_CHANNEL) == []


async def test_late_compaction_event_keeps_its_time(harness, pool, listener) -> None:
    h = await harness()
    await h.start()
    await h.stored_through(h.last_fake_seq())

    # a late event on an empty column sets its own (old) time, never a later one
    h.emit_at("compacted", T1, {"trigger": "auto"})
    await h.stored_through(h.last_fake_seq())
    assert await compaction_at(pool, h.agent_id) == datetime.fromisoformat(T1)

    h.emit_at("compacted", T2, {"trigger": "auto"})
    await h.stored_through(h.last_fake_seq())
    assert await compaction_at(pool, h.agent_id) == datetime.fromisoformat(T2)

    # an older compaction ingested after a newer one never moves it backwards
    h.emit_at("compacting", T1, {"trigger": "auto"})
    h.emit_at("compacted", OLD_SINCE, {"trigger": "auto"})
    await h.stored_through(h.last_fake_seq())
    assert await compaction_at(pool, h.agent_id) == datetime.fromisoformat(T2)

    # a replayed stream (already stored rows) is not re-projected and changes nothing
    await h.stop()
    h.replay_all = True
    await h.start()
    await wait_until(lambda: len(h.opened) >= 2)
    await h.stored_through(h.last_fake_seq())
    assert await compaction_at(pool, h.agent_id) == datetime.fromisoformat(T2)

    # and the repository rule on its own
    async with pool.connection() as conn:
        await status.set_compaction(conn, h.agent_id, T1)
    assert await compaction_at(pool, h.agent_id) == datetime.fromisoformat(T2)


async def test_turn_projection(harness, pool) -> None:
    h = await harness()
    await h.start()
    await h.stored_through(h.last_fake_seq())

    accepted = await h.client.submit_turn(h.wrapper_session_id, "do the thing")
    turn_id = accepted.turn_id
    await h.stored_through(h.last_fake_seq())
    running = (await fetch(pool, "SELECT * FROM turns WHERE turn_id=%s", (turn_id,)))[0]
    assert running["agent_id"] == h.agent_id
    assert running["origin"] == "client"
    assert running["status"] == "running"
    assert running["completed_at"] is None
    assert (await status_row(pool, h.agent_id))["current_turn_id"] == turn_id

    await h.fake.complete_turn(turn_id, "all done")
    await h.stored_through(h.last_fake_seq())
    done = (await fetch(pool, "SELECT * FROM turns WHERE turn_id=%s", (turn_id,)))[0]
    assert done["status"] == "completed"
    assert done["final_text"] == "all done"
    assert done["completed_at"] is not None
    assert done["started_at"] == running["started_at"]
    assert (await status_row(pool, h.agent_id))["current_turn_id"] is None

    # usage travels on the terminal record; a failed turn projects too
    record = {
        "turn_id": "turn-failed-1",
        "session_id": h.wrapper_session_id,
        "origin": "peer",
        "state": "interrupted",
        "text": "x",
        "final_text": "partial",
        "usage": {"input_tokens": 10, "output_tokens": 3},
        "started_at": "2026-10-23T10:00:00+00:00",
        "completed_at": "2026-10-23T10:00:05+00:00",
    }
    h.fake.emit("turn_failed", {"record": record})
    h.fake.emit(
        "turn_completed", {"record": {**record, "turn_id": "turn-ok-1", "state": "completed"}}
    )
    await h.stored_through(h.last_fake_seq())
    failed = (await fetch(pool, "SELECT * FROM turns WHERE turn_id='turn-failed-1'"))[0]
    assert failed["status"] == "interrupted"
    assert failed["origin"] == "peer"
    assert failed["final_text"] == "partial"
    assert failed["usage"] == {"input_tokens": 10, "output_tokens": 3}
    assert failed["started_at"] == datetime.fromisoformat(record["started_at"])
    assert failed["completed_at"] == datetime.fromisoformat(record["completed_at"])
    ok = (await fetch(pool, "SELECT * FROM turns WHERE turn_id='turn-ok-1'"))[0]
    assert ok["status"] == "completed" and ok["usage"] == {"input_tokens": 10, "output_tokens": 3}


async def test_user_message_stored_as_event_row(harness, pool) -> None:
    h = await harness()
    await h.start()
    await h.stored_through(h.last_fake_seq())

    h.fake.emit(
        "user_message",
        {"turn_id": "turn-peer-1", "origin": "peer", "text": "hi from b", "files": []},
    )
    h.fake.emit(
        "user_message", {"turn_id": None, "origin": "command", "text": "/compact", "files": []}
    )
    h.fake.emit("user_message", {"turn_id": "turn-r", "origin": "restart", "text": "continue"})
    await h.stored_through(h.last_fake_seq())

    rows = [r for r in await events_rows(pool, h.agent_id) if r["type"] == "user_message"]
    assert [r["data"]["origin"] for r in rows] == ["peer", "command", "restart"]
    assert rows[0]["data"]["text"] == "hi from b"
    assert rows[0]["turn_id"] == "turn-peer-1"
    assert rows[1]["turn_id"] is None
    assert await fetch(pool, "SELECT * FROM turns WHERE agent_id=%s", (h.agent_id,)) == []


async def test_reconnect_backoff_after_disconnect(harness, pool) -> None:
    h = await harness()
    await h.start()
    await h.stored_through(h.last_fake_seq())

    await h.fake.set_unreachable(True)
    await wait_until(lambda: len(h.sleeps) >= 4)
    assert h.sleeps[:4] == [1.0, 2.0, 4.0, 8.0]
    await wait_until(lambda: len(h.sleeps) >= 7)
    assert h.sleeps[5:7] == [30.0, 30.0]  # capped

    opened = len(h.opened)
    await h.fake.set_unreachable(False)
    await wait_until(lambda: len(h.opened) > opened)
    h.fake.emit("assistant_text", {"text": "back"})
    await h.stored_through(h.last_fake_seq())

    # a received event reset the backoff: the next drop waits 1 s again
    n = len(h.sleeps)
    h.fake.drop_streams()
    await wait_until(lambda: len(h.sleeps) > n)
    assert h.sleeps[n] == 1.0


async def test_agent_removed_stops_stream(harness, pool) -> None:
    h = await harness()
    ingester = await h.start()
    await h.stored_through(h.last_fake_seq())
    assert h.agent_id in ingester.agent_ids()
    ses = h.fake.sessions[h.wrapper_session_id]
    await wait_until(lambda: len(ses.subscribers) == 1)

    async with pool.connection() as conn:
        await conn.execute("UPDATE agents SET gone_at=now() WHERE id=%s", (h.agent_id,))
    await wait_until(lambda: h.agent_id not in ingester.agent_ids())
    await wait_until(lambda: len(ses.subscribers) == 0)

    seq = h.last_fake_seq()
    h.fake.emit("assistant_text", {"text": "nobody listens"})
    await asyncio.sleep(0.3)
    assert (await status_row(pool, h.agent_id))["last_seq"] == seq
    assert ingester.agent_ids() == set()


class StubbornClient(RecordingClient):
    """A client whose stream absorbs the first cancellation, as a shielded close can."""

    async def events(self, wrapper_session_id: str, last_event_id: int | str | None = None):
        async for env in super().events(wrapper_session_id, last_event_id):
            yield env
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.Event().wait()  # the next cancel ends it


async def test_cancel_retried_when_stream_absorbs_it(harness, pool) -> None:
    h = await harness()
    h.client_cls = StubbornClient
    ingester = await h.start()
    ingester.cancel_grace = 0.05
    await h.stored_through(h.last_fake_seq())
    h.fake.drop_streams()  # the stream ends and parks in the stubborn wait
    await asyncio.sleep(0.1)

    async with pool.connection() as conn:
        await conn.execute("UPDATE agents SET gone_at=now() WHERE id=%s", (h.agent_id,))
    await wait_until(lambda: h.agent_id not in ingester.agent_ids(), timeout=3)

    # the refresh loop is not wedged: a new agent still gets its stream
    snap = await h.client.create_session("a1", "w")
    other = await insert_agent(
        pool,
        h.session_id,
        url=h.fake.base_url,
        wrapper_session_id=snap.wrapper_session_id,
        ready=True,
    )
    await wait_until(lambda: other["id"] in ingester.agent_ids(), timeout=3)
    await asyncio.wait_for(h.stop(), 3)


async def test_new_session_restarts_stream(harness, pool) -> None:
    h = await harness()
    ingester = await h.start()
    await h.stored_through(h.last_fake_seq())

    snap = await h.client.create_session("a0-again", "w")
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE agents SET wrapper_session_id=%s WHERE id=%s",
            (snap.wrapper_session_id, h.agent_id),
        )

    async def stored_new() -> bool:
        rows = await fetch(
            pool,
            "SELECT count(*) AS n FROM events WHERE wrapper_session_id=%s",
            (snap.wrapper_session_id,),
        )
        return rows[0]["n"] >= 2

    await wait_until(stored_new)
    assert ingester.agent_ids() == {h.agent_id}


async def test_session_closed_marks_dead_and_stops(harness, pool, listener) -> None:
    """A session_closed whose wrapper snapshot is dead ends the stream and leaves the agent dead.

    Amended for deploy-safe-agent-sessions D3: the old rule was "no reconnect until the
    reconciler records a new session". A resume of the same wrapper session now re-attaches, so a
    close is honoured only when the wrapper's live snapshot maps to dead. The first D3 rule,
    "no resume recorded after the stored close, by database order", was wrong: a close the
    wrapper emitted before a resume is stored after it when the ingester was down across the
    resume, so database order honoured it for every later attach. `agents.wrapper_resumed_at`
    is a re-attach trigger only; a recorded resume with a stopped snapshot still ends the task.
    """
    h = await harness()
    ingester = await h.start()
    await h.stored_through(h.last_fake_seq())
    opened = len(h.opened)
    listener.got.clear()

    # the session stops without a session_state event: the close and its snapshot mark it dead
    h.fake.sessions[h.wrapper_session_id].state = "stopped"
    h.fake.emit("session_closed", {})
    await h.stored_through(h.last_fake_seq())

    async def dead() -> bool:
        return await h.state() == "dead"

    await wait_until(dead)
    assert h.gets[-1] == "stopped"
    want = {"session_id": str(h.session_id), "agent_id": str(h.agent_id)}
    await wait_until(lambda: want in listener.on("status"))
    await asyncio.sleep(0.3)
    assert len(h.opened) == opened  # no reconnect: the wrapper says the session is dead
    assert h.agent_id in ingester.agent_ids()

    # a recorded resume does not lift it while the snapshot is stopped, also for a fresh
    # ingester process
    await h.stop()
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE agents SET wrapper_resumed_at = clock_timestamp() WHERE id = %s",
            (h.agent_id,),
        )
    gets = len(h.gets)
    ingester = await h.start()
    await wait_until(lambda: len(h.gets) > gets)
    await asyncio.sleep(0.3)
    assert h.gets[gets:] == ["stopped"]
    assert await h.state() == "dead"
    assert len(h.opened) == opened  # honoured on attach: no stream opened


async def test_close_replayed_after_resume_not_honoured(harness, pool, listener) -> None:
    """The incident path: the ingester was down across the shutdown and the resume.

    The close the wrapper emitted before the resume reaches the ingester only on the replay
    after it, so it is stored after `wrapper_resumed_at`. The live snapshot decides: the
    resumed session is live, so the stream re-attaches from the close seq with the backoff
    reset, and the resumed `session_state ready` frees the agent.
    """
    h = await harness()
    await h.start()
    stored = h.last_fake_seq()  # M: the stored log ends before the shutdown
    await h.stored_through(stored)
    await h.stop()
    closed_seq = h.shut_down()  # N, never streamed: the ingester is down
    await h.resume()  # N + 1, session_state ready
    opened, sleeps = len(h.opened), len(h.sleeps)
    listener.got.clear()

    ingester = await h.start()
    await h.stored_through(closed_seq + 1)

    async def idle() -> bool:
        return await h.state() == "idle"

    await wait_until(idle)
    assert h.opened[opened:] == [stored, closed_seq]
    assert h.sleeps[sleeps:] == [1.0]  # the received events reset the backoff
    seqs = [r["seq"] for r in await events_rows(pool, h.agent_id)]
    assert closed_seq in seqs and closed_seq + 1 in seqs
    want = {"session_id": str(h.session_id), "agent_id": str(h.agent_id)}
    await wait_until(lambda: listener.on("agent_free"))
    await asyncio.sleep(0.3)
    assert listener.on("agent_free") == [want]

    # the stream runs: a live event is stored
    h.fake.emit("assistant_text", {"text": "live"})
    await h.stored_through(h.last_fake_seq())
    assert (await events_rows(pool, h.agent_id))[-1]["data"] == {"text": "live"}
    assert h.agent_id in ingester.agent_ids()


async def test_wrapper_resume_after_an_honoured_close_reattaches(harness, pool) -> None:
    """A busy pod deleted by hand (kind e2e AC6, CI run 40676464845).

    Its wrapper's graceful shutdown stops the session and emits `session_closed`; the snapshot
    is stopped, so the ingester honours the close, marks the agent dead and ends the task. The
    new pod's wrapper resumes the session itself (`sessions.resume_on_start`, D5) and continues
    the turn, so nothing the ingester watches changes. The reconciler's bootstrap then lists the
    stored session live; its adopt of a dead agent records the resume marker, so the ingester
    re-attaches from the close seq and the continued turn completes.
    """
    h = await harness()
    await h.start()
    accepted = await h.client.submit_turn(h.wrapper_session_id, "a long task")
    await h.stored_through(h.last_fake_seq())
    closed_seq = h.shut_down()
    await h.stored_through(closed_seq)

    async def state_is(want: str) -> bool:
        return await h.state() == want

    await wait_until(lambda: state_is("dead"))
    opened = len(h.opened)

    await h.client.resume_session(h.wrapper_session_id)  # the wrapper's own: nothing recorded
    h.fake.set_session_state("busy")
    h.fake.sessions[h.wrapper_session_id].current_turn_id = accepted.turn_id
    await asyncio.sleep(0.3)
    assert await h.state() == "dead" and len(h.opened) == opened  # nothing re-attaches by itself

    async with pool.connection() as conn:
        agent = await agents_repo.get_agent(conn, h.agent_id)
    assert agent is not None
    boot = WrapperSessionBootstrapper(pool, lambda url, token: WrapperClient(url, token))
    token = derive_tokens(MASTER_KEY.encode(), h.session_id).auth
    assert await boot.bootstrap(agent, SessionSpec(), token) is None

    await wait_until(lambda: state_is("busy"))  # the attach projects the snapshot first
    await wait_until(lambda: len(h.opened) > opened)
    assert h.opened[opened:] == [closed_seq]
    assert (await status_row(pool, h.agent_id))["current_turn_id"] == accepted.turn_id
    await h.fake.complete_turn(accepted.turn_id, "continued")
    await h.stored_through(h.last_fake_seq())
    (turn,) = await fetch(
        pool, "SELECT status, final_text FROM turns WHERE turn_id=%s", (accepted.turn_id,)
    )
    assert turn == {"status": "completed", "final_text": "continued"}
    assert await h.state() == "idle"


async def test_resume_reattaches_from_last_stored_seq(harness, pool) -> None:
    h = await harness()
    await h.start()
    await h.stored_through(h.last_fake_seq())
    closed_seq = h.shut_down()
    await h.stored_through(closed_seq)
    assert await h.state() == "dead"
    opened = len(h.opened)

    await h.resume()
    await wait_until(lambda: len(h.opened) > opened)
    assert h.opened[opened] == closed_seq  # from the last stored seq, the close itself
    h.fake.emit("assistant_text", {"text": "after the resume"})
    await h.stored_through(h.last_fake_seq())

    rows = await events_rows(pool, h.agent_id)
    seqs = [r["seq"] for r in rows]
    assert seqs == [e["seq"] for e in h.fake.sessions[h.wrapper_session_id].events]
    assert seqs[-1] > closed_seq + 1  # the resume's session_state and the later event
    assert rows[-1]["data"] == {"text": "after the resume"}


async def test_closed_before_resume_not_honoured(harness, pool) -> None:
    h = await harness()
    closed_seq = h.shut_down()
    # a stored log that ends with the close, as the ingester left it before the pod roll
    for env in h.fake.sessions[h.wrapper_session_id].events:
        await insert_event(
            pool,
            h.session_id,
            h.agent_id,
            wrapper_session_id=h.wrapper_session_id,
            seq=env["seq"],
            type=env["type"],
            data=env["data"],
        )
    await set_agent_status(pool, h.agent_id, state="dead", free_since=None, last_seq=closed_seq)
    await h.resume()  # recorded after the close was stored

    ingester = await h.start()
    await h.stored_through(h.last_fake_seq())
    assert await h.state() == "idle"
    assert h.opened == [closed_seq]

    # the stream runs: a live event is stored
    h.fake.emit("assistant_text", {"text": "live"})
    await h.stored_through(h.last_fake_seq())
    assert (await events_rows(pool, h.agent_id))[-1]["data"] == {"text": "live"}
    assert h.agent_id in ingester.agent_ids()


async def test_reattach_marks_idle_and_notifies(harness, pool, listener) -> None:
    h = await harness()
    await h.start()
    await h.stored_through(h.last_fake_seq())
    await h.stored_through(h.shut_down())
    assert await h.state() == "dead"
    message = await insert_message(pool, h.session_id, text="queued while dead")
    async with pool.connection() as conn:
        assert await queue.claim_and_pick(conn) is None  # a dead agent takes nothing
    listener.got.clear()

    before = await db_now(pool)
    await h.resume()

    async def idle() -> bool:
        return await h.state() == "idle"

    await wait_until(idle)
    row = await status_row(pool, h.agent_id)
    assert row["free_since"] >= before  # the database clock, at the re-attach
    want = {"session_id": str(h.session_id), "agent_id": str(h.agent_id)}
    await wait_until(lambda: listener.on("agent_free"))
    assert listener.on("agent_free") == [want]

    # the dispatcher's claim now delivers the queued message to the agent
    async with pool.connection() as conn:
        claim = await queue.claim_and_pick(conn)
    assert claim is not None
    assert (claim.message_id, claim.agent_id) == (message["id"], h.agent_id)


async def test_reattach_does_not_reproject_replayed_events(harness, pool, listener) -> None:
    h = await harness()
    await h.start()
    await h.stored_through(h.last_fake_seq())
    h.emit_at("compacting", T1, {"trigger": "auto"})
    compacting_seq = h.last_fake_seq()
    await h.stored_through(compacting_seq)
    assert await compaction_at(pool, h.agent_id) == datetime.fromisoformat(T1)
    closed_seq = h.shut_down()
    await h.stored_through(closed_seq)
    await wait_until(lambda: h.gets[-1:] == ["stopped"])  # the fresh close read the snapshot
    # cleared, so a re-projected compacting event would show here again
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE agent_status SET compaction_at = NULL WHERE agent_id = %s", (h.agent_id,)
        )
    opened, gets = len(h.opened), len(h.gets)
    listener.got.clear()

    # the first re-attach stream replays from before the compaction through the stored close
    h.replay_from.append(compacting_seq - 1)
    await h.resume()
    await wait_until(lambda: len(h.opened) >= opened + 2)
    await h.stored_through(h.last_fake_seq())
    assert h.opened[opened : opened + 2] == [compacting_seq - 1, closed_seq]

    assert await compaction_at(pool, h.agent_id) is None
    assert listener.on(LEADERSHIP_CHANNEL) == []
    assert await h.state() == "idle"
    # the replayed close was not projected: the agent became free once, at the first attach
    await asyncio.sleep(0.3)
    want = {"session_id": str(h.session_id), "agent_id": str(h.agent_id)}
    assert listener.on("agent_free") == [want]
    seqs = [r["seq"] for r in await events_rows(pool, h.agent_id)]
    assert seqs == sorted(set(seqs))
    # one snapshot per attach and no further stream: the replayed, already stored close reads
    # no snapshot
    assert h.opened[opened:] == [compacting_seq - 1, closed_seq]
    assert h.gets[gets:] == ["ready", "ready"]


async def test_peers_view_recorded(harness, pool) -> None:
    members = [
        {
            "instance": "agent-0",
            "url": "http://agent-0:8080",
            "sessions": [{"session_id": "s0", "name": "a0", "cwd": "/w"}],
            "last_seen_at": "2026-10-23T10:00:00Z",
            "missed": 0,
        },
        {
            "instance": "agent-1",
            "url": "http://agent-1:8080",
            "sessions": [],
            "last_seen_at": None,
            "missed": 2,
        },
    ]
    view = {"enabled": True, "self": {"instance": "agent-0", "url": "http://a0"}}
    h = await harness(peers={**view, "members": members})
    await h.start()
    await h.stored_through(h.last_fake_seq())

    async def recorded() -> bool:
        return len((await status_row(pool, h.agent_id))["peers"]) == 2

    await wait_until(recorded)
    assert (await status_row(pool, h.agent_id))["peers"] == members

    # peers are refreshed on a transition into idle; a disabled mesh counts 0 even when the
    # view still lists members
    h.fake.peers = {"enabled": False, "self": None, "members": members}
    h.fake.emit("session_state", {"state": "busy", "since": OLD_SINCE})
    h.fake.emit("session_state", {"state": "ready", "since": OLD_SINCE})

    async def zero() -> bool:
        return (await status_row(pool, h.agent_id))["peers"] == []

    await wait_until(zero)
    assert status.peer_count({"enabled": False}) == 0
    assert status.peer_count({"enabled": False, "members": members}) == 0
    assert status.peer_count({"enabled": True, "members": members}) == 2


async def _command_message(
    pool, h: Harness, command_id: str, *, status: str = "sent"
) -> dict[str, Any]:
    """A command message of the agent's session in `status`, carrying `command_id` once sent."""
    row = await insert_message(pool, h.session_id, status=status, agent_id=h.agent_id)
    sent = status == "sent"
    return (
        await fetch(
            pool,
            "UPDATE messages SET kind = 'command', wrapper_command_id = %s WHERE id = %s"
            " RETURNING *",
            (command_id if sent else None, row["id"]),
        )
    )[0]


def _command_record(command_id: str, result: str, turn_id: str | None) -> dict[str, Any]:
    return {
        "record": {
            "id": command_id,
            "name": "x",
            "args": "",
            "origin": "client",
            "turn_id": turn_id,
            "state": "completed",
            "result": result,
        }
    }


async def _message_turn(pool, message_id: int) -> str | None:
    return (await fetch(pool, "SELECT turn_id FROM messages WHERE id=%s", (message_id,)))[0][
        "turn_id"
    ]


async def test_command_completed_turn_links_message(harness, pool) -> None:
    h = await harness()
    await h.start()
    await h.stored_through(h.last_fake_seq())
    promoted = await _command_message(pool, h, "cmd_turn")
    plain = await _command_message(pool, h, "cmd_ok")
    other = await _command_message(pool, h, "cmd_other")

    h.fake.emit("command_completed", _command_record("cmd_ok", "ok", "turn-ignored"))
    h.fake.emit("command_completed", _command_record("cmd_turn", "turn", "turn-promoted"))
    await h.stored_through(h.last_fake_seq())
    assert await _message_turn(pool, promoted["id"]) == "turn-promoted"
    assert await _message_turn(pool, plain["id"]) is None
    assert await _message_turn(pool, other["id"]) is None

    # a second event for the same command never overwrites the linked turn
    h.fake.emit("command_completed", _command_record("cmd_turn", "turn", "turn-again"))
    await h.stored_through(h.last_fake_seq())
    assert await _message_turn(pool, promoted["id"]) == "turn-promoted"

    # a turn result without a turn id links nothing
    h.fake.emit("command_completed", _command_record("cmd_other", "turn", None))
    await h.stored_through(h.last_fake_seq())
    assert await _message_turn(pool, other["id"]) is None


async def test_command_turn_linked_when_event_precedes_sent(harness, pool) -> None:
    h = await harness()
    await h.start()
    await h.stored_through(h.last_fake_seq())
    promoted = await _command_message(pool, h, "cmd_early", status="dispatching")
    plain = await _command_message(pool, h, "cmd_early_ok", status="dispatching")

    # the events are stored while both messages are still dispatching: nothing to link yet
    h.fake.emit("command_completed", _command_record("cmd_early", "turn", "turn-early"))
    h.fake.emit("command_completed", _command_record("cmd_early_ok", "ok", "turn-ignored"))
    await h.stored_through(h.last_fake_seq())
    assert await _message_turn(pool, promoted["id"]) is None

    async with pool.connection() as conn:
        assert await queue.mark_sent_command(conn, promoted["id"], "cmd_early")
        assert await queue.mark_sent_command(conn, plain["id"], "cmd_early_ok")
    rows = await fetch(pool, "SELECT id, status, turn_id FROM messages ORDER BY id")
    assert [(r["status"], r["turn_id"]) for r in rows] == [("sent", "turn-early"), ("sent", None)]


async def _lock_waiting(pool, pid: int | None = None, *, advisory: bool) -> bool:
    """A backend (`pid`, or any backend of this database) waits on a heavyweight lock: an
    advisory lock when `advisory`, otherwise a row or transaction lock."""
    rows = await fetch(
        pool,
        "SELECT 1 FROM pg_stat_activity WHERE datname = current_database()"
        " AND wait_event_type = 'Lock' AND (wait_event = 'advisory') = %s"
        " AND (%s::int IS NULL OR pid = %s::int)",
        (advisory, pid, pid),
    )
    return bool(rows)


async def test_command_turn_linked_when_event_commits_during_mark_sent(harness, pool, dsn) -> None:
    """The event is stored while mark_sent_command's UPDATE is already running: either the
    UPDATE or the ingester's link must see the other's write (D10 hop guard, both orders)."""
    h = await harness()
    await h.start()
    await h.stored_through(h.last_fake_seq())
    msg = await _command_message(pool, h, "cmd_race", status="dispatching")
    blocker = await psycopg.AsyncConnection.connect(dsn)
    sender = await psycopg.AsyncConnection.connect(dsn)
    send: asyncio.Task[bool] | None = None
    try:
        # the row lock holds mark_sent_command's UPDATE after its statement snapshot is taken
        await blocker.execute("SELECT 1 FROM messages WHERE id = %s FOR UPDATE", (msg["id"],))
        send = asyncio.create_task(queue.mark_sent_command(sender, msg["id"], "cmd_race"))
        await wait_until(lambda: _lock_waiting(pool, sender.info.backend_pid, advisory=False))

        h.fake.emit("command_completed", _command_record("cmd_race", "turn", "turn-race"))
        seq = h.last_fake_seq()

        async def ingested_or_held() -> bool:
            row = await status_row(pool, h.agent_id)
            stored = row is not None and (row["last_seq"] or 0) >= seq
            return stored or await _lock_waiting(pool, advisory=True)

        await wait_until(ingested_or_held)
        await blocker.commit()
        assert await asyncio.wait_for(send, 20)
        await h.stored_through(seq)
        assert await _message_turn(pool, msg["id"]) == "turn-race"
    finally:
        if send is not None and not send.done():
            send.cancel()
        await blocker.close()
        await sender.close()


async def test_command_turn_linked_when_mark_sent_starts_before_event_commits(
    harness, pool, dsn
) -> None:
    """mark_sent_command starts while the ingester's transaction (event stored, link found a
    dispatching row) is still open: the ingester commits before the UPDATE runs, so the
    UPDATE must read the event with a snapshot taken after that commit."""
    h = await harness()
    msg = await _command_message(pool, h, "cmd_race2", status="dispatching")
    ingest = await psycopg.AsyncConnection.connect(dsn)
    sender = await psycopg.AsyncConnection.connect(dsn)
    send: asyncio.Task[bool] | None = None
    try:
        await ingest.execute(
            "INSERT INTO events (session_id, agent_id, wrapper_session_id, seq, ts, type, data)"
            " VALUES (%s, %s, %s, 1, now(), 'command_completed', %s)",
            (
                h.session_id,
                h.agent_id,
                h.wrapper_session_id,
                Jsonb(_command_record("cmd_race2", "turn", "turn-race2")),
            ),
        )
        assert not await queue.link_command_turn(ingest, "cmd_race2", "turn-race2")
        send = asyncio.create_task(queue.mark_sent_command(sender, msg["id"], "cmd_race2"))
        await wait_until(
            lambda: send.done() or _lock_waiting(pool, sender.info.backend_pid, advisory=True)
        )
        await ingest.commit()
        assert await asyncio.wait_for(send, 20)
        assert await _message_turn(pool, msg["id"]) == "turn-race2"
    finally:
        if send is not None and not send.done():
            send.cancel()
        await ingest.close()
        await sender.close()

"""WS /api/v1/agents/{agent_id}/terminals/{terminal_id}/attach: the terminal relay (D2, D5, D6).

The api is served by uvicorn on a loopback port inside the test loop (Starlette `TestClient`
runs its own loop, which the async psycopg pool cannot share, and `httpx.ASGITransport` has no
WebSocket support), and clients connect with the `websockets` asyncio client. Agents are
`FakeWrapper`s; the close-code cases use `ScriptedUpstream`, a bare `websockets` server the
test drives frame by frame, because they need closes no well-behaved wrapper sends (an empty
close frame, a dropped TCP connection, a reserved code on the wire).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import socket
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import cast

import pytest
import pytest_asyncio
import uvicorn
from factories import insert_agent, insert_session, set_agent_status
from fakes.fake_wrapper import FakeWrapper
from test_api_files import KEY, Rig, token_of
from test_api_sessions import client_for, make_settings
from websockets.asyncio.client import ClientConnection, connect
from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed, InvalidStatus

from agents_orchestrator.api import workspace
from agents_orchestrator.api.app import create_app

GOOD_ORIGIN = "https://orch.example"
RELAY_TASK_PREFIX = "terminal-relay"
WAIT = 5.0


def _bind() -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    sock.setblocking(False)
    return sock


@contextlib.asynccontextmanager
async def serve_api(app) -> AsyncIterator[int]:
    """The api on a loopback port in this loop (production's ws implementation: auto)."""
    sock = _bind()
    config = uvicorn.Config(app, log_level="warning", lifespan="off", timeout_graceful_shutdown=1)
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve(sockets=[sock]))
    while not server.started:
        if task.done():
            task.result()
        await asyncio.sleep(0.01)
    try:
        yield sock.getsockname()[1]
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 10)


@dataclass
class RelayRig(Rig):
    app: object = None
    port: int = 0
    scripts: list[ScriptedUpstream] = field(default_factory=list)

    def ws_url(self, agent_id, terminal_id: str) -> str:
        return f"ws://127.0.0.1:{self.port}/api/v1/agents/{agent_id}/terminals/{terminal_id}/attach"

    def attach(self, agent_id, terminal_id: str, **kw) -> connect:
        kw.setdefault("proxy", None)  # the sandbox may export HTTP(S)_PROXY
        kw.setdefault("open_timeout", WAIT)
        return connect(self.ws_url(agent_id, terminal_id), **kw)

    async def terminal(self, row: dict) -> str:
        """A new terminal on that agent, created through the api's POST route."""
        r = await self.http.post(f"/api/v1/agents/{row['id']}/terminals", json={})
        assert r.status_code == 201, r.text
        return r.json()["terminal_id"]

    async def scripted(self, ordinal: int) -> tuple[ScriptedUpstream, dict]:
        """A scripted upstream registered as a ready agent of the session."""
        upstream = ScriptedUpstream()
        await upstream.start()
        self.scripts.append(upstream)
        sid = f"ses_{uuid.uuid4().hex[:12]}"
        row = await insert_agent(
            self.pool,
            self.session["id"],
            ordinal=ordinal,
            url=upstream.base_url,
            wrapper_session_id=sid,
            ready=True,
        )
        await set_agent_status(self.pool, row["id"], state="idle")
        return upstream, row


class ScriptedUpstream:
    """A wrapper stand-in that hands every accepted connection to the test."""

    def __init__(self) -> None:
        self.conns: asyncio.Queue[ServerConnection] = asyncio.Queue()
        self.paths: list[str] = []
        self.auth: list[str | None] = []
        self._server = None

    async def _handler(self, conn: ServerConnection) -> None:
        self.paths.append(conn.request.path)
        self.auth.append(conn.request.headers.get("Authorization"))
        self.conns.put_nowait(conn)
        await conn.wait_closed()

    async def start(self) -> None:
        self._server = await serve(self._handler, "127.0.0.1", 0)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    @property
    def base_url(self) -> str:
        assert self._server is not None
        port = self._server.sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{port}"

    async def next_conn(self) -> ServerConnection:
        return await asyncio.wait_for(self.conns.get(), WAIT)


@pytest_asyncio.fixture
async def rig(pool, dsn) -> AsyncIterator[RelayRig]:
    settings = make_settings(
        dsn, master_key=KEY, allowed_origins=GOOD_ORIGIN, allowed_cidrs="127.0.0.0/8,10.1.2.0/24"
    )
    app = create_app(settings, pool)
    session = await insert_session(pool)
    state = RelayRig(None, pool, session, [], app)
    async with serve_api(app) as port, client_for(app) as http:
        state.port, state.http = port, http
        # FastAPI builds every route of a new app on its first request (about 0.5 s of CPU);
        # take that here, so no WebSocket opening handshake (WAIT) pays for it
        assert (await http.get("/healthz")).status_code == 200
        try:
            yield state
        finally:
            await workspace.close_client(app)
            for fake in state.fakes:
                await fake.stop()
            for script in state.scripts:
                await script.stop()


def ws_calls(fake: FakeWrapper) -> list[str]:
    """Paths of the authenticated WebSocket attaches the fake accepted."""
    return [path for method, path in fake.requests if method == "websocket"]


async def recv(ws: ClientConnection) -> str | bytes:
    return await asyncio.wait_for(ws.recv(), WAIT)


async def closed_code(ws: ClientConnection) -> tuple[int | None, str]:
    """(code, reason) of the close frame the api sent; None when there was no close frame."""
    with pytest.raises(ConnectionClosed) as info:
        while True:
            await recv(ws)
    rcvd = info.value.rcvd
    return (rcvd.code, rcvd.reason) if rcvd else (None, "")


def relay_tasks() -> list[asyncio.Task]:
    return [t for t in asyncio.all_tasks() if t.get_name().startswith(RELAY_TASK_PREFIX)]


async def no_relay_tasks() -> None:
    for _ in range(int(WAIT / 0.02)):
        if not relay_tasks():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"relay tasks still running: {relay_tasks()}")


async def until(predicate, what: str) -> None:
    for _ in range(int(WAIT / 0.02)):
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


# ----------------------------------------------------------------------------------- tests


async def test_relay_round_trip(rig):
    fake, sid, row = await rig.agent(0)
    tid = await rig.terminal(row)
    replay = b"$ make\r\n\x1b[32mok\x1b[0m\r\n\xff\xfe\x00raw"
    fake.terminal_output(replay)

    async with rig.attach(row["id"], tid) as ws:
        assert await recv(ws) == replay  # the scrollback comes first, byte for byte
        await ws.send(b"echo hi\r")
        assert await recv(ws) == b"echo hi\r"  # the fake shell echoes input as output
        fake.terminal_output(b"hi\r\n\x80\x81")
        assert await recv(ws) == b"hi\r\n\x80\x81"
        big = os.urandom(60000)  # under the wrapper's 64 KiB frame cap
        await ws.send(big)
        echoed = await recv(ws)
        assert isinstance(echoed, bytes) and echoed == big
        assert len(relay_tasks()) == 2
    assert ws_calls(fake) == [f"/v1/sessions/{sid}/terminals/{tid}/attach"]
    await no_relay_tasks()


async def test_control_frames_relayed(rig):
    fake, _, row = await rig.agent(0)
    tid = await rig.terminal(row)

    async with rig.attach(row["id"], tid, origin=GOOD_ORIGIN) as ws:
        await ws.send(json.dumps({"type": "resize", "cols": 132, "rows": 43}))
        term = fake.terminal(tid)
        await until(lambda: (term.rows, term.cols) == (43, 132), "the resize to reach the shell")
        await ws.send(json.dumps({"type": "ping"}))
        pong = await recv(ws)
        assert isinstance(pong, str) and json.loads(pong) == {"type": "pong"}
        fake.exit_terminal(tid, code=3)
        exit_frame = await recv(ws)
        assert isinstance(exit_frame, str) and json.loads(exit_frame) == {"type": "exit", "code": 3}
        assert await closed_code(ws) == (1000, "")
    await no_relay_tasks()


async def test_bearer_sent_upstream(rig):
    # the fake records an attach only when the bearer is the session's derived token
    fake, sid, row = await rig.agent(0)
    tid = await rig.terminal(row)
    async with rig.attach(row["id"], tid) as ws:
        await ws.send(json.dumps({"type": "ping"}))
        assert json.loads(await recv(ws)) == {"type": "pong"}
    assert ws_calls(fake) == [f"/v1/sessions/{sid}/terminals/{tid}/attach"]

    # and the literal header, as a scripted upstream sees it
    upstream, srow = await rig.scripted(1)
    sid_tid = "term_0123456789ab"
    async with rig.attach(srow["id"], sid_tid):
        conn = await upstream.next_conn()
        assert upstream.auth == [f"Bearer {token_of(rig.session['id'])}"]
        assert upstream.paths == [
            f"/v1/sessions/{srow['wrapper_session_id']}/terminals/{sid_tid}/attach"
        ]
        assert token_of(rig.session["id"]) not in conn.request.path
    await no_relay_tasks()


async def test_origin_refused_before_upstream(rig):
    fake, _, row = await rig.agent(0)
    tid = await rig.terminal(row)
    for origin in ("https://evil.example", "null", "https://orch.example:8443"):
        with pytest.raises(InvalidStatus) as info:
            async with rig.attach(row["id"], tid, origin=origin):
                pass
        assert info.value.response.status_code == 403, origin
    assert ws_calls(fake) == []
    assert relay_tasks() == []

    # the listed origin, and no Origin at all (a headless client), both get through
    async with rig.attach(row["id"], tid, origin=GOOD_ORIGIN) as ws:
        await ws.send(b"x")
        assert await recv(ws) == b"x"
    async with rig.attach(row["id"], tid) as ws:
        assert await recv(ws) == b"x"  # the replay of the first attach's echo
        await ws.send(b"y")
        assert await recv(ws) == b"y"
    assert len(ws_calls(fake)) == 2


async def test_failure_close_codes(rig):
    fake, _, row = await rig.agent(0)
    tid = await rig.terminal(row)
    _, _, pending = await rig.agent(1, ready=False)
    missing = "term_000000000000"

    async def code_of(agent_id, terminal_id: str) -> int | None:
        async with rig.attach(agent_id, terminal_id) as ws:
            code, _reason = await closed_code(ws)
        return code

    # unknown agent (well-formed or not): 4404, nothing opened upstream
    assert await code_of(uuid.uuid4(), tid) == 4404
    assert await code_of("not-a-uuid", tid) == 4404
    # the wrapper does not know the terminal: its 404 becomes 4404
    assert await code_of(row["id"], missing) == 4404
    # the agent is not ready: 4409
    assert await code_of(pending["id"], tid) == 4409
    # the wrapper session is not active: the wrapper's 409 is also an agent that is not ready
    fake.set_session_state("stopped")
    assert await code_of(row["id"], tid) == 4409
    fake.set_session_state("ready")
    # a wrapper that rejects the bearer: 1011
    token, fake.token = fake.token, "someone-else"
    assert await code_of(row["id"], tid) == 1011
    fake.token = token
    # an unreachable wrapper: 1011
    await fake.set_unreachable(True)
    assert await code_of(row["id"], tid) == 1011
    await fake.set_unreachable(False)
    # wrapper access not configured (no master key): any other failure is 1011
    settings = rig.app.state.settings
    rig.app.state.settings = settings.model_copy(update={"master_key": None})
    try:
        assert await code_of(row["id"], tid) == 1011
    finally:
        rig.app.state.settings = settings
    # and the terminal is still reachable once things are back
    async with rig.attach(row["id"], tid) as ws:
        await ws.send(b"ok")
        assert await recv(ws) == b"ok"
    await no_relay_tasks()


async def test_invalid_terminal_id_refused(rig):
    fake, _, row = await rig.agent(0)
    await rig.terminal(row)
    upstream, srow = await rig.scripted(1)
    bad_ids = (
        "term_XYZ",
        "term_0123456789AB",  # upper case
        "term_0123456789abc",  # 13 hex digits
        "%2E%2E",  # decodes to ".."
        "term_0123456789ab%0A",  # trailing newline must not slip past $
        "sessions",
    )
    for agent in (row, srow):
        for bad in bad_ids:
            async with rig.attach(agent["id"], bad) as ws:
                assert (await closed_code(ws))[0] == 1008, bad
    assert ws_calls(fake) == []
    assert upstream.paths == []
    await no_relay_tasks()


@pytest.mark.parametrize(
    ("sent", "expected"),
    [
        (1000, 1000),
        (1003, 1003),
        (1008, 1008),
        (1009, 1009),
        (1011, 1011),
        (1013, 1013),
        (4000, 4000),
        (4321, 4321),
        (4999, 4999),
    ],
)
def test_close_code_mapping_passes_listed_codes(sent, expected):
    assert workspace.relay_close_code(sent) == expected


async def test_close_propagates_without_leak(rig):
    # the mapping table, including what the transports below cannot put on the wire
    mapping = {1005: 1000, 1006: 1011, 1015: 1011, None: 1011}
    for code in (1000, 1003, 1008, 1009, 1011, 1013, 4000, 4404, 4999):
        mapping[code] = code
    for sent, expected in mapping.items():
        assert workspace.relay_close_code(sent) == expected, sent
    for reserved in (1005, 1006, 1015, None):
        assert workspace.relay_close_code(reserved) not in (1005, 1006, 1015)

    upstream, row = await rig.scripted(0)
    tid = "term_00000000abcd"

    async def session() -> tuple[ClientConnection, ServerConnection]:
        ws = await rig.attach(row["id"], tid)
        conn = await upstream.next_conn()
        await conn.send(b"hello")  # both sides open and relaying
        assert await recv(ws) == b"hello"
        assert len(relay_tasks()) == 2
        return ws, conn

    # upstream closes: the client gets the mapped code
    for code in (1000, 1003, 1009, 1013, 4000, 4404):
        ws, conn = await session()
        await conn.close(code, "bye")
        assert await closed_code(ws) == (code, "bye"), code
        await no_relay_tasks()

    # an empty close frame (1005 on the receiving side) becomes 1000
    ws, conn = await session()
    conn.transport.write(b"\x88\x00")
    assert (await closed_code(ws))[0] == 1000
    await no_relay_tasks()

    # a dropped connection, no close frame (1006): 1011
    ws, conn = await session()
    conn.transport.abort()
    assert (await closed_code(ws))[0] == 1011
    await no_relay_tasks()

    # 1015 on the wire is a protocol error for the relay's client: no usable code, 1011
    ws, conn = await session()
    conn.transport.write(b"\x88\x02\x03\xf7")
    assert (await closed_code(ws))[0] == 1011
    await no_relay_tasks()

    # the client closes: the upstream gets the mapped code
    for code in (1000, 1003, 1009, 1013, 4000, 4999):
        ws, conn = await session()
        await ws.close(code, "later")
        await asyncio.wait_for(conn.wait_closed(), WAIT)
        assert conn.close_code == code, code
        assert conn.close_reason == "later"
        await no_relay_tasks()

    # the client sends an empty close frame (1005): the upstream gets 1000, never 1005
    ws, conn = await session()
    await ws.close(cast(int, None))  # no code: an empty close frame
    await asyncio.wait_for(conn.wait_closed(), WAIT)
    assert conn.close_code == 1000
    await no_relay_tasks()

    # the client vanishes without a close frame: the upstream is closed, nothing lingers
    ws, conn = await session()
    ws.transport.abort()
    await asyncio.wait_for(conn.wait_closed(), WAIT)
    assert conn.protocol.close_rcvd is not None
    assert conn.close_code not in (1005, 1006, 1015)
    await no_relay_tasks()


async def test_token_not_in_logs(rig, caplog):
    caplog.set_level(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger="websockets")
    fake, _, row = await rig.agent(0)
    token = token_of(rig.session["id"])
    tid = await rig.terminal(row)
    fake.terminal_output(b"replayed\r\n")
    frames: list[str | bytes] = []
    reasons: list[str] = []

    async with rig.attach(row["id"], tid, origin=GOOD_ORIGIN) as ws:
        frames.append(await recv(ws))
        await ws.send(b"ls\r")
        frames.append(await recv(ws))
        await ws.send(json.dumps({"type": "resize", "cols": 90, "rows": 30}))
        await ws.send(json.dumps({"type": "ping"}))
        frames.append(await recv(ws))
        fake.exit_terminal(tid, code=0)
        frames.append(await recv(ws))
        reasons.append((await closed_code(ws))[1])
    # failure paths too: a wrapper 404 and a rejected bearer
    for terminal_id in ("term_000000000000", tid):
        async with rig.attach(row["id"], terminal_id) as ws:
            reasons.append((await closed_code(ws))[1])
    fake.token = "rejects-the-derived-one"
    async with rig.attach(row["id"], tid) as ws:
        reasons.append((await closed_code(ws))[1])
    await no_relay_tasks()

    assert caplog.records, "DEBUG capture is live"
    # websockets' own DEBUG handshake logging is captured (the test's client logs its headers)
    assert any(r.name == "websockets.client" and "> GET" in r.getMessage() for r in caplog.records)
    for record in caplog.records:
        assert token not in record.getMessage(), record.name
        assert token not in repr(record.args), record.name
        assert token not in (record.exc_text or ""), record.name
    assert token not in caplog.text
    for frame in frames:
        payload = frame if isinstance(frame, bytes) else frame.encode()
        assert token.encode() not in payload
    assert all(token not in reason for reason in reasons)

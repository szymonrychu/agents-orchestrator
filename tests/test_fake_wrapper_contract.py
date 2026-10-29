"""One contract suite for the wrapper API, run against FakeWrapper and the real wrapper image.

The `real` leg needs ORCH_TEST_WRAPPER_IMAGE (see tests/fakes/real_wrapper.py) and is marked
`integration`. Both legs run the same assertions, so the fake cannot drift from the wrapper.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from typing import Any

import httpx
import pytest
import websockets
from fakes.fake_wrapper import FakeWrapper
from fakes.real_wrapper import RealWrapper

from agents_orchestrator.wrapper import (
    Ambiguous,
    Busy,
    CommandAccepted,
    ConfigError,
    Envelope,
    Gone,
    Rejected,
    TurnAccepted,
    Unavailable,
    WrapperClient,
)

TOKEN = "ci-token"
SLOW = "slow please"  # the real leg's scenario sleeps for a few seconds on this prompt
TIMEOUT = 40.0


@pytest.fixture(scope="session")
def real_wrapper() -> Iterator[RealWrapper]:
    image = os.environ["ORCH_TEST_WRAPPER_IMAGE"]  # integration leg: required, KeyError if unset
    wrapper = RealWrapper(image, token=TOKEN)
    wrapper.start()
    try:
        yield wrapper
    finally:
        wrapper.stop()


@dataclass
class Leg:
    kind: str
    base_url: str
    fake: FakeWrapper | None
    http: httpx.AsyncClient
    sessions: list[str]

    def client(self, **kw: Any) -> WrapperClient:
        return WrapperClient(self.base_url, TOKEN, **kw)

    async def create(self, name: str = "contract") -> str:
        resp = await self.http.post("/v1/sessions", json={"name": name}, timeout=120)
        assert resp.status_code == 201, resp.text
        sid = resp.json()["session_id"]
        self.sessions.append(sid)
        return sid

    async def submit(self, sid: str, text: str = "hello", **extra: Any) -> httpx.Response:
        return await self.http.post(f"/v1/sessions/{sid}/turns", json={"text": text, **extra})

    async def finish(self, sid: str, turn_id: str) -> None:
        """Let the turn end: the fake completes on demand, the real fake claude on its own."""
        if self.fake is not None:
            await self.fake.complete_turn(turn_id, "done")
        await self.wait_turn(sid, turn_id)

    async def wait_turn(self, sid: str, turn_id: str) -> dict[str, Any]:
        end = time.monotonic() + TIMEOUT
        while True:
            rec = (await self.http.get(f"/v1/sessions/{sid}/turns/{turn_id}")).json()
            if rec["state"] != "running":
                return rec
            assert time.monotonic() < end, "turn did not finish"
            await asyncio.sleep(0.1)

    async def wait_ready(self, sid: str) -> dict[str, Any]:
        end = time.monotonic() + TIMEOUT
        while True:
            snap = (await self.http.get(f"/v1/sessions/{sid}")).json()
            if snap["state"] == "ready":
                return snap
            assert time.monotonic() < end, f"session stuck in {snap['state']}"
            await asyncio.sleep(0.1)

    async def turn(self, sid: str, text: str = "hello", **extra: Any) -> str:
        resp = await self.submit(sid, text, **extra)
        assert resp.status_code == 202, resp.text
        turn_id = resp.json()["turn_id"]
        await self.finish(sid, turn_id)
        await self.wait_ready(sid)
        return turn_id

    async def history(self, sid: str, **params: Any) -> httpx.Response:
        return await self.http.get(f"/v1/sessions/{sid}/history", params=params)

    async def collect(self, sid: str, done: Any, after: int | None = None) -> list[Any]:
        """Follow the SSE stream until `done(events)` is true."""
        client = self.client()
        out: list[Any] = []
        try:
            async with asyncio.timeout(TIMEOUT):
                async for ev in client.events(sid, last_event_id=after):
                    out.append(ev)
                    if done(out):
                        break
        finally:
            await client.aclose()
        return out


@pytest.fixture(params=["fake", pytest.param("real", marks=pytest.mark.integration)])
async def leg(request: pytest.FixtureRequest) -> AsyncIterator[Leg]:
    fake: FakeWrapper | None = None
    if request.param == "fake":
        fake = FakeWrapper(token=TOKEN)
        await fake.start()
        base_url = fake.base_url
    else:
        base_url = request.getfixturevalue("real_wrapper").base_url
    http = httpx.AsyncClient(
        base_url=base_url, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=30
    )
    state = Leg(request.param, base_url, fake, http, [])
    try:
        yield state
    finally:
        for sid in state.sessions:
            await http.delete(f"/v1/sessions/{sid}", params={"purge": "true"})
        await http.aclose()
        if fake is not None:
            await fake.stop()


async def test_turn_lifecycle_events(leg: Leg) -> None:
    sid = await leg.create()
    await leg.turn(sid)
    events = await leg.collect(sid, lambda evs: "turn_completed" in {e.type for e in evs}, after=0)
    seqs = [e.seq for e in events]
    assert seqs == sorted(set(seqs))
    marks = [
        e.type if e.type != "session_state" else f"state:{e.data['state']}"
        for e in events
        if e.type in ("user_message", "turn_completed", "session_state")
    ]
    tail = marks[marks.index("state:busy") :]
    # the wrapper logs busy, then the prompt, and the idle (ready) transition around the end
    assert tail[:2] == ["state:busy", "user_message"]
    assert set(tail[2:]) == {"turn_completed", "state:ready"}
    ready = [e for e in events if e.type == "session_state" and e.data["state"] == "ready"][-1]
    assert ready.data["since"]
    done = next(e for e in events if e.type == "turn_completed")
    assert done.data["record"]["state"] == "completed"
    assert done.data["record"]["turn_id"] in {
        e.data.get("turn_id") for e in events if e.type == "user_message"
    }


async def test_busy_session_returns_409(leg: Leg) -> None:
    sid = await leg.create()
    first = await leg.submit(sid, SLOW)
    assert first.status_code == 202
    turn_id = first.json()["turn_id"]
    assert first.json()["state"] == "running"
    second = await leg.submit(sid, "again")
    assert second.status_code == 409
    assert second.json()["error"]["code"] == "busy"
    assert isinstance(await leg.client().submit_turn(sid, "again"), Busy)
    await leg.finish(sid, turn_id)


async def test_last_event_id_replay(leg: Leg) -> None:
    sid = await leg.create()
    await leg.turn(sid)
    all_events = (await leg.history(sid, limit=1000)).json()["events"]
    last = all_events[-1]["seq"]
    cut = all_events[len(all_events) // 2]["seq"]
    replayed = await leg.collect(sid, lambda evs: evs[-1].seq >= last, after=cut)
    assert [e.seq for e in replayed] == [e["seq"] for e in all_events if e["seq"] > cut]
    assert all(e.seq > cut for e in replayed)
    bad = await leg.http.get(f"/v1/sessions/{sid}/events", headers={"Last-Event-ID": "nope"})
    assert bad.status_code == 400
    assert bad.json()["error"]["code"] == "invalid_last_event_id"


async def test_snapshot_fields(leg: Leg) -> None:
    sid = await leg.create()
    snap = await leg.wait_ready(sid)
    assert "context" in snap  # null until the first assistant message
    assert snap["idle_since"]
    assert snap["current_turn_id"] is None
    resp = await leg.submit(sid, SLOW)
    busy = (await leg.http.get(f"/v1/sessions/{sid}")).json()
    assert busy["state"] == "busy"
    assert busy["idle_since"] is None
    assert busy["current_turn_id"] == resp.json()["turn_id"]
    await leg.finish(sid, resp.json()["turn_id"])
    after = await leg.wait_ready(sid)
    assert after["idle_since"] and after["idle_since"] >= snap["idle_since"]
    typed = await leg.client().get_session(sid)
    assert typed.idle_since is not None
    listed = (await leg.http.get("/v1/sessions")).json()["sessions"]
    assert sid in [s["session_id"] for s in listed]


async def test_peers_view_and_auth(leg: Leg) -> None:
    peers = await leg.http.get("/v1/peers")
    assert peers.status_code == 200
    assert isinstance(peers.json()["enabled"], bool)
    typed = await leg.client().peers()
    assert typed.enabled == peers.json()["enabled"]
    for headers in ({"Authorization": "Bearer wrong"}, {}):
        async with httpx.AsyncClient(base_url=leg.base_url, headers=headers) as anon:
            resp = await anon.get("/v1/sessions")
        assert resp.status_code == 401
        assert resp.json()["error"]["code"] == "unauthorized"
        assert resp.json()["error"]["message"]


async def test_history_backward(leg: Leg) -> None:
    sid = await leg.create()
    await leg.turn(sid)
    types = "user_message,turn_completed"
    big = 10**9
    full = (await leg.history(sid, before_seq=big, types=types, limit=100)).json()
    assert [e["type"] for e in full["events"]] == ["user_message", "turn_completed"]
    seqs = [e["seq"] for e in full["events"]]
    assert seqs == sorted(seqs)
    assert full["has_more_before"] is False
    assert full["prev_seq"] is None
    page = (await leg.history(sid, before_seq=big, types=types, limit=1)).json()
    assert [e["type"] for e in page["events"]] == ["turn_completed"]
    assert page["has_more_before"] is True
    older = (await leg.history(sid, before_seq=page["prev_seq"], types=types, limit=1)).json()
    assert [e["type"] for e in older["events"]] == ["user_message"]
    assert older["has_more_before"] is False
    assert older["prev_seq"] is None
    forward = (await leg.history(sid, after_seq=0, limit=1000)).json()
    assert "prev_seq" not in forward
    bad = await leg.history(sid, before_seq=big, types="user_message,nonsense")
    assert bad.status_code == 400
    assert bad.json()["error"]["code"] == "invalid_request"
    both = await leg.history(sid, before_seq=big, after_seq=0)
    assert both.status_code == 400


async def test_resume_stopped_session(leg: Leg) -> None:
    with_transcript = await leg.create("with-transcript")
    await leg.turn(with_transcript)
    bare = await leg.create("no-transcript")
    for sid in (with_transcript, bare):
        stopped = await leg.http.delete(f"/v1/sessions/{sid}")
        assert stopped.status_code == 200, stopped.text
        assert (await leg.http.get(f"/v1/sessions/{sid}")).json()["state"] == "stopped"
    refused = await leg.http.post(f"/v1/sessions/{bare}/resume")
    assert refused.status_code == 409
    assert refused.json()["error"]["code"] == "not_resumable"
    resumed = await leg.http.post(f"/v1/sessions/{with_transcript}/resume", timeout=120)
    assert resumed.status_code == 200, resumed.text
    assert resumed.json()["state"] == "ready"
    typed = await leg.client().get_session(with_transcript)
    assert typed.state == "ready"
    missing = await leg.http.post("/v1/sessions/ses_nope/resume")
    assert missing.status_code == 404


async def test_turn_metadata_on_record(leg: Leg) -> None:
    sid = await leg.create()
    plain = await leg.turn(sid, "plain")
    tagged = await leg.turn(sid, "tagged", metadata={"orch_message_id": "msg-7"})
    rec = (await leg.http.get(f"/v1/sessions/{sid}/turns/{tagged}")).json()
    assert rec["metadata"] == {"orch_message_id": "msg-7"}
    assert rec["state"] == "completed"
    assert rec["text"] == "tagged"
    assert (await leg.http.get(f"/v1/sessions/{sid}/turns/{plain}")).json()["metadata"] == {}
    listed = (await leg.http.get(f"/v1/sessions/{sid}/turns")).json()["turns"]
    assert [t["turn_id"] for t in listed] == [plain, tagged]
    typed = await leg.client().get_turn(sid, tagged)
    assert typed.metadata == {"orch_message_id": "msg-7"}
    assert [t.turn_id for t in await leg.client().list_turns(sid)] == [plain, tagged]
    missing = await leg.http.get(f"/v1/sessions/{sid}/turns/turn-nope")
    assert missing.status_code == 404


# ---------------------------------------------------------------- fake-only controls


@pytest.fixture
async def fake() -> AsyncIterator[FakeWrapper]:
    wrapper = FakeWrapper(token=TOKEN)
    await wrapper.start()
    try:
        yield wrapper
    finally:
        await wrapper.stop()


async def _session(fake: FakeWrapper) -> tuple[WrapperClient, str]:
    client = WrapperClient(fake.base_url, TOKEN, send_timeout=0.5)
    snap = await client.create_session("c", "c")
    return client, snap.wrapper_session_id


async def test_fake_status_delay_and_reset_controls(fake: FakeWrapper) -> None:
    client, sid = await _session(fake)
    try:
        expected = {
            404: Gone,
            410: Gone,
            401: ConfigError,
            403: ConfigError,
            413: Rejected,
            422: Rejected,
            500: Ambiguous,
            502: Ambiguous,
            503: Unavailable,
            504: Unavailable,
        }
        for code, kind in expected.items():
            fake.set_turn_status(code)
            assert isinstance(await client.submit_turn(sid, "x"), kind), code
        assert fake.received_turns == 0
        fake.set_turn_status(None)

        fake.delay_turn_response(2.0)
        out = await client.submit_turn(sid, "slow", metadata={"orch_message_id": "m1"})
        assert isinstance(out, Ambiguous)
        assert fake.received_turns == 1  # recorded although the client gave up
        assert fake.turn_texts == ["slow"]
        fake.delay_turn_response(0)
        await fake.complete_turn(fake.last_turn_id, "ok")

        fake.reset_connection_after_receive()
        out = await client.submit_turn(sid, "reset")
        assert isinstance(out, Ambiguous)
        assert fake.received_turns == 2
        await fake.complete_turn(fake.last_turn_id, "ok")

        fake.reset_connection_after_receive(False)
        out = await client.submit_turn(sid, "fine")
        assert isinstance(out, TurnAccepted)
        assert fake.received_turns == 3
        assert fake.turn_metadata[0] == {"orch_message_id": "m1"}
    finally:
        await client.aclose()


async def test_fake_ambiguity_controls(fake: FakeWrapper) -> None:
    client, sid = await _session(fake)
    try:
        fake.reset_connection_before_receive()
        out = await client.submit_turn(sid, "lost")
        assert isinstance(out, Ambiguous)
        assert fake.received_turns == 0  # the wrapper never saw the turn
        assert (await client.get_session(sid)).state == "ready"
        fake.reset_connection_before_receive(False)

        await fake.set_unreachable(True)
        out = await client.submit_turn(sid, "nobody home")
        assert isinstance(out, Unavailable)
        assert fake.received_turns == 0
        await fake.set_unreachable(False)

        out = await client.submit_turn(sid, "back")
        assert isinstance(out, TurnAccepted)
        assert fake.received_turns == 1
    finally:
        await client.aclose()


async def test_fake_states_and_controls(fake: FakeWrapper) -> None:
    client, sid = await _session(fake)
    try:
        fake.set_booting(True)
        out = await client.submit_turn(sid, "x")
        assert isinstance(out, Unavailable)
        fake.set_booting(False)

        fake.set_session_state("failed", has_transcript=True)
        assert isinstance(await client.submit_turn(sid, "x"), Unavailable)
        assert (await client.get_session(sid)).state == "failed"
        assert (await client.resume_session(sid)).state == "ready"
        fake.set_session_state("stopped", has_transcript=False)
        with pytest.raises(Exception, match="not_resumable"):
            await client.resume_session(sid)

        fake.set_session_state("ready", has_transcript=True)
        fake.set_context(used_tokens=100, window_tokens=1000)
        snap = await client.get_session(sid)
        assert snap.context is not None
        assert snap.context.used_percentage == 10.0

        seen: list[Any] = []

        async def follow() -> None:
            async for ev in client.events(sid):
                seen.append(ev)

        task = asyncio.create_task(follow())
        await asyncio.sleep(0.2)
        fake.emit("assistant_text", {"text": "hi"}, session_id=sid)
        await asyncio.sleep(0.2)
        assert seen[-1].type == "assistant_text"
        fake.drop_streams()
        with pytest.raises(Exception):  # noqa: B017 - the dropped stream ends or errors
            await asyncio.wait_for(task, 5)
            raise RuntimeError("stream ended")
        before = seen[-1].seq
        fake.skip_seq(10)
        fake.emit("assistant_text", {"text": "after rotation"}, session_id=sid)
        replay = [ev async for ev in _take(client, sid, before, 1)]
        assert replay[0].seq == before + 11
    finally:
        await client.aclose()


async def _take(client: WrapperClient, sid: str, after: int, n: int) -> AsyncIterator[Any]:
    count = 0
    async for ev in client.events(sid, last_event_id=after):
        yield ev
        count += 1
        if count >= n:
            return


async def _tree(leg: Leg) -> str:
    """A session whose workdir holds a small tree, written through the turn `files` field."""
    sid = await leg.create("filetree")
    files = {
        "src/main.py": "print('hi')\n",
        "src/pkg/mod.py": "x = 1\n",
        "README.md": "# demo\n",
        ".hidden": "dot\n",
        "blob.bin": "\x00\x01\x02 binary",
        "notes.txt": "café\n",
    }
    await leg.turn(sid, "write files", files=files)
    return sid


async def test_files_list(leg: Leg) -> None:
    sid = await _tree(leg)
    resp = await leg.http.get(f"/v1/sessions/{sid}/files")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body) == {"session_id", "root", "path", "entries", "next_offset"}
    assert body["session_id"] == sid and body["path"] == "" and body["next_offset"] is None
    assert body["root"].startswith("/")
    names = [(e["name"], e["type"]) for e in body["entries"]]
    mine = [".hidden", "README.md", "blob.bin", "notes.txt", "src"]
    assert [n for n, _ in names if n in mine] == [
        "src",
        ".hidden",
        "README.md",
        "blob.bin",
        "notes.txt",
    ]
    assert dict(names)["src"] == "dir" and dict(names)["README.md"] == "file"
    by_name = {e["name"]: e for e in body["entries"]}
    assert set(by_name["README.md"]) == {"name", "type", "size", "mtime", "target_type"}
    assert by_name["README.md"]["target_type"] is None and by_name["src"]["target_type"] is None
    assert by_name["README.md"]["size"] == len("# demo\n")
    assert by_name["src"]["size"] is None
    assert by_name["README.md"]["mtime"].endswith("Z")

    sub = (await leg.http.get(f"/v1/sessions/{sid}/files", params={"path": "src"})).json()
    assert sub["path"] == "src"
    assert [e["name"] for e in sub["entries"]] == ["pkg", "main.py"]
    # a path is normalised
    again = (await leg.http.get(f"/v1/sessions/{sid}/files", params={"path": "src/./pkg/"})).json()
    assert again["path"] == "src/pkg"


async def test_files_content(leg: Leg) -> None:
    sid = await _tree(leg)
    url = f"/v1/sessions/{sid}/files/content"
    text = await leg.http.get(url, params={"path": "src/main.py"})
    assert text.status_code == 200, text.text
    body = text.json()
    assert set(body) == {"session_id", "path", "size", "mtime", "mime", "kind", "content"}
    assert body["kind"] == "text" and body["content"] == "print('hi')\n"
    assert body["path"] == "src/main.py" and body["size"] == len("print('hi')\n")
    assert body["mime"] == "text/x-python"
    unicode_body = (await leg.http.get(url, params={"path": "notes.txt"})).json()
    assert unicode_body["kind"] == "text" and unicode_body["content"] == "café\n"
    binary = (await leg.http.get(url, params={"path": "blob.bin"})).json()
    assert binary["kind"] == "binary" and binary["content"] is None
    assert binary["size"] == len("\x00\x01\x02 binary")


async def test_files_errors(leg: Leg) -> None:
    sid = await _tree(leg)
    listing, content = f"/v1/sessions/{sid}/files", f"/v1/sessions/{sid}/files/content"

    async def expect(url: str, params: dict[str, str], status: int, code: str) -> None:
        resp = await leg.http.get(url, params=params)
        assert resp.status_code == status, (url, params, resp.text)
        assert resp.json()["error"]["code"] == code, (url, params, resp.text)

    for url in (listing, content):
        await expect(url, {"path": "../outside"}, 400, "invalid_path")
        await expect(url, {"path": "/etc/passwd"}, 400, "invalid_path")
        await expect(url, {"path": "a\x00b"}, 400, "invalid_path")
        await expect(url, {"path": "src/../../x"}, 400, "invalid_path")
        await expect(url, {"path": "missing"}, 404, "not_found")
    await expect(listing, {"path": "README.md"}, 400, "not_a_directory")
    await expect(content, {"path": "src"}, 400, "not_a_file")
    await expect(content, {}, 400, "not_a_file")  # the workdir itself is a directory
    bad = await leg.http.get(listing, params={"offset": "-1"})
    assert bad.status_code == 422 and bad.json()["error"]["code"] == "invalid_request"
    for url in ("/v1/sessions/ses_unknown/files", "/v1/sessions/ses_unknown/files/content"):
        resp = await leg.http.get(url)
        assert resp.status_code == 404 and resp.json()["error"]["code"] == "not_found"
        async with httpx.AsyncClient(base_url=leg.base_url) as anon:
            assert (await anon.get(url)).status_code == 401


async def test_files_download(leg: Leg) -> None:
    sid = await _tree(leg)
    url = f"/v1/sessions/{sid}/files/download"
    resp = await leg.http.get(url, params={"path": "src/main.py"})
    assert resp.status_code == 200, resp.text
    assert resp.content == b"print('hi')\n"
    assert resp.headers["content-length"] == str(len(b"print('hi')\n"))
    assert resp.headers["content-type"] == "text/x-python"
    assert resp.headers["content-disposition"] == (
        "attachment; filename=\"main.py\"; filename*=UTF-8''main.py"
    )
    assert resp.headers["x-content-type-options"] == "nosniff"
    assert resp.headers["cache-control"] == "no-store"
    blob = await leg.http.get(url, params={"path": "blob.bin"})
    assert blob.status_code == 200, blob.text
    assert blob.content == b"\x00\x01\x02 binary"
    assert blob.headers["content-length"] == str(len(b"\x00\x01\x02 binary"))
    assert blob.headers["content-type"] == "application/octet-stream"
    assert blob.headers["content-disposition"] == (
        "attachment; filename=\"blob.bin\"; filename*=UTF-8''blob.bin"
    )
    # the name is the last segment of the normalised requested path
    nested = await leg.http.get(url, params={"path": "src/./pkg/../pkg/mod.py"})
    assert nested.status_code == 200 and nested.content == b"x = 1\n"
    assert 'filename="mod.py"' in nested.headers["content-disposition"]
    # a non-ASCII name: ASCII fallback plus the percent-encoded UTF-8 form
    await leg.turn(sid, "more", files={'caf\u00e9 "q".txt': "x"})
    odd = await leg.http.get(url, params={"path": 'caf\u00e9 "q".txt'})
    assert odd.status_code == 200, odd.text
    assert odd.headers["content-disposition"] == (
        "attachment; filename=\"caf_ _q_.txt\"; filename*=UTF-8''caf%C3%A9%20%22q%22.txt"
    )


async def test_files_download_errors(leg: Leg) -> None:
    sid = await _tree(leg)
    url = f"/v1/sessions/{sid}/files/download"

    async def expect(params: dict[str, str], status: int, code: str) -> None:
        resp = await leg.http.get(url, params=params)
        assert resp.status_code == status, (params, resp.text)
        assert resp.json()["error"]["code"] == code, (params, resp.text)

    await expect({"path": "../outside"}, 400, "invalid_path")
    await expect({"path": "/etc/passwd"}, 400, "invalid_path")
    await expect({"path": "a\x00b"}, 400, "invalid_path")
    await expect({"path": "src/../../x"}, 400, "invalid_path")
    await expect({"path": "src"}, 400, "not_a_file")
    await expect({}, 400, "not_a_file")  # the workdir itself is a directory
    await expect({"path": ""}, 400, "not_a_file")
    await expect({"path": "missing"}, 404, "not_found")
    unknown = "/v1/sessions/ses_unknown/files/download"
    resp = await leg.http.get(unknown)
    assert resp.status_code == 404 and resp.json()["error"]["code"] == "not_found"
    async with httpx.AsyncClient(base_url=leg.base_url) as anon:
        assert (await anon.get(url, params={"path": "README.md"})).status_code == 401
        assert (await anon.get(unknown)).status_code == 401


async def _put(
    leg: Leg, sid: str, path: str, content: Any, expected: Any, **body: Any
) -> httpx.Response:
    payload = {"content": content, "expected": expected, **body}
    return await leg.http.put(
        f"/v1/sessions/{sid}/files/content", params={"path": path}, json=payload
    )


async def _stamp(leg: Leg, sid: str, path: str) -> dict[str, Any]:
    """The `expected` an editor sends: the mtime and size of the files/content answer."""
    resp = await leg.http.get(f"/v1/sessions/{sid}/files/content", params={"path": path})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    return {"mtime": body["mtime"], "size": body["size"]}


async def _read(leg: Leg, sid: str, path: str) -> dict[str, Any]:
    resp = await leg.http.get(f"/v1/sessions/{sid}/files/content", params={"path": path})
    assert resp.status_code == 200, resp.text
    return resp.json()


async def test_files_write(leg: Leg) -> None:
    sid = await _tree(leg)
    text = "caf\u00e9 \u0141\u00f3d\u017a\r\nsecond line\r\n"
    stamp = await _stamp(leg, sid, "src/main.py")
    resp = await _put(leg, sid, "src/main.py", text, stamp)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body) == {"session_id", "path", "size", "mtime"}
    assert body["session_id"] == sid and body["path"] == "src/main.py"
    assert body["size"] == len(text.encode("utf-8"))
    after = await _read(leg, sid, "src/main.py")
    assert after["kind"] == "text" and after["content"] == text  # CRLF kept byte for byte
    assert (after["size"], after["mtime"]) == (body["size"], body["mtime"])


async def test_files_write_conflict(leg: Leg) -> None:
    sid = await _tree(leg)
    path = "src/main.py"
    loaded = await _stamp(leg, sid, path)
    first = await _put(leg, sid, path, "one\n", loaded)
    assert first.status_code == 200, first.text
    saved = first.json()
    # the answer of a save is the stamp for the next one
    second = await _put(leg, sid, path, "two\n", {"mtime": saved["mtime"], "size": saved["size"]})
    assert second.status_code == 200, second.text
    current = await _stamp(leg, sid, path)
    for stale in (
        {"mtime": "1970-01-01T00:00:00.000Z", "size": current["size"]},
        {"mtime": current["mtime"], "size": current["size"] + 1},
        loaded,
    ):
        resp = await _put(leg, sid, path, "stale\n", stale)
        assert resp.status_code == 409, (stale, resp.text)
        assert resp.json()["error"]["code"] == "file_changed"
        assert (await _read(leg, sid, path))["content"] == "two\n"
    forced = await _put(leg, sid, path, "forced\n", None)  # an explicit unconditional overwrite
    assert forced.status_code == 200, forced.text
    assert (await _read(leg, sid, path))["content"] == "forced\n"


async def test_files_write_errors(leg: Leg) -> None:
    sid = await _tree(leg)
    url = f"/v1/sessions/{sid}/files/content"

    async def expect(path: str, status: int, code: str) -> None:
        resp = await _put(leg, sid, path, "x", None)
        assert resp.status_code == status, (path, resp.text)
        assert resp.json()["error"]["code"] == code, (path, resp.text)

    await expect("../outside", 400, "invalid_path")
    await expect("/etc/passwd", 400, "invalid_path")
    await expect("a\x00b", 400, "invalid_path")
    await expect("src/../../x", 400, "invalid_path")
    await expect("src", 400, "not_a_file")
    await expect("", 400, "not_a_file")  # the workdir itself is a directory
    await expect("missing", 404, "not_found")
    names = [e["name"] for e in (await leg.http.get(f"/v1/sessions/{sid}/files")).json()["entries"]]
    assert "missing" not in names  # a write never creates a file
    unknown = await leg.http.put(
        "/v1/sessions/ses_unknown/files/content",
        params={"path": "a"},
        json={"content": "x", "expected": None},
    )
    assert unknown.status_code == 404 and unknown.json()["error"]["code"] == "not_found"
    async with httpx.AsyncClient(base_url=leg.base_url) as anon:
        resp = await anon.put(
            url, params={"path": "src/main.py"}, json={"content": "x", "expected": None}
        )
        assert resp.status_code == 401
    stamp = {"mtime": "2020-01-01T00:00:00.000Z", "size": 0}
    bad_bodies: list[dict[str, Any]] = [
        {"content": "x"},  # expected is required
        {"expected": None},  # content is required
        {"content": "x", "expected": None, "force": True},  # no other key
        {"content": 5, "expected": None},
        {"content": None, "expected": None},
        {"content": "x", "expected": {"mtime": stamp["mtime"]}},  # a stamp has both keys
        {"content": "x", "expected": {**stamp, "extra": 1}},
        {"content": "x", "expected": {"mtime": stamp["mtime"], "size": -1}},
        {"content": "x", "expected": {"mtime": 5, "size": 0}},
    ]
    for payload in bad_bodies:
        resp = await leg.http.put(url, params={"path": "src/main.py"}, json=payload)
        assert resp.status_code == 422, (payload, resp.text)
        assert resp.json()["error"]["code"] == "invalid_request", (payload, resp.text)
    lone = await leg.http.put(  # a lone surrogate is not text a file can hold
        url,
        params={"path": "src/main.py"},
        content=b'{"content": "\\ud800", "expected": null}',
        headers={"Content-Type": "application/json"},
    )
    assert lone.status_code == 422 and lone.json()["error"]["code"] == "invalid_request"
    assert (await _read(leg, sid, "src/main.py"))["content"] == "print('hi')\n"


@pytest.fixture
async def fake_leg(fake: FakeWrapper) -> AsyncIterator[Leg]:
    """A Leg over the fake alone, for controls the real image does not expose."""
    http = httpx.AsyncClient(
        base_url=fake.base_url, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=30
    )
    try:
        yield Leg("fake", fake.base_url, fake, http, [])
    finally:
        await http.aclose()


async def test_fake_files_paging_and_too_large(fake_leg: Leg, fake: FakeWrapper) -> None:
    """Caps are config on the wrapper (2000 entries, 1 MiB); only the fake can lower them."""
    leg = fake_leg
    sid = await _tree(leg)
    fake.files_max_entries = 2
    fake.file_preview_max_bytes = 8
    first = (await leg.http.get(f"/v1/sessions/{sid}/files")).json()
    assert [e["name"] for e in first["entries"]] == ["src", ".hidden"]
    assert first["next_offset"] == 2
    rest = (await leg.http.get(f"/v1/sessions/{sid}/files", params={"offset": 2})).json()
    assert [e["name"] for e in rest["entries"]] == ["README.md", "blob.bin"]
    assert rest["next_offset"] == 4
    last = (await leg.http.get(f"/v1/sessions/{sid}/files", params={"offset": 4})).json()
    assert [e["name"] for e in last["entries"]] == ["notes.txt"] and last["next_offset"] is None

    url = f"/v1/sessions/{sid}/files/content"
    small = (await leg.http.get(url, params={"path": ".hidden"})).json()  # 4 bytes
    assert small["kind"] == "text" and small["content"] == "dot\n"
    long = (await leg.http.get(url, params={"path": "src/main.py"})).json()  # 12 bytes
    assert long["kind"] == "too_large" and long["content"] is None and long["size"] == 12


async def test_fake_files_download_ignores_preview_cap(fake_leg: Leg, fake: FakeWrapper) -> None:
    """Download is not capped by the preview limit: a file over it arrives whole."""
    leg = fake_leg
    sid = await _tree(leg)
    fake.file_preview_max_bytes = 8
    data = bytes(range(256)) * 4096  # 1 MiB, every byte value, spans several chunks
    (fake.workdir(sid) / "big.dat").write_bytes(data)
    preview = (
        await leg.http.get(f"/v1/sessions/{sid}/files/content", params={"path": "big.dat"})
    ).json()
    assert preview["kind"] == "too_large"
    resp = await leg.http.get(f"/v1/sessions/{sid}/files/download", params={"path": "big.dat"})
    assert resp.status_code == 200, resp.text[:200]
    assert resp.headers["content-length"] == str(len(data))
    assert resp.content == data


async def test_fake_files_symlink_target_type(fake_leg: Leg, fake: FakeWrapper) -> None:
    """The real image cannot be given links or a FIFO; the fake must classify them as it does."""
    leg = fake_leg
    sid = await _tree(leg)
    workdir = fake.workdir(sid)
    outside = workdir.parent / f"{workdir.name}-outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("s")
    os.mkfifo(workdir / "pipe")
    (workdir / "link_file").symlink_to("README.md")
    (workdir / "link_dir").symlink_to("src")
    (workdir / "link_fifo").symlink_to("pipe")
    (workdir / "link_dangling").symlink_to("nowhere")
    (workdir / "link_escape_dir").symlink_to(outside)
    (workdir / "link_escape_file").symlink_to(outside / "secret.txt")
    resp = await leg.http.get(f"/v1/sessions/{sid}/files")
    assert resp.status_code == 200, resp.text
    by_name = {e["name"]: e for e in resp.json()["entries"]}
    for name in (
        "link_file",
        "link_dir",
        "link_fifo",
        "link_dangling",
        "link_escape_dir",
        "link_escape_file",
    ):
        assert by_name[name]["type"] == "symlink", name
    assert by_name["link_file"]["target_type"] == "file"
    assert by_name["link_dir"]["target_type"] == "dir"
    assert by_name["link_fifo"]["target_type"] == "other"
    assert by_name["link_dangling"]["target_type"] is None
    assert by_name["link_escape_dir"]["target_type"] is None
    assert by_name["link_escape_file"]["target_type"] is None
    for name in ("pipe", "README.md", "src"):
        assert by_name[name]["target_type"] is None, name


async def test_fake_files_write_cap(fake_leg: Leg, fake: FakeWrapper) -> None:
    """The cap is config on the wrapper (file_preview_max_bytes, counted in UTF-8 bytes)."""
    leg = fake_leg
    sid = await _tree(leg)
    fake.file_preview_max_bytes = 8
    path = "README.md"
    for over in ("123456789", "\u00e9\u00e9\u00e9\u00e9\u00e9"):  # 9 bytes, 5 chars / 10 bytes
        resp = await _put(leg, sid, path, over, None)
        assert resp.status_code == 413, resp.text
        assert resp.json()["error"]["code"] == "too_large"
    assert (fake.workdir(sid) / path).read_text() == "# demo\n"
    assert sorted(p.name for p in fake.workdir(sid).iterdir() if p.name.endswith(".tmp")) == []
    resp = await _put(leg, sid, path, "12345678", None)  # exactly at the cap
    assert resp.status_code == 200, resp.text
    assert resp.json()["size"] == 8
    assert (fake.workdir(sid) / path).read_text() == "12345678"
    resp = await _put(leg, sid, path, "\u00e9\u00e9\u00e9\u00e9", None)  # 4 chars, 8 bytes
    assert resp.status_code == 200, resp.text
    assert (fake.workdir(sid) / path).read_text(encoding="utf-8") == "\u00e9\u00e9\u00e9\u00e9"


async def test_fake_files_write_through_symlink(fake_leg: Leg, fake: FakeWrapper) -> None:
    """The real image cannot be given links; a write follows one inside the workdir to its
    target and keeps the link, and never reaches a target outside it. Permission bits survive."""
    leg = fake_leg
    sid = await _tree(leg)
    workdir = fake.workdir(sid)
    outside = workdir.parent / f"{workdir.name}-outside"
    outside.mkdir()
    secret = outside / "secret.txt"
    secret.write_text("s")
    (workdir / "link_file").symlink_to("README.md")
    (workdir / "link_escape").symlink_to(secret)
    (workdir / "README.md").chmod(0o640)

    resp = await _put(leg, sid, "link_file", "via link\n", None)
    assert resp.status_code == 200, resp.text
    assert resp.json()["path"] == "README.md"  # the target, as the wrapper reports it
    assert (workdir / "link_file").is_symlink()
    assert (workdir / "link_file").readlink().as_posix() == "README.md"
    assert (workdir / "README.md").read_text() == "via link\n"
    assert (workdir / "README.md").stat().st_mode & 0o777 == 0o640

    resp = await _put(leg, sid, "link_escape", "pwned", None)
    assert resp.status_code == 400, resp.text
    assert resp.json()["error"]["code"] == "invalid_path"
    assert secret.read_text() == "s"
    assert (workdir / "link_escape").is_symlink()


# ------------------------------------------------------------------------------ terminals

TERM_FIELDS = {
    "terminal_id",
    "session_id",
    "created_at",
    "alive",
    "exit_code",
    "attached",
    "rows",
    "cols",
}
MARK = b"contract-marker"


async def _term(leg: Leg, **body: Any) -> tuple[str, dict[str, Any]]:
    sid = await leg.create("term")
    resp = await leg.http.post(f"/v1/sessions/{sid}/terminals", json=body)
    assert resp.status_code == 201, resp.text
    return sid, resp.json()


def _attach_url(leg: Leg, sid: str, terminal_id: str) -> str:
    return (
        f"{leg.base_url.replace('http', 'ws', 1)}/v1/sessions/{sid}/terminals/{terminal_id}/attach"
    )


def _bearer(token: str = TOKEN) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def _read_until(ws: Any, needle: bytes, seen: bytes = b"") -> bytes:
    """Binary output accumulated until `needle` shows up; control frames are skipped."""
    async with asyncio.timeout(TIMEOUT):
        while needle not in seen:
            frame = await ws.recv()
            if isinstance(frame, bytes):
                seen += frame
    return seen


async def _control(ws: Any, frame: dict[str, Any]) -> dict[str, Any]:
    """Send a control frame and read until the next JSON control frame."""
    await ws.send(json.dumps(frame))
    async with asyncio.timeout(TIMEOUT):
        while True:
            got = await ws.recv()
            if isinstance(got, str):
                return json.loads(got)


async def test_terminals_http_lifecycle(leg: Leg) -> None:
    sid, made = await _term(leg, rows=30, cols=100)
    assert set(made) == TERM_FIELDS
    assert made["session_id"] == sid and made["terminal_id"].startswith("term_")
    assert made["alive"] is True and made["exit_code"] is None
    assert type(made["attached"]) is int and made["attached"] == 0  # a subscriber count
    assert (made["rows"], made["cols"]) == (30, 100)
    base = f"/v1/sessions/{sid}/terminals"

    default = (await leg.http.post(base)).json()
    assert (default["rows"], default["cols"]) == (24, 80)

    listed = (await leg.http.get(base)).json()
    assert set(listed) == {"terminals"}
    assert {t["terminal_id"] for t in listed["terminals"]} == {
        made["terminal_id"],
        default["terminal_id"],
    }

    assert (await leg.http.delete(f"{base}/{made['terminal_id']}")).status_code == 204
    gone = await leg.http.delete(f"{base}/{made['terminal_id']}")
    assert gone.status_code == 404 and gone.json()["error"]["code"] == "terminal_not_found"
    left = (await leg.http.get(base)).json()["terminals"]
    assert [t["terminal_id"] for t in left] == [default["terminal_id"]]


async def test_terminals_http_errors(leg: Leg) -> None:
    sid = await leg.create("term-errors")
    base = f"/v1/sessions/{sid}/terminals"
    for body in ({"rows": 0}, {"cols": 1001}, {"rows": 1001, "cols": 1}):
        resp = await leg.http.post(base, json=body)
        assert resp.status_code == 422, (body, resp.text)
        assert resp.json()["error"]["code"] == "invalid_size", (body, resp.text)
    resp = await leg.http.post(base, json={"shell": "/bin/zsh"})
    assert resp.status_code == 422 and resp.json()["error"]["code"] == "invalid_request"
    resp = await leg.http.delete(f"{base}/not-a-terminal")
    assert resp.status_code == 422 and resp.json()["error"]["code"] == "invalid_request"
    resp = await leg.http.post("/v1/sessions/ses_unknown/terminals")
    assert resp.status_code == 404 and resp.json()["error"]["code"] == "not_found"
    async with httpx.AsyncClient(base_url=leg.base_url) as anon:
        assert (await anon.post(base)).status_code == 401
        assert (await anon.get(base)).status_code == 401


async def test_terminal_attach_echo_and_replay(leg: Leg) -> None:
    sid, made = await _term(leg)
    url = _attach_url(leg, sid, made["terminal_id"])
    async with websockets.connect(url, additional_headers=_bearer()) as ws:
        await ws.send(b"echo " + MARK + b"\n")
        seen = await _read_until(ws, MARK)
        assert MARK in seen
        listed = (await leg.http.get(f"/v1/sessions/{sid}/terminals")).json()["terminals"]
        assert type(listed[0]["attached"]) is int and listed[0]["attached"] == 1
    # a second attach replays the scrollback printed meanwhile
    async with websockets.connect(url, additional_headers=_bearer()) as ws:
        assert MARK in await _read_until(ws, MARK)


async def test_terminal_attach_control_frames(leg: Leg) -> None:
    sid, made = await _term(leg)
    url = _attach_url(leg, sid, made["terminal_id"])
    async with websockets.connect(url, additional_headers=_bearer()) as ws:
        assert await _control(ws, {"type": "ping"}) == {"type": "pong"}
        await ws.send(json.dumps({"type": "resize", "cols": 120, "rows": 40}))
        # out of range and malformed resizes are ignored, the socket stays open
        for bad in (
            {"type": "resize", "cols": 0, "rows": 40},
            {"type": "resize", "cols": 120, "rows": 1001},
            {"type": "resize", "cols": "x", "rows": 40},
            {"type": "resize"},
        ):
            await ws.send(json.dumps(bad))
        assert await _control(ws, {"type": "ping"}) == {"type": "pong"}
        listed = (await leg.http.get(f"/v1/sessions/{sid}/terminals")).json()["terminals"]
        assert (listed[0]["rows"], listed[0]["cols"]) == (40, 120)
    async with websockets.connect(url, additional_headers=_bearer()) as ws:
        await ws.send(json.dumps({"type": "nonsense"}))
        with pytest.raises(websockets.ConnectionClosed) as closed:
            async with asyncio.timeout(TIMEOUT):
                while True:
                    await ws.recv()
        assert closed.value.rcvd is not None and closed.value.rcvd.code == 1003


async def test_terminal_attach_denied_before_accept(leg: Leg) -> None:
    sid, made = await _term(leg)
    tid = made["terminal_id"]

    async def denied(url: str, headers: dict[str, str]) -> int:
        with pytest.raises(websockets.InvalidStatus) as err:
            async with websockets.connect(url, additional_headers=headers):
                pass
        return err.value.response.status_code

    assert await denied(_attach_url(leg, sid, tid), {}) == 401
    assert await denied(_attach_url(leg, sid, tid), _bearer("wrong")) == 401
    assert await denied(_attach_url(leg, sid, "term_0123456789ab"), _bearer()) == 404
    assert await denied(_attach_url(leg, "ses_unknown", tid), _bearer()) == 404
    assert await denied(_attach_url(leg, sid, "term_zz"), _bearer()) == 422


async def test_fake_terminal_limit_exit_and_session_state(fake_leg: Leg, fake: FakeWrapper) -> None:
    leg = fake_leg
    sid = await leg.create("term-fake")
    base = f"/v1/sessions/{sid}/terminals"
    fake.terminals_max = 1
    made = (await leg.http.post(base)).json()
    resp = await leg.http.post(base)
    assert resp.status_code == 409 and resp.json()["error"]["code"] == "terminal_limit"

    # shell exit: scrollback, then the exit frame, then close 1000; later attaches replay too
    url = _attach_url(leg, sid, made["terminal_id"])
    async with websockets.connect(url, additional_headers=_bearer()) as ws:
        await ws.send(b"hello\n")
        await _read_until(ws, b"hello")
        fake.exit_terminal(made["terminal_id"], 7)
        assert json.loads(await asyncio.wait_for(ws.recv(), 5)) == {"type": "exit", "code": 7}
        with pytest.raises(websockets.ConnectionClosedOK):
            await asyncio.wait_for(ws.recv(), 5)
    info = (await leg.http.get(base)).json()["terminals"][0]
    assert info["alive"] is False and info["exit_code"] == 7
    async with websockets.connect(url, additional_headers=_bearer()) as ws:
        assert await _read_until(ws, b"hello")
        assert json.loads(await asyncio.wait_for(ws.recv(), 5)) == {"type": "exit", "code": 7}

    # a stopped session is known but not active
    await leg.http.delete(f"{base}/{made['terminal_id']}")
    await leg.http.delete(f"/v1/sessions/{sid}")
    resp = await leg.http.post(base)
    assert resp.status_code == 409 and resp.json()["error"]["code"] == "session_not_active"


async def _note_leg(leg: Leg) -> str:
    """A session on a wrapper with notes enabled; the real image may run with them disabled."""
    sid = await leg.create()
    probe = await leg.client().post_note(sid, "probe")
    if probe == "disabled":
        pytest.skip("the wrapper under test runs with notes.enabled false")
    assert probe == "ok"
    return sid


async def test_notes_route_accepts_keyed_and_appended(leg: Leg) -> None:
    sid = await _note_leg(leg)
    keyed = await leg.http.post(
        f"/v1/sessions/{sid}/notes", json={"text": "a", "key": "hive_state", "source": "orch"}
    )
    assert keyed.status_code == 202
    assert keyed.json()["key"] == "hive_state" and isinstance(keyed.json()["pending"], int)
    plain = await leg.http.post(f"/v1/sessions/{sid}/notes", json={"text": "b"})
    assert plain.status_code == 202 and plain.json()["key"] is None
    client = leg.client()
    assert await client.post_note(sid, "c", key="hive_state") == "ok"
    if leg.fake is not None:
        # the keyed note was replaced, the appended ones stay
        assert [n["text"] for n in leg.fake.held_notes(sid)] == ["c", "probe", "b"]
        assert leg.fake.notes[sid][0] == {"text": "probe", "key": None, "source": "orchestrator"}


async def test_notes_route_errors(leg: Leg) -> None:
    sid = await _note_leg(leg)
    url = f"/v1/sessions/{sid}/notes"
    for body in ({"text": ""}, {}, {"text": "x", "key": "Bad Key"}, {"text": "x", "extra": 1}):
        resp = await leg.http.post(url, json=body)
        assert resp.status_code == 422, body
        assert resp.json()["error"]["code"] == "invalid_request"
    missing = await leg.http.post("/v1/sessions/nope/notes", json={"text": "x"})
    assert missing.status_code == 404
    async with httpx.AsyncClient(base_url=leg.base_url) as anon:
        assert (await anon.post(url, json={"text": "x"})).status_code == 401
    assert await leg.client().post_note("nope", "x") == "failed"


async def test_fake_notes_disabled_and_stopped_session(fake_leg: Leg, fake: FakeWrapper) -> None:
    leg = fake_leg
    sid = await leg.create()
    fake.disable_notes()
    resp = await leg.http.post(f"/v1/sessions/{sid}/notes", json={"text": "x"})
    assert resp.status_code == 409 and resp.json()["error"]["code"] == "notes_disabled"
    assert await leg.client().post_note(sid, "x") == "disabled"
    fake.disable_notes(False)
    await leg.http.delete(f"/v1/sessions/{sid}")
    stopped = await leg.http.post(f"/v1/sessions/{sid}/notes", json={"text": "x"})
    assert stopped.status_code == 409 and stopped.json()["error"]["code"] == "invalid_state"
    assert fake.notes.get(sid) is None


# ---------------------------------------------------------------- slash commands

COMMAND_RECORD_KEYS = {
    "id",
    "name",
    "args",
    "origin",
    "turn_id",
    "state",
    "result",
    "error",
    "output",
    "created_at",
    "started_at",
    "finished_at",
}


def test_fake_deny_mirrors_wrapper_default() -> None:
    """The fake's COMMAND_DENY is the pinned wrapper's DEFAULT_COMMAND_DENY, lower-cased."""
    from claude_code_wrapper.config import DEFAULT_COMMAND_DENY
    from fakes.fake_wrapper import COMMAND_DENY

    assert {name.lower() for name in DEFAULT_COMMAND_DENY} == COMMAND_DENY
    assert len(COMMAND_DENY) == 64


async def test_commands_any_name_and_denied(leg: Leg) -> None:
    sid = await leg.create()
    url = f"/v1/sessions/{sid}/commands"
    for name, args in (("context", ""), ("my-plugin:do_it", "one two\nthree")):
        resp = await leg.http.post(url, json={"name": name, "args": args})
        assert resp.status_code == 202, resp.text
        record = resp.json()
        assert set(record) >= COMMAND_RECORD_KEYS
        assert isinstance(record["id"], str) and record["id"]
        assert (record["name"], record["args"]) == (name, args)
        assert record["state"] == "queued" and record["origin"] == "client"
    denied_names = (
        "exit",
        "Reset",
        "plugin",
        "Marketplace",
        "routines",
        "memory",
        "usage-credits",
        "ultrareview",
        "MCP",
        "allowed-tools",
        "remote",
        "settings",
    )
    for name in denied_names:
        denied = await leg.http.post(url, json={"name": name})
        assert denied.status_code == 403, name
        assert denied.json()["error"]["code"] == "command_denied"
    empty = await leg.http.post(url, json={"name": ""})
    assert empty.status_code == 422
    assert empty.json()["error"]["code"] == "invalid_command"
    client = leg.client()
    try:
        refused = await client.command(sid, "EXIT", "now")
        assert isinstance(refused, Rejected) and refused.code == "command_denied"
        assert isinstance(await client.command(sid, ""), Rejected)
        assert isinstance(await client.command("ses_nope", "context"), Gone)
    finally:
        await client.aclose()


def _command_marks(events: list[Any]) -> list[str]:
    """Command events and state changes, as `type` or `state:<state>`."""
    return [
        f"state:{e.data['state']}" if e.type == "session_state" else e.type
        for e in events
        if e.type.startswith("command_") or e.type in ("session_state", "turn_completed")
    ]


async def test_fake_command_events_and_turn_ordering(fake_leg: Leg, fake: FakeWrapper) -> None:
    leg = fake_leg
    sid = await leg.create()
    start = (await leg.history(sid, limit=1000)).json()["next_seq"]
    client = leg.client()
    try:
        idle = await client.command(sid, "model", "opus")
        assert isinstance(idle, CommandAccepted)
        assert idle.record["state"] == "queued" and idle.record["turn_id"] is None
        events = (await leg.history(sid, after_seq=start, limit=1000)).json()["events"]
        typed = [Envelope.model_validate(e) for e in events]
        assert _command_marks(typed) == [
            "command_queued",
            "state:busy",
            "command_started",
            "command_completed",
            "state:ready",
        ]
        records = [e.data["record"] for e in typed if e.type.startswith("command_")]
        assert {r["id"] for r in records} == {idle.command_id}
        assert [r["state"] for r in records] == ["queued", "running", "completed"]
        done = records[-1]
        assert done["result"] == "ok" and done["started_at"] and done["finished_at"]
        assert (done["name"], done["args"], done["origin"]) == ("model", "opus", "client")
        echo = [e.data for e in typed if e.type == "user_message"]
        assert echo == [{"turn_id": None, "origin": "command", "text": "/model opus", "files": []}]
        assert (await client.get_session(sid)).state == "ready"

        turn = (await leg.submit(sid, "work")).json()["turn_id"]
        mid = (await leg.history(sid, limit=1000)).json()["next_seq"]
        queued = await client.command(sid, "reload-plugins")
        assert isinstance(queued, CommandAccepted) and queued.record["turn_id"] == turn
        events = (await leg.history(sid, after_seq=mid, limit=1000)).json()["events"]
        assert [e["type"] for e in events] == ["command_queued"]
        assert (await client.get_session(sid)).state == "busy"
        await fake.complete_turn(turn)
        events = (await leg.history(sid, after_seq=mid, limit=1000)).json()["events"]
        typed = [Envelope.model_validate(e) for e in events]
        assert _command_marks(typed) == [
            "command_queued",
            "state:ready",
            "turn_completed",
            "state:busy",
            "command_started",
            "command_completed",
            "state:ready",
        ]
        last = typed[-2].data["record"]
        assert last["id"] == queued.command_id and last["state"] == "completed"
        assert (await client.get_session(sid)).state == "ready"
    finally:
        await client.aclose()


async def test_fake_command_refusals(fake_leg: Leg, fake: FakeWrapper) -> None:
    leg = fake_leg
    sid = await leg.create()
    url = f"/v1/sessions/{sid}/commands"

    async def post(body: dict[str, Any]) -> tuple[int, str]:
        resp = await leg.http.post(url, json=body)
        code = resp.json()["error"]["code"] if resp.status_code >= 300 else ""
        return resp.status_code, code

    # the wrapper's DEFAULT_COMMAND_DENY, any letter case, before the allowlist
    for name in ("exit", "QUIT", "Clear", "new", "resume", "cd", "bg", "tp", "rc", "update"):
        assert await post({"name": name}) == (403, "command_denied"), name
    for name in ("logout", "setup-bedrock", "web-setup", "design-login", "remote-control"):
        assert await post({"name": name}) == (403, "command_denied"), name
    for name in ("", "/x", "a.b", "-x", "a b", "notes.txt"):
        assert await post({"name": name}) == (422, "invalid_command"), name
    assert await post({"name": "_x" + "a" * 100}) == (202, "")
    for body in ({}, {"name": 1}, {"name": "x", "args": 1}, {"name": "x", "extra": 1}):
        assert (await post(body))[0] == 422, body
    for args in ("a\x1bb", "a\rb", "a\x00", "a b", "x" * 20001):
        assert await post({"name": "x", "args": args}) == (422, "invalid_command"), args
    ok = await leg.http.post(url, json={"name": "x", "args": "a\r\nb\tc"})
    assert ok.status_code == 202 and ok.json()["args"] == "a\nb\tc"

    fake.commands_client = ["compact"]
    assert await post({"name": "context"}) == (403, "command_not_allowed")
    assert await post({"name": "exit"}) == (403, "command_denied")
    assert await post({"name": "compact"}) == (202, "")
    fake.commands_client = ["*"]

    # queued during a turn: an identical one coalesces, the fifth distinct one is refused
    turn = (await leg.submit(sid, "work")).json()["turn_id"]
    first = (await leg.http.post(url, json={"name": "a", "args": "1"})).json()
    again = (await leg.http.post(url, json={"name": "a", "args": "1"})).json()
    assert again["id"] == first["id"]
    for name in ("b", "c", "d"):
        assert (await post({"name": name}))[0] == 202
    assert await post({"name": "e"}) == (429, "command_queue_full")
    client = leg.client()
    try:
        full = await client.command(sid, "e")
        assert isinstance(full, Busy) and full.code == "command_queue_full"
        # an interrupted turn cancels the commands queued for it
        mid = (await leg.history(sid, limit=1000)).json()["next_seq"]
        await leg.http.post(f"/v1/sessions/{sid}/interrupt")
        events = (await leg.history(sid, after_seq=mid, limit=1000)).json()["events"]
        failed = [e["data"]["record"] for e in events if e["type"] == "command_failed"]
        assert [(r["name"], r["state"]) for r in failed] == [
            ("a", "cancelled"),
            ("b", "cancelled"),
            ("c", "cancelled"),
            ("d", "cancelled"),
        ]
        assert not any(e["type"] == "command_started" for e in events)
        assert {r["turn_id"] for r in failed} == {turn}

        fake.set_booting(True)
        booting = await client.command(sid, "context")
        assert isinstance(booting, Busy) and booting.code == "booting"
        fake.set_booting(False)
        fake.set_session_state("dead")
        dead = await client.command(sid, "context")
        assert isinstance(dead, Busy) and dead.code == "dead"
    finally:
        await client.aclose()

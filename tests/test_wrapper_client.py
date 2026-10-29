"""WrapperClient and SSEParser against httpx.MockTransport."""

from __future__ import annotations

import json

import httpx
import pytest

from agents_orchestrator.wrapper import (
    Ambiguous,
    Busy,
    CommandAccepted,
    ConfigError,
    Gone,
    Rejected,
    SSEParser,
    TurnAccepted,
    Unavailable,
    WrapperClient,
    WrapperError,
    WrapperUnreachable,
)

BASE = "http://w:8080"
SNAP = {
    "session_id": "ses_1",
    "name": "a",
    "state": "ready",
    "workdir": "/workdir/a",
    "model": "sonnet",
    "current_turn_id": None,
    "idle_since": "2026-10-20T09:00:14.800+00:00",
    "turns_completed": 1,
    "something_new": 1,
    "context": {
        "used_tokens": 41200,
        "window_tokens": 200000,
        "used_percentage": 20.6,
        "model": "claude-sonnet-4-5",
        "source": "statusline",
        "updated_at": "2026-10-20T09:00:20.100Z",
    },
}


def client(handler, **kw) -> WrapperClient:
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url=BASE)
    return WrapperClient(BASE, "tok", http=http, **kw)


STREAM = (
    b": ping\r\n\r\n"
    b'id: 1\nevent: turn_started\ndata: {"a":\ndata: 1}\nretry: 500\n\n'
    b"id: 2\r\nevent: usage\r\ndata: x\r\n\r\n"
    b"data: last\n\n"
)


def parse_in_chunks(sizes) -> list:
    p = SSEParser()
    out, i = [], 0
    while i < len(STREAM):
        n = sizes(i)
        out += p.feed(STREAM[i : i + n])
        i += n
    return out


def test_sse_parser_chunk_boundaries():
    whole = parse_in_chunks(lambda i: len(STREAM))
    assert [(e.id, e.event, e.data) for e in whole] == [
        ("1", "turn_started", '{"a":\n1}'),
        ("2", "usage", "x"),
        ("2", "message", "last"),
    ]
    assert whole[0].retry == 500
    assert parse_in_chunks(lambda i: 1) == whole
    for k in range(2, 9):
        assert parse_in_chunks(lambda i, k=k: k) == whole
    # a multibyte character split across chunks
    p = SSEParser()
    raw = "data: é\n\n".encode()
    assert p.feed(raw[:7]) == [] and p.feed(raw[7:])[0].data == "é"


def test_sse_comments_and_multiline():
    p = SSEParser()
    ev = p.feed(": ping\n\n: hi\nid: 7\ndata: a\ndata: b\ndata:\ndata: c\n\n")
    assert len(ev) == 1 and ev[0].data == "a\nb\n\nc" and ev[0].id == "7"
    assert p.feed("data:nospace\n\n")[0].data == "nospace"
    assert p.feed("lone: cr\rdata: z\r\r\n")[0].data == "z"


@pytest.mark.parametrize(
    "resp",
    [
        httpx.Response(202, json={"turn_id": "t1", "state": "running"}),
        httpx.Response(409, json={"error": {"code": "busy", "message": "m"}}),
        httpx.Response(503, json={"error": {"code": "booting", "message": "m"}}),
    ],
)
async def test_submit_turn_maps_202_409_503(resp):
    c = client(lambda r: resp)
    out = await c.submit_turn("ses_1", "hi")
    assert type(out) is {202: TurnAccepted, 409: Busy, 503: Unavailable}[resp.status_code]
    if resp.status_code == 202:
        assert out.turn_id == "t1"
    if resp.status_code == 503:
        assert "booting" in out.reason

    def boom(r):
        raise httpx.ConnectError("refused")

    assert isinstance(await client(boom).submit_turn("ses_1", "hi"), Unavailable)


async def test_submit_5xx_mapping():
    expect = {500: Ambiguous, 502: Ambiguous, 503: Unavailable, 504: Unavailable}
    for status, cls in expect.items():
        c = client(lambda r, s=status: httpx.Response(s, text="x"))
        assert type(await c.submit_turn("s", "x")) is cls, status

    def boom(r):
        raise httpx.ConnectError("refused")

    assert type(await client(boom).submit_turn("s", "x")) is Unavailable


async def test_submit_turn_maps_every_status():
    expect = {
        404: Gone,
        410: Gone,
        401: ConfigError,
        403: ConfigError,
        413: Rejected,
        422: Rejected,
        400: Rejected,
        418: Rejected,
        429: Rejected,
        500: Ambiguous,
        502: Ambiguous,
        504: Unavailable,
        599: Unavailable,
        409: Busy,
        301: Rejected,
    }
    for status, cls in expect.items():
        c = client(lambda r, s=status: httpx.Response(s, text="not json"))
        assert isinstance(await c.submit_turn("s", "x"), cls), status


async def test_connect_error_unavailable_read_timeout_ambiguous():
    cases = [
        (httpx.ConnectError("dns"), Unavailable),
        (httpx.ConnectTimeout("t"), Unavailable),
        (httpx.ReadTimeout("t"), Ambiguous),
        (httpx.ReadError("reset"), Ambiguous),
        (httpx.RemoteProtocolError("closed"), Ambiguous),
        (httpx.WriteError("w"), Ambiguous),
    ]
    for exc, cls in cases:

        def h(r, exc=exc):
            raise exc

        out = await client(h).submit_turn("s", "x")
        assert isinstance(out, cls), exc
        assert out.reason


async def test_send_timeout_applies_to_submit_only():
    seen = {}

    def h(r):
        seen[(r.method, r.url.path)] = r.extensions["timeout"]
        if r.url.path.endswith("/turns"):
            return httpx.Response(202, json={"turn_id": "t"})
        return httpx.Response(200, json=SNAP)

    c = client(h, send_timeout=3.0)
    await c.submit_turn("s", "x")
    await c.get_session("s")
    assert seen[("POST", "/v1/sessions/s/turns")]["read"] == 3.0
    assert seen[("POST", "/v1/sessions/s/turns")]["connect"] == 3.0
    assert seen[("GET", "/v1/sessions/s")]["read"] != 3.0


async def test_snapshot_parses_idle_since_and_context():
    c = client(lambda r: httpx.Response(200, json=SNAP))
    s = await c.get_session("ses_1")
    assert s.state == "ready" and s.idle_since.year == 2026 and s.current_turn_id is None
    assert s.context.used_tokens == 41200 and s.context.used_percentage == 20.6
    assert s.model == "sonnet" and s.workdir == "/workdir/a"
    s2 = await client(
        lambda r: httpx.Response(200, json={**SNAP, "context": None, "idle_since": None})
    ).get_session("ses_1")
    assert s2.context is None and s2.idle_since is None


async def test_peers_view_disabled_and_enabled():
    off = await client(lambda r: httpx.Response(200, json={"enabled": False})).peers()
    assert off.enabled is False and off.self is None and off.members == []
    on = {
        "enabled": True,
        "self": {"instance": "a", "url": "http://a:8080"},
        "members": [
            {
                "instance": "b",
                "url": "http://b:8080",
                "last_seen_at": None,
                "missed": 1,
                "sessions": [{"session_id": "ses_2", "name": "n", "cwd": "/w"}],
            }
        ],
    }
    v = await client(lambda r: httpx.Response(200, json=on)).peers()
    assert v.enabled and v.self.instance == "a"
    assert v.members[0].sessions[0].session_id == "ses_2" and v.members[0].missed == 1


async def test_events_sends_last_event_id_and_bearer():
    seen = {}

    def h(r):
        seen["h"] = dict(r.headers)
        seen["path"] = r.url.path
        body = (
            b": ping\n\nid: 5\nevent: usage\ndata: "
            b'{"seq":5,"ts":"t","session_id":"s","type":"usage","data":{"x":1}}\n\n'
        )
        return httpx.Response(200, content=body, headers={"content-type": "text/event-stream"})

    got = [e async for e in client(h).events("s", last_event_id=4)]
    assert seen["h"]["last-event-id"] == "4"
    assert seen["h"]["authorization"] == "Bearer tok"
    assert seen["path"] == "/v1/sessions/s/events"
    assert got[0].seq == 5 and got[0].type == "usage" and got[0].data == {"x": 1}
    seen.clear()
    [e async for e in client(h).events("s")]
    assert "last-event-id" not in seen["h"]


async def test_error_envelope_raises():
    body = {"error": {"code": "not_found", "message": "no such session"}}
    c = client(lambda r: httpx.Response(404, json=body))
    with pytest.raises(WrapperError) as ei:
        await c.get_session("x")
    assert (ei.value.status, ei.value.code, ei.value.message) == (
        404,
        "not_found",
        "no such session",
    )
    with pytest.raises(WrapperError) as ei:
        await client(lambda r: httpx.Response(502, text="bad gateway")).interrupt("x")
    assert ei.value.status == 502 and ei.value.code == "http_502"


async def test_resume_session_posts_resume():
    seen = {}

    def h(r):
        seen.update(m=r.method, p=r.url.path, a=r.headers["authorization"])
        return httpx.Response(200, json=SNAP)

    s = await client(h).resume_session("ses_1")
    assert seen == {"m": "POST", "p": "/v1/sessions/ses_1/resume", "a": "Bearer tok"}
    assert s.state == "ready"


async def test_turn_metadata_and_turn_records():
    reqs = []
    rec = {
        "turn_id": "t1",
        "session_id": "s",
        "origin": "client",
        "state": "running",
        "text": "hi",
        "started_at": "2026-10-20T09:01:00.010+00:00",
        "completed_at": None,
        "metadata": {"message_id": "m-1"},
        "extra": 1,
    }

    def h(r):
        reqs.append(r)
        p = r.url.path
        if r.method == "POST":
            return httpx.Response(202, json={"turn_id": "t1", "state": "running"})
        if p == "/v1/sessions/s/turns":
            return httpx.Response(
                200,
                json={
                    "turns": [
                        {
                            k: rec[k]
                            for k in ("turn_id", "origin", "state", "started_at", "completed_at")
                        }
                    ]
                },
            )
        if p.endswith("/turns/t1"):
            return httpx.Response(200, json=rec)
        return httpx.Response(404, json={"error": {"code": "not_found", "message": "m"}})

    c = client(h)
    await c.submit_turn("s", "hi", metadata={"message_id": "m-1"})
    assert json.loads(reqs[0].content) == {"text": "hi", "metadata": {"message_id": "m-1"}}
    await c.submit_turn("s", "hi")
    assert json.loads(reqs[1].content) == {"text": "hi"}
    turns = await c.list_turns("s")
    assert turns[0].turn_id == "t1" and turns[0].state == "running"
    t = await c.get_turn("s", "t1")
    assert t.metadata == {"message_id": "m-1"} and t.text == "hi"
    with pytest.raises(WrapperError):
        await c.get_turn("s", "nope")
    with pytest.raises(WrapperError):
        await c.list_turns("nope")

    def down(r):
        raise httpx.ConnectError("x")

    def slow(r):
        raise httpx.ReadTimeout("x")

    for fn in (down, slow):
        with pytest.raises(WrapperUnreachable):
            await client(fn).list_turns("s")
        with pytest.raises(WrapperUnreachable):
            await client(fn).get_turn("s", "t")


async def test_sessions_and_control_methods():
    seen = []

    def h(r):
        seen.append((r.method, r.url.path, r.content))
        if r.url.path == "/v1/sessions" and r.method == "GET":
            return httpx.Response(
                200, json={"sessions": [SNAP, {**SNAP, "state": "stopped", "turns_completed": 0}]}
            )
        if r.url.path == "/v1/sessions":
            return httpx.Response(201, json=SNAP)
        if r.url.path.endswith("/commands"):
            return httpx.Response(202, json={**COMMAND_RECORD, "name": "compact", "args": ""})
        return httpx.Response(202, json={})

    c = client(h)
    ss = await c.list_sessions()
    assert [s.has_transcript for s in ss] == [True, None]
    await c.create_session("a", "/workdir/a", model="sonnet", effort="high")
    assert json.loads(seen[1][2]) == {
        "name": "a",
        "workdir": "/workdir/a",
        "model": "sonnet",
        "effort": "high",
    }
    await c.create_session("b", "/w")
    assert json.loads(seen[2][2]) == {"name": "b", "workdir": "/w"}
    await c.interrupt("s")
    out = await c.command("s", "compact")
    assert seen[3][:2] == ("POST", "/v1/sessions/s/interrupt")
    assert seen[4][:2] == ("POST", "/v1/sessions/s/commands")
    assert json.loads(seen[4][2]) == {"name": "compact", "args": ""}
    assert out == CommandAccepted(
        COMMAND_RECORD["id"], {**COMMAND_RECORD, "name": "compact", "args": ""}
    )


COMMAND_RECORD = {
    "id": "cmd_0123456789ab",
    "name": "model",
    "args": "opus",
    "origin": "client",
    "turn_id": None,
    "state": "queued",
    "result": None,
    "error": None,
    "output": None,
    "created_at": "2026-10-27T09:00:00.000Z",
    "started_at": None,
    "finished_at": None,
}


async def test_command_outcomes():
    seen = []

    def accept(r):
        seen.append((r.method, r.url.path, json.loads(r.content), r.extensions["timeout"]))
        return httpx.Response(202, json=COMMAND_RECORD)

    out = await client(accept, send_timeout=3.0).command("s", "model", "opus")
    assert out == CommandAccepted("cmd_0123456789ab", COMMAND_RECORD)
    method, path, body, timeout = seen[0]
    assert (method, path, body) == (
        "POST",
        "/v1/sessions/s/commands",
        {"name": "model", "args": "opus"},
    )
    assert timeout["read"] == 3.0 and timeout["connect"] == 3.0

    def err(status, code):
        return lambda r: httpx.Response(status, json={"error": {"code": code, "message": "m"}})

    cases = [
        (409, "booting", Busy),
        (409, "dead", Busy),
        (429, "command_queue_full", Busy),
        (401, "unauthorized", ConfigError),
        (403, "command_denied", Rejected),
        (403, "command_not_allowed", Rejected),
        (422, "invalid_command", Rejected),
        (422, "invalid_request", Rejected),
        (404, "not_found", Gone),
        (410, "gone", Gone),
        (500, "internal", Ambiguous),
        (502, "bad_gateway", Ambiguous),
        (503, "unavailable", Unavailable),
        (504, "timeout", Unavailable),
    ]
    for status, code, cls in cases:
        got = await client(err(status, code)).command("s", "x")
        assert type(got) is cls, (status, code)
        assert got.reason == f"{status} {code}: m", (status, code)
        if cls in (Busy, Rejected):
            assert got.code == code, (status, code)

    # no envelope: the outcome carries no wrapper code
    bare = await client(lambda r: httpx.Response(403, text="nope")).command("s", "x")
    assert bare == Rejected("", "403 http_403: nope")
    no_id = await client(lambda r: httpx.Response(202, json={"state": "queued"})).command("s", "x")
    assert isinstance(no_id, Ambiguous)
    not_json = await client(lambda r: httpx.Response(202, text="ok")).command("s", "x")
    assert isinstance(not_json, Ambiguous)

    transport = [
        (httpx.ConnectError("refused"), Unavailable),
        (httpx.ConnectTimeout("t"), Unavailable),
        (httpx.ReadTimeout("t"), Ambiguous),
        (httpx.ReadError("reset"), Ambiguous),
    ]
    for exc, cls in transport:

        def boom(r, exc=exc):
            raise exc

        got = await client(boom).command("s", "x")
        assert type(got) is cls, exc
        assert got.reason


async def test_request_json_passthrough():
    seen = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["auth"] = req.headers["authorization"]
        seen["url"] = str(req.url)
        seen["body"] = req.content
        if req.url.path.endswith("/boom"):
            return httpx.Response(404, json={"error": {"code": "no_such", "message": "gone"}})
        if req.url.path.endswith("/empty"):
            return httpx.Response(204)
        return httpx.Response(201, json={"ok": [1, 2]})

    c = client(handler)
    status, body = await c.request_json(
        "POST", "/files/x", params={"path": "a b", "n": 1}, json={"k": "v"}
    )
    assert (status, body) == (201, {"ok": [1, 2]})
    assert seen["auth"] == "Bearer tok"
    assert seen["url"] == f"{BASE}/v1/files/x?path=a+b&n=1"
    assert json.loads(seen["body"]) == {"k": "v"}
    assert await c.request_json("GET", "/empty") == (204, None)
    with pytest.raises(WrapperError) as err:
        await c.request_json("GET", "/boom")
    assert (err.value.status, err.value.code, err.value.message) == (404, "no_such", "gone")


async def test_request_json_transport_failure():
    def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    with pytest.raises(WrapperUnreachable):
        await client(handler).request_json("GET", "/files/x")


async def test_post_note_body_and_outcomes():
    seen = []

    def h(r):
        seen.append((r.method, r.url.path, json.loads(r.content), r.extensions["timeout"]["read"]))
        return httpx.Response(202, json={"key": "hive_state", "pending": 1})

    c = client(h)
    assert await c.post_note("s1", "hello", key="hive_state") == "ok"
    assert await c.post_note("s1", "more", source="peer") == "ok"
    assert seen[0][:3] == (
        "POST",
        "/v1/sessions/s1/notes",
        {"text": "hello", "key": "hive_state", "source": "orchestrator"},
    )
    assert seen[1][2] == {"text": "more", "source": "peer"}
    assert seen[0][3] == 3.0

    def err(status, code):
        return lambda r: httpx.Response(status, json={"error": {"code": code, "message": "m"}})

    assert await client(err(409, "notes_disabled")).post_note("s", "x") == "disabled"
    assert await client(err(409, "invalid_state")).post_note("s", "x") == "failed"
    assert await client(err(404, "not_found")).post_note("s", "x") == "failed"

    def boom(r):
        raise httpx.ConnectError("down")

    assert await client(boom).post_note("s", "x") == "failed"


class _TrackedStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


async def test_open_download_streams_raw_bytes():
    seen = {}
    chunks = [b"\x00\xff\xfe binary", b"\x1f\x8b not really gzip \x00"]
    stream = _TrackedStream(chunks)

    def h(r: httpx.Request) -> httpx.Response:
        seen["method"] = r.method
        seen["path"] = r.url.path
        seen["query"] = dict(r.url.params)
        seen["url"] = str(r.url)
        seen["auth"] = r.headers.get("authorization")
        return httpx.Response(
            200,
            stream=stream,
            headers={
                "content-type": "application/octet-stream",
                "content-length": str(sum(map(len, chunks))),
                "content-encoding": "gzip",
                "content-disposition": 'attachment; filename="a b.bin"',
            },
        )

    resp = await client(h).open_download("ses_1", "dir/a b.bin")
    try:
        assert seen["method"] == "GET"
        assert seen["path"] == "/v1/sessions/ses_1/files/download"
        assert seen["query"] == {"path": "dir/a b.bin"}
        assert seen["auth"] == "Bearer tok"
        assert "tok" not in seen["url"]
        assert resp.status_code == 200 and not stream.closed
        assert resp.headers["content-disposition"] == 'attachment; filename="a b.bin"'
        assert resp.headers["content-type"] == "application/octet-stream"
        assert b"".join([c async for c in resp.aiter_raw()]) == b"".join(chunks)
    finally:
        await resp.aclose()
    assert stream.closed


async def test_open_download_errors():
    body = {"error": {"code": "not_a_file", "message": "is a directory"}}
    for status, payload, code, message in [
        (400, body, "not_a_file", "is a directory"),
        (404, {"error": {"code": "not_found", "message": "gone"}}, "not_found", "gone"),
        (503, None, "http_503", "down"),
    ]:
        stream = _TrackedStream([json.dumps(payload).encode() if payload else b"down"])
        c = client(lambda r, s=status, st=stream: httpx.Response(s, stream=st))
        with pytest.raises(WrapperError) as ei:
            await c.open_download("ses_1", "x")
        assert (ei.value.status, ei.value.code, ei.value.message) == (status, code, message)
        assert stream.closed

    def boom(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    with pytest.raises(WrapperUnreachable):
        await client(boom).open_download("ses_1", "x")

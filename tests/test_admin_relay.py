"""O9: the admin relay forwards superleader calls to the webhook, schedule, connector and history
upstreams.

The upstreams are `FakeUpstream`s held to the OpenAPI documents vendored in `tests/fakes` (each
`*_openapi.source` names the commit it was copied from), so the relay's idea of every route is
pinned to what the services really serve. No case needs Docker beyond the shared Postgres.
"""

from __future__ import annotations

import json
import re
import secrets as stdlib_secrets
import uuid

import httpx
import pytest
from fakes import fake_upstream as fu
from fakes.fake_kube import FakeKube
from psycopg.types.json import Jsonb
from test_agent_session_messaging import alone
from test_api_agent import KEY, client_for, headers, make_settings
from test_api_sessions import assert_error
from test_api_sessions import client_for as main_client

from agents_orchestrator.api import admin_relay, forward
from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.settings import Settings

HOSTS = {"webhook": "http://webhook.test", "schedule": "http://schedule.test"}
HOSTS["connector"] = "http://connector.test"
HOSTS["history"] = "http://history.test"
DOCS = {name: fu.load(name) for name in HOSTS}
NEXT = "x-next-before-id"
RELAY_PREFIX = {name: spec[0] for name, spec in admin_relay.SERVICES.items()}
UPSTREAM_PREFIX = {name: spec[1] for name, spec in admin_relay.SERVICES.items()}


class Hosts(httpx.AsyncBaseTransport):
    """Routes each request to the fake whose host it names; `broken` makes a host unreachable."""

    def __init__(self, fakes: dict[str, fu.FakeUpstream]) -> None:
        self.by_host = {
            httpx.URL(HOSTS[n]).host: httpx.ASGITransport(app=f.app) for n, f in fakes.items()
        }
        self.broken: set[str] = set()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.url.host in self.broken:
            raise httpx.ConnectError("refused", request=request)
        return await self.by_host[request.url.host].handle_async_request(request)


def make_app(dsn, pool, **overrides):
    tokens = {n: f"{n}-" + stdlib_secrets.token_hex(16) for n in HOSTS}
    values = {}
    for name in HOSTS:
        values[f"{name}_url"] = HOSTS[name]
        values[f"{name}_token"] = tokens[name]
    values.update(overrides)
    base = make_settings(dsn).model_dump()
    settings = Settings(**{**base, "master_key": KEY, **values})
    fakes = {n: fu.FakeUpstream(DOCS[n], tokens[n]) for n in HOSTS}
    kube = FakeKube(namespace=settings.namespace)
    app = create_app(settings, pool, kube=kube)
    hosts = Hosts(fakes)
    app.state.admin_http = httpx.AsyncClient(transport=hosts)
    return app, fakes, kube, hosts, tokens


@pytest.fixture
async def rig(pool, dsn):
    boss = await alone(pool, "rel-boss", superleader=True)
    app, fakes, kube, hosts, tokens = make_app(dsn, pool)

    class Rig:
        pass

    r = Rig()
    r.boss, r.app, r.fakes, r.kube, r.hosts, r.tokens, r.pool = (
        boss, app, fakes, kube, hosts, tokens, pool,
    )  # fmt: skip
    r.vis = str(await add_preset(pool, True))
    r.hid = str(await add_preset(pool, False))
    r.priv = str(await add_preset(pool, True, {"kube_access": True}))
    seed(r)

    async def call(method, path, **kw):
        extra = kw.pop("headers", {})
        async with client_for(app, boss.leader_ip) as http:
            return await http.request(method, path, headers={**headers(boss.id), **extra}, **kw)

    r.call = call
    return r


async def add_preset(pool, manageable: bool, spec: dict | None = None) -> uuid.UUID:
    pid = uuid.uuid4()
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO presets (id, name, spec, superleader_manageable) VALUES (%s,%s,%s,%s)",
            (pid, f"p-{pid.hex[:8]}", Jsonb(spec or {}), manageable),
        )
    return pid


# object id -> which preset it sits on: 7 visible, 8 hidden, 9 none
ID_FIELD = {"EndpointView": "id", "EventView": "id", "ScheduleView": "id", "RunView": "id"}
ID_FIELD["RoomView"] = ID_FIELD["EventDetail"] = ID_FIELD["EventView"]
ID_FIELD["RoomView"] = "room_id"


def ident(service: str, n: int | str, sample=None):
    """Object id `n` as the upstream spells it: schedules, runs and history sessions use UUIDs."""
    if service in ("schedule", "history"):
        return f"00000000-0000-4000-8000-{int(n):012d}"
    return n if isinstance(sample, int) else str(n)


def make(service: str, schema: str, n: int, **over):
    obj = fu.sample(DOCS[service], {"$ref": f"#/components/schemas/{schema}"})
    field = ID_FIELD[schema]
    obj[field] = ident(service, n, obj[field])
    obj.update(over)
    return obj


def seed(rig) -> None:
    """Give the fakes one object on a manageable preset (7), one on a hidden preset (8) and,
    for rooms and runs, one without a preset (9); mutations answer a minimal valid success."""
    by = {7: rig.vis, 8: rig.hid, 9: None, 10: rig.priv}
    ends = [make("webhook", "EndpointView", n, preset_id=by[n]) for n in (7, 8, 10)]
    events = [make("webhook", "EventView", n, preset_id=by[n]) for n in (7, 8, 9)]
    details = [make("webhook", "EventDetail", n, preset_id=by[n]) for n in (7, 8, 9)]
    scheds = [make("schedule", "ScheduleView", n, preset_id=by[n]) for n in (7, 8, 10)]
    runs = [make("schedule", "RunView", n, schedule_id=ident("schedule", n)) for n in (7, 8, 9)]
    runs[2]["schedule_id"] = ident("schedule", 99)  # a schedule the upstream does not list
    rooms = [make("connector", "RoomView", n, preset_id=by[n]) for n in (7, 8, 9, 10)]
    for name, fake in rig.fakes.items():
        for key in admin_ops(name):
            fake.answers[key] = fu.success_sample(fake, key)
    w, sc, c = rig.fakes["webhook"], rig.fakes["schedule"], rig.fakes["connector"]
    w.answers[("GET", "/webhooks/api/v1/endpoints")] = (200, ends)
    w.answers[("GET", "/webhooks/api/v1/events")] = (200, events)
    w.answers[("GET", "/webhooks/api/v1/events/{id}")] = lambda call, p: (
        200,
        next(e for e in details if str(e["id"]) == p["id"]),
    )
    sc.answers[("GET", "/schedules/api/v1/schedules")] = (200, scheds)
    sc.answers[("GET", "/schedules/api/v1/runs")] = (200, runs)
    sc.answers[("GET", "/schedules/api/v1/runs/{run_id}")] = lambda call, p: (
        200,
        next(r for r in runs if str(r["id"]) == p["run_id"]),
    )
    c.answers[("GET", "/chat/v1/admin/rooms")] = (200, rooms)
    rule = fu.sample(DOCS["connector"], {"$ref": "#/components/schemas/RuleView"})
    rule.update(id=7, list="allow", kind="room", value="7")  # the rule delete reads the list
    c.answers[("GET", "/chat/v1/admin/rules")] = (200, {"allow": [rule], "deny": []})
    seed_history(rig, by)


def history_session(schema: str, n: int, preset) -> dict:
    """A history session: database id `n`, session UUID `ident("history", n)`."""
    obj = fu.sample(DOCS["history"], {"$ref": f"#/components/schemas/{schema}"})
    obj.update(id=n, session_id=ident("history", n), preset_id=preset)
    return obj


def seed_history(rig, by: dict) -> None:
    rig.summaries = [history_session("SessionSummary", n, by[n]) for n in (7, 8, 9)]
    details = [history_session("SessionDetail", n, by[n]) for n in (7, 8, 9)]
    missing = (404, {"error": {"code": "not_found", "message": "no such session"}})
    fake = rig.fakes["history"]
    fake.answers[("GET", "/history/v1/sessions")] = lambda call, p: (200, rig.summaries)
    fake.answers[("GET", "/history/v1/sessions/{session_id}")] = lambda call, p: next(
        ((200, d) for d in details if d["session_id"] == p["session_id"]), missing
    )


SEEDED = {
    ("GET", "/webhooks/api/v1/endpoints"),
    ("GET", "/webhooks/api/v1/events"),
    ("GET", "/webhooks/api/v1/events/{id}"),
    ("GET", "/schedules/api/v1/schedules"),
    ("GET", "/schedules/api/v1/runs"),
    ("GET", "/schedules/api/v1/runs/{run_id}"),
    ("GET", "/chat/v1/admin/rooms"),
    ("GET", "/chat/v1/admin/rules"),
    ("GET", "/history/v1/sessions"),
    ("GET", "/history/v1/sessions/{session_id}"),
}


def relay_url(service: str, template: str, n: int = 7) -> str:
    fill = str(ident(service, n))
    return ADMIN_PREFIX + RELAY_PREFIX[service] + re.sub(r"\{[^}]+\}", fill, template)


def admin_ops(service: str) -> dict[tuple[str, str], dict]:
    ops = fu.operations(DOCS[service])
    if service == "connector":  # the chat relay owns the non-admin routes
        ops = {k: v for k, v in ops.items() if k[1].startswith("/chat/v1/admin/")}
    return ops


# -------------------------------------------------------------------------------- contract


@pytest.mark.parametrize("name", list(HOSTS))
def test_source_line_names_a_full_sha(name):
    assert re.fullmatch(r"agents-[a-z-]+ [0-9a-f]{40}\n", fu.source_line(name))


@pytest.mark.parametrize("name", list(HOSTS))
def test_routes_map_one_to_one_to_the_document(name):
    """Every admin route of the upstream is relayed, and nothing else is."""
    table = {
        (m, UPSTREAM_PREFIX[name] + t): s for m, t, s in admin_relay.SERVICES[name][3]
    }  # fmt: skip
    ops = admin_ops(name)
    assert set(table) == set(ops)
    for key, status in table.items():
        assert str(status) in ops[key]["responses"], key  # the documented success status


def test_relay_routes_are_served_on_the_admin_router(rig):
    served = rig.app.openapi()["paths"]
    for prefix, _, _, routes in admin_relay.SERVICES.values():
        for method, template, status in routes:
            op = served[ADMIN_PREFIX + prefix + template][method.lower()]
            assert str(status) in op["responses"]


@pytest.mark.parametrize("name", list(HOSTS))
async def test_every_route_round_trips_the_documented_answer(rig, name):
    fake = rig.fakes[name]
    for method, template, status in admin_relay.SERVICES[name][3]:
        key = (method, UPSTREAM_PREFIX[name] + template)
        if key not in SEEDED:  # the seeded lists and single answers stay: guards read them
            fake.answers[key] = fu.success_sample(fake, key)
        body = None
        if _has_body(fake, key):
            body = fu.sample(DOCS[name], _body_schema(fake, key))
            if isinstance(body, dict) and "preset_id" in body:
                body["preset_id"] = rig.vis
            if isinstance(body, dict) and "list" in body:  # a chat rule: room 7 is visible
                body.update(list="allow", kind="room", value="7")
        r = await rig.call(method, relay_url(name, template), json=body)
        assert r.status_code == status, (key, r.text)
        if key not in SEEDED:
            want = fake.answers[key][1]
            assert (r.json() if r.content else None) == want, key
    assert fake.calls


def _has_body(fake, key):
    return "requestBody" in fake._ops[key]


def _body_schema(fake, key):
    return fake._ops[key]["requestBody"]["content"]["application/json"]["schema"]


# ------------------------------------------------------------------------------ the request


async def test_only_the_service_bearer_and_session_headers_are_sent(rig):
    fake = rig.fakes["schedule"]
    key = ("GET", "/schedules/api/v1/schedules")
    fake.answers[key] = fu.success_sample(fake, key)
    agent_token = headers(rig.boss.id)["Authorization"]
    r = await rig.call(
        "GET",
        ADMIN_PREFIX + "/schedules/schedules?limit=5",
        headers={"X-Evil": "1", "Cookie": "a=b", "X-Forwarded-For": "6.6.6.6"},
    )
    assert r.status_code == 200
    (call,) = fake.calls
    assert call.query == "limit=5"
    assert call.headers["authorization"] == f"Bearer {rig.tokens['schedule']}"
    assert call.headers["x-orch-session"] == str(rig.boss.id)
    assert call.headers["x-orch-slug"] == "rel-boss"
    sent = json.dumps(call.headers)
    assert agent_token.removeprefix("Bearer ") not in sent
    for name in ("x-evil", "cookie", "x-forwarded-for"):
        assert name not in call.headers
    assert "content-type" not in call.headers  # no body, no content type
    assert not rig.fakes["webhook"].calls and not rig.fakes["connector"].calls


async def test_each_upstream_gets_its_own_token(rig):
    for name, template in (("webhook", "/meta"), ("connector", "/rules")):
        fake = rig.fakes[name]
        key = ("GET", UPSTREAM_PREFIX[name] + template)
        fake.answers[key] = fu.success_sample(fake, key)
        assert (await rig.call("GET", relay_url(name, template))).status_code == 200
        assert fake.calls[0].headers["authorization"] == f"Bearer {rig.tokens[name]}"


async def test_body_and_content_type_are_forwarded_on_a_post(rig):
    fake = rig.fakes["connector"]
    key = ("POST", "/chat/v1/admin/rules")
    fake.answers[key] = fu.success_sample(fake, key)
    body = fu.sample(DOCS["connector"], _body_schema(fake, key))
    body["list"] = "deny"  # an allow rule is checked against the room first (see below)
    r = await rig.call("POST", relay_url("connector", "/rules"), json=body)
    assert r.status_code == 201, r.text
    assert fake.calls[0].json() == body
    assert fake.calls[0].headers["content-type"] == "application/json"


async def test_path_parameters_cannot_walk_up_the_upstream_path(rig):
    fake = rig.fakes["schedule"]
    r = await rig.call("GET", ADMIN_PREFIX + "/schedules/schedules/..")
    assert r.status_code in (404, 405, 422) or fake.calls == []
    key = ("GET", "/schedules/api/v1/schedules/{schedule_id}")
    fake.answers[key] = (404, {"error": {"code": "not_found", "message": "x"}})
    r = await rig.call("GET", ADMIN_PREFIX + "/schedules/schedules/a%2Fb")
    assert r.status_code in (404, 422)
    assert all("/../" not in c.path and c.path.count("/") == 5 for c in fake.calls)


# --------------------------------------------------------------------------- failure modes


async def test_upstream_status_and_json_pass_through(rig):
    fake = rig.fakes["schedule"]
    key = ("GET", "/schedules/api/v1/schedules/{schedule_id}")
    fake.answers[key] = (404, {"error": {"code": "not_found", "message": "gone"}})
    r = await rig.call("GET", relay_url("schedule", "/schedules/{schedule_id}"))
    assert r.status_code == 404 and r.json()["error"]["code"] == "not_found"


@pytest.mark.parametrize("name", list(HOSTS))
async def test_unconfigured_upstream_is_503(pool, dsn, name):
    boss = await alone(pool, f"rel-un-{name}", superleader=True)
    app, fakes, *_ = make_app(dsn, pool, **{f"{name}_url": "", f"{name}_token": ""})
    template = admin_relay.SERVICES[name][3][0][1]
    async with client_for(app, boss.leader_ip) as http:
        r = await http.get(relay_url(name, template), headers=headers(boss.id))
    assert_error(r, 503, "upstream_unconfigured")
    assert not any(f.calls for f in fakes.values())


async def test_unavailable_upstream_is_503_without_a_leak(rig):
    rig.hosts.broken.add("webhook.test")
    r = await rig.call("GET", relay_url("webhook", "/meta"))
    assert_error(r, 503, "upstream_unavailable")
    assert rig.tokens["webhook"] not in r.text


async def test_non_json_answer_is_503(rig):
    fake = rig.fakes["webhook"]

    async def html(request):
        return httpx.Response(502, content=b"<html>bad gateway</html>")

    rig.app.state.admin_http = httpx.AsyncClient(transport=httpx.MockTransport(html))
    r = await rig.call("GET", relay_url("webhook", "/meta"))
    assert_error(r, 503, "upstream_unavailable")
    assert not fake.calls


async def test_oversize_body_is_413_and_never_sent(rig):
    big = b'{"a":"' + b"x" * (forward.MAX_BODY_BYTES + 1) + b'"}'
    r = await rig.call(
        "POST",
        relay_url("webhook", "/templates/preview"),
        content=big,
        headers={"Content-Type": "application/json"},
    )
    assert_error(r, 413, "payload_too_large")
    assert not rig.fakes["webhook"].calls


async def test_upstream_validation_error_passes_through(rig):
    fake = rig.fakes["connector"]
    r = await rig.call("POST", relay_url("connector", "/rules"), json={"list": "deny", "bogus": 1})
    assert r.status_code == 422
    assert fake.calls and r.json() == {"detail": [{"msg": r.json()["detail"][0]["msg"]}]}


async def test_nothing_is_logged_with_a_token_or_body(rig, caplog):
    canary = "canary-" + stdlib_secrets.token_hex(8)
    fake = rig.fakes["connector"]
    key = ("POST", "/chat/v1/admin/rules")
    fake.answers[key] = fu.success_sample(fake, key)
    body = fu.sample(DOCS["connector"], _body_schema(fake, key))
    body.update(list="deny", value=canary)  # the rule value is a personal identifier
    with caplog.at_level("DEBUG"):
        await rig.call("POST", relay_url("connector", "/rules"), json=body)
    assert canary not in caplog.text
    assert rig.tokens["connector"] not in caplog.text


# ------------------------------------------------------------------------- path encoding


@pytest.mark.parametrize(
    ("raw", "want"),
    [
        ("..", "%2E%2E"),
        (".", "%2E"),
        ("", ""),
        ("%2e%2e", "%252e%252e"),
        ("a%2Fb", "a%252Fb"),
        ("a/b", "a%2Fb"),
        ("a\\b", "a%5Cb"),
    ],
)
def test_segment_is_always_one_inert_path_segment(raw, want):
    assert admin_relay._segment(raw) == want
    assert "/" not in want and "\\" not in want and want not in (".", "..")


async def test_encoded_dots_reach_the_upstream_as_one_segment(rig):
    seen = []

    async def record(request):
        seen.append(request.url.raw_path.decode())
        return httpx.Response(404, json={"error": {"code": "not_found", "message": "x"}})

    async def answer(request):
        if request.method == "GET":  # the rule lookup: allow rules, so the delete is forwarded
            ids = ("..", ".", "..\\", "a")
            return httpx.Response(200, json={"allow": [{"id": i} for i in ids], "deny": []})
        return await record(request)

    rig.app.state.admin_http = httpx.AsyncClient(transport=httpx.MockTransport(answer))
    for raw in ("%2e%2e", "%2E%2E", "%2e", "..%5c"):
        r = await rig.call("DELETE", ADMIN_PREFIX + f"/chats/rules/{raw}")
        assert r.status_code == 404, (raw, r.text)
    assert len(seen) == 4
    # a decoded slash splits the path before routing: it never matches a relay route
    await rig.call("DELETE", ADMIN_PREFIX + "/chats/rules/a%2f..%2fb")
    assert len(seen) == 4
    for path in seen:
        head = "/chat/v1/admin/rules/"
        assert path.startswith(head), path
        tail = path[len(head) :]
        assert "/" not in tail and tail not in (".", "..") and "\\" not in tail, path


# ------------------------------------------------------------------------------- the guards


def ids(response, field: str = "id") -> set[str]:
    return {str(i[field]) for i in response.json()}


@pytest.mark.parametrize(
    ("service", "template", "field"),
    [
        ("webhook", "/endpoints", "id"),
        ("webhook", "/events", "id"),
        ("schedule", "/schedules", "id"),
        ("schedule", "/runs", "id"),
        ("connector", "/rooms", "room_id"),
    ],
)
async def test_lists_filtered(rig, service, template, field):
    r = await rig.call("GET", relay_url(service, template))
    assert r.status_code == 200
    # object 10 (endpoint, schedule, room) sits on the visible privileged preset: listed,
    # not writable
    seen = (7, 10) if template in ("/endpoints", "/schedules", "/rooms") else (7,)
    assert ids(r, field) == {str(ident(service, n)) for n in seen}


async def test_list_with_non_list_payload_fails_closed(rig):
    async def answer(request):
        return httpx.Response(200, json={"items": [{"id": 8, "preset_id": rig.vis}]})

    rig.app.state.admin_http = httpx.AsyncClient(transport=httpx.MockTransport(answer))
    r = await rig.call("GET", relay_url("webhook", "/endpoints"))
    assert_error(r, 502, "upstream_shape")
    assert "items" not in r.text


def _body_for(rig, service: str, method: str, template: str, preset):
    key = (method, UPSTREAM_PREFIX[service] + template)
    fake = rig.fakes[service]
    if "requestBody" not in fake._ops[key]:
        return None
    body = fu.sample(DOCS[service], _body_schema(fake, key))
    body = body if isinstance(body, dict) else {}
    body["preset_id"] = preset
    return body


def _sent(rig, service: str, method: str, template: str, n: int = 7) -> bool:
    path = UPSTREAM_PREFIX[service] + re.sub(r"\{[^}]+\}", str(ident(service, n)), template)
    return any(c.method == method and c.path == path for c in rig.fakes[service].calls)


STORED = sorted(
    (s, m, t) for (s, m, t), g in admin_relay.GUARDS.items() if g.stored
)  # fmt: skip
BODY = sorted((s, m, t) for (s, m, t), g in admin_relay.GUARDS.items() if g.body_preset)


def _success(service: str, method: str, template: str) -> int:
    return next(st for m, t, st in admin_relay.SERVICES[service][3] if (m, t) == (method, template))


@pytest.mark.parametrize(("service", "method", "template"), STORED)
async def test_hidden_mutation_never_forwarded(rig, service, method, template):
    for n in (8, 9, 404):  # hidden preset, no preset, unknown
        body = _body_for(rig, service, method, template, rig.vis)
        r = await rig.call(method, relay_url(service, template, n), json=body)
        assert_error(r, 404, "not_found")
        assert not _sent(rig, service, method, template, n), (n, r.text)


@pytest.mark.parametrize(("service", "method", "template"), STORED)
async def test_visible_mutation_is_forwarded(rig, service, method, template):
    body = _body_for(rig, service, method, template, rig.vis)
    r = await rig.call(method, relay_url(service, template), json=body)
    assert r.status_code == _success(service, method, template), r.text
    assert _sent(rig, service, method, template)


@pytest.mark.parametrize(("service", "method", "template"), BODY)
async def test_body_hidden_preset_404(rig, service, method, template):
    for preset in (rig.hid, None, str(uuid.uuid4())):
        body = _body_for(rig, service, method, template, preset)
        r = await rig.call(method, relay_url(service, template), json=body)
        assert_error(r, 404, "not_found")
        assert not _sent(rig, service, method, template), preset
    body = _body_for(rig, service, method, template, rig.vis)
    r = await rig.call(method, relay_url(service, template), json=body)
    assert r.status_code == _success(service, method, template), r.text


@pytest.mark.parametrize(
    ("service", "template"), [("webhook", "/events/{id}"), ("schedule", "/runs/{run_id}")]
)
async def test_answer_hidden_404(rig, service, template):
    for n in (8, 9):
        assert_error(await rig.call("GET", relay_url(service, template, n)), 404, "not_found")
    assert (await rig.call("GET", relay_url(service, template))).status_code == 200


async def test_unguarded_routes_unchanged(rig):
    for service, method, template in (
        ("webhook", "GET", "/meta"),
        ("webhook", "POST", "/templates/preview"),
        ("connector", "GET", "/rules"),
    ):
        key = (method, UPSTREAM_PREFIX[service] + template)
        body = None
        if "requestBody" in rig.fakes[service]._ops[key]:
            body = fu.sample(DOCS[service], _body_schema(rig.fakes[service], key))
        r = await rig.call(method, relay_url(service, template), json=body)
        assert r.status_code == _success(service, method, template), (key, r.text)
        assert rig.fakes[service].answers[key] is not None


async def test_null_preset_room_hidden(rig):
    r = await rig.call("GET", relay_url("connector", "/rooms"))
    assert "9" not in ids(r, "room_id")
    body = _body_for(rig, "connector", "PUT", "/rooms/{room_id}/settings", rig.vis)
    url = relay_url("connector", "/rooms/{room_id}/settings", 9)
    assert_error(await rig.call("PUT", url, json=body), 404, "not_found")
    assert not _sent(rig, "connector", "PUT", "/rooms/{room_id}/settings", 9)


# ------------------------------------------------------------------------------ the history


def history_paths(rig) -> list[str]:
    return [c.path for c in rig.fakes["history"].calls]


async def test_history_list_filtered_by_preset(rig):
    r = await rig.call("GET", relay_url("history", "/sessions"))
    assert r.status_code == 200
    # visible, hidden and null presets in the upstream page: only the visible one is shown
    assert ids(r, "session_id") == {ident("history", 7)}
    assert NEXT not in r.headers  # a short page is the end: no cursor


async def test_history_list_full_page_carries_the_upstream_cursor(rig):
    r = await rig.call("GET", relay_url("history", "/sessions"), params={"limit": 3})
    assert ids(r, "session_id") == {ident("history", 7)}
    assert r.headers[NEXT] == "7"  # the smallest id of the upstream page, hidden rows included
    rig.summaries[:] = rig.summaries[1:]  # a page of hidden sessions only: [] and still a cursor
    r = await rig.call("GET", relay_url("history", "/sessions"), params={"limit": 2})
    assert r.status_code == 200 and r.json() == []
    assert r.headers[NEXT] == "8"
    r = await rig.call("GET", relay_url("history", "/sessions"), params={"limit": 3})
    assert NEXT not in r.headers  # two rows for a limit of three: the end


async def test_history_list_default_limit_full_page_carries_the_cursor(rig):
    rig.summaries[:] = [history_session("SessionSummary", n, None) for n in range(100, 50, -1)]
    r = await rig.call("GET", relay_url("history", "/sessions"))  # no `limit`: the default of 50
    assert r.status_code == 200 and r.json() == []
    assert r.headers[NEXT] == "51"  # 50 rows is a full default page
    rig.summaries[:] = rig.summaries[1:]
    r = await rig.call("GET", relay_url("history", "/sessions"))
    assert NEXT not in r.headers  # 49 rows: the end


async def test_history_list_non_ascii_digit_limit_is_not_a_page_size(rig):
    r = await rig.call("GET", relay_url("history", "/sessions"), params={"limit": "\u0663"})
    assert NEXT not in r.headers  # three rows against the default of 50, not against 3


def test_history_list_documents_the_cursor_header(rig):
    op = rig.app.openapi()["paths"][ADMIN_PREFIX + "/history/sessions"]["get"]
    assert "X-Next-Before-Id" in op["responses"]["200"]["headers"]
    one = rig.app.openapi()["paths"][ADMIN_PREFIX + "/history/sessions/{session_id}"]["get"]
    assert "headers" not in one["responses"]["200"]


async def test_history_get_hidden_is_404(rig):
    for n in (8, 9):
        r = await rig.call("GET", relay_url("history", "/sessions/{session_id}", n))
        assert_error(r, 404, "not_found")
    r = await rig.call("GET", relay_url("history", "/sessions/{session_id}"))
    assert r.status_code == 200 and r.json()["session_id"] == ident("history", 7)


async def test_history_records_hidden_never_forwarded(rig):
    for n in (8, 9):  # hidden preset, no preset
        r = await rig.call("GET", relay_url("history", "/sessions/{session_id}/records", n))
        assert_error(r, 404, "not_found")
    lookups = [f"/history/v1/sessions/{ident('history', n)}" for n in (8, 9)]
    assert history_paths(rig) == lookups  # the lookups only: no /records call
    r = await rig.call("GET", relay_url("history", "/sessions/{session_id}/records"))
    assert r.status_code == 200
    assert history_paths(rig)[-1] == f"/history/v1/sessions/{ident('history', 7)}/records"


async def test_history_lookup_404(rig):
    r = await rig.call("GET", relay_url("history", "/sessions/{session_id}/records", 404))
    assert_error(r, 404, "not_found")
    assert history_paths(rig) == [f"/history/v1/sessions/{ident('history', 404)}"]


async def test_history_lookup_is_bare_and_the_records_call_keeps_the_query(rig):
    url = relay_url("history", "/sessions/{session_id}/records")
    r = await rig.call("GET", url, params={"q": "needle", "limit": 5})
    assert r.status_code == 200
    lookup, records = rig.fakes["history"].calls
    assert lookup.query == "" and records.query == "q=needle&limit=5"


async def test_history_delete_refused(rig):
    """The superleader cannot delete a stored session (user decision Q6): the router has no
    DELETE route, so it answers 405 and nothing reaches the upstream, visible session or not."""
    for n in (7, 8):
        r = await rig.call("DELETE", relay_url("history", "/sessions/{session_id}", n))
        assert r.status_code == 405, r.text
        assert r.headers["allow"] == "GET"
    assert rig.fakes["history"].calls == []  # no DELETE and no lookup


async def test_history_query_string_passed(rig):
    r = await rig.call("GET", ADMIN_PREFIX + "/history/sessions?q=needle&regex=true")
    assert r.status_code == 200
    (call,) = rig.fakes["history"].calls
    assert call.path == "/history/v1/sessions" and call.query == "q=needle&regex=true"
    assert call.headers["authorization"] == f"Bearer {rig.tokens['history']}"


# ------------------------------------------------------------------------- privileged targets

PRIVILEGED_WRITES = [
    ("webhook", "POST", "/endpoints"),
    ("webhook", "PATCH", "/endpoints/{id}"),
    ("webhook", "POST", "/endpoints/{id}/clone"),
    ("webhook", "PUT", "/endpoints/{id}/filters"),
    ("webhook", "POST", "/endpoints/{id}/rotate"),
    ("schedule", "POST", "/schedules"),
    ("schedule", "PUT", "/schedules/{schedule_id}"),
    ("schedule", "POST", "/schedules/{schedule_id}/run"),
    ("connector", "PUT", "/rooms/{room_id}/settings"),
    ("connector", "POST", "/rules"),
    ("connector", "DELETE", "/rules/{rule_id}"),
]
PRIV_BODY = [r for r in PRIVILEGED_WRITES if r in BODY]
PRIV_STORED = [r for r in PRIVILEGED_WRITES if r in STORED]


def test_privileged_guard_rows_are_exactly_the_epic_list():
    assert {k for k, g in admin_relay.GUARDS.items() if g.privileged} == set(PRIVILEGED_WRITES)


async def _last_admin_call(rig) -> dict:
    async with rig.pool.connection() as conn:
        cur = await conn.execute(
            "SELECT data FROM events WHERE session_id = %s AND type = 'admin_call'"
            " ORDER BY id DESC LIMIT 1",
            (rig.boss.id,),
        )
        return (await cur.fetchone())[0]


@pytest.mark.parametrize(("service", "method", "template"), PRIV_BODY)
async def test_relay_body_privileged_target_403(rig, service, method, template):
    body = _body_for(rig, service, method, template, rig.priv)
    r = await rig.call(method, relay_url(service, template), json=body)
    assert_error(r, 403, "privileged_target")
    assert r.json()["error"]["fields"] == ["kube_access"]
    assert not _sent(rig, service, method, template)
    event = await _last_admin_call(rig)
    assert event["status"] == 403
    assert event["fields"] == ["kube_access"]


@pytest.mark.parametrize(("service", "method", "template"), PRIV_STORED)
@pytest.mark.parametrize("with_preset", [False, True])
async def test_relay_stored_privileged_target_403(rig, service, method, template, with_preset):
    body = _body_for(rig, service, method, template, rig.vis)
    if body is not None and not with_preset:
        body.pop("preset_id")
    r = await rig.call(method, relay_url(service, template, 10), json=body)
    assert_error(r, 403, "privileged_target")
    assert r.json()["error"]["fields"] == ["kube_access"]
    assert not _sent(rig, service, method, template, 10)


@pytest.mark.parametrize(("service", "method", "template"), PRIV_STORED)
async def test_relay_safe_preset_still_forwarded(rig, service, method, template):
    body = _body_for(rig, service, method, template, rig.vis)
    r = await rig.call(method, relay_url(service, template), json=body)
    assert r.status_code == _success(service, method, template), r.text
    assert _sent(rig, service, method, template)


async def test_relay_hidden_privileged_is_404(rig):
    hidden = str(await add_preset(rig.pool, False, {"kube_access": True}))
    body = _body_for(rig, "webhook", "POST", "/endpoints", hidden)
    r = await rig.call("POST", relay_url("webhook", "/endpoints"), json=body)
    assert_error(r, 404, "not_found")
    assert not _sent(rig, "webhook", "POST", "/endpoints")


@pytest.mark.parametrize(
    ("service", "method", "template"),
    [
        ("webhook", "DELETE", "/endpoints/{id}"),
        ("schedule", "DELETE", "/schedules/{schedule_id}"),
    ],
)
async def test_relay_non_write_routes_ignore_privileged(rig, service, method, template):
    r = await rig.call(method, relay_url(service, template, 10))
    assert r.status_code == _success(service, method, template), r.text
    assert _sent(rig, service, method, template, 10)


async def test_relay_is_agent_port_only(rig):
    test_relay_routes_are_served_on_the_admin_router(rig)
    body = _body_for(rig, "webhook", "POST", "/endpoints", rig.vis)
    async with main_client(rig.app) as http:
        r = await http.post(
            ADMIN_PREFIX + "/webhooks/endpoints", json=body, headers={"authorization": "Bearer x"}
        )
    assert not 200 <= r.status_code < 300
    assert not _sent(rig, "webhook", "POST", "/endpoints")


# ------------------------------------------------------------------------- chat routing rules
# An allow rule, or the removal of a deny rule, lets inbound chats start sessions from the
# room's preset; the superleader may widen routing only to a room on a preset it manages that
# has no privileged flag. Rooms (seeded): 7 visible, 8 hidden, 9 no preset, 10 privileged.

RULES = ("connector", "POST", "/rules")
RULE_DELETE = ("connector", "DELETE", "/rules/{rule_id}")


def _rule(list_: str, kind: str, value: str) -> dict:
    return {"list": list_, "kind": kind, "value": value, "note": None}


@pytest.mark.parametrize("n", [7])
async def test_rule_allow_visible_room_forwarded(rig, n):
    r = await rig.call("POST", relay_url(*RULES[::2]), json=_rule("allow", "room", str(n)))
    assert r.status_code == 201, r.text
    assert _sent(rig, *RULES)


async def test_rule_allow_privileged_room_403(rig):
    r = await rig.call("POST", relay_url(*RULES[::2]), json=_rule("allow", "room", "10"))
    assert_error(r, 403, "privileged_target")
    assert r.json()["error"]["fields"] == ["kube_access"]
    assert not _sent(rig, *RULES)
    event = await _last_admin_call(rig)
    assert event["status"] == 403 and event["fields"] == ["kube_access"]


@pytest.mark.parametrize("room", ["8", "9", "404"])
async def test_rule_allow_hidden_room_404(rig, room):
    """Hidden preset, no preset (the connector's default preset, unknown here) or no such room."""
    r = await rig.call("POST", relay_url(*RULES[::2]), json=_rule("allow", "room", room))
    assert_error(r, 404, "not_found")
    assert not _sent(rig, *RULES)


@pytest.mark.parametrize(
    "body",
    [
        _rule("allow", "user", "@victim:example.org"),
        _rule("allow", "facebook", "100012345"),
        _rule("allow", "alias", "#ops:example.org"),
        _rule("allow", "Room", "7"),
        "not an object",
    ],
)
async def test_rule_allow_beyond_one_room_403(rig, body, canary="canary-rule-value"):
    """A user, facebook or alias allow rule covers rooms on any preset, present and future: only
    a human adds one. A kind other than exactly `room` counts as one (fail closed)."""
    if isinstance(body, dict) and "value" in body and body["kind"] != "room":
        body = {**body, "value": canary}
    r = await rig.call("POST", relay_url(*RULES[::2]), json=body)
    assert_error(r, 403, "human_only_field")
    assert r.json()["error"]["fields"] == ["kind"]
    assert canary not in r.text
    assert not _sent(rig, *RULES)


@pytest.mark.parametrize("body", [_rule("ALLOW", "room", "10"), {"kind": "room", "value": "10"}])
async def test_rule_list_other_than_deny_is_checked_as_allow(rig, body):
    r = await rig.call("POST", relay_url(*RULES[::2]), json=body)
    assert_error(r, 403, "privileged_target")
    assert not _sent(rig, *RULES)


@pytest.mark.parametrize("kind", ["user", "facebook", "room", "alias", "display_name"])
async def test_rule_deny_forwarded(rig, kind):
    r = await rig.call("POST", relay_url(*RULES[::2]), json=_rule("deny", kind, "10"))
    assert r.status_code == 201, r.text
    assert _sent(rig, *RULES)


def _seed_rules(rig) -> None:
    view = fu.sample(DOCS["connector"], {"$ref": "#/components/schemas/RuleView"})
    allow = [{**view, "id": 1, "list": "allow", "kind": "room", "value": "10"}]
    deny = [
        {**view, "id": 2, "list": "deny", "kind": "room", "value": "10"},
        {**view, "id": 3, "list": "deny", "kind": "room", "value": "7"},
        {**view, "id": 4, "list": "deny", "kind": "user", "value": "@x:example.org"},
        {**view, "id": 5, "list": "deny", "kind": "room", "value": "8"},
    ]
    rig.fakes["connector"].answers[("GET", "/chat/v1/admin/rules")] = (
        200,
        {"allow": allow, "deny": deny},
    )


@pytest.mark.parametrize("n", [1, 3])
async def test_rule_delete_allow_or_safe_deny_forwarded(rig, n):
    _seed_rules(rig)
    r = await rig.call("DELETE", relay_url(*RULE_DELETE[::2], n))
    assert r.status_code == 204, r.text
    assert _sent(rig, *RULE_DELETE, n)


async def test_rule_delete_deny_of_privileged_room_403(rig):
    _seed_rules(rig)
    r = await rig.call("DELETE", relay_url(*RULE_DELETE[::2], 2))
    assert_error(r, 403, "privileged_target")
    assert r.json()["error"]["fields"] == ["kube_access"]
    assert not _sent(rig, *RULE_DELETE, 2)


async def test_rule_delete_deny_beyond_one_room_403(rig):
    _seed_rules(rig)
    r = await rig.call("DELETE", relay_url(*RULE_DELETE[::2], 4))
    assert_error(r, 403, "human_only_field")
    assert "@x:example.org" not in r.text
    assert not _sent(rig, *RULE_DELETE, 4)


@pytest.mark.parametrize("n", [5, 99])
async def test_rule_delete_hidden_room_or_unknown_rule_404(rig, n):
    _seed_rules(rig)
    r = await rig.call("DELETE", relay_url(*RULE_DELETE[::2], n))
    assert_error(r, 404, "not_found")
    assert not _sent(rig, *RULE_DELETE, n)

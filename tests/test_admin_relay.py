"""O9: the admin relay forwards superleader calls to webhook, schedule and connector upstreams.

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

from agents_orchestrator.api import admin_relay, forward
from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.settings import Settings

HOSTS = {"webhook": "http://webhook.test", "schedule": "http://schedule.test"}
HOSTS["connector"] = "http://connector.test"
DOCS = {name: fu.load(name) for name in HOSTS}
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
    seed(r)

    async def call(method, path, **kw):
        extra = kw.pop("headers", {})
        async with client_for(app, boss.leader_ip) as http:
            return await http.request(method, path, headers={**headers(boss.id), **extra}, **kw)

    r.call = call
    return r


async def add_preset(pool, manageable: bool) -> uuid.UUID:
    pid = uuid.uuid4()
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO presets (id, name, spec, superleader_manageable) VALUES (%s,%s,%s,%s)",
            (pid, f"p-{pid.hex[:8]}", Jsonb({}), manageable),
        )
    return pid


# object id -> which preset it sits on: 7 visible, 8 hidden, 9 none
ID_FIELD = {"EndpointView": "id", "EventView": "id", "ScheduleView": "id", "RunView": "id"}
ID_FIELD["RoomView"] = ID_FIELD["EventDetail"] = ID_FIELD["EventView"]
ID_FIELD["RoomView"] = "room_id"


def ident(service: str, n: int | str, sample=None):
    """Object id `n` as the upstream spells it: schedules and runs use UUIDs."""
    if service == "schedule":
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
    by = {7: rig.vis, 8: rig.hid, 9: None}
    ends = [make("webhook", "EndpointView", n, preset_id=by[n]) for n in (7, 8)]
    events = [make("webhook", "EventView", n, preset_id=by[n]) for n in (7, 8, 9)]
    details = [make("webhook", "EventDetail", n, preset_id=by[n]) for n in (7, 8, 9)]
    scheds = [make("schedule", "ScheduleView", n, preset_id=by[n]) for n in (7, 8)]
    runs = [make("schedule", "RunView", n, schedule_id=ident("schedule", n)) for n in (7, 8, 9)]
    runs[2]["schedule_id"] = ident("schedule", 99)  # a schedule the upstream does not list
    rooms = [make("connector", "RoomView", n, preset_id=by[n]) for n in (7, 8, 9)]
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


SEEDED = {
    ("GET", "/webhooks/api/v1/endpoints"),
    ("GET", "/webhooks/api/v1/events"),
    ("GET", "/webhooks/api/v1/events/{id}"),
    ("GET", "/schedules/api/v1/schedules"),
    ("GET", "/schedules/api/v1/runs"),
    ("GET", "/schedules/api/v1/runs/{run_id}"),
    ("GET", "/chat/v1/admin/rooms"),
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
    r = await rig.call("POST", relay_url("connector", "/rules"), json={"bogus": 1})
    assert r.status_code == 422
    assert fake.calls and r.json() == {"detail": [{"msg": r.json()["detail"][0]["msg"]}]}


async def test_nothing_is_logged_with_a_token_or_body(rig, caplog):
    canary = "canary-" + stdlib_secrets.token_hex(8)
    fake = rig.fakes["connector"]
    key = ("POST", "/chat/v1/admin/rules")
    fake.answers[key] = fu.success_sample(fake, key)
    body = fu.sample(DOCS["connector"], _body_schema(fake, key))
    first = next(iter(body))
    body[first] = canary if isinstance(body[first], str) else body[first]
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

    rig.app.state.admin_http = httpx.AsyncClient(transport=httpx.MockTransport(record))
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
    assert ids(r, field) == {str(ident(service, 7))}


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
        ("connector", "POST", "/rules"),
        ("connector", "DELETE", "/rules/{rule_id}"),
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

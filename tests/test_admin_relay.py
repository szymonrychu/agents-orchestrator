"""O9: the admin relay forwards superleader calls to webhook, schedule and connector upstreams.

The upstreams are `FakeUpstream`s held to the OpenAPI documents vendored in `tests/fakes` (each
`*_openapi.source` names the commit it was copied from), so the relay's idea of every route is
pinned to what the services really serve. No case needs Docker beyond the shared Postgres.
"""

from __future__ import annotations

import json
import re
import secrets as stdlib_secrets

import httpx
import pytest
from fakes import fake_upstream as fu
from fakes.fake_kube import FakeKube
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

    async def call(method, path, **kw):
        extra = kw.pop("headers", {})
        async with client_for(app, boss.leader_ip) as http:
            return await http.request(method, path, headers={**headers(boss.id), **extra}, **kw)

    r.call = call
    return r


def relay_url(service: str, template: str) -> str:
    return ADMIN_PREFIX + RELAY_PREFIX[service] + re.sub(r"\{[^}]+\}", "7", template)


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
        fake.answers[key] = fu.success_sample(fake, key)
        body = fu.sample(DOCS[name], _body_schema(fake, key)) if _has_body(fake, key) else None
        r = await rig.call(method, relay_url(name, template), json=body)
        assert r.status_code == status, (key, r.text)
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

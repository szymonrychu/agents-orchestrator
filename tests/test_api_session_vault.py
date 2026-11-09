"""O5: session create pins the preset's vault refs through the vault, stores the pins in
`secret_versions`, releases them when the create fails, and refuses refs anywhere but a preset."""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest
import pytest_asyncio
from fakes.fake_kube import FakeKube
from test_admin_session_patch import audits
from test_agent_session_messaging import alone
from test_api_agent import client_for as agent_client
from test_api_agent import headers
from test_api_agent import make_settings as agent_settings
from test_api_sessions import assert_error, client_for, make_settings
from test_session_spec_service import _seed

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import presets as presets_repo

C = "/api/v1/sessions"
ENTRY = "11111111-1111-4111-8111-111111111111"
NOTE = "22222222-2222-4222-8222-222222222222"
CANARY = "canary-value-must-never-show"


class FakeVault:
    """Pins `TOKEN` of ENTRY as a secret version and `NOTE` of NOTE as a readable value."""

    def __init__(self):
        self.pin_calls: list[dict] = []
        self.releases: list[str] = []
        self.down = False
        self.refuse: dict | None = None
        self.gate: asyncio.Event | None = None
        self.entered = asyncio.Event()
        self.after_pin = None

    async def handler(self, req: httpx.Request) -> httpx.Response:
        if req.method == "DELETE":
            self.releases.append(req.url.path.rsplit("/", 1)[1])
            return httpx.Response(204)
        if self.down:
            return httpx.Response(503, text="down")
        body = json.loads(req.content)
        self.pin_calls.append(body)
        self.entered.set()
        if self.gate is not None:
            await self.gate.wait()
        if self.refuse is not None:
            return httpx.Response(422, json={"error": self.refuse})
        pins, values = {}, {}
        for env, ref in body["refs"].items():
            if ref["entry"] == NOTE:
                values[env] = "plain"
            else:
                pins[env] = {
                    "secret": f"vault-{ref['entry']}-v1",
                    "key": ref["env"],
                    "version": 1,
                }
        if self.after_pin is not None:
            await self.after_pin()
        return httpx.Response(200, json={"pins": pins, "values": values})


@pytest.fixture
def vault():
    return FakeVault()


def build(dsn, pool, vault, **kw):
    settings = make_settings(dsn, vault_url="http://vault", vault_token="t", **kw)
    return create_app(settings, pool, vault_transport=httpx.MockTransport(vault.handler))


@pytest_asyncio.fixture
async def http(pool, dsn, vault):
    async with client_for(build(dsn, pool, vault)) as c:
        yield c


def ref(entry=ENTRY, env="TOKEN"):
    return {"vault": entry, "env": env}


async def make_preset(pool, name="p", env=None, **kw):
    spec = {"env": env if env is not None else {"X": ref(), "Y": ref(NOTE, "NOTE")}}
    async with pool.connection() as conn:
        return await presets_repo.create(conn, name, spec, **kw)


async def versions_of(pool, slug):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT id, secret_versions FROM sessions WHERE slug = %s", (slug,)
        )
        return await cur.fetchone()


async def count(pool) -> int:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT count(*) FROM sessions")
        return (await cur.fetchone())[0]


async def test_create_pins_and_stores(http, pool, vault):
    preset = await make_preset(pool, labels=["team-a"])
    r = await http.post(C, json={"preset_id": str(preset.id), "slug": "pinned"})
    assert r.status_code == 201, r.text
    sid, versions = await versions_of(pool, "pinned")
    assert str(sid) == r.json()["id"]
    assert versions == {
        "X": {"secret": f"vault-{ENTRY}-v1", "key": "TOKEN", "version": 1},
        "Y": {"value": "plain"},
    }
    (call,) = vault.pin_calls
    assert call["session_id"] == str(sid)
    assert call["refs"] == {
        "X": {"entry": ENTRY, "env": "TOKEN"},
        "Y": {"entry": NOTE, "env": "NOTE"},
    }
    assert call["preset_labels"] == ["project:default", "team-a"]
    assert vault.releases == []


async def test_generated_slug_reuses_the_pinned_id(http, pool, vault):
    preset = await make_preset(pool, "gen")
    r = await http.post(C, json={"preset_id": str(preset.id)})
    assert r.status_code == 201, r.text
    assert vault.pin_calls[0]["session_id"] == r.json()["id"]


async def test_vault_down_no_session_no_lock(http, pool, vault):
    refs = await make_preset(pool, "refs")
    plain = await make_preset(pool, "plain", env={"A": {"value": "1"}})
    vault.down = True
    r = await http.post(C, json={"preset_id": str(refs.id), "slug": "down"})
    assert_error(r, 503, "vault_unavailable")
    assert await count(pool) == 0

    vault.down = False
    vault.gate = asyncio.Event()
    slow = asyncio.create_task(http.post(C, json={"preset_id": str(refs.id), "slug": "slow"}))
    await asyncio.wait_for(vault.entered.wait(), 5)
    other = await asyncio.wait_for(http.post(C, json={"preset_id": str(plain.id)}), 5)
    assert other.status_code == 201, other.text
    vault.gate.set()
    assert (await slow).status_code == 201


async def test_vault_422_maps_through(http, pool, vault):
    preset = await make_preset(pool)
    vault.refuse = {
        "code": "vault_label_mismatch",
        "message": "labels do not match",
        "envs": ["X"],
    }
    r = await http.post(C, json={"preset_id": str(preset.id), "slug": "mismatch"})
    assert_error(r, 422, "vault_label_mismatch")
    assert r.json()["error"]["envs"] == ["X"]
    assert await count(pool) == 0


async def test_vault_invalid_request_is_unavailable(http, pool, vault):
    preset = await make_preset(pool)
    vault.refuse = {"code": "invalid_request", "message": "bad shape"}
    r = await http.post(C, json={"preset_id": str(preset.id), "slug": "bug"})
    assert_error(r, 503, "vault_unavailable")


async def test_release_on_failed_insert(pool, dsn, vault):
    app = build(dsn, pool, vault, max_sessions=1)
    preset = await make_preset(pool)
    async with client_for(app) as c:
        assert (await c.post(C, json={"slug": "full", "spec": {}})).status_code == 201
        r = await c.post(C, json={"preset_id": str(preset.id), "slug": "over"})
    assert_error(r, 409, "session_limit")
    assert vault.releases == [vault.pin_calls[0]["session_id"]]
    assert await count(pool) == 1


@pytest.mark.parametrize("change", ["refs", "labels"])
async def test_preset_changed_between_pin_and_insert(http, pool, vault, change):
    preset = await make_preset(pool)

    async def move():
        async with pool.connection() as conn:
            if change == "refs":
                await conn.execute(
                    "UPDATE presets SET spec = %s WHERE id = %s",
                    (json.dumps({"env": {"X": ref(ENTRY, "OTHER")}}), preset.id),
                )
            else:
                await conn.execute(
                    "UPDATE presets SET labels = '{team-b}' WHERE id = %s", (preset.id,)
                )

    vault.after_pin = move
    r = await http.post(C, json={"preset_id": str(preset.id), "slug": "moved"})
    assert_error(r, 409, "conflict")
    assert "retry" in r.json()["error"]["message"]
    assert vault.releases == [vault.pin_calls[0]["session_id"]]
    assert await count(pool) == 0


async def test_spec_body_with_ref_422(http, pool, vault):
    r = await http.post(C, json={"slug": "raw", "spec": {"env": {"X": ref(), "A": {"value": "1"}}}})
    assert_error(r, 422, "vault_ref_not_allowed")
    assert r.json()["error"]["envs"] == ["X"]
    assert vault.pin_calls == [] and await count(pool) == 0


async def test_preset_without_refs_never_calls_vault(http, pool, vault):
    preset = await make_preset(pool, env={"A": {"value": "1"}})
    r = await http.post(C, json={"preset_id": str(preset.id), "slug": "plain"})
    assert r.status_code == 201, r.text
    assert vault.pin_calls == [] and vault.releases == []
    assert (await versions_of(pool, "plain"))[1] is None


async def test_no_vault_configured_with_refs_is_unavailable(pool, dsn):
    preset = await make_preset(pool)
    async with client_for(create_app(make_settings(dsn), pool)) as c:
        r = await c.post(C, json={"preset_id": str(preset.id), "slug": "novault"})
    assert_error(r, 503, "vault_unavailable")


async def test_admin_create_pins_too(pool, dsn, vault):
    preset = await make_preset(pool, "adm", superleader_manageable=True)
    me = await alone(pool, "boss", superleader=True)
    settings = agent_settings(dsn).model_copy(
        update={"vault_url": "http://vault", "vault_token": "t"}
    )
    app = create_app(settings, pool, vault_transport=httpx.MockTransport(vault.handler))
    async with agent_client(app, me.leader_ip) as c:
        r = await c.post(
            f"{ADMIN_PREFIX}/sessions",
            headers=headers(me.id),
            json={"preset_id": str(preset.id), "slug": "child"},
        )
    assert r.status_code == 201, r.text
    assert (await versions_of(pool, "child"))[1]["Y"] == {"value": "plain"}


@pytest.mark.parametrize(
    "patch",
    [
        {"env": {"X": ref(NOTE, "NOTE")}},  # changed
        {"env": {"Z": ref()}},  # added
        {"env": {"X": None}},  # removed
    ],
)
async def test_session_patch_ref_change_422(pool, dsn, vault, patch):
    me = await alone(pool, "sp-ref")
    settings = agent_settings(dsn)
    await _seed(pool, settings, me, {"env": {"X": ref(), "A": {"value": "1"}}})
    app = create_app(settings, pool, kube=FakeKube(namespace="agents"))
    async with agent_client(app, me.leader_ip) as c:
        r = await c.patch(
            f"{ADMIN_PREFIX}/sessions/{me.slug}", headers=headers(me.id), json={"spec": patch}
        )
    assert_error(r, 422, "vault_ref_not_allowed")
    assert r.json()["error"]["envs"] == [next(iter(patch["env"]))]
    assert CANARY not in r.text
    assert [a["status"] for a in await audits(pool, me.id)] == [422]


async def test_session_patch_without_ref_change_passes(pool, dsn):
    me = await alone(pool, "sp-keep")
    settings = agent_settings(dsn)
    await _seed(pool, settings, me, {"env": {"X": ref(), "A": {"value": "1"}}})
    app = create_app(settings, pool, kube=FakeKube(namespace="agents"))
    async with agent_client(app, me.leader_ip) as c:
        r = await c.patch(
            f"{ADMIN_PREFIX}/sessions/{me.slug}",
            headers=headers(me.id),
            json={"spec": {"env": {"A": {"value": "2"}}}},
        )
    assert r.status_code == 200, r.text

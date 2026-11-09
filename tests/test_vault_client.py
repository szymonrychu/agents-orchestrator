"""The agents-vault service client: resolve, pin, release, entries."""

from __future__ import annotations

import json
import logging
import uuid
from types import SimpleNamespace

import httpx
import pytest

from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.vault_client import (
    Pin,
    Ref,
    VaultClient,
    VaultRefused,
    VaultUnavailable,
    effective_labels,
    vault_of,
)

ENTRY = uuid.UUID("11111111-1111-4111-8111-111111111111")
SESSION = uuid.UUID("22222222-2222-4222-8222-222222222222")
TOKEN = "vault-service-token"
REFS = {"GH": Ref(ENTRY, "TOKEN")}


def make(handler) -> VaultClient:
    return VaultClient("http://vault:8001", TOKEN, transport=httpx.MockTransport(handler))


async def test_resolve_posts_refs_and_maps_answer():
    seen = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["url"] = str(req.url)
        seen["auth"] = req.headers["Authorization"]
        seen["body"] = json.loads(req.content)
        return httpx.Response(
            200,
            json={
                "resolved": {
                    "GH": {
                        "entry": str(ENTRY),
                        "name": "github",
                        "env": "TOKEN",
                        "protected": True,
                        "labels": [],
                        "version": 1,
                        "matches": True,
                    }
                },
                "unknown": ["A"],
                "bad_env": ["B"],
                "mismatched": ["C"],
            },
        )

    out = await make(handler).resolve(REFS, ["project:x"])
    assert seen["url"] == "http://vault:8001/vault/v1/entries/resolve"
    assert seen["auth"] == f"Bearer {TOKEN}"
    assert seen["body"] == {
        "preset_labels": ["project:x"],
        "refs": {"GH": {"entry": str(ENTRY), "env": "TOKEN"}},
    }
    assert (out.unknown, out.bad_env, out.mismatched) == (["A"], ["B"], ["C"])
    assert out.names == {"GH": "github"}


async def test_pin_maps_pins_and_values():
    seen = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["url"] = str(req.url)
        seen["body"] = json.loads(req.content)
        return httpx.Response(
            200,
            json={
                "pins": {"GH": {"secret": "vault-abc-v1", "key": "TOKEN", "version": 1}},
                "values": {"MODE": "fast"},
            },
        )

    pins = await make(handler).pin(SESSION, REFS, ["a"])
    assert seen["url"] == "http://vault:8001/vault/v1/pins"
    assert seen["body"]["session_id"] == str(SESSION)
    assert seen["body"]["preset_labels"] == ["a"]
    assert pins["GH"].to_json() == {"secret": "vault-abc-v1", "key": "TOKEN", "version": 1}
    assert pins["MODE"].to_json() == {"value": "fast"}
    assert pins["MODE"] == Pin(None, None, None, "fast")


async def test_pin_label_mismatch_is_refused():
    def handler(req: httpx.Request) -> httpx.Response:
        body = {"error": {"code": "vault_label_mismatch", "message": "m", "envs": ["GH"]}}
        return httpx.Response(422, json=body)

    with pytest.raises(VaultRefused) as exc:
        await make(handler).pin(SESSION, REFS, [])
    assert exc.value.code == "vault_label_mismatch"
    assert exc.value.extra == {"envs": ["GH"]}


async def test_invalid_request_422_is_unavailable_and_logs_names_only(caplog):
    def handler(req: httpx.Request) -> httpx.Response:
        body = {"error": {"code": "invalid_request", "message": "secret-ish detail"}}
        return httpx.Response(422, json=body)

    caplog.set_level(logging.WARNING)
    with pytest.raises(VaultUnavailable):
        await make(handler).resolve(REFS, [])
    assert "secret-ish detail" not in caplog.text


def _timeout(req):
    raise httpx.ReadTimeout("slow", request=req)


def _connect(req):
    raise httpx.ConnectError("down", request=req)


@pytest.mark.parametrize(
    "handler",
    [
        _timeout,
        _connect,
        lambda r: httpx.Response(500, json={}),
        lambda r: httpx.Response(401, json={"error": {"code": "unauthorized", "message": "x"}}),
        lambda r: httpx.Response(403, json={}),
        lambda r: httpx.Response(200, content=b"<html>"),
        lambda r: httpx.Response(200, json={"unexpected": 1}),
    ],
    ids=["timeout", "connect", "500", "401", "403", "non-json", "bad-shape"],
)
async def test_failures_are_unavailable(handler):
    client = make(handler)
    with pytest.raises(VaultUnavailable):
        await client.resolve(REFS, [])
    with pytest.raises(VaultUnavailable):
        await client.pin(SESSION, REFS, [])
    with pytest.raises(VaultUnavailable):
        await client.entries(None)


async def test_release_deletes_pins():
    seen = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["m"], seen["u"] = req.method, str(req.url)
        return httpx.Response(204)

    await make(handler).release(SESSION)
    assert seen == {"m": "DELETE", "u": f"http://vault:8001/vault/v1/pins/{SESSION}"}


async def test_release_swallows_and_logs(caplog):
    caplog.set_level(logging.WARNING)
    await make(_connect).release(SESSION)
    assert f"vault pin release failed: session={SESSION} (VaultUnavailable)" in caplog.text


async def test_entries_passes_q():
    seen = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["url"] = str(req.url)
        return httpx.Response(200, json=[{"id": str(ENTRY), "name": "n"}])

    assert await make(handler).entries("git") == [{"id": str(ENTRY), "name": "n"}]
    assert seen["url"] == "http://vault:8001/vault/v1/entries?q=git"
    await make(handler).entries(None)
    assert seen["url"] == "http://vault:8001/vault/v1/entries"


def test_effective_labels_dedup_and_cut():
    p = SimpleNamespace(project="proj", labels=["a", "project:proj", "b", "a"])
    assert effective_labels(p) == ["project:proj", "a", "b"]
    many = SimpleNamespace(project="p", labels=[f"l{i}" for i in range(40)])
    out = effective_labels(many)
    assert len(out) == 20 and out[0] == "project:p"


def test_vault_of_unconfigured_is_503():
    req = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(vault=None)))
    with pytest.raises(ApiError) as exc:
        vault_of(req)
    assert (exc.value.status, exc.value.code) == (503, "vault_unavailable")
    client = object()
    req.app.state.vault = client
    assert vault_of(req) is client

"""Secret declaration in specs (superleader-crud D6): names only, refs by name, no values."""

from __future__ import annotations

import pytest
import pytest_asyncio
from pydantic import ValidationError
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app
from agents_orchestrator.api.sessions import check_spec
from agents_orchestrator.spec import SessionSpec

P = "/api/v1/presets"


def spec(**kw):
    return SessionSpec.model_validate(kw)


def hook(**kw):
    return {"wrapper": {"hooks": {"onStart": [{"type": "webhook", "url": "http://h/x", **kw}]}}}


def mcp_stdio(**kw):
    return {"wrapper": {"mcp_servers": {"s": {"type": "stdio", "command": "c", **kw}}}}


def mcp_remote(**kw):
    return {"wrapper": {"mcp_servers": {"s": {"type": "http", "url": "http://m/x", **kw}}}}


def test_secrets_accepted_and_sparse():
    assert SessionSpec().secrets is None
    assert "secrets" not in SessionSpec().model_dump(exclude_none=True)
    assert spec(secrets=["TOKEN_A", "B"]).secrets == ["TOKEN_A", "B"]
    assert spec(secrets=[]).secrets == []


@pytest.mark.parametrize(
    "names",
    [
        ["lower"],
        ["1BAD"],
        ["HAS-DASH"],
        [""],
        ["A", "A"],
        ["ORCH_X"],
        ["CCW_X"],
        ["CLAUDE_X"],
        ["ANTHROPIC_X"],
        ["POD_NAME"],
        [f"S{i}" for i in range(51)],
    ],
)
def test_secrets_rejected(names):
    with pytest.raises(ValidationError):
        spec(secrets=names)


def test_fifty_names_ok():
    assert len(spec(secrets=[f"S{i}" for i in range(50)]).secrets) == 50


def test_secret_name_colliding_with_env_key_rejected():
    with pytest.raises(ValidationError, match="env"):
        spec(secrets=["TOKEN"], env={"TOKEN": "x"})
    assert spec(secrets=["TOKEN"], env={"OTHER": "x"}).env == {"OTHER": "x"}


@pytest.mark.parametrize(
    "body",
    [
        hook(secret={"secret": "TOK"}),
        hook(headers={"Authorization": {"secret": "TOK"}, "X-Plain": "p"}),
        mcp_stdio(env={"KEY": {"secret": "TOK"}}),
        mcp_remote(headers={"Authorization": {"secret": "TOK"}}),
    ],
)
def test_declared_reference_accepted(body):
    assert spec(secrets=["TOK"], **body).secrets == ["TOK"]


@pytest.mark.parametrize(
    "body",
    [
        hook(secret={"secret": "TOK"}),
        hook(headers={"Authorization": {"secret": "TOK"}}),
        mcp_stdio(env={"KEY": {"secret": "TOK"}}),
        mcp_remote(headers={"Authorization": {"secret": "TOK"}}),
    ],
)
def test_undeclared_reference_rejected(body):
    with pytest.raises(ValidationError, match="TOK"):
        spec(**body)
    with pytest.raises(ValidationError, match="TOK"):
        spec(secrets=["OTHER"], **body)


def test_reference_shape_is_strict():
    with pytest.raises(ValidationError):
        spec(secrets=["TOK"], **hook(secret={"secret": "TOK", "value": "x"}))
    with pytest.raises(ValidationError):
        spec(secrets=["TOK"], **hook(secret={"name": "TOK"}))


def test_plain_strings_still_accepted_without_declaration():
    s = spec(**hook(secret="plain"), **{})
    assert s.wrapper.hooks["onStart"][0].secret == "plain"


def test_check_spec_validates_declared_spec():
    s = spec(secrets=["TOK"], **hook(secret={"secret": "TOK"}))
    assert check_spec(s, make_settings("postgresql://x")).secrets == ["TOK"]


@pytest_asyncio.fixture
async def http(pool, dsn):
    async with client_for(create_app(make_settings(dsn, max_replicas=4), pool)) as c:
        yield c


async def test_preset_create_routes_undeclared_to_422(http):
    r = await http.post(P, json={"name": "u", "spec": hook(secret={"secret": "TOK"})})
    assert_error(r, 422, "invalid_request")


async def test_preset_create_with_declared_secret(http):
    body = {"name": "d", "spec": {"secrets": ["TOK"], **hook(secret={"secret": "TOK"})}}
    r = await http.post(P, json=body)
    assert r.status_code == 201, r.text
    assert r.json()["spec"]["secrets"] == ["TOK"]

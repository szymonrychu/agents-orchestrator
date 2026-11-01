"""Env variables, GitLab CI style (superleader-crud D1, Q5): `{value, protected, expand}`."""

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


def var(value="v", **kw):
    return {"value": value, **kw}


def prot(**kw):
    return {"protected": True, **kw}


def hook(**kw):
    return {"wrapper": {"hooks": {"onStart": [{"type": "webhook", "url": "http://h/x", **kw}]}}}


def mcp_stdio(**kw):
    return {"wrapper": {"mcp_servers": {"s": {"type": "stdio", "command": "c", **kw}}}}


def mcp_remote(**kw):
    return {"wrapper": {"mcp_servers": {"s": {"type": "http", "url": "http://m/x", **kw}}}}


def test_entry_defaults_and_plain_views():
    s = spec(env={"A": var("1"), "B": var("$x", expand=True), "C": prot()})
    assert s.env["A"].protected is False and s.env["A"].expand is False
    assert s.env["B"].expand is True
    assert s.plain_env == {"A": "1", "B": "$x"}
    assert s.protected_env == ["C"]


def test_protected_entry_dumps_without_a_value():
    dumped = spec(env={"C": prot()}).model_dump(exclude_none=True)["env"]
    assert dumped == {"C": {"protected": True, "expand": False}}
    assert "value" not in spec(env={"C": prot()}).model_dump()["env"]["C"]


def test_plain_string_entry_is_refused_no_shim():
    with pytest.raises(ValidationError):
        spec(env={"A": "plain"})


def test_protected_entry_cannot_carry_a_value():
    with pytest.raises(ValidationError, match="no value"):
        spec(env={"A": prot(value="x")})


def test_unprotected_entry_needs_a_value():
    with pytest.raises(ValidationError, match="needs a value"):
        spec(env={"A": {"protected": False}})


def test_protected_with_expand_is_refused():
    with pytest.raises(ValidationError, match="ignores expand"):
        spec(env={"A": prot(expand=True)})


@pytest.mark.parametrize("bad", [{"value": "x", "protected": "yes"}, {"value": "x", "expand": 1}])
def test_flags_are_strict_booleans(bad):
    with pytest.raises(ValidationError):
        spec(env={"A": bad})


def test_unknown_entry_field_refused():
    with pytest.raises(ValidationError):
        spec(env={"A": {"value": "x", "masked": True}})


@pytest.mark.parametrize(
    "name",
    ["lower", "1BAD", "HAS-DASH", "", "ORCH_X", "CCW_X", "CLAUDE_X", "ANTHROPIC_X", "POD_NAME"]
    + ["X_PROXY", "X_BASE_URL"],
)
@pytest.mark.parametrize("entry", [var(), prot()])
def test_env_names_rejected(name, entry):
    with pytest.raises(ValidationError):
        spec(env={name: entry})


def test_at_most_fifty_protected():
    assert len(spec(env={f"S{i}": prot() for i in range(50)}).protected_env) == 50
    with pytest.raises(ValidationError, match="at most 50"):
        spec(env={f"S{i}": prot() for i in range(51)})
    plain = {f"P{i}": var() for i in range(60)}
    assert len(spec(env={**plain, **{f"S{i}": prot() for i in range(50)}}).env) == 110


@pytest.mark.parametrize("removed", [{"secrets": ["TOK"]}, {"secrets": []}])
def test_secrets_list_is_removed(removed):
    with pytest.raises(ValidationError):
        spec(**removed)


@pytest.mark.parametrize(
    "body",
    [
        hook(secret={"secret": "TOK"}),
        hook(headers={"Authorization": {"secret": "TOK"}}),
        mcp_stdio(env={"KEY": {"secret": "TOK"}}),
        mcp_remote(headers={"Authorization": {"secret": "TOK"}}),
    ],
)
def test_secret_refs_are_removed(body):
    with pytest.raises(ValidationError):
        spec(env={"TOK": prot()}, **body)


REFS = [
    hook(secret="${TOK}"),
    hook(headers={"Authorization": "Bearer ${TOK}", "X-Plain": "p"}),
    mcp_stdio(env={"KEY": "${TOK}"}),
    mcp_remote(headers={"Authorization": "Bearer ${TOK}"}),
]


@pytest.mark.parametrize("body", REFS)
@pytest.mark.parametrize("entry", [prot(), var("x")])
def test_reference_to_an_env_variable_accepted(body, entry):
    assert "TOK" in spec(env={"TOK": entry}, **body).env


@pytest.mark.parametrize("body", REFS)
def test_reference_without_an_env_variable_rejected(body):
    with pytest.raises(ValidationError, match="TOK"):
        spec(**body)
    with pytest.raises(ValidationError, match="TOK"):
        spec(env={"OTHER": var()}, **body)


def test_dollar_dollar_is_an_escape_not_a_reference():
    assert spec(**hook(secret="$${TOK}")).wrapper is not None
    assert spec(**hook(secret="cost $$5")).wrapper is not None
    with pytest.raises(ValidationError, match="TOK"):
        spec(**hook(secret="$$${TOK}"))


def test_reference_with_a_default_needs_no_variable():
    assert spec(**hook(secret="${TOK:-none}")).wrapper is not None


def test_platform_names_need_no_variable():
    assert spec(**hook(headers={"X": "${ORCH_SESSION_ID}", "Y": "${POD_NAME}"})).wrapper


def test_check_spec_validates_entries():
    s = spec(env={"TOK": prot()}, **hook(secret="${TOK}"))
    assert check_spec(s, make_settings("postgresql://x")).protected_env == ["TOK"]


@pytest_asyncio.fixture
async def http(pool, dsn):
    async with client_for(create_app(make_settings(dsn, max_replicas=4), pool)) as c:
        yield c


async def test_preset_create_with_unknown_reference_is_422(http):
    r = await http.post(P, json={"name": "u", "spec": hook(secret="${TOK}")})
    assert_error(r, 422, "invalid_request")


async def test_preset_create_with_plain_string_env_is_422(http):
    r = await http.post(P, json={"name": "s", "spec": {"env": {"A": "x"}}})
    assert_error(r, 422, "invalid_request")


async def test_preset_create_with_protected_variable(http):
    body = {"name": "d", "spec": {"env": {"TOK": prot()}, **hook(secret="${TOK}")}}
    r = await http.post(P, json=body)
    assert r.status_code == 201, r.text
    # reads show a protected variable as metadata (no value), here not yet set
    assert r.json()["spec"]["env"] == {"TOK": {"protected": True, "expand": False, "set": False}}

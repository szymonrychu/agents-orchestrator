"""Vault reference env entries: `{vault, env}` (central secrets vault, O2)."""

from __future__ import annotations

import uuid

import pytest
from pydantic import ValidationError

from agents_orchestrator.spec import VAULT_REFS_MAX, SessionSpec

U1 = "0b8f6f2e-3c1d-4f7e-9a55-1c2d3e4f5a6b"
U2 = "9d1c7a10-aaaa-4bbb-8ccc-0123456789ab"


def spec(**env):
    return SessionSpec.model_validate({"env": env})


def test_ref_parses_and_serialises_exactly():
    s = spec(TOKEN={"vault": U1, "env": "GH_TOKEN"})
    var = s.env["TOKEN"]
    assert var.is_ref
    assert var.vault == uuid.UUID(U1)
    assert var.env == "GH_TOKEN"
    assert s.model_dump(mode="json")["env"]["TOKEN"] == {"vault": U1, "env": "GH_TOKEN"}
    assert s.model_dump()["env"]["TOKEN"] == {"vault": U1, "env": "GH_TOKEN"}


def test_uppercase_uuid_serialises_canonical_lowercase():
    s = spec(TOKEN={"vault": U1.upper(), "env": "X"})
    assert s.model_dump(mode="json")["env"]["TOKEN"]["vault"] == U1


def test_plain_variable_is_not_a_ref():
    assert not spec(A={"value": "v"}).env["A"].is_ref


@pytest.mark.parametrize("extra", [{"value": "v"}, {"expand": True}])
def test_ref_with_value_or_expand_refused(extra):
    with pytest.raises(ValidationError, match="a vault reference carries only vault and env"):
        spec(TOKEN={"vault": U1, "env": "X", **extra})


def test_ref_with_protected_refused():
    with pytest.raises(ValidationError):
        spec(TOKEN={"vault": U1, "env": "X", "protected": True})


def test_env_without_vault_refused():
    with pytest.raises(ValidationError):
        spec(TOKEN={"env": "X"})


def test_vault_without_env_refused():
    with pytest.raises(ValidationError):
        spec(TOKEN={"vault": U1})


@pytest.mark.parametrize("bad", ["lower", "1ABC", "A-B", "", "A" * 129])
def test_ref_env_must_match_pattern(bad):
    with pytest.raises(ValidationError):
        spec(TOKEN={"vault": U1, "env": bad})


def test_ref_env_max_length_accepted():
    spec(TOKEN={"vault": U1, "env": "A" * 128})


def test_bad_vault_uuid_refused():
    with pytest.raises(ValidationError):
        spec(TOKEN={"vault": "nope", "env": "X"})


def test_too_many_refs_refused():
    env = {f"K{i}": {"vault": str(uuid.uuid4()), "env": "X"} for i in range(VAULT_REFS_MAX + 1)}
    with pytest.raises(ValidationError, match="env has 51 vault references, at most 50"):
        SessionSpec.model_validate({"env": env})


def test_max_refs_accepted():
    env = {f"K{i}": {"vault": str(uuid.uuid4()), "env": "X"} for i in range(VAULT_REFS_MAX)}
    assert len(SessionSpec.model_validate({"env": env}).vault_refs) == 50


def test_non_object_entry_message():
    with pytest.raises(
        ValidationError, match=r"env\.A must be an object \{value, expand\} or \{vault, env\}"
    ):
        SessionSpec.model_validate({"env": {"A": "x"}})


def test_plain_env_skips_refs_and_vault_refs_sorted():
    s = spec(
        B={"vault": U2, "env": "BB"},
        A={"vault": U1, "env": "AA"},
        P={"value": "v"},
    )
    assert s.plain_env == {"P": "v"}
    refs = s.vault_refs
    assert list(refs) == ["A", "B"]
    assert refs["A"].vault == uuid.UUID(U1)
    assert refs["A"].env == "AA"


def test_interpolation_may_reference_a_ref_key():
    s = SessionSpec.model_validate(
        {
            "env": {"TOKEN": {"vault": U1, "env": "X"}},
            "wrapper": {
                "mcp_servers": {
                    "s": {"type": "http", "url": "http://m", "headers": {"A": "${TOKEN}"}}
                }
            },
        }
    )
    assert "TOKEN" in s.vault_refs

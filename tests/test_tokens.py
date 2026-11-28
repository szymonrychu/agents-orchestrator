import dataclasses
import hashlib
import hmac
from uuid import UUID

import pytest

from agents_orchestrator.settings import Settings, SettingsError
from agents_orchestrator.tokens import SessionTokens, derive_tokens, tokens_for

pytestmark = pytest.mark.critical

KEY = b"k" * 32
CID = UUID("11111111-1111-1111-1111-111111111111")
OTHER = UUID("22222222-2222-2222-2222-222222222222")


def test_deterministic():
    assert derive_tokens(KEY, CID) == derive_tokens(KEY, CID)


def test_auth_and_agent_differ():
    t = derive_tokens(KEY, CID)
    assert t.auth != t.agent


def test_depends_on_session_and_master():
    t = derive_tokens(KEY, CID)
    assert derive_tokens(KEY, OTHER) != t
    assert derive_tokens(b"z" * 32, CID) != t


def test_length_at_least_16():
    t = derive_tokens(KEY, CID)
    assert len(t.auth) == len(t.agent) == 64


def test_repr_masks():
    t = derive_tokens(KEY, CID)
    for text in (repr(t), str(t), f"{t}"):
        assert t.auth not in text and t.agent not in text
        assert "***" in text


def test_tokens_for_uses_master_key(monkeypatch):
    monkeypatch.setenv("ORCH_DATABASE_URL", "postgresql://u:p@h/db")
    monkeypatch.setenv("ORCH_MASTER_KEY", "s" * 32)
    assert tokens_for(Settings(), CID) == derive_tokens(b"s" * 32, CID)
    monkeypatch.delenv("ORCH_MASTER_KEY")
    with pytest.raises(SettingsError):
        tokens_for(Settings(), CID)


def test_frozen():
    assert isinstance(derive_tokens(KEY, CID), SessionTokens)
    with pytest.raises(dataclasses.FrozenInstanceError):
        derive_tokens(KEY, CID).auth = "x"  # type: ignore[misc]


def test_known_answer_vectors():
    t = derive_tokens(KEY, CID)
    assert t.auth == "".join(
        ["f623e2ff1b6e6ee8", "981aaf0e5775fa00", "af60213c589bb49d", "6b402fea88d98c22"]
    )


def test_agent_token_purpose():
    t = derive_tokens(KEY, CID)
    assert t.agent != t.auth
    assert len(t.agent) == 64
    assert t.agent == derive_tokens(KEY, CID).agent
    assert derive_tokens(KEY, OTHER).agent != t.agent
    assert t.agent == hmac.new(KEY, f"agent:{CID}".encode(), hashlib.sha256).hexdigest()
    for text in (repr(t), str(t)):
        assert t.agent not in text
        assert "agent='***'" in text

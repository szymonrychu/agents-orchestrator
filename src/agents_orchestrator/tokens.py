"""Per-session token derivation: every process derives the same tokens from the master key."""

import hashlib
import hmac
from dataclasses import dataclass, field
from uuid import UUID

from agents_orchestrator.settings import Settings


@dataclass(frozen=True)
class SessionTokens:
    auth: str = field(repr=False)
    agent: str = field(repr=False)

    def __repr__(self) -> str:
        return "SessionTokens(auth='***', agent='***')"

    __str__ = __repr__


def _mac(master_key: bytes, purpose: str, session_id: UUID) -> str:
    return hmac.new(master_key, f"{purpose}:{session_id}".encode(), hashlib.sha256).hexdigest()


def derive_tokens(master_key: bytes, session_id: UUID) -> SessionTokens:
    return SessionTokens(
        auth=_mac(master_key, "auth", session_id),
        agent=_mac(master_key, "agent", session_id),
    )


def tokens_for(settings: Settings, session_id: UUID) -> SessionTokens:
    return derive_tokens(settings.require_master_key().encode(), session_id)

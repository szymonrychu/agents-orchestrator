"""Client of the agents-vault service listener: resolve refs, pin them to a session, release
the pins and list entries. Values never pass through here except readable ones the vault
returns for a pin; nothing is logged beyond env names and entry ids."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any

import httpx

from agents_orchestrator.api.errors import ApiError

log = logging.getLogger(__name__)

TIMEOUT_SECONDS = 10.0
LABELS_MAX = 20
#: Vault 422 codes that are the caller's refs to fix; any other 422 is our contract bug.
REFUSAL_CODES = ("vault_entry_unknown", "vault_env_unknown", "vault_label_mismatch")
#: The only extra keys of a refusal that reach the caller (env names and entry ids).
REFUSAL_EXTRA = ("envs", "entries")


class VaultUnavailable(Exception):
    """The vault cannot answer (unreachable, timeout, 5xx, 401/403, bad body, contract bug)."""


class VaultShape(VaultUnavailable):
    """The vault answered 2xx with a body of the wrong shape."""


class VaultRefused(Exception):
    """The vault refused the refs with a 422; `code` and `extra` pass through to the caller."""

    def __init__(self, code: str, extra: dict[str, Any]):
        super().__init__(code)
        self.code = code
        self.extra = extra


@dataclass(frozen=True)
class Ref:
    entry: uuid.UUID
    env: str

    def to_json(self) -> dict[str, str]:
        return {"entry": str(self.entry), "env": self.env}


@dataclass(frozen=True)
class Resolved:
    unknown: list[str]
    bad_env: list[str]
    mismatched: list[str]
    names: dict[str, str]  # preset env -> entry name


@dataclass(frozen=True)
class Pin:
    secret: str | None
    key: str | None
    version: int | None
    value: str | None

    def to_json(self) -> dict[str, Any]:
        if self.value is not None:
            return {"value": self.value}
        return {"secret": self.secret, "key": self.key, "version": self.version}


def effective_labels(preset: Any) -> list[str]:
    """The labels sent to the vault: one project label, then the preset's others, at most 20."""
    labels = [
        f"project:{preset.project}",
        *(x for x in preset.labels if not x.startswith("project:")),
    ]
    return list(dict.fromkeys(labels))[:LABELS_MAX]


def _refs_json(refs: dict[str, Ref]) -> dict[str, dict[str, str]]:
    return {env: ref.to_json() for env, ref in refs.items()}


class VaultClient:
    def __init__(
        self,
        url: str,
        token: str,
        timeout: float = TIMEOUT_SECONDS,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self._client = httpx.AsyncClient(
            base_url=url.rstrip("/"),
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _call(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """One request; raises VaultUnavailable for anything but a success or a 422 refusal."""
        try:
            resp = await self._client.request(method, path, **kwargs)
        except httpx.HTTPError as exc:
            raise VaultUnavailable(type(exc).__name__) from None
        if resp.status_code == 422:
            self._refuse(resp)
        if resp.status_code >= 300:
            raise VaultUnavailable(f"status {resp.status_code}")
        return resp

    @staticmethod
    def _refuse(resp: httpx.Response) -> None:
        try:
            error = resp.json()["error"]
            code = error["code"]
        except (ValueError, KeyError, TypeError):
            raise VaultUnavailable("status 422") from None
        if code not in REFUSAL_CODES:
            # a request the vault cannot parse is our bug, never the caller's: names only
            log.warning("vault refused the request shape: code=%s", code)
            raise VaultUnavailable("contract mismatch")
        extra = {k: v for k, v in error.items() if k in REFUSAL_EXTRA}
        raise VaultRefused(code, extra)

    async def _json(self, method: str, path: str, **kwargs: Any) -> Any:
        resp = await self._call(method, path, **kwargs)
        try:
            return resp.json()
        except ValueError:
            raise VaultUnavailable("non-JSON answer") from None

    async def resolve(self, refs: dict[str, Ref], labels: list[str]) -> Resolved:
        body = {"preset_labels": labels, "refs": _refs_json(refs)}
        data = await self._json("POST", "/vault/v1/entries/resolve", json=body)
        try:
            return Resolved(
                unknown=list(data["unknown"]),
                bad_env=list(data["bad_env"]),
                mismatched=list(data["mismatched"]),
                names={env: r["name"] for env, r in data["resolved"].items()},
            )
        except (KeyError, TypeError, AttributeError):
            raise VaultUnavailable("bad resolve answer") from None

    async def pin(
        self, session_id: uuid.UUID, refs: dict[str, Ref], labels: list[str]
    ) -> dict[str, Pin]:
        body = {"session_id": str(session_id), "preset_labels": labels, "refs": _refs_json(refs)}
        data = await self._json("POST", "/vault/v1/pins", json=body)
        try:
            out = {
                env: Pin(p["secret"], p["key"], p["version"], None)
                for env, p in data["pins"].items()
            }
            out.update({env: Pin(None, None, None, str(v)) for env, v in data["values"].items()})
        except (KeyError, TypeError, AttributeError):
            raise VaultUnavailable("bad pins answer") from None
        return out

    async def release(self, session_id: uuid.UUID) -> None:
        """Best effort: the vault sweeper releases unclaimed pins after its grace."""
        try:
            await self._call("DELETE", f"/vault/v1/pins/{session_id}")
        except (VaultUnavailable, VaultRefused) as exc:
            log.warning("vault pin release failed: session=%s (%s)", session_id, type(exc).__name__)

    async def entries(self, q: str | None) -> list[dict]:
        data = await self._json("GET", "/vault/v1/entries", params={"q": q} if q else None)
        if not isinstance(data, list):
            raise VaultShape("bad entries answer")
        return data


def vault_of(request: Any) -> VaultClient:
    return vault_of_client(request.app.state.vault)


def vault_of_client(client: VaultClient | None) -> VaultClient:
    if client is None:
        raise ApiError(503, "vault_unavailable", "agents-vault is not configured")
    return client

"""Shared vault reference checks for preset saves: the check through the vault, the diff between
two saves and the events that record it. Names and entry ids only, never a value. Whether a
label matches is decided by the vault, never here."""

from __future__ import annotations

import uuid
from typing import Any

import psycopg

from agents_orchestrator.api import vault_client
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.vault_client import VaultClient, VaultRefused, VaultUnavailable
from agents_orchestrator.db import presets as repo
from agents_orchestrator.spec import Ref, SessionSpec

#: An attached or detached ref as `(preset env, entry id, entry env)`.
Change = tuple[str, str, str]


def unavailable() -> ApiError:
    return ApiError(503, "vault_unavailable", "agents-vault cannot answer; try again later")


def refs_in(spec: dict[str, Any]) -> dict[str, Ref]:
    """The vault references of a stored spec dict by preset env key; malformed entries are
    skipped (a stored spec is not trusted to be valid)."""
    env = spec.get("env")
    out: dict[str, Ref] = {}
    for key, entry in (env if isinstance(env, dict) else {}).items():
        if isinstance(entry, dict) and "vault" in entry and "env" in entry:
            try:
                out[key] = Ref(uuid.UUID(str(entry["vault"])), str(entry["env"]))
            except ValueError:
                continue
    return out


def refused(exc: VaultRefused) -> ApiError:
    """The vault's 422 as ours: same code, same `envs` and `entries`."""
    return ApiError(
        422, exc.code, f"the vault refused the references ({exc.code})", extra=exc.extra
    )


async def pin_refs(
    vault: VaultClient, session_id: uuid.UUID, refs: dict[str, Ref], labels: list[str]
) -> dict[str, vault_client.Pin]:
    """Pin the refs of a session about to be created; 422 as the vault refuses, 503
    `vault_unavailable`. The vault, not this code, decides whether labels match."""
    wire = {env: vault_client.Ref(ref.vault, ref.env) for env, ref in refs.items()}
    try:
        return await vault.pin(session_id, wire, labels)
    except VaultUnavailable:
        raise unavailable() from None
    except VaultRefused as exc:
        raise refused(exc) from None


async def check_refs(vault: VaultClient, spec: SessionSpec, labels: list[str]) -> dict[str, str]:
    """Ask the vault whether the spec's refs exist, name real envs and match `labels`; returns
    preset env -> entry name. 422 `vault_entry_unknown`, `vault_env_unknown`,
    `vault_label_mismatch` in that order, 503 `vault_unavailable`. No call without refs."""
    refs = spec.vault_refs
    if not refs:
        return {}
    wire = {env: vault_client.Ref(ref.vault, ref.env) for env, ref in refs.items()}
    try:
        got = await vault.resolve(wire, labels)
    except VaultUnavailable:
        raise unavailable() from None
    except VaultRefused as exc:
        raise refused(exc) from None
    if got.unknown:
        raise ApiError(
            422,
            "vault_entry_unknown",
            f"unknown vault entry for: {', '.join(got.unknown)}",
            extra={
                "envs": got.unknown,
                "entries": sorted({str(refs[e].vault) for e in got.unknown if e in refs}),
            },
        )
    if got.bad_env:
        raise ApiError(
            422,
            "vault_env_unknown",
            f"the entry has no such env for: {', '.join(got.bad_env)}",
            extra={"envs": got.bad_env},
        )
    if got.mismatched:
        raise ApiError(
            422,
            "vault_label_mismatch",
            f"the entry labels do not match the preset for: {', '.join(got.mismatched)}",
            extra={"envs": got.mismatched},
        )
    return got.names


def ref_diff(old: dict[str, Ref], new: dict[str, Ref]) -> tuple[list[Change], list[Change]]:
    """(attached, detached) between two saves, sorted by env; a ref re-pointed to another entry
    or entry env is a detach of the old one and an attach of the new one."""
    attached = [(e, str(r.vault), r.env) for e, r in sorted(new.items()) if old.get(e) != r]
    detached = [(e, str(r.vault), r.env) for e, r in sorted(old.items()) if new.get(e) != r]
    return attached, detached


async def record_ref_events(
    conn: psycopg.AsyncConnection,
    preset_id: uuid.UUID,
    actor: str,
    attached: list[Change],
    detached: list[Change],
) -> None:
    """One event row per attached and per detached ref; `actor` is `user` or a session slug."""
    for kind, changes in (("preset_vault_attached", attached), ("preset_vault_detached", detached)):
        for env, entry, entry_env in changes:
            await repo.insert_vault_event(
                conn,
                kind,
                actor,
                {"preset_id": str(preset_id), "env": env, "entry": entry, "entry_env": entry_env},
            )

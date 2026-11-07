"""Change proposals service (preset-change-proposals 3.4): create, revise, withdraw and read.

Stage A handles `kind: "preset"` only; the routes and gates live in `proposal_routes`, the human
review and apply come later. A proposal is never applied here and never runs the agent guards
(human-only, privileged): it may carry any path, security, grant or not, for a human to decide.

A protected value arrives as a `SecretStr` in `secrets` and goes only into a staged Secret's
`stringData` (`stage_secret`); no row, view, error, log line or audit entry ever holds it. The
staged Secrets are written before the transaction commits and deleted again when it rolls back.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from fastapi import Request
from pydantic import SecretStr

from agents_orchestrator.api import admin, schemas, secret_store, session_spec
from agents_orchestrator.api import presets as presets_api
from agents_orchestrator.api.errors import ApiError, not_found
from agents_orchestrator.api.spec_edit import merged_spec_in, note_fields
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import proposals as repo
from agents_orchestrator.db.sessions import Session
from agents_orchestrator.preset_policy import changed_paths, classify
from agents_orchestrator.secret_view import EnvMeta, redact_spec_dict
from agents_orchestrator.settings import Settings

MAX_SECRETS = 20

STATUSES: dict[str, tuple[str, ...]] = {
    "active": repo.ACTIVE,
    "open": ("open",),
    "closed": repo.TERMINAL,
    "all": repo.ACTIVE + repo.TERMINAL,
}

# what a protected env entry keeps: in a revision's base (plan 3.2) and in an item's `after`
_BASE_KEYS = ("protected", "expand", "set", "version")
_AFTER_KEYS = ("protected", "expand")


@dataclass(frozen=True)
class _Plan:
    """One revision as it will be stored: redacted base, fingerprint and items."""

    base: dict[str, Any]
    fingerprint: str
    items: list[repo.Item]


def fingerprint_preset(preset: presets_repo.Preset, versions: dict[str, int]) -> str:
    """The preset as a proposal revision was based on it (Q6 B): every row write bumps
    `updated_at`, and a value rotation bumps a live secret version without touching the row."""
    doc = {"updated_at": preset.updated_at.isoformat(), "secrets": versions}
    return hashlib.sha256(json.dumps(doc, sort_keys=True).encode()).hexdigest()


def _not_open() -> ApiError:
    return ApiError(409, "not_open", "the proposal is closed")


def _unsupported(kind: str) -> ApiError:
    return ApiError(422, "invalid_request", f"kind {kind} not supported yet")


def _redacted(
    spec: Mapping[str, Any], metas: Mapping[str, EnvMeta], keys: tuple[str, ...]
) -> dict[str, Any]:
    """`redact_spec_dict`, each protected entry cut to `keys` (JSON-safe, no timestamps)."""
    out = redact_spec_dict(spec, metas)
    env = out.get("env")
    if isinstance(env, dict):
        out["env"] = {
            name: {k: e[k] for k in keys if k in e}
            if isinstance(e, dict) and e.get("protected")
            else e
            for name, e in env.items()
        }
    return out


def _at(spec: Mapping[str, Any], path: str) -> Any:
    """The value at a `changed_paths` path (`env.X`, `wrapper.X` one level deep); None = absent."""
    head, _, sub = path.partition(".")
    value = spec.get(head)
    if not sub:
        return value
    return value.get(sub) if isinstance(value, dict) else None


def _check_values(secrets: Mapping[str, SecretStr]) -> None:
    if len(secrets) > MAX_SECRETS:
        raise ApiError(
            422, "invalid_request", f"at most {MAX_SECRETS} protected values per proposal"
        )
    for value in secrets.values():
        presets_api.check_secret_size(value)


async def _metas(conn, preset_id: uuid.UUID) -> dict[str, EnvMeta]:
    found = (await secret_store.list_metas(conn, [preset_id])).get(preset_id, [])
    return {m.name: EnvMeta(m.version, m.updated_at) for m in found}


async def _locked(conn, preset_id: uuid.UUID) -> presets_repo.Preset:
    """The target preset under a share lock (it cannot change until the commit); one the
    superleader may not manage is as missing as an unknown."""
    preset = await presets_repo.get_for_share(conn, preset_id)
    if preset is None or not preset.superleader_manageable:
        raise not_found("preset")
    return preset


async def _plan(
    conn,
    settings: Settings,
    preset: presets_repo.Preset,
    patch: dict[str, Any],
    secrets: Mapping[str, SecretStr],
) -> _Plan:
    """Merge, check and diff the patch against the locked preset, as the PATCH does, without the
    agent guards. Items carry redacted values from the stored and the merged spec only."""
    _, spec_in = merged_spec_in(preset.spec, patch)  # 422 secret_in_spec for a value in `patch`
    spec, _ = spec_in.split()  # no values: secret_in_patch refused every protected one
    for name in sorted(secrets):
        entry = spec.env.get(name)
        if entry is None or not entry.protected:
            raise ApiError(
                422, "invalid_request", f"{name} is not a protected variable of the proposed spec"
            )
    new = await presets_api.validated_spec(spec, settings, preset.spec, preset.id)
    paths = sorted(set(changed_paths(preset.spec, new)) | {f"env.{n}" for n in secrets})
    if not paths:
        raise ApiError(422, "nothing_changed", "the patch changes nothing")
    metas = await _metas(conn, preset.id)
    base = _redacted(preset.spec, metas, _BASE_KEYS)
    after = _redacted(new, {}, _AFTER_KEYS)
    classes = classify(paths)
    items = []
    for path in paths:
        name = path.removeprefix("env.") if path.startswith("env.") else None
        secret = None
        if name is not None and name in secrets:
            secret = "changed" if name in metas else "set"
        items.append(
            repo.Item(
                path=path,
                before=_at(base, path),
                after=_at(after, path),
                security=classes[path][0],
                grant=classes[path][1],
                secret=secret,
            )
        )
    versions = {n: m.version for n, m in metas.items()}
    return _Plan(base, fingerprint_preset(preset, versions), items)


async def _stage(
    request: Request,
    conn,
    settings: Settings,
    preset_id: uuid.UUID,
    secrets: Mapping[str, SecretStr],
    proposal_id: int,
    revision: int,
    staged: list[str],
) -> None:
    """Stage each value in the caller's transaction; `staged` collects the Secret names written
    so far, for the caller to delete when the transaction rolls back."""
    if not secrets:
        return
    kube = secret_store.kube_of(request)
    for name in sorted(secrets):  # one lock order across concurrent proposals
        staged.append(
            await secret_store.stage_secret(
                conn,
                kube,
                settings.namespace,
                preset_id,
                name,
                secrets[name],
                proposal_id=proposal_id,
                revision=revision,
            )
        )


async def _rolled_back(request: Request, settings: Settings, staged: list[str]) -> None:
    """The transaction did not commit: delete the staged Secrets `release` would never find."""
    if staged:
        await presets_api._forget(secret_store.kube_of(request), settings.namespace, staged)


async def create(
    request: Request,
    conn,
    settings: Settings,
    body: schemas.ProposalIn,
    *,
    caller: Session,
    superleader: bool,
) -> tuple[schemas.ProposalView, bool]:
    """File a proposal; the bool is False on a `client_key` replay (nothing staged again)."""
    admin.touched(request, *body.secrets)
    if body.client_key is not None:
        first = await repo.by_client_key(conn, caller.id, body.client_key)
        if first is not None:
            return await get_view(conn, settings, first, proposer=caller.id), False
    if body.kind != "preset":
        raise _unsupported(body.kind)
    if not superleader:
        raise session_spec.not_superleader()
    _check_values(body.secrets)
    target = await admin._manageable_preset(conn, body.target)
    staged: list[str] = []
    try:
        async with conn.transaction():
            preset = await _locked(conn, target.id)
            plan = await _plan(conn, settings, preset, body.patch, body.secrets)
            if await repo.count_active(conn, caller.id) >= settings.proposals_open_max:
                raise ApiError(
                    409,
                    "too_many_proposals",
                    f"at most {settings.proposals_open_max} open proposals per session;"
                    " withdraw one first",
                )
            proposal_id = await repo.insert(
                conn,
                kind="preset",
                target_id=str(preset.id),
                target_name=preset.name,
                proposer_session=caller.id,
                proposer_slug=caller.slug,
                proposer_superleader=superleader,
                client_key=body.client_key,
                reason=body.reason,
                base=plan.base,
                fingerprint=plan.fingerprint,
                items=plan.items,
                ttl_seconds=settings.proposal_ttl_seconds,
            )
            await _stage(request, conn, settings, preset.id, body.secrets, proposal_id, 1, staged)
    except repo.ProposalExists as exc:  # a concurrent replay won the insert; nothing staged
        return await get_view(conn, settings, exc.proposal_id, proposer=caller.id), False
    except secret_store.SecretStoreError:
        await _rolled_back(request, settings, staged)
        raise presets_api._store_failed() from None
    except BaseException:
        await _rolled_back(request, settings, staged)
        raise
    note_fields(request, [i.path for i in plan.items])
    return await get_view(conn, settings, proposal_id, proposer=caller.id), True


async def revise(
    request: Request,
    conn,
    settings: Settings,
    proposal_id: int,
    body: schemas.ProposalRevise,
    *,
    caller: Session,
) -> schemas.ProposalView:
    """A new revision against the target read now (D2): re-based, open, expiry reset; the old
    revision's staged values are released after the commit (D4)."""
    admin.touched(request, *body.secrets)
    _check_values(body.secrets)
    staged: list[str] = []
    try:
        async with conn.transaction():
            current = await repo.lock(conn, proposal_id)
            if current is None or current.proposer_session != caller.id:
                raise not_found("proposal")
            if current.status not in repo.ACTIVE:
                raise _not_open()
            if current.kind != "preset":
                raise _unsupported(current.kind)
            preset = await _locked(conn, uuid.UUID(current.target_id))
            plan = await _plan(conn, settings, preset, body.patch, body.secrets)
            previous = current.revisions[0]  # `lock` loads the current revision only
            revision = await repo.add_revision(
                conn,
                proposal_id,
                from_states=repo.ACTIVE,
                reason=body.reason if body.reason is not None else previous.reason,
                base=plan.base,
                fingerprint=plan.fingerprint,
                items=plan.items,
                ttl_seconds=settings.proposal_ttl_seconds,
            )
            await _stage(
                request, conn, settings, preset.id, body.secrets, proposal_id, revision, staged
            )
    except secret_store.SecretStoreError:
        await _rolled_back(request, settings, staged)
        raise presets_api._store_failed() from None
    except BaseException:
        await _rolled_back(request, settings, staged)
        raise
    note_fields(request, [i.path for i in plan.items])
    if any(i.secret for i in previous.items):
        await secret_store.release(
            conn,
            secret_store.kube_of(request),
            settings.namespace,
            proposal_id=proposal_id,
            revision=previous.revision,
        )
    return await get_view(conn, settings, proposal_id, proposer=caller.id)


async def withdraw(
    request: Request, conn, settings: Settings, proposal_id: int, *, caller: Session
) -> schemas.ProposalView:
    """The proposer closes its own proposal; every staged value is released after the commit."""
    found = await repo.get(conn, proposal_id, caller.id)
    if found is None:
        raise not_found("proposal")
    try:
        await repo.transition(
            conn, proposal_id, from_states=repo.ACTIVE, to="withdrawn", outcome="withdrawn"
        )
    except repo.NotInState:
        raise _not_open() from None
    if any(i.secret for r in found.revisions for i in r.items):
        await secret_store.release(
            conn, secret_store.kube_of(request), settings.namespace, proposal_id=proposal_id
        )
    return await get_view(conn, settings, proposal_id, proposer=caller.id)


async def _stale(conn, proposal: repo.Proposal) -> bool:
    """Read-only (never changes state): the live target differs from the current revision's."""
    if proposal.status not in repo.ACTIVE or proposal.kind != "preset":
        return False
    current = next(r for r in proposal.revisions if r.revision == proposal.revision)
    preset = await presets_repo.get(conn, uuid.UUID(proposal.target_id))
    if preset is None:
        return True
    versions = {n: m.version for n, m in (await _metas(conn, preset.id)).items()}
    return fingerprint_preset(preset, versions) != current.fingerprint


async def _view(conn, proposal: repo.Proposal) -> schemas.ProposalView:
    """The view: no client_key, no base or fingerprint (values stay redacted either way)."""
    data = {
        name: getattr(proposal, name)
        for name in schemas.ProposalView.model_fields
        if name not in ("stale", "revisions")
    }
    return schemas.ProposalView.model_validate(
        {
            **data,
            "stale": await _stale(conn, proposal),
            "revisions": [
                schemas.ProposalRevisionView.model_validate(r, from_attributes=True)
                for r in proposal.revisions
            ],
        }
    )


async def get_view(
    conn, settings: Settings, proposal_id: int, *, proposer: uuid.UUID | None
) -> schemas.ProposalView:
    """One proposal with every revision; with `proposer`, only that session's (else 404)."""
    found = await repo.get(conn, proposal_id, proposer)
    if found is None:
        raise not_found("proposal")
    return await _view(conn, found)


async def list_views(
    conn, settings: Settings, *, proposer: uuid.UUID | None, status: str, limit: int
) -> list[schemas.ProposalView]:
    """Newest first, the current revision only; `status` is active, open, closed or all."""
    statuses = STATUSES.get(status)
    if statuses is None:
        raise ApiError(422, "invalid_request", f"status must be one of: {', '.join(STATUSES)}")
    found = await repo.list_for(conn, proposer_session=proposer, statuses=statuses, limit=limit)
    return [await _view(conn, p) for p in found]

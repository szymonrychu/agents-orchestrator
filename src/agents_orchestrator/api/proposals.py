"""Change proposals service (preset-change-proposals 3.4, 3.5): create, revise, withdraw, read
and the human review that applies the approved items.

It handles `kind: "preset"`, `kind: "schedule"` (stage B) and `kind: "session"` (stage C); the
routes and gates live in `proposal_routes`. A proposal never runs the agent guards (human-only,
privileged): it may carry any path, security, grant or not, for a human to decide; only `review`
(a human) writes the target.

A schedule lives in agents-schedule, so there is no shared transaction: its fingerprint is the
upstream `updated_at` exactly as the schedule view spells it, and the review applies with `PUT`
and `If-Match: "<that value>"` (412 there is stale here). Views never call the upstream, so a
schedule proposal reads `stale: false` until a review compares.

A session's fingerprint is its stored spec plus `secret_versions`; its protected values are staged
in its preset's Secret family and bound into `secret_versions` by the review, which writes through
`db.sessions.replace_spec` (generation + 1). Lock order of that apply: the proposal row, the preset
FOR SHARE, the variable locks (`bind_staged`), the session row (`set_secret` takes the preset FOR
UPDATE and then the variable lock, so any other order can deadlock with a rotation).

A protected value arrives as a `SecretStr` in `secrets` and goes only into a staged Secret's
`stringData` (`stage_secret`); no row, view, error, log line or audit entry ever holds it. The
staged Secrets are written before the transaction commits and deleted again when it rolls back.

Every write runs in a real transaction (the connection is idle when it starts, `_idle`), so the
work after it, releasing a superseded revision's staged Secrets, runs after the commit: a request
that fails late never deletes the Secret of a revision that is still current.
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import uuid
from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from typing import Any

import psycopg
from fastapi import Request
from fastapi.exceptions import RequestValidationError
from pydantic import SecretStr, ValidationError

from agents_orchestrator.api import admin, admin_relay, schemas, secret_store, session_spec
from agents_orchestrator.api import forward as forward_core
from agents_orchestrator.api import presets as presets_api
from agents_orchestrator.api.errors import ApiError, not_found
from agents_orchestrator.api.sessions import PLACE_ATTEMPTS, _secret_unset, hold_project
from agents_orchestrator.api.spec_edit import merged_spec_in, note_fields
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import proposals as repo
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.db.queue import SessionBeingDeleted, enqueue_message
from agents_orchestrator.db.sessions import Session
from agents_orchestrator.preset_policy import (
    changed_paths,
    classify,
    classify_schedule,
    fixed_changes,
    merge_patch,
)
from agents_orchestrator.secret_view import EnvMeta, redact_spec_dict, session_metas
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec, SessionSpecIn

log = logging.getLogger(__name__)

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
SCHEDULE_FIELDS = tuple(schemas.ScheduleIn.model_fields)


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


def fingerprint_session(stored: Mapping[str, Any], versions: Mapping[str, str]) -> str:
    """The session as a proposal revision was based on it (Q6 B): the stored spec JSONB and the
    bound Secret names, canonical JSON; any spec write counts, a scale included."""
    doc = {"spec": stored, "secret_versions": versions}
    canonical = json.dumps(doc, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _not_open() -> ApiError:
    return ApiError(409, "not_open", "the proposal is closed")


def _deleting() -> ApiError:
    return ApiError(409, "session_deleting", "the session is being discarded")


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


async def _locked(conn, preset_id: uuid.UUID, *, hidden_as: str = "preset") -> presets_repo.Preset:
    """The target preset under a share lock (it cannot change until the commit); one deleted or
    one the superleader may not manage is as missing as an unknown (`hidden_as` names what the
    404 says is missing)."""
    preset = await presets_repo.get_for_share(conn, preset_id)
    if preset is None or not preset.superleader_manageable:
        raise not_found(hidden_as)
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
    spec = _merged(preset.spec, patch, secrets)
    new = await presets_api.validated_spec(spec, settings, preset.spec, preset.id)
    metas = await _metas(conn, preset.id)
    base, items = _items(preset.spec, new, metas, secrets, existing=metas)
    versions = {n: m.version for n, m in metas.items()}
    return _Plan(base, fingerprint_preset(preset, versions), items)


def _merged(
    stored: dict[str, Any], patch: dict[str, Any], secrets: Mapping[str, SecretStr]
) -> SessionSpec:
    """The stored spec with the patch merged in (422 `secret_in_spec` for a value in `patch`),
    parsed; each staged value must name a protected variable of it."""
    _, spec_in = merged_spec_in(stored, patch)
    spec, _ = spec_in.split()  # no values: secret_in_patch refused every protected one
    for name in sorted(secrets):
        entry = spec.env.get(name)
        if entry is None or not entry.protected:
            raise ApiError(
                422, "invalid_request", f"{name} is not a protected variable of the proposed spec"
            )
    return spec


def _items(
    stored: Mapping[str, Any],
    new: Mapping[str, Any],
    metas: Mapping[str, EnvMeta],
    secrets: Mapping[str, SecretStr],
    *,
    existing: Collection[str],
) -> tuple[dict[str, Any], list[repo.Item]]:
    """The redacted base and one item per changed path plus one per staged value (`existing`
    names the variables that already have a value: their item is `changed`, else `set`);
    values come from the stored and the merged spec only, redacted."""
    paths = sorted(set(changed_paths(stored, new)) | {f"env.{n}" for n in secrets})
    if not paths:
        raise ApiError(422, "nothing_changed", "the patch changes nothing")
    base = _redacted(stored, metas, _BASE_KEYS)
    after = _redacted(new, {}, _AFTER_KEYS)
    classes = classify(paths)
    items = []
    for path in paths:
        name = path.removeprefix("env.") if path.startswith("env.") else None
        secret = None
        if name is not None and name in secrets:
            secret = "changed" if name in existing else "set"
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
    return base, items


async def _session_plan(
    settings: Settings,
    session: Session,
    stored: dict[str, Any],
    patch: dict[str, Any],
    secrets: Mapping[str, SecretStr],
) -> _Plan:
    """`_plan` for a live session: merged and checked in the session PATCH's order (fixed fields,
    then `check_spec`, then every protected variable with a value bound or staged), no agent
    guard."""
    spec = _merged(stored, patch, secrets)
    new = spec.model_dump(mode="json", exclude_unset=True)
    if fixed := fixed_changes(stored, new):
        raise session_spec.fixed_refused(fixed)
    await session_spec.validated(spec, settings, session)
    protected = {n for n, e in spec.env.items() if e.protected}
    if unset := sorted(protected - set(session.secret_versions) - set(secrets)):
        raise _secret_unset(unset)
    metas = session_metas(session.secret_versions, session.created_at)
    base, items = _items(stored, new, metas, secrets, existing=session.secret_versions)
    return _Plan(base, fingerprint_session(stored, session.secret_versions), items)


async def live_session(conn, session_id: uuid.UUID) -> tuple[Session, dict[str, Any]] | None:
    """The session and its stored spec while it is active; None when purged or being deleted."""
    found = await sessions_repo.get_stored(conn, session_id)
    return found if found is not None and found[0].desired == "active" else None


async def _visible_session(
    conn, session_id: uuid.UUID, caller: Session, *, hidden_as: str
) -> tuple[Session, dict[str, Any]]:
    """The target session as stored now (a plain read, no row lock: the review rechecks the
    fingerprint under `replace_spec`'s lock), if the caller may still see it (`admin._target`:
    its own, or one of a manageable preset; else as missing as an unknown); 409 while
    deleting."""
    found = await sessions_repo.get_stored(conn, session_id)
    if found is None:
        raise not_found(hidden_as)
    try:
        await admin._target(conn, found[0].slug, caller)
    except ApiError:
        raise not_found(hidden_as) from None
    if found[0].desired != "active":
        raise _deleting()
    return found


async def _family(conn, session: Session, secrets: Mapping[str, SecretStr]) -> uuid.UUID | None:
    """The preset whose Secret family stages the session's values, held FOR SHARE (the lock
    order of every stager: preset, then the variable); a session with no preset takes none."""
    if not secrets:
        return None
    family = session.preset_id
    if family is None or await presets_repo.get_for_share(conn, family) is None:
        raise ApiError(
            422, "invalid_request", "protected values need a session started from a preset"
        )
    return family


async def _based(
    conn,
    settings: Settings,
    kind: str,
    target_id: uuid.UUID,
    body: schemas.ProposalIn | schemas.ProposalRevise,
    caller: Session,
    *,
    hidden_as: str,
) -> tuple[uuid.UUID | None, str, _Plan]:
    """A preset (under its row lock) or session (a plain read) target read now and the
    revision planned against it: the preset whose Secret family stages the values, the
    target's display name, the plan."""
    if kind == "preset":
        preset = await _locked(conn, target_id, hidden_as=hidden_as)
        return preset.id, preset.name, await _plan(conn, settings, preset, body.patch, body.secrets)
    session, stored = await _visible_session(conn, target_id, caller, hidden_as=hidden_as)
    family = await _family(conn, session, body.secrets)
    plan = await _session_plan(settings, session, stored, body.patch, body.secrets)
    return family, session.slug, plan


async def _stage(
    request: Request,
    conn,
    settings: Settings,
    preset_id: uuid.UUID | None,
    secrets: Mapping[str, SecretStr],
    proposal_id: int,
    revision: int,
    staged: list[tuple[uuid.UUID, str, int]],
) -> None:
    """Stage each value in the caller's transaction; `staged` collects the versions written
    so far, for the caller to delete when the transaction rolls back."""
    if not secrets:
        return
    if preset_id is None:
        raise RuntimeError("protected values need the preset of their Secret family")
    kube = secret_store.kube_of(request)
    for name in sorted(secrets):  # one lock order across concurrent proposals
        full = await secret_store.stage_secret(
            conn,
            kube,
            settings.namespace,
            preset_id,
            name,
            secrets[name],
            proposal_id=proposal_id,
            revision=revision,
        )
        staged.append((preset_id, name, secret_store.secret_version(full)))


def _idle(conn) -> None:
    """Refuse a connection inside a transaction: psycopg's `conn.transaction()` would open a
    savepoint there, so "after the commit" work would run before the real commit, with this
    write's row and advisory locks still held."""
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        raise RuntimeError("proposal writes need a connection outside a transaction")


async def _release(
    request: Request, conn, settings: Settings, proposal_id: int, revision: int | None = None
) -> None:
    """Best effort, after the commit: drop the staged values of the proposal (one revision, or
    all) in their own committed transaction; the sweeper retries what fails here."""
    try:
        async with conn.transaction():
            await secret_store.release(
                conn,
                secret_store.kube_of(request),
                settings.namespace,
                proposal_id=proposal_id,
                revision=revision,
            )
    except Exception as exc:  # the write committed; never turn it into an error
        log.warning("proposal %d: staged value release failed: %s", proposal_id, type(exc).__name__)


async def _rolled_back(
    request: Request, conn, settings: Settings, staged: list[tuple[uuid.UUID, str, int]]
) -> None:
    """The transaction did not commit: delete the staged Secrets `release` would never find
    (never one a concurrent writer has made live since, `presets.forget`)."""
    if not staged:
        return
    try:
        await presets_api.forget(conn, secret_store.kube_of(request), settings.namespace, staged)
    except Exception as exc:  # never mask the error that caused the rollback
        log.error("could not delete %d staged secrets: %s", len(staged), type(exc).__name__)


async def _insert(
    conn,
    settings: Settings,
    body: schemas.ProposalIn,
    caller: Session,
    superleader: bool,
    target_id: str,
    target_name: str,
    plan: _Plan,
) -> int:
    """Insert revision 1 in the caller's transaction, within the open-proposal cap (soft)."""
    if await repo.count_active(conn, caller.id) >= settings.proposals_open_max:
        raise ApiError(
            409,
            "too_many_proposals",
            f"at most {settings.proposals_open_max} open proposals per session; withdraw one first",
        )
    return await repo.insert(
        conn,
        kind=body.kind,
        target_id=target_id,
        target_name=target_name,
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


def _own_session_only(superleader: bool, kind: str, own: bool) -> None:
    """A regular leader proposes for its own session only (plan 3.3): any other target, unknown
    and hidden ones included, answers this one body before any lookup (no existence leak)."""
    if not superleader and not (kind == "session" and own):
        raise ApiError(
            403, "not_superleader", "a regular session's leader proposes for its own session only"
        )


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
    _idle(conn)
    admin.touched(request, *body.secrets)
    _own_session_only(superleader, body.kind, body.target == caller.slug)
    if body.client_key is not None:
        async with conn.transaction():
            first = await repo.by_client_key(conn, caller.id, body.client_key)
        if first is not None:
            return await get_view(
                conn, settings, first, proposer=caller.id, visible_only=True
            ), False
    if body.kind == "schedule":
        return await _create_schedule(request, conn, settings, body, caller, superleader)
    _check_values(body.secrets)
    async with conn.transaction():
        if body.kind == "session":
            target_id = (await admin._target(conn, body.target, caller)).id
        else:
            target_id = (await admin.manageable_preset(conn, body.target)).id
    staged: list[tuple[uuid.UUID, str, int]] = []
    try:
        async with conn.transaction():
            family, name, plan = await _based(
                conn, settings, body.kind, target_id, body, caller, hidden_as=body.kind
            )
            proposal_id = await _insert(
                conn, settings, body, caller, superleader, str(target_id), name, plan
            )
            await _stage(request, conn, settings, family, body.secrets, proposal_id, 1, staged)
    except repo.ProposalExists as exc:  # a concurrent replay won the insert; nothing staged
        view = await get_view(
            conn, settings, exc.proposal_id, proposer=caller.id, visible_only=True
        )
        return view, False
    except secret_store.SecretStoreError:
        await _rolled_back(request, conn, settings, staged)
        raise presets_api.store_failed() from None
    except BaseException:
        await _rolled_back(request, conn, settings, staged)
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
    revision's staged values are released after the commit (D4), never while this revision's
    locks are held (one lock order with every stager)."""
    _idle(conn)
    admin.touched(request, *body.secrets)
    _check_values(body.secrets)
    staged: list[tuple[uuid.UUID, str, int]] = []
    try:
        async with conn.transaction():
            current = await repo.lock(conn, proposal_id)
            if current is None or current.proposer_session != caller.id:
                raise not_found("proposal")
            _own_session_only(
                bool(caller.spec.session_messaging),
                current.kind,
                current.target_id == str(caller.id),
            )
            if current.status not in repo.ACTIVE:
                raise _not_open()
            if current.kind == "schedule":
                _no_values(body.secrets)
                live = await _fetch_schedule(request, settings, current.target_id, caller)
                if live is None:  # deleted: as unknown, like a deleted preset
                    raise not_found("proposal")
                visible = await _manageable(conn, live, hidden_as="proposal")
                plan = _schedule_plan(live, body.patch, visible)
            else:
                family, _, plan = await _based(
                    conn,
                    settings,
                    current.kind,
                    uuid.UUID(current.target_id),
                    body,
                    caller,
                    hidden_as="proposal",
                )
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
            if current.kind != "schedule":
                await _stage(
                    request, conn, settings, family, body.secrets, proposal_id, revision, staged
                )
    except secret_store.SecretStoreError:
        await _rolled_back(request, conn, settings, staged)
        raise presets_api.store_failed() from None
    except BaseException:
        await _rolled_back(request, conn, settings, staged)
        raise
    note_fields(request, [i.path for i in plan.items])
    if any(i.secret for i in previous.items):
        await _release(request, conn, settings, proposal_id, previous.revision)
    return await get_view(conn, settings, proposal_id, proposer=caller.id)


async def withdraw(
    request: Request, conn, settings: Settings, proposal_id: int, *, caller: Session
) -> schemas.ProposalView:
    """The proposer closes its own proposal; every staged value is released after the commit."""
    _idle(conn)
    async with conn.transaction():
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
        await _release(request, conn, settings, proposal_id)
    return await get_view(conn, settings, proposal_id, proposer=caller.id)


async def live_fingerprint(conn, preset: presets_repo.Preset) -> str:
    """`fingerprint_preset` of the preset row as given and its live secret versions now."""
    versions = {n: m.version for n, m in (await _metas(conn, preset.id)).items()}
    return fingerprint_preset(preset, versions)


async def _stale(conn, proposal: repo.Proposal) -> bool:
    """Read-only (never changes state): the live preset or session differs from the current
    revision's (a gone one reads stale). A schedule is never read upstream here (module
    docstring): the review compares."""
    if proposal.status not in repo.ACTIVE or proposal.kind == "schedule":
        return False
    current = next(r for r in proposal.revisions if r.revision == proposal.revision)
    if proposal.kind == "session":
        found = await live_session(conn, uuid.UUID(proposal.target_id))
        if found is None:
            return True
        return fingerprint_session(found[1], found[0].secret_versions) != current.fingerprint
    preset = await presets_repo.get(conn, uuid.UUID(proposal.target_id))
    if preset is None:
        return True
    return await live_fingerprint(conn, preset) != current.fingerprint


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
    conn,
    settings: Settings,
    proposal_id: int,
    *,
    proposer: uuid.UUID | None,
    visible_only: bool = False,
) -> schemas.ProposalView:
    """One proposal with every revision; with `proposer`, only that session's, and with
    `visible_only`, only on a target the proposer may still see (else 404)."""
    found = await repo.get(conn, proposal_id, proposer, visible_only=visible_only)
    if found is None:
        raise not_found("proposal")
    return await _view(conn, found)


async def list_views(
    conn,
    settings: Settings,
    *,
    proposer: uuid.UUID | None,
    status: str,
    limit: int,
    visible_only: bool = False,
) -> list[schemas.ProposalView]:
    """Newest first, the current revision only; `status` is active, open, closed or all."""
    statuses = STATUSES.get(status)
    if statuses is None:
        raise ApiError(422, "invalid_request", f"status must be one of: {', '.join(STATUSES)}")
    found = await repo.list_for(
        conn, proposer_session=proposer, statuses=statuses, limit=limit, visible_only=visible_only
    )
    return [await _view(conn, p) for p in found]


# ---------------------------------------------------------------- schedules (stage B)


def _no_values(secrets: Mapping[str, SecretStr]) -> None:
    if secrets:
        raise ApiError(422, "invalid_request", "a schedule has no protected values: drop secrets")


def _upstream_failed(status: int) -> ApiError:
    return ApiError(503, "upstream_unavailable", f"the schedule upstream answered {status}")


def _upstream_message(payload: Any, status: int) -> str:
    """The upstream's error message (`error.message`, or FastAPI's `loc: msg`), never its input."""
    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict) and isinstance(error.get("message"), str):
            return error["message"]
        detail = payload.get("detail")
        if isinstance(detail, list):
            return "; ".join(
                f"{'.'.join(str(x) for x in d.get('loc', []))}: {d.get('msg')}"
                for d in detail
                if isinstance(d, dict)
            )
    return f"the schedule upstream answered {status}"


async def _schedule_call(
    request: Request,
    settings: Settings,
    method: str,
    schedule_id: str,
    by: Session | repo.Proposal,
    *,
    body: dict[str, Any] | None = None,
    if_match: str | None = None,
) -> tuple[int, Any]:
    """One call to `/schedules/{id}` of the schedule upstream, as the proposer (a session, or the
    proposal's proposer for a review): 503 `upstream_unconfigured` or `upstream_unavailable`."""
    session, slug = (
        (by.id, by.slug) if isinstance(by, Session) else (by.proposer_session, by.proposer_slug)
    )
    path = f"{admin_relay.SERVICES['schedule'][1]}/schedules/{uuid.UUID(schedule_id)}"
    status, content = await forward_core.forward(
        request,
        admin_relay.upstream_target(settings, "schedule"),
        method,
        path,
        None if body is None else json.dumps(body).encode(),
        str(session),
        slug,
        query="",
        extra_headers=None if if_match is None else {"If-Match": f'"{if_match}"'},
    )
    return status, json.loads(content)


async def _fetch_schedule(
    request: Request, settings: Settings, schedule_id: str, by: Session | repo.Proposal
) -> dict[str, Any] | None:
    """The schedule view; None when the upstream does not know it (or the id is no UUID)."""
    try:
        uuid.UUID(schedule_id)
    except ValueError:
        return None
    status, payload = await _schedule_call(request, settings, "GET", schedule_id, by)
    if status == 404:
        return None
    if status != 200:
        raise _upstream_failed(status)
    if not isinstance(payload, dict) or not isinstance(payload.get("updated_at"), str):
        raise ApiError(502, "upstream_shape", "the schedule upstream answer is not a schedule")
    return payload


def _schedule_fields(obj: Mapping[str, Any]) -> dict[str, Any]:
    """The `ScheduleIn` fields of a schedule view or a stored base, in their JSON form."""
    present = {k: obj[k] for k in SCHEDULE_FIELDS if obj.get(k) is not None}
    try:
        return schemas.ScheduleIn.model_validate(present).model_dump(mode="json")
    except ValidationError:
        raise ApiError(
            502, "upstream_shape", "the schedule upstream answer is not a schedule"
        ) from None


def _schedule_with(obj: Mapping[str, Any], approved: Mapping[str, Any]) -> dict[str, Any]:
    """The full `ScheduleIn` of `obj` with each approved field set to its proposed value (None:
    removed, the upstream default), re-validated (422 `invalid_spec`)."""
    out = _schedule_fields(obj)
    for path, value in approved.items():
        _put(out, path, value)
    try:
        return schemas.ScheduleIn.model_validate(out).model_dump(mode="json")
    except ValidationError as exc:
        raise ApiError(422, "invalid_spec", _pydantic_text(exc)) from None


async def _manageable(conn, live: Mapping[str, Any], *, hidden_as: str) -> set[str]:
    """The manageable preset ids; a schedule on any other preset is as missing as an unknown."""
    visible = {str(i) for i in await presets_repo.manageable_ids(conn)}
    if str(live.get("preset_id")) not in visible:
        raise not_found(hidden_as)
    return visible


def _schedule_plan(live: Mapping[str, Any], patch: dict[str, Any], visible: set[str]) -> _Plan:
    """Merge the patch (RFC 7396, `ScheduleIn` fields only) into the schedule, validate it as
    `ScheduleIn` and make one item per changed field; the fingerprint is the view's `updated_at`
    verbatim, the `If-Match` value the review sends."""
    unknown = sorted(set(patch) - set(SCHEDULE_FIELDS))
    if unknown:
        raise ApiError(422, "invalid_request", f"not a schedule field: {', '.join(unknown)}")
    if "timezone" in patch and patch["timezone"] is None:
        raise ApiError(422, "invalid_request", "give an explicit timezone, not null")
    base = _schedule_fields(live)
    try:
        new = schemas.ScheduleIn.model_validate(merge_patch(base, patch)).model_dump(mode="json")
    except ValidationError as exc:
        errors = [{**e, "loc": ("body", "patch", *e["loc"])} for e in exc.errors()]
        raise RequestValidationError(errors) from None
    if new["preset_id"] != base["preset_id"] and new["preset_id"] not in visible:
        raise not_found("preset")
    new["cron"] = " ".join(new["cron"].split())  # agents-schedule stores it normalised
    paths = [f for f in SCHEDULE_FIELDS if new[f] != base[f]]
    if not paths:
        raise ApiError(422, "nothing_changed", "the patch changes nothing")
    classes = classify_schedule(paths)
    items = [
        repo.Item(
            path=f, before=base[f], after=new[f], security=classes[f][0], grant=False, secret=None
        )
        for f in paths
    ]
    return _Plan(base, live["updated_at"], items)


async def _create_schedule(
    request: Request,
    conn,
    settings: Settings,
    body: schemas.ProposalIn,
    caller: Session,
    superleader: bool,
) -> tuple[schemas.ProposalView, bool]:
    """`create` for a schedule on a manageable preset; the upstream read runs before the
    transaction, which only reads presets and inserts (the fingerprint pins the read)."""
    _no_values(body.secrets)
    live = await _fetch_schedule(request, settings, body.target, caller)
    if live is None:
        raise not_found("schedule")
    try:
        async with conn.transaction():
            plan = _schedule_plan(
                live, body.patch, await _manageable(conn, live, hidden_as="schedule")
            )
            proposal_id = await _insert(
                conn, settings, body, caller, superleader, str(live["id"]), live["name"], plan
            )
    except repo.ProposalExists as exc:  # a concurrent replay won the insert
        view = await get_view(
            conn, settings, exc.proposal_id, proposer=caller.id, visible_only=True
        )
        return view, False
    note_fields(request, [i.path for i in plan.items])
    return await get_view(conn, settings, proposal_id, proposer=caller.id), True


# ---------------------------------------------------------------- review (plan 3.5)

_DECISION = {"approve": "approved", "rework": "rework", "deny": "denied"}
_REVIEWABLE = ("open", "stale")  # a stale proposal takes a rework or deny submit only

Decisions = dict[str, tuple[str, str | None]]


@dataclass
class _Done:
    """What a review transaction leaves for after its commit: the error to answer (a refusal
    whose state change must stick), the proposals whose staged values go, the preset to `gc`."""

    error: ApiError | None = None
    release: list[repo.Proposal] = field(default_factory=list)
    gc: uuid.UUID | None = None


def _stale_error(kind: str) -> ApiError:
    return ApiError(
        409,
        "proposal_stale",
        f"the {kind} changed since this revision was based; only rework or deny remain",
        extra={"fields": []},
    )


def _gone_error(kind: str) -> ApiError:
    return ApiError(409, "target_gone", f"the {kind} no longer exists")


def _head(p: repo.Proposal) -> str:
    return f"Proposal P-{p.id} r{p.revision} ({p.kind} {p.target_name})"


def _entry(item: repo.Item, note: str | None) -> str:
    """A path for a notice: secret items say only that a value is set or changed."""
    label = f"{item.path} (value {item.secret})" if item.secret else item.path
    return f"{label}: {note}" if note else label


def _reviewed_text(p: repo.Proposal, decisions: Decisions) -> str:
    """The proposer's notice for a review submit (D1): paths, verdicts and notes, never a value."""
    by: dict[str, list[str]] = {"approved": [], "rework": [], "denied": []}
    for item in p.revisions[0].items:
        decision, note = decisions[item.path]
        by[decision].append(_entry(item, note))
    if by["rework"]:
        parts = ["; ".join(by["rework"])]
        if by["approved"]:
            parts.append("approved as proposed: " + ", ".join(by["approved"]))
        if by["denied"]:
            parts.append("denied: " + ", ".join(by["denied"]))
        return f"{_head(p)} needs rework: {'; '.join(parts)}; revise with proposal_revise {p.id}."
    parts = []
    if by["approved"]:
        parts.append("applied " + ", ".join(by["approved"]))
    if by["denied"]:
        parts.append("denied " + ", ".join(by["denied"]))
    return f"{_head(p)} was reviewed: {'; '.join(parts)}. Details: proposal_get {p.id}."


_CLOSED_TEXT = {
    "stale": "is stale: the {kind} changed since this revision was based; nothing was applied."
    " Revise it against the current {kind} with proposal_revise {id}.",
    "expired": "expired before it was reviewed; nothing was applied."
    " File it again with proposal_create if it is still needed.",
    "target_gone": "was closed: the {kind} no longer exists or is no longer open to changes from"
    " you; nothing was applied.",
}


async def _notify(conn, p: repo.Proposal, text: str) -> None:
    """Queue the proposer's notice in the caller's transaction (it exists iff that commits). A
    proposer session being deleted, or already purged, gets none; the outcome is still recorded."""
    try:
        await enqueue_message(conn, p.proposer_session, text, lane="regular", notice="proposal")
    except (SessionBeingDeleted, psycopg.errors.ForeignKeyViolation):
        log.info("proposal %d: proposer session gone, no notice", p.id)


async def mark_closed(
    conn, p: repo.Proposal, to: str, *, from_states: tuple[str, ...] = repo.ACTIVE
) -> None:
    """Move the proposal to `to` (stale, expired, target_gone) and queue the proposer's one-line
    notice, in the caller's transaction. Status-guarded: NotInState when another writer won."""
    await repo.transition(conn, p.id, from_states=from_states, to=to, outcome=to)
    await _notify(conn, p, f"{_head(p)} {_CLOSED_TEXT[to].format(id=p.id, kind=p.kind)}")


async def _close(conn, p: repo.Proposal, to: str, error: ApiError) -> _Done:
    """`mark_closed`, then answer `error` once the transaction committed; its staged values are
    released after the commit."""
    await mark_closed(conn, p, to)
    return _Done(error=error, release=[p])


def _decisions(current: repo.Revision, body: schemas.ReviewIn) -> Decisions:
    """The body decides every item of the revision exactly once; a rework carries a note."""
    paths = [i.path for i in body.items]
    expected = {i.path for i in current.items}
    if len(set(paths)) != len(paths):
        raise ApiError(422, "invalid_request", "each item may be decided only once")
    missing, unknown = sorted(expected - set(paths)), sorted(set(paths) - expected)
    if missing or unknown:
        detail = [f"undecided: {', '.join(missing)}"] if missing else []
        detail += [f"not an item of this revision: {', '.join(unknown)}"] if unknown else []
        raise ApiError(422, "invalid_request", "; ".join(detail))
    reworked = sorted(
        i.path for i in body.items if i.decision == "rework" and not (i.note or "").strip()
    )
    if reworked:
        raise ApiError(422, "invalid_request", f"rework needs a note: {', '.join(reworked)}")
    return {i.path: (_DECISION[i.decision], i.note) for i in body.items}


def _put(spec: dict[str, Any], path: str, value: Any) -> None:
    """Set the value at a `changed_paths` path (None removes it), as `_at` reads it."""
    head, _, sub = path.partition(".")
    if not sub:
        if value is None:
            spec.pop(head, None)
        else:
            spec[head] = value
        return
    inner = dict(spec.get(head) or {})
    if value is None:
        inner.pop(sub, None)
    else:
        inner[sub] = value
    spec[head] = inner


def _pydantic_text(exc: ValidationError) -> str:
    """`loc` and `msg` only: never the input."""
    return "; ".join(
        f"{'.'.join(str(x) for x in e['loc'])}: {e['msg']}" if e["loc"] else e["msg"]
        for e in exc.errors()
    )


def _approved(current: repo.Revision, decisions: Decisions) -> list[repo.Item]:
    return [i for i in current.items if decisions[i.path][0] == "approved"]


def _approved_spec(stored: Mapping[str, Any], approved: list[repo.Item]) -> SessionSpec:
    """The stored spec with each approved path set to its proposed value (or removed), parsed
    (422 `invalid_spec` with the pydantic text, never the input)."""
    out = copy.deepcopy(dict(stored))
    for item in approved:
        _put(out, item.path, item.after)
    try:
        spec_in = SessionSpecIn.model_validate(out)
    except ValidationError as exc:
        raise ApiError(422, "invalid_spec", _pydantic_text(exc)) from None
    spec, _ = spec_in.split()  # no values: an item `after` is a redacted declaration
    return spec


async def _apply(
    request: Request,
    conn,
    settings: Settings,
    preset: presets_repo.Preset,
    p: repo.Proposal,
    decisions: Decisions,
) -> bool:
    """Write the approved items onto the locked stored spec (step 5-6); True when the secret store
    was touched (the caller runs `gc` after the commit).

    The name, labels and manageable flag (and any other column) are passed through unchanged
    (F8): items are spec paths only. No agent guard runs: a human approved. Approved staged values
    are promoted here, in the same transaction as the preset write: `promote_staged` is a database
    change only (the Secret was written when it was staged), so the new version goes live exactly
    when the preset write commits, and a rollback leaves the reservation staged for a retry."""
    current = p.revisions[0]
    approved = _approved(current, decisions)
    spec = _approved_spec(preset.spec, approved)
    try:
        new = await presets_api.validated_spec(spec, settings, preset.spec, preset.id)
    except ApiError as exc:
        raise ApiError(422, "invalid_spec", exc.message) from None
    await presets_repo.update(
        conn, preset.id, preset.name, new, expected_updated_at=preset.updated_at
    )
    promoted = sorted(i.path.removeprefix("env.") for i in approved if i.secret)
    if not promoted and not await secret_store.has_rows(conn, preset.id):
        return False
    for name in promoted:  # one lock order across concurrent writers
        await secret_store.promote_staged(
            conn, preset.id, name, proposal_id=p.id, revision=current.revision
        )
    declared = sorted(n for n, e in new.get("env", {}).items() if e.get("protected"))
    await secret_store.sync_declared(
        conn, secret_store.kube_of(request), settings.namespace, preset.id, declared, collect=False
    )
    return True


async def _stale_others(conn, p: repo.Proposal) -> list[repo.Proposal]:
    """The apply changed the target: every other open proposal on it is stale (step 7). Rows
    another transaction holds are skipped: that review, revise or withdraw sees the new target
    once this commits (the review its fingerprint mismatch, the revise a new base)."""
    out = []
    for other_id in await repo.active_on_target(conn, p.kind, p.target_id, exclude=p.id, lock=True):
        other = await repo.lock(conn, other_id)
        if other is None:  # locked above, and proposals are never deleted
            raise RuntimeError(f"proposal {other_id} vanished while locked")
        try:
            await mark_closed(conn, other, "stale", from_states=("open",))
        except repo.NotInState:
            continue  # rework or stale already: re-based on its next revision anyway
        out.append(other)
    return out


async def _locked_target(conn, preset_id: uuid.UUID, hold: bool) -> presets_repo.Preset | None:
    """The target preset FOR UPDATE, or None when it is gone. With `hold` (an apply writes it),
    its project row is taken first (`hold_project`: 422 `project_archived` freezes the config of
    a preset in an archived project), in the epic's lock order: project row, then preset row. The
    project is peeked unlocked and confirmed under the preset lock; a preset moved meanwhile is
    read again."""
    for _ in range(PLACE_ATTEMPTS):
        peek = await presets_repo.get(conn, preset_id)
        if peek is None:
            return None
        if hold:
            await hold_project(conn, peek.project)
        preset = await presets_repo.get_for_update(conn, preset_id)
        if preset is None or not hold or preset.project == peek.project:
            return preset
    raise ApiError(409, "conflict", "the preset moved to another project meanwhile; retry")


async def _review_locked(
    request: Request, conn, settings: Settings, proposal_id: int, body: schemas.ReviewIn
) -> _Done:
    """Plan 3.5 steps 1-8 inside the caller's transaction. A raised error rolls everything back;
    a returned `_Done.error` is answered after the state change it made commits."""
    p = await repo.lock(conn, proposal_id)
    if p is None:
        raise not_found("proposal")
    if p.status not in _REVIEWABLE:
        raise _not_open()
    if await repo.is_expired(conn, p.id):  # the database clock, as expires_at was set
        return await _close(conn, p, "expired", ApiError(409, "proposal_expired", "expired"))
    if body.revision != p.revision:
        raise ApiError(
            409, "revision_changed", f"revision {p.revision} is the one under review now"
        )
    current = p.revisions[0]  # `lock` loads the current revision only
    decisions = _decisions(current, body)
    verdicts = {d for d, _ in decisions.values()}
    if p.status == "stale" and "approved" in verdicts:
        raise ApiError(
            422, "invalid_request", "a stale proposal takes only rework or deny; revise it first"
        )
    if p.kind == "schedule":
        return await _review_schedule(request, conn, settings, p, decisions)
    if p.kind == "session":
        return await _review_session(conn, settings, p, decisions)
    to = _outcome(decisions)
    preset = await _locked_target(conn, uuid.UUID(p.target_id), hold=to == "applied")
    if preset is None:
        return await _close(conn, p, "target_gone", _gone_error(p.kind))
    if p.status == "open" and await live_fingerprint(conn, preset) != current.fingerprint:
        return await _close(conn, p, "stale", _stale_error(p.kind))
    done = _Done(release=[p])
    if to == "applied":
        try:
            async with conn.transaction():  # a stale staged value undoes the write, not the mark
                touched = await _apply(request, conn, settings, preset, p, decisions)
        except secret_store.StagedStale:
            return await _close(conn, p, "stale", _stale_error(p.kind))
        done.gc = preset.id if touched else None
    return await _record(conn, p, decisions, to, done)


def _outcome(decisions: Decisions) -> str:
    """Any rework makes a rework request (D1); else any approval applies; else all denied."""
    verdicts = {d for d, _ in decisions.values()}
    if "rework" in verdicts:
        return "rework"
    return "applied" if "approved" in verdicts else "denied"


async def _record(conn, p: repo.Proposal, decisions: Decisions, to: str, done: _Done) -> _Done:
    """Steps 7-8: verdicts, outcome, the other open proposals on the target stale after an
    apply, and the proposer's notice, in the caller's transaction."""
    await repo.record_review(conn, p.id, p.revision, decisions, statuses=_REVIEWABLE)
    await repo.transition(conn, p.id, from_states=_REVIEWABLE, to=to, outcome=to)
    if to == "applied":
        done.release += await _stale_others(conn, p)
    await _notify(conn, p, _reviewed_text(p, decisions))
    return done


async def _review_schedule(
    request: Request, conn, settings: Settings, p: repo.Proposal, decisions: Decisions
) -> _Done:
    """Plan 3.5, schedules (no shared transaction): read the schedule, compare its `updated_at`
    with the fingerprint, `PUT` the full object with `If-Match`, then record in the caller's
    transaction (it holds the proposal row). When that commit failed after a PUT that landed,
    the retried review finds a changed `updated_at` whose fields equal the revision's base plus
    the approved items, and records `applied` without a second PUT."""
    current = p.revisions[0]
    to = _outcome(decisions)
    live = await _fetch_schedule(request, settings, p.target_id, p)
    if live is None:
        return await _close(conn, p, "target_gone", _gone_error(p.kind))
    visible = {str(i) for i in await presets_repo.manageable_ids(conn)}
    if str(live.get("preset_id")) not in visible:  # parity with the sweeper's preset branch
        return await _close(conn, p, "target_gone", _gone_error(p.kind))
    approved = {i.path: i.after for i in current.items if decisions[i.path][0] == "approved"}
    if p.status == "open" and live["updated_at"] != current.fingerprint:
        expected = _schedule_with(current.base, approved)
        landed = to == "applied" and _schedule_fields(live) == expected
        if not landed:
            return await _close(conn, p, "stale", _stale_error(p.kind))
    elif to == "applied":
        target_preset = approved.get("preset_id")
        if target_preset is not None and str(target_preset) not in visible:
            # the proposed preset stopped being manageable since revise (parity with the relay)
            return await _close(conn, p, "stale", _stale_error(p.kind))
        status, payload = await _schedule_call(
            request,
            settings,
            "PUT",
            p.target_id,
            p,
            body=_schedule_with(live, approved),
            if_match=current.fingerprint,
        )
        if status == 412:
            return await _close(conn, p, "stale", _stale_error(p.kind))
        if status == 404:
            return await _close(conn, p, "target_gone", _gone_error(p.kind))
        if status in (409, 422):  # a name taken, an unknown preset, a bad cron or prompt
            raise ApiError(422, "invalid_spec", _upstream_message(payload, status))
        if not 200 <= status < 300:
            raise _upstream_failed(status)
    return await _record(conn, p, decisions, to, _Done(release=[p]))


class _SessionChanged(Exception):
    """The locked session no longer matches the revision's fingerprint."""


async def _review_session(
    conn, settings: Settings, p: repo.Proposal, decisions: Decisions
) -> _Done:
    """Plan 3.5, sessions: the preset steps with the session row, written by `replace_spec`
    (row lock, fingerprint recheck under it, generation + 1); a session being deleted closes
    the proposal `target_gone` with 409 `session_deleting`."""
    current = p.revisions[0]
    found = await sessions_repo.get_stored(conn, uuid.UUID(p.target_id))
    if found is None:
        return await _close(conn, p, "target_gone", _gone_error(p.kind))
    session, stored = found
    if session.desired != "active":
        return await _close(conn, p, "target_gone", _deleting())
    if p.status == "open" and fingerprint_session(stored, session.secret_versions) != (
        current.fingerprint
    ):
        return await _close(conn, p, "stale", _stale_error(p.kind))
    to = _outcome(decisions)
    if to == "applied":
        try:
            async with conn.transaction():  # a stale target undoes the binds, not the mark
                await _apply_session(conn, settings, session, stored, p, decisions)
        except (secret_store.StagedStale, _SessionChanged, sessions_repo.SpecChanged):
            return await _close(conn, p, "stale", _stale_error(p.kind))
        except SessionBeingDeleted:
            return await _close(conn, p, "target_gone", _deleting())
        except sessions_repo.SessionNotFound:
            return await _close(conn, p, "target_gone", _gone_error(p.kind))
    return await _record(conn, p, decisions, to, _Done(release=[p]))


async def _apply_session(
    conn,
    settings: Settings,
    session: Session,
    stored: dict[str, Any],
    p: repo.Proposal,
    decisions: Decisions,
) -> None:
    """Write the approved items onto the stored session spec and bind the approved staged values
    into `secret_versions`, one `replace_spec` (one generation bump). No agent guard: a human
    approved. Lock order: the preset FOR SHARE before `bind_staged` takes the variable lock, then
    the session row (`set_secret` holds the preset FOR UPDATE while it takes the variable)."""
    current = p.revisions[0]
    approved = _approved(current, decisions)
    spec = _approved_spec(stored, approved)
    try:
        filled = await session_spec.validated(spec, settings, session)
    except ApiError as exc:
        raise ApiError(422, "invalid_spec", exc.message) from None
    bound = sorted(i.path.removeprefix("env.") for i in approved if i.secret)
    protected = {n for n, e in spec.env.items() if e.protected}
    if undeclared := [n for n in bound if n not in protected]:  # replace_spec does not check
        raise ApiError(
            422,
            "invalid_spec",
            f"not a protected variable of the approved spec: {', '.join(undeclared)}",
        )
    if unset := sorted(protected - set(session.secret_versions) - set(bound)):
        raise ApiError(
            422, "invalid_spec", f"protected variables with no value: {', '.join(unset)}"
        )
    versions: dict[str, str] = {}
    if bound:
        family = session.preset_id
        if family is None or await presets_repo.get_for_share(conn, family) is None:
            raise secret_store.StagedStale("the session has no preset to bind values from")
        for name in bound:  # one lock order across concurrent writers
            versions[name] = await secret_store.bind_staged(
                conn, family, name, proposal_id=p.id, revision=current.revision
            )

    async def recheck(locked: sessions_repo.LockedSession) -> None:
        if fingerprint_session(locked.spec, locked.secret_versions) != current.fingerprint:
            raise _SessionChanged

    await sessions_repo.replace_spec(
        conn, session.id, stored, filled, recheck, secret_versions=versions or None
    )


async def _after_commit(request: Request, conn, settings: Settings, done: _Done) -> None:
    """Best effort (step 9): release the staged values of every proposal this review closed or
    made stale, then collect the preset's old versions; the sweeper retries what fails here.

    Its own transaction: the pooled connection is not autocommit, so without it the row deletes
    would roll back with the connection when the review answers an error (stale, expired)."""
    for p in done.release:  # one transaction each: a failure leaves the others untouched
        if any(i.secret for i in p.revisions[0].items):
            await _release(request, conn, settings, p.id)
    if done.gc is not None:
        try:
            async with conn.transaction():
                await secret_store.gc(
                    conn, secret_store.kube_of(request), settings.namespace, preset_id=done.gc
                )
        except Exception as exc:  # the review committed; never turn it into an error
            log.warning("proposal cleanup after review failed: %s", type(exc).__name__)


async def review(
    request: Request, conn, settings: Settings, proposal_id: int, body: schemas.ReviewIn
) -> schemas.ProposalView:
    """The human review of the current revision (plan 3.5): a rework request records verdicts
    only; a final submit applies the approved items all or nothing on an unchanged target (Q6 B,
    Q14 A) or closes the proposal denied. Every outcome notifies the proposer."""
    _idle(conn)
    async with conn.transaction():
        done = await _review_locked(request, conn, settings, proposal_id, body)
    await _after_commit(request, conn, settings, done)
    if done.error is not None:
        raise done.error
    return await get_view(conn, settings, proposal_id, proposer=None)

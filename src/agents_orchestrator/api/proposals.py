"""Change proposals service (preset-change-proposals 3.4, 3.5): create, revise, withdraw, read
and the human review that applies the approved items.

It handles `kind: "preset"` and `kind: "schedule"` (stage B); the routes and gates live in
`proposal_routes`. A proposal never runs the agent guards (human-only, privileged): it may carry
any path, security, grant or not, for a human to decide; only `review` (a human) writes the target.

A schedule lives in agents-schedule, so there is no shared transaction: its fingerprint is the
upstream `updated_at` exactly as the schedule view spells it, and the review applies with `PUT`
and `If-Match: "<that value>"` (412 there is stale here). Views never call the upstream, so a
schedule proposal reads `stale: false` until a review compares.

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
from collections.abc import Mapping
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
from agents_orchestrator.api.sessions import PLACE_ATTEMPTS, hold_project
from agents_orchestrator.api.spec_edit import merged_spec_in, note_fields
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import proposals as repo
from agents_orchestrator.db.queue import SessionBeingDeleted, enqueue_message
from agents_orchestrator.db.sessions import Session
from agents_orchestrator.preset_policy import (
    changed_paths,
    classify,
    classify_schedule,
    merge_patch,
)
from agents_orchestrator.secret_view import EnvMeta, redact_spec_dict
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpecIn

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
    staged: list[tuple[uuid.UUID, str, int]],
) -> None:
    """Stage each value in the caller's transaction; `staged` collects the versions written
    so far, for the caller to delete when the transaction rolls back."""
    if not secrets:
        return
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
    if body.client_key is not None:
        async with conn.transaction():
            first = await repo.by_client_key(conn, caller.id, body.client_key)
        if first is not None:
            return await get_view(
                conn, settings, first, proposer=caller.id, visible_only=True
            ), False
    if body.kind == "session":
        raise _unsupported(body.kind)
    if not superleader:
        raise session_spec.not_superleader()
    if body.kind == "schedule":
        return await _create_schedule(request, conn, settings, body, caller, superleader)
    _check_values(body.secrets)
    async with conn.transaction():
        target = await admin.manageable_preset(conn, body.target)
    staged: list[tuple[uuid.UUID, str, int]] = []
    try:
        async with conn.transaction():
            preset = await _locked(conn, target.id)
            plan = await _plan(conn, settings, preset, body.patch, body.secrets)
            proposal_id = await _insert(
                conn, settings, body, caller, superleader, str(preset.id), preset.name, plan
            )
            await _stage(request, conn, settings, preset.id, body.secrets, proposal_id, 1, staged)
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
            if current.status not in repo.ACTIVE:
                raise _not_open()
            if current.kind == "schedule":
                _no_values(body.secrets)
                live = await _fetch_schedule(request, settings, current.target_id, caller)
                if live is None:  # deleted: as unknown, like a deleted preset
                    raise not_found("proposal")
                visible = await _manageable(conn, live, hidden_as="proposal")
                plan = _schedule_plan(live, body.patch, visible)
            elif current.kind == "preset":
                preset = await _locked(conn, uuid.UUID(current.target_id), hidden_as="proposal")
                plan = await _plan(conn, settings, preset, body.patch, body.secrets)
            else:
                raise _unsupported(current.kind)
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
            if current.kind == "preset":
                await _stage(
                    request, conn, settings, preset.id, body.secrets, proposal_id, revision, staged
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
    """Read-only (never changes state): the live preset differs from the current revision's. A
    schedule is never read upstream here (module docstring): the review compares."""
    if proposal.status not in repo.ACTIVE or proposal.kind != "preset":
        return False
    current = next(r for r in proposal.revisions if r.revision == proposal.revision)
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
    "target_gone": "was closed: the {kind} no longer exists or is no longer open to superleader"
    " changes; nothing was applied.",
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
    approved = [i for i in current.items if decisions[i.path][0] == "approved"]
    stored = copy.deepcopy(preset.spec)
    for item in approved:
        _put(stored, item.path, item.after)
    try:
        spec_in = SessionSpecIn.model_validate(stored)
    except ValidationError as exc:
        raise ApiError(422, "invalid_spec", _pydantic_text(exc)) from None
    spec, _ = spec_in.split()  # no values: an item `after` is a redacted declaration
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
    if p.kind != "preset":
        raise _unsupported(p.kind)
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

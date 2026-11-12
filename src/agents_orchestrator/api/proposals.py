"""Change proposals service (preset-change-proposals 3.4, 3.5): create, revise, withdraw, read
and the human review that applies the approved items.

It handles `kind: "preset"`, `kind: "schedule"` (stage B), `kind: "session"` (stage C) and the
project grants `kind: "session_projects"` and `kind: "preset_projects"` (T-120: one item
`projects.<slug>` per granted project, filed by `project_grant_routes`, never revised); the
routes and gates live in `proposal_routes`. A proposal never runs the agent guards (human-only,
privileged): it may carry any path, security, grant or not, for a human to decide; only `review`
(a human) writes the target.

A schedule lives in agents-schedule, so there is no shared transaction: its fingerprint is the
upstream `updated_at` exactly as the schedule view spells it, and the review applies with `PUT`
and `If-Match: "<that value>"` (412 there is stale here). Views never call the upstream, so a
schedule proposal reads `stale: false` until a review compares.

A session's fingerprint is its stored spec plus `secret_versions` (its vault pins); the review
writes through `db.sessions.replace_spec` (generation + 1). A grant's fingerprint is the
target's home project plus its extra projects; the review writes the extra projects under the
row lock (no spec change, no generation bump: scope is read live). A proposal never carries a
value: a preset proposal may carry vault references (`{"vault": <entry id>, "env": <entry
env>}`), which the vault checks at create and revise (early 422) and again when the review saves
the preset; a session proposal refuses reference changes (422 `vault_ref_not_allowed`), refs
enter and leave a live session only through its preset.

Every write runs in a real transaction (the connection is idle when it starts, `_idle`).
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import psycopg
from fastapi import Request
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

from agents_orchestrator.api import admin, admin_relay, project_grants, schemas, session_spec
from agents_orchestrator.api import forward as forward_core
from agents_orchestrator.api import presets as presets_api
from agents_orchestrator.api.errors import ApiError, not_found
from agents_orchestrator.api.sessions import PLACE_ATTEMPTS, hold_project
from agents_orchestrator.api.spec_edit import merged_spec, note_fields
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
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec, legacy_context

log = logging.getLogger(__name__)

STATUSES: dict[str, tuple[str, ...]] = {
    "active": repo.ACTIVE,
    "open": ("open",),
    "closed": repo.TERMINAL,
    "all": repo.ACTIVE + repo.TERMINAL,
}

SCHEDULE_FIELDS = tuple(schemas.ScheduleIn.model_fields)

# project grants (T-120): the target is a session or a preset, the items `projects.<slug>`
PROJECT_KINDS = ("session_projects", "preset_projects")
# the target a kind names, for texts
_NOUN = {"session_projects": "session", "preset_projects": "preset"}


@dataclass(frozen=True)
class _Filed:
    """What `_insert` reads of a proposal body, for a grant filed by `create_projects`."""

    kind: str
    reason: str
    client_key: str | None


@dataclass(frozen=True)
class _Plan:
    """One revision as it will be stored: base, fingerprint and items."""

    base: dict[str, Any]
    fingerprint: str
    items: list[repo.Item]


def fingerprint_preset(preset: presets_repo.Preset) -> str:
    """The preset as a proposal revision was based on it (Q6 B): every row write bumps
    `updated_at`. The empty `secrets` map stays in the document so a revision fingerprinted
    before migration 0041, on a preset without protected variables, still matches."""
    doc = {"updated_at": preset.updated_at.isoformat(), "secrets": {}}
    return hashlib.sha256(json.dumps(doc, sort_keys=True).encode()).hexdigest()


def fingerprint_session(stored: Mapping[str, Any], versions: Mapping[str, str]) -> str:
    """The session as a proposal revision was based on it (Q6 B): the stored spec JSONB and its
    pins, canonical JSON; any spec write counts, a scale included."""
    doc = {"spec": stored, "secret_versions": versions}
    canonical = json.dumps(doc, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def fingerprint_projects(home: str, extras: Sequence[str]) -> str:
    """A grant target as a proposal revision was based on it: its home and extra projects."""
    doc = {"project": home, "extra_projects": sorted(extras)}
    canonical = json.dumps(doc, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


async def live_projects_fingerprint(conn, kind: str, target_id: str) -> str | None:
    """The live fingerprint of a grant's target; None when the session is purged or being
    deleted, or the preset deleted or no longer superleader-manageable."""
    if kind == "session_projects":
        found = await live_session(conn, uuid.UUID(target_id))
        if found is None:
            return None
        return fingerprint_projects(found[0].project, found[0].extra_projects)
    preset = await presets_repo.get(conn, uuid.UUID(target_id))
    if preset is None or not preset.superleader_manageable:
        return None
    return fingerprint_projects(preset.project, preset.extra_projects)


def _not_open() -> ApiError:
    return ApiError(409, "not_open", "the proposal is closed")


def _deleting() -> ApiError:
    return ApiError(409, "session_deleting", "the session is being discarded")


def _at(spec: Mapping[str, Any], path: str) -> Any:
    """The value at a `changed_paths` path (`env.X`, `wrapper.X` one level deep); None = absent."""
    head, _, sub = path.partition(".")
    value = spec.get(head)
    if not sub:
        return value
    return value.get(sub) if isinstance(value, dict) else None


async def _locked(conn, preset_id: uuid.UUID, *, hidden_as: str = "preset") -> presets_repo.Preset:
    """The target preset under a share lock (it cannot change until the commit); one deleted or
    one the superleader may not manage is as missing as an unknown (`hidden_as` names what the
    404 says is missing)."""
    preset = await presets_repo.get_for_share(conn, preset_id)
    if preset is None or not preset.superleader_manageable:
        raise not_found(hidden_as)
    return preset


async def _plan(
    request: Request,
    conn,
    settings: Settings,
    preset: presets_repo.Preset,
    patch: dict[str, Any],
) -> _Plan:
    """Merge, check and diff the patch against the locked preset, as the PATCH does, without the
    agent guards; the vault checks the references. Items carry the stored and the merged spec
    only."""
    spec = _merged(preset.spec, patch)
    new = await presets_api.validated_spec(spec, settings, preset.spec, preset.id)
    await presets_api.check_vault(request, spec, preset.labels, preset.project)
    base, items = _items(preset.spec, new)
    return _Plan(base, fingerprint_preset(preset), items)


def _merged(
    stored: dict[str, Any], patch: dict[str, Any], context: dict[str, Any] | None = None
) -> SessionSpec:
    """The stored spec with the patch merged in, parsed (`context`: a session's
    `legacy_context`)."""
    return merged_spec(stored, patch, context)[1]


def _items(
    stored: Mapping[str, Any], new: Mapping[str, Any]
) -> tuple[dict[str, Any], list[repo.Item]]:
    """The base and one item per changed path; values come from the stored and the merged spec
    only (a spec holds no secret value: a vault ref names its entry)."""
    paths = sorted(changed_paths(stored, new))
    if not paths:
        raise ApiError(422, "nothing_changed", "the patch changes nothing")
    base, after = copy.deepcopy(dict(stored)), copy.deepcopy(dict(new))
    classes = classify(paths)
    items = [
        repo.Item(
            path=path,
            before=_at(base, path),
            after=_at(after, path),
            security=classes[path][0],
            grant=classes[path][1],
        )
        for path in paths
    ]
    return base, items


async def _session_plan(
    settings: Settings,
    session: Session,
    stored: dict[str, Any],
    patch: dict[str, Any],
) -> _Plan:
    """`_plan` for a live session: merged and checked in the session PATCH's order (fixed fields,
    reference changes, then `check_spec`), no agent guard."""
    spec = _merged(stored, patch, legacy_context(session.secret_versions))
    new = spec.model_dump(mode="json", exclude_unset=True)
    if fixed := fixed_changes(stored, new):
        raise session_spec.fixed_refused(fixed)
    session_spec.refuse_ref_changes(stored, new)
    await session_spec.validated(spec, settings, session)
    base, items = _items(stored, new)
    return _Plan(base, fingerprint_session(stored, session.secret_versions), items)


async def live_session(conn, session_id: uuid.UUID) -> tuple[Session, dict[str, Any]] | None:
    """The session and its stored spec while it is active; None when purged or being deleted."""
    found = await sessions_repo.get_stored(conn, session_id)
    return found if found is not None and found[0].desired == "active" else None


async def _visible_session(
    conn, session_id: uuid.UUID, caller: Session, *, hidden_as: str
) -> tuple[Session, dict[str, Any]]:
    """The target session as stored now (a plain read, no row lock: the review rechecks the
    fingerprint under `replace_spec`'s lock), if the caller may still write it
    (`admin._writable_target`: its own, or one of a manageable preset; else as missing as an
    unknown, the race with a change of manageability); 409 while deleting."""
    found = await sessions_repo.get_stored(conn, session_id)
    if found is None:
        raise not_found(hidden_as)
    try:
        await admin._writable_target(conn, found[0].slug, caller)
    except ApiError:
        raise not_found(hidden_as) from None
    if found[0].desired != "active":
        raise _deleting()
    return found


async def _based(
    request: Request,
    conn,
    settings: Settings,
    kind: str,
    target_id: uuid.UUID,
    body: schemas.ProposalIn | schemas.ProposalRevise,
    caller: Session,
    *,
    hidden_as: str,
) -> tuple[str, _Plan]:
    """A preset (under its row lock) or session (a plain read) target read now and the
    revision planned against it: the target's display name and the plan."""
    if kind == "preset":
        preset = await _locked(conn, target_id, hidden_as=hidden_as)
        return preset.name, await _plan(request, conn, settings, preset, body.patch)
    session, stored = await _visible_session(conn, target_id, caller, hidden_as=hidden_as)
    return session.slug, await _session_plan(settings, session, stored, body.patch)


def _idle(conn) -> None:
    """Refuse a connection inside a transaction: psycopg's `conn.transaction()` would open a
    savepoint there, so "after the commit" work would run before the real commit, with this
    write's row and advisory locks still held."""
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        raise RuntimeError("proposal writes need a connection outside a transaction")


async def _insert(
    conn,
    settings: Settings,
    body: schemas.ProposalIn | _Filed,
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
    """File a proposal; the bool is False on a `client_key` replay."""
    _idle(conn)
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
    async with conn.transaction():
        if body.kind == "session":
            target_id = (await admin._writable_target(conn, body.target, caller)).id
        else:
            target_id = (await admin.writable_preset(conn, body.target)).id
    try:
        async with conn.transaction():
            name, plan = await _based(
                request, conn, settings, body.kind, target_id, body, caller, hidden_as=body.kind
            )
            proposal_id = await _insert(
                conn, settings, body, caller, superleader, str(target_id), name, plan
            )
    except repo.ProposalExists as exc:  # a concurrent replay won the insert
        view = await get_view(
            conn, settings, exc.proposal_id, proposer=caller.id, visible_only=True
        )
        return view, False
    note_fields(request, [i.path for i in plan.items])
    return await get_view(conn, settings, proposal_id, proposer=caller.id), True


async def _grant_target(
    conn, kind: str, ref: str, caller: Session
) -> tuple[str, str, str, tuple[str, ...]]:
    """The grant's target as the superleader may see it (else 404 like an unknown): id, display
    name, home project and extra projects. A session being deleted is 409."""
    if kind == "session_projects":
        session = await admin._writable_target(conn, ref, caller)
        if session.desired != "active":
            raise _deleting()
        return str(session.id), session.slug, session.project, session.extra_projects
    preset = await admin.writable_preset(conn, ref)
    return str(preset.id), preset.name, preset.project, preset.extra_projects


async def create_projects(
    request: Request,
    conn,
    settings: Settings,
    *,
    kind: str,
    ref: str,
    body: schemas.ProjectGrantIn,
    caller: Session,
) -> tuple[schemas.ProposalView, bool]:
    """File a grant of `body.project` to the session (slug) or preset (id or name) `ref` as a
    proposal of `kind` (T-120, user answer Q4 A); the bool is False on a `client_key` replay.
    The checks run again when the review applies it."""
    _idle(conn)
    filed = _Filed(kind=kind, reason=body.reason, client_key=body.client_key)
    if body.client_key is not None:
        async with conn.transaction():
            first = await repo.by_client_key(conn, caller.id, body.client_key)
        if first is not None:
            view = await get_view(conn, settings, first, proposer=caller.id, visible_only=True)
            return view, False
    note_fields(request, ["projects"])
    try:
        async with conn.transaction():
            target_id, name, home, extras = await _grant_target(conn, kind, ref, caller)
            await project_grants.check_grant(conn, home, extras, body.project)
            item = repo.Item(
                path=project_grants.item_path(body.project),
                before=None,
                after=True,
                security=True,
                grant=True,
            )
            plan = _Plan(
                {"project": home, "extra_projects": list(extras)},
                fingerprint_projects(home, extras),
                [item],
            )
            proposal_id = await _insert(conn, settings, filed, caller, True, target_id, name, plan)
    except repo.ProposalExists as exc:  # a concurrent replay won the insert
        view = await get_view(
            conn, settings, exc.proposal_id, proposer=caller.id, visible_only=True
        )
        return view, False
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
    """A new revision against the target read now (D2): re-based, open, expiry reset."""
    _idle(conn)
    async with conn.transaction():
        current = await repo.lock(conn, proposal_id)
        if current is None or current.proposer_session != caller.id:
            raise not_found("proposal")
        _own_session_only(
            bool(caller.spec.session_messaging),
            current.kind,
            current.target_id == str(caller.id),
        )
        if current.kind in PROJECT_KINDS:
            raise ApiError(
                422,
                "not_revisable",
                "a project grant is not revised; withdraw it and file the grant again",
            )
        if current.status not in repo.ACTIVE:
            raise _not_open()
        if current.kind == "schedule":
            live = await _fetch_schedule(request, settings, current.target_id, caller)
            if live is None:  # deleted: as unknown, like a deleted preset
                raise not_found("proposal")
            visible = await _manageable(conn, live, hidden_as="proposal")
            plan = _schedule_plan(live, body.patch, visible)
        else:
            _, plan = await _based(
                request,
                conn,
                settings,
                current.kind,
                uuid.UUID(current.target_id),
                body,
                caller,
                hidden_as="proposal",
            )
        previous = current.revisions[0]  # `lock` loads the current revision only
        await repo.add_revision(
            conn,
            proposal_id,
            from_states=repo.ACTIVE,
            reason=body.reason if body.reason is not None else previous.reason,
            base=plan.base,
            fingerprint=plan.fingerprint,
            items=plan.items,
            ttl_seconds=settings.proposal_ttl_seconds,
        )
    note_fields(request, [i.path for i in plan.items])
    return await get_view(conn, settings, proposal_id, proposer=caller.id)


async def withdraw(
    request: Request, conn, settings: Settings, proposal_id: int, *, caller: Session
) -> schemas.ProposalView:
    """The proposer closes its own proposal."""
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
    return await get_view(conn, settings, proposal_id, proposer=caller.id)


async def _stale(conn, proposal: repo.Proposal) -> bool:
    """Read-only (never changes state): the live preset or session differs from the current
    revision's (a gone one reads stale). A schedule is never read upstream here (module
    docstring): the review compares."""
    if proposal.status not in repo.ACTIVE or proposal.kind == "schedule":
        return False
    current = next(r for r in proposal.revisions if r.revision == proposal.revision)
    if proposal.kind in PROJECT_KINDS:
        live = await live_projects_fingerprint(conn, proposal.kind, proposal.target_id)
        return live != current.fingerprint
    if proposal.kind == "session":
        found = await live_session(conn, uuid.UUID(proposal.target_id))
        if found is None:
            return True
        return fingerprint_session(found[1], found[0].secret_versions) != current.fingerprint
    preset = await presets_repo.get(conn, uuid.UUID(proposal.target_id))
    if preset is None:
        return True
    return fingerprint_preset(preset) != current.fingerprint


async def _view(conn, proposal: repo.Proposal) -> schemas.ProposalView:
    """The view: no client_key, no base or fingerprint (a spec holds no secret value)."""
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
        repo.Item(path=f, before=base[f], after=new[f], security=classes[f][0], grant=False)
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
    whose state change must stick)."""

    error: ApiError | None = None


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
    return f"{item.path}: {note}" if note else item.path


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
    kind = _NOUN.get(p.kind, p.kind)
    await _notify(conn, p, f"{_head(p)} {_CLOSED_TEXT[to].format(id=p.id, kind=kind)}")


async def _close(conn, p: repo.Proposal, to: str, error: ApiError) -> _Done:
    """`mark_closed`, then answer `error` once the transaction committed."""
    await mark_closed(conn, p, to)
    return _Done(error=error)


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


def _approved_spec(
    stored: Mapping[str, Any], approved: list[repo.Item], context: dict[str, Any] | None = None
) -> SessionSpec:
    """The stored spec with each approved path set to its proposed value (or removed), parsed
    (422 `invalid_spec` with the pydantic text, never the input; `context`: a session's
    `legacy_context`)."""
    out = copy.deepcopy(dict(stored))
    for item in approved:
        _put(out, item.path, item.after)
    try:
        return SessionSpec.model_validate(out, context=context)
    except ValidationError as exc:
        raise ApiError(422, "invalid_spec", _pydantic_text(exc)) from None


async def _apply(
    request: Request,
    conn,
    settings: Settings,
    preset: presets_repo.Preset,
    p: repo.Proposal,
    decisions: Decisions,
) -> None:
    """Write the approved items onto the locked stored spec (step 5-6).

    The name, labels and manageable flag (and any other column) are passed through unchanged
    (F8): items are spec paths only. No agent guard runs: a human approved. The vault checks the
    references again here, before the write: a 422 or 503 rolls the review back and the proposal
    stays open."""
    current = p.revisions[0]
    approved = _approved(current, decisions)
    spec = _approved_spec(preset.spec, approved)
    try:
        new = await presets_api.validated_spec(spec, settings, preset.spec, preset.id)
    except ApiError as exc:
        raise ApiError(422, "invalid_spec", exc.message) from None
    await presets_api.check_vault(request, spec, preset.labels, preset.project)
    await presets_repo.update(
        conn, preset.id, preset.name, new, expected_updated_at=preset.updated_at
    )


async def _stale_others(conn, p: repo.Proposal) -> None:
    """The apply changed the target: every other open proposal on it is stale (step 7). Rows
    another transaction holds are skipped: that review, revise or withdraw sees the new target
    once this commits (the review its fingerprint mismatch, the revise a new base)."""
    for other_id in await repo.active_on_target(conn, p.kind, p.target_id, exclude=p.id, lock=True):
        other = await repo.lock(conn, other_id)
        if other is None:  # locked above, and proposals are never deleted
            raise RuntimeError(f"proposal {other_id} vanished while locked")
        try:
            await mark_closed(conn, other, "stale", from_states=("open",))
        except repo.NotInState:
            continue  # rework or stale already: re-based on its next revision anyway


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
    if p.kind in PROJECT_KINDS:
        return await _review_projects(conn, p, decisions)
    to = _outcome(decisions)
    preset = await _locked_target(conn, uuid.UUID(p.target_id), hold=to == "applied")
    if preset is None:
        return await _close(conn, p, "target_gone", _gone_error(p.kind))
    if p.status == "open" and fingerprint_preset(preset) != current.fingerprint:
        return await _close(conn, p, "stale", _stale_error(p.kind))
    if to == "applied":
        async with conn.transaction():
            await _apply(request, conn, settings, preset, p, decisions)
    return await _record(conn, p, decisions, to, _Done())


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
        await _stale_others(conn, p)
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
    return await _record(conn, p, decisions, to, _Done())


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
            async with conn.transaction():  # a stale target undoes the write, not the mark
                await _apply_session(conn, settings, session, stored, p, decisions)
        except (_SessionChanged, sessions_repo.SpecChanged):
            return await _close(conn, p, "stale", _stale_error(p.kind))
        except SessionBeingDeleted:
            return await _close(conn, p, "target_gone", _deleting())
        except sessions_repo.SessionNotFound:
            return await _close(conn, p, "target_gone", _gone_error(p.kind))
    return await _record(conn, p, decisions, to, _Done())


async def _apply_session(
    conn,
    settings: Settings,
    session: Session,
    stored: dict[str, Any],
    p: repo.Proposal,
    decisions: Decisions,
) -> None:
    """Write the approved items onto the stored session spec, one `replace_spec` (one generation
    bump, pins untouched). No agent guard: a human approved; reference changes are refused again
    here (`vault_ref_not_allowed`)."""
    current = p.revisions[0]
    approved = _approved(current, decisions)
    spec = _approved_spec(stored, approved, legacy_context(session.secret_versions))
    session_spec.refuse_ref_changes(stored, spec.model_dump(mode="json", exclude_unset=True))
    try:
        filled = await session_spec.validated(spec, settings, session)
    except ApiError as exc:
        raise ApiError(422, "invalid_spec", exc.message) from None

    async def recheck(locked: sessions_repo.LockedSession) -> None:
        if fingerprint_session(locked.spec, locked.secret_versions) != current.fingerprint:
            raise _SessionChanged

    await sessions_repo.replace_spec(conn, session.id, stored, filled, recheck)


async def _review_projects(conn, p: repo.Proposal, decisions: Decisions) -> _Done:
    """Plan 3.5 for a project grant (T-120): the approved projects are held active first (lock
    order: project rows, then the target row; 422 `project_archived` rolls the review back and
    the proposal stays open), then the target row is locked, its fingerprint compared, and the
    approved projects added to its extra projects. A granted session gets a `project_grant`
    notice. No spec changes and no pod rolls: scope is read live per request."""
    current = p.revisions[0]
    to = _outcome(decisions)
    noun = _NOUN[p.kind]
    added = [project_grants.project_of(i.path) for i in _approved(current, decisions)]
    if to == "applied":
        for slug in added:
            await project_grants.require_active(conn, slug)
    target_id = uuid.UUID(p.target_id)
    if p.kind == "session_projects":
        try:
            session = await sessions_repo.lock_active(conn, target_id)
        except sessions_repo.SessionNotFound:
            return await _close(conn, p, "target_gone", _gone_error(noun))
        except SessionBeingDeleted:
            return await _close(conn, p, "target_gone", _deleting())
        home, extras = session.project, session.extra_projects
    else:
        preset = await presets_repo.get_for_update(conn, target_id)
        if preset is None or not preset.superleader_manageable:
            return await _close(conn, p, "target_gone", _gone_error(noun))
        home, extras = preset.project, preset.extra_projects
    if p.status == "open" and fingerprint_projects(home, extras) != current.fingerprint:
        return await _close(conn, p, "stale", _stale_error(noun))
    if to == "applied":
        wanted = sorted({*extras, *added} - {home})
        project_grants.check_count(wanted)
        if p.kind == "session_projects":
            await sessions_repo.set_extra_projects(conn, target_id, wanted)
            for slug in added:
                text = project_grants.granted_text(slug, f"proposal P-{p.id}")
                await project_grants.notify(conn, target_id, text)
        else:
            await presets_repo.set_extra_projects(conn, target_id, wanted)
    return await _record(conn, p, decisions, to, _Done())


async def review(
    request: Request, conn, settings: Settings, proposal_id: int, body: schemas.ReviewIn
) -> schemas.ProposalView:
    """The human review of the current revision (plan 3.5): a rework request records verdicts
    only; a final submit applies the approved items all or nothing on an unchanged target (Q6 B,
    Q14 A) or closes the proposal denied. Every outcome notifies the proposer."""
    _idle(conn)
    async with conn.transaction():
        done = await _review_locked(request, conn, settings, proposal_id, body)
    if done.error is not None:
        raise done.error
    return await get_view(conn, settings, proposal_id, proposer=None)

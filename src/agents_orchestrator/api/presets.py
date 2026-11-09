"""Preset routes: named SessionSpec templates. The api is the only writer of the presets table."""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import Any

from fastapi import APIRouter, Request, Response
from fastapi.concurrency import run_in_threadpool
from pydantic import ValidationError

from agents_orchestrator.api import schemas, vault_client
from agents_orchestrator.api.errors import ApiError, not_found, project_error
from agents_orchestrator.api.sessions import Cfg, Pool, check_spec, hold_project
from agents_orchestrator.api.spec_edit import (
    guard_agent_edit,
    human_only_refused,
    merged_spec,
    note_fields,
)
from agents_orchestrator.api.vault_refs import (
    check_refs,
    note_vault,
    record_ref_events,
    ref_diff,
    refs_in,
    unavailable,
)
from agents_orchestrator.db import presets as repo
from agents_orchestrator.db import projects
from agents_orchestrator.preset_policy import (
    HUMAN_ONLY_COLUMNS,
    changed_paths,
    privileged,
    strip_grants,
)
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import Ref, SessionSpec, default_cache_key

router = APIRouter(prefix="/api/v1")

_COPY = " copy"
_COPY_TRIES = 5
_PATCH_TRIES = 3


def _view(preset: repo.Preset) -> schemas.PresetView:
    return schemas.PresetView(
        id=preset.id,
        name=preset.name,
        is_default=preset.is_default,
        superleader_manageable=preset.superleader_manageable,
        labels=preset.labels,
        project=preset.project,
        privileged=privileged(preset.spec),
        vault_reattach=preset.vault_reattach,
        spec=preset.spec,
        created_at=preset.created_at,
        updated_at=preset.updated_at,
    )


def _checked_name(name: str) -> str:
    try:
        return repo.validate_preset_name(name)
    except ValueError as exc:
        raise ApiError(422, "invalid_request", str(exc)) from None


async def _validated(
    body: schemas.PresetIn,
    settings: Settings,
    stored: dict[str, Any] | None = None,
    preset_id: uuid.UUID | None = None,
    human_only_base: dict[str, Any] | None = None,
    request: Request | None = None,
    plain_to_ref: bool = False,
) -> tuple[dict[str, Any], SessionSpec]:
    """Check name and spec; return the spec as the caller sent it (no defaults filled) and the
    parsed spec.

    With `human_only_base` (the stored spec, `{}` on create) the human-only fields may only move
    toward safe, else 403 before anything is written."""
    _checked_name(body.name)
    spec = body.spec
    if human_only_base is not None:
        dumped = spec.model_dump(mode="json", exclude_unset=True)
        guard_agent_edit(
            human_only_base, dumped, request, privileged=True, plain_to_ref=plain_to_ref
        )
    return await validated_spec(spec, settings, stored, preset_id), spec


async def validated_spec(
    spec: SessionSpec,
    settings: Settings,
    stored: dict[str, Any] | None = None,
    preset_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    """`check_spec` with the stored spec's exemptions (a model or image that left the allowlist
    stays accepted while unchanged) and the preset's derived cache key; returns the spec as it is
    stored (no defaults filled). No agent guard runs here."""
    stored_spec = None
    if stored is not None:
        try:
            stored_spec = SessionSpec.model_validate(stored)
        except ValidationError:
            stored_spec = None
    derived = default_cache_key(preset_id) if preset_id is not None else None
    await run_in_threadpool(check_spec, spec, settings, stored_spec, derived_cache_key=derived)
    return spec.model_dump(mode="json", exclude_unset=True)


def _refuse_column_moves(request: Request, stored: repo.Preset, new: dict[str, Any]) -> None:
    """403 `human_only_field` when an agent edit changes a human-only column (`project`). The
    stored row is the one the save compares `updated_at` against, so a concurrent move by the
    user makes the save a 409 rather than slipping past this check."""
    moved = [c for c in HUMAN_ONLY_COLUMNS if new[c] != getattr(stored, c)]
    if moved:
        note_fields(request, moved)
        raise human_only_refused(moved)


async def check_vault(
    request: Request | None, spec: SessionSpec, labels: list[str], project: str
) -> None:
    """Have the vault check the spec's refs against the preset's effective labels (its labels
    plus its project); no call, and no vault needed, when the spec has no refs. Runs before the
    preset row is locked."""
    if not spec.vault_refs:
        return
    vault = vault_client.vault_of(request)
    await check_refs(
        vault, spec, vault_client.effective_labels(SimpleNamespace(project=project, labels=labels))
    )


def _changed_meanwhile() -> ApiError:
    return ApiError(409, "conflict", "the preset changed meanwhile; retry")


def _unchanged_since(
    stored: repo.Preset, *, labels: bool, project: bool
) -> Callable[[repo.Preset], None]:
    """The recheck of a save whose vault check ran on `stored`: on the locked row, the labels
    and project the save inherits (`labels`, `project`) and the stored refs must be as checked,
    else 409 `conflict`."""
    refs = refs_in(stored.spec)

    def recheck(row: repo.Preset) -> None:
        if (
            (labels and row.labels != stored.labels)
            or (project and row.project != stored.project)
            or refs_in(row.spec) != refs
        ):
            raise _changed_meanwhile()

    return recheck


def _actor(request: Request, superleader: bool) -> str:
    """`user`, or the slug of the superleader session making the call."""
    caller = getattr(request.state, "admin_caller", None) if superleader else None
    return "user" if caller is None else caller[1].slug


async def _save(
    request: Request,
    conn,
    spec: dict[str, Any],
    project: str,
    save: Callable[[], Awaitable[repo.Preset]],
    actor: str = "user",
    old_refs: dict[str, Ref] | None = None,
) -> repo.Preset:
    """Save the preset row in one transaction, the preset's `project` checked and held first
    (`hold_project`). In the same transaction: the names the new spec defines leave the preset's
    re-attach list, and the vault refs attached or detached against `old_refs` are recorded as
    events by `actor`.
    """
    async with conn.transaction():
        await hold_project(conn, project)
        preset = await _after_save(conn, await save(), spec, actor, old_refs or {})
    note_vault(request, *ref_diff(old_refs or {}, refs_in(spec)))
    return preset


async def _after_save(
    conn, preset: repo.Preset, spec: dict[str, Any], actor: str, old_refs: dict[str, Ref]
) -> repo.Preset:
    """Prune the re-attach list of every name the new spec defines (any shape) and record the
    ref changes; both inside the save's transaction. Returns the preset as it now stands."""
    defined = set(spec.get("env") or {})
    if any(n in defined for n in preset.vault_reattach):
        preset = await repo.set_vault_reattach(
            conn, preset.id, [n for n in preset.vault_reattach if n not in defined]
        )
    attached, detached = ref_diff(old_refs, refs_in(spec))
    await record_ref_events(conn, preset.id, actor, attached, detached)
    return preset


def _exists(name: str) -> ApiError:
    return ApiError(409, "conflict", f"preset {name!r} already exists")


def _default_error(exc: Exception) -> ApiError:
    return ApiError(409, "preset_default", "the default preset cannot be renamed or deleted")


def _copy_name(source: str, taken: set[str]) -> str:
    """`<source> copy`, then `copy 2`, `copy 3`...; the source is cut so the name fits 40."""
    n = 1
    while True:
        suffix = _COPY if n == 1 else f"{_COPY} {n}"
        name = source[: repo.NAME_MAX - len(suffix)].rstrip(" ") + suffix
        if name.lower() not in taken:
            return name
        n += 1


def manageable_main_port_only() -> ApiError:
    return ApiError(
        422, "manageable_main_port_only", "superleader_manageable is set only on the main port"
    )


async def list_all(
    conn, only_manageable: bool = False, project: str | None = None
) -> list[schemas.PresetView]:
    """Every preset, or one project's (422 `unknown_project`; an archived one still lists)."""
    if project is not None and await projects.get_project(conn, project) is None:
        raise project_error(projects.UnknownProject(project))
    presets = await repo.list_presets(conn, project=project)
    if only_manageable:
        presets = [p for p in presets if p.superleader_manageable]
    return [_view(p) for p in presets]


async def create(
    request: Request,
    conn,
    settings: Settings,
    body: schemas.PresetIn,
    superleader: bool = False,
) -> schemas.PresetView:
    if superleader and body.superleader_manageable is False:
        raise manageable_main_port_only()
    spec, model = await _validated(
        body, settings, human_only_base={} if superleader else None, request=request
    )
    manageable = superleader or bool(body.superleader_manageable)
    project = projects.DEFAULT if body.project is None else body.project
    await check_vault(request, model, body.labels or [], project)
    try:
        preset = await _save(
            request,
            conn,
            spec,
            project,
            lambda: repo.create(conn, body.name, spec, manageable, body.labels, project),
            _actor(request, superleader),
        )
    except repo.PresetExists:
        raise _exists(body.name) from None
    if superleader:
        extra = (["labels"] if body.labels else []) + (
            ["project"] if project != projects.DEFAULT else []
        )
        note_fields(request, changed_paths({}, spec) + ["name"] + extra)
    return _view(preset)


async def get_one(conn, preset_id: uuid.UUID) -> schemas.PresetView:
    preset = await repo.get(conn, preset_id)
    if preset is None:
        raise not_found("preset")
    return _view(preset)


async def replace(
    request: Request,
    conn,
    settings: Settings,
    preset_id: uuid.UUID,
    body: schemas.PresetIn,
    superleader: bool = False,
) -> schemas.PresetView:
    if superleader and body.superleader_manageable is False:
        raise manageable_main_port_only()
    flag = None if superleader else body.superleader_manageable
    stored = await repo.get(conn, preset_id)
    if stored is None or (superleader and not stored.superleader_manageable):
        raise not_found("preset")
    # an omitted project keeps the one read here; the save holds it and writes it back
    project = stored.project if body.project is None else body.project
    if superleader:
        _refuse_column_moves(request, stored, {**body.model_dump(), "project": project})
    spec, model = await _validated(
        body,
        settings,
        stored.spec,
        preset_id,
        human_only_base=stored.spec if superleader else None,
        request=request,
    )
    labels = stored.labels if body.labels is None else body.labels
    await check_vault(request, model, labels, project)
    recheck = _unchanged_since(stored, labels=body.labels is None, project=body.project is None)
    expected = stored.updated_at if superleader else None
    try:
        preset = await _save(
            request,
            conn,
            spec,
            project,
            lambda: repo.update(
                conn, preset_id, body.name, spec, flag, body.labels, expected, project, recheck
            ),
            _actor(request, superleader),
            refs_in(stored.spec),
        )
    except repo.PresetNotFound:
        raise not_found("preset") from None
    except repo.PresetChanged:
        raise ApiError(409, "conflict", "the preset changed since it was read; retry") from None
    except repo.PresetDefault as exc:
        raise _default_error(exc) from None
    except repo.PresetExists:
        raise _exists(body.name) from None
    if superleader:
        paths = changed_paths(stored.spec, spec)
        if body.name != stored.name:
            paths.append("name")
        if body.labels is not None and body.labels != stored.labels:
            paths.append("labels")
        note_fields(request, paths)
    return _view(preset)


async def _entry_envs(request: Request, entry_id: uuid.UUID) -> list[str]:
    """The env names of a vault entry (fixed at create); 422 `vault_entry_unknown` when the vault
    lists no such entry, 503 when it cannot answer."""
    vault = vault_client.vault_of(request)
    try:
        listed = await vault.entries(None)
    except vault_client.VaultUnavailable:
        raise unavailable() from None
    for entry in listed:
        if isinstance(entry, dict) and str(entry.get("id")) == str(entry_id):
            return sorted(e["env"] for e in entry.get("envs") or [])
    raise _entry_unknown(entry_id)


def _entry_unknown(entry_id: uuid.UUID) -> ApiError:
    return ApiError(
        422,
        "vault_entry_unknown",
        f"unknown vault entry: {entry_id}",
        extra={"envs": [], "entries": [str(entry_id)]},
    )


def _attach_edit(
    entry_id: uuid.UUID, mapping: dict[str, str], replace: bool
) -> Callable[[dict[str, Any]], tuple[dict[str, Any], list[str]]]:
    """The env edit of an attach: every `entry env -> preset env` of `mapping` becomes a ref;
    the other envs, earlier refs included, stay. A preset env that exists and is not already
    that very ref is a conflict, overwritten only with `replace`."""
    wanted = {pe: {"vault": str(entry_id), "env": ee} for ee, pe in mapping.items()}

    def edit(env: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
        conflicts = sorted(pe for pe, ref in wanted.items() if pe in env and env[pe] != ref)
        return {**env, **wanted}, ([] if replace else conflicts)

    return edit


def _detach_edit(
    entry_id: uuid.UUID, preset_env: str | None
) -> Callable[[dict[str, Any]], tuple[dict[str, Any], list[str]]]:
    """The env edit of a detach: the refs to the entry (only `preset_env` when given) go."""

    def edit(env: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
        gone = {
            pe
            for pe, ref in refs_in({"env": env}).items()
            if ref.vault == entry_id and preset_env in (None, pe)
        }
        if not gone:
            raise ApiError(404, "not_attached", "the entry is not attached to the preset")
        return {k: v for k, v in env.items() if k not in gone}, []

    return edit


async def attach_vault(
    request: Request,
    conn,
    settings: Settings,
    preset_id: uuid.UUID,
    entry_id: uuid.UUID,
    body: schemas.VaultAttachIn,
) -> schemas.PresetView:
    """Attach the entry's envs to a manageable preset as vault refs, all in one save. Without
    `body.envs` every env of the entry goes in under its own name."""
    if body.envs is None:
        mapping = {e: e for e in await _entry_envs(request, entry_id)}
        if not mapping:
            raise _entry_unknown(entry_id)
    else:
        mapping = body.envs
    return await _edit_refs(
        request, conn, settings, preset_id, _attach_edit(entry_id, mapping, body.replace)
    )


async def detach_vault(
    request: Request,
    conn,
    settings: Settings,
    preset_id: uuid.UUID,
    entry_id: uuid.UUID,
    preset_env: str | None,
) -> schemas.PresetView:
    """Remove every ref to the entry from a manageable preset (only `preset_env` when given)."""
    return await _edit_refs(request, conn, settings, preset_id, _detach_edit(entry_id, preset_env))


async def _edit_refs(
    request: Request,
    conn,
    settings: Settings,
    preset_id: uuid.UUID,
    edit: Callable[[dict[str, Any]], tuple[dict[str, Any], list[str]]],
) -> schemas.PresetView:
    """Superleader edit of a preset's env through `edit(env) -> (new env, conflicting envs)`,
    saved like a patch: the vault checks the refs outside the lock, the locked row must still be
    as read, a concurrent edit is re-read and retried. The conflicts are refused (409
    `env_exists`) once the vault has had its say."""
    for _ in range(_PATCH_TRIES):
        stored = await repo.get(conn, preset_id)
        if stored is None or not stored.superleader_manageable:
            raise not_found("preset")
        env, conflicts = edit(dict(stored.spec.get("env") or {}))
        _, merged = merged_spec({**stored.spec, "env": env}, {})
        body = schemas.PresetIn(name=stored.name, spec=merged, project=stored.project)
        spec, model = await _validated(
            body,
            settings,
            stored.spec,
            preset_id,
            human_only_base=stored.spec,
            request=request,
            plain_to_ref=True,
        )
        await check_vault(request, model, stored.labels, stored.project)
        if conflicts:
            raise ApiError(
                409,
                "env_exists",
                f"the preset already has: {', '.join(conflicts)}; send replace to overwrite",
                extra={"envs": conflicts},
            )
        recheck = _unchanged_since(stored, labels=True, project=True)
        try:
            preset = await _save(
                request,
                conn,
                spec,
                stored.project,
                lambda st=stored, sp=spec, rc=recheck: repo.update(
                    conn, preset_id, st.name, sp, None, None, st.updated_at, st.project, rc
                ),
                _actor(request, True),
                refs_in(stored.spec),
            )
        except repo.PresetNotFound:
            raise not_found("preset") from None
        except repo.PresetChanged:
            continue
        note_fields(request, changed_paths(stored.spec, spec))
        return _view(preset)
    raise ApiError(409, "conflict", "the preset changed since it was read; retry")


def _merged_body(stored: repo.Preset, body: schemas.PresetPatch) -> schemas.PresetIn:
    """The stored spec with the patch merged in, as the body a PUT would carry."""
    _, spec = merged_spec(stored.spec, body.spec or {})
    return schemas.PresetIn(
        name=stored.name if body.name is None else body.name,
        spec=spec,
        labels=body.labels,
        project=stored.project if body.project is None else body.project,
    )


async def patch(
    request: Request, conn, settings: Settings, preset_id: uuid.UUID, body: schemas.PresetPatch
) -> schemas.PresetView:
    """Merge-patch a preset for the superleader: the patch is applied to the stored spec, so
    what it does not name (a grant, a vault ref) is kept; a concurrent edit makes
    it re-read and retry."""
    for _ in range(_PATCH_TRIES):
        stored = await repo.get(conn, preset_id)
        if stored is None or not stored.superleader_manageable:
            raise not_found("preset")
        merged = _merged_body(stored, body)
        _refuse_column_moves(request, stored, merged.model_dump())
        spec, model = await _validated(
            merged,
            settings,
            stored.spec,
            preset_id,
            human_only_base=stored.spec,
            request=request,
        )
        labels = stored.labels if merged.labels is None else merged.labels
        await check_vault(request, model, labels, merged.project)
        recheck = _unchanged_since(stored, labels=merged.labels is None, project=True)
        try:
            preset = await _save(
                request,
                conn,
                spec,
                merged.project,
                lambda m=merged, sp=spec, at=stored.updated_at, rc=recheck: repo.update(
                    conn, preset_id, m.name, sp, None, m.labels, at, m.project, rc
                ),
                _actor(request, True),
                refs_in(stored.spec),
            )
        except repo.PresetNotFound:
            raise not_found("preset") from None
        except repo.PresetChanged:
            continue
        except repo.PresetDefault as exc:
            raise _default_error(exc) from None
        except repo.PresetExists:
            raise _exists(merged.name) from None
        paths = changed_paths(stored.spec, spec)
        if merged.name != stored.name:
            paths.append("name")
        if merged.labels is not None and merged.labels != stored.labels:
            paths.append("labels")
        note_fields(request, paths)
        return _view(preset)
    raise ApiError(409, "conflict", "the preset changed since it was read; retry")


def privileged_delete_refused(flags: list[str]) -> ApiError:
    return ApiError(
        403,
        "privileged_target",
        "only a human can delete a preset with kube_cluster_access, kube_namespace_access, "
        "session_messaging or docker on",
        extra={"fields": flags},
    )


async def remove(request: Request, conn, preset_id: uuid.UUID, superleader: bool = False) -> None:
    """Delete a preset.

    The superleader may not delete a privileged preset (403 `privileged_target`, `fields` the
    flags that are on). The check runs on the spec read under the row lock, before the delete,
    so a human turning a flag on concurrently cannot be raced."""

    def recheck(spec: dict) -> None:
        if superleader and (flags := privileged(spec)):
            note_fields(request, flags)
            raise privileged_delete_refused(flags)

    try:
        await repo.delete(conn, preset_id, recheck=recheck)
    except repo.PresetNotFound:
        raise not_found("preset") from None
    except repo.PresetDefault as exc:
        raise _default_error(exc) from None
    except repo.LastPreset:
        raise ApiError(409, "last_preset", "the only preset cannot be deleted") from None


@router.get("/presets", response_model=list[schemas.PresetView])
async def list_presets(pool: Pool, project: str | None = None) -> list[schemas.PresetView]:
    async with pool.connection() as conn:
        return await list_all(conn, project=project)


@router.post("/presets", status_code=201, response_model=schemas.PresetView)
async def create_preset(
    body: schemas.PresetIn, request: Request, pool: Pool, settings: Cfg
) -> schemas.PresetView:
    async with pool.connection() as conn:
        return await create(request, conn, settings, body)


@router.get("/presets/{preset_id}", response_model=schemas.PresetView)
async def get_preset(preset_id: uuid.UUID, pool: Pool) -> schemas.PresetView:
    async with pool.connection() as conn:
        return await get_one(conn, preset_id)


@router.put("/presets/{preset_id}", response_model=schemas.PresetView)
async def replace_preset(
    preset_id: uuid.UUID, body: schemas.PresetIn, request: Request, pool: Pool, settings: Cfg
) -> schemas.PresetView:
    async with pool.connection() as conn:
        return await replace(request, conn, settings, preset_id, body)


@router.delete("/presets/{preset_id}", status_code=204, response_model=None)
async def delete_preset(preset_id: uuid.UUID, request: Request, pool: Pool) -> Response:
    async with pool.connection() as conn:
        await remove(request, conn, preset_id)
    return Response(status_code=204)


def _without_derived_key(spec: dict[str, Any], preset_id: uuid.UUID) -> dict[str, Any]:
    """The copy derives its own keys, so keys equal to the source's derived key are dropped."""
    cache = spec.get("cache")
    derived = default_cache_key(preset_id)
    if isinstance(cache, dict) and derived in (cache.get("restore_key"), cache.get("save_key")):
        keys = ("restore_key", "save_key")
        return {**spec, "cache": {k: v for k, v in cache.items() if k not in keys}}
    return spec


async def copy(
    conn,
    preset_id: uuid.UUID,
    explicit: str | None,
    superleader: bool = False,
    request: Request | None = None,
    project: str | None = None,
) -> schemas.PresetView:
    """Copy a preset under `explicit` (or a free `<name> copy`) into `project` (None: the
    source's project)."""
    if explicit is not None:
        _checked_name(explicit)
    source = await repo.get(conn, preset_id)
    if source is None or (superleader and not source.superleader_manageable):
        raise not_found("preset")
    project = source.project if project is None else project
    spec = _without_derived_key(source.spec, preset_id)
    if superleader:
        spec, removed = strip_grants(spec)
        if request is not None:
            note_fields(request, removed + (["project"] if project != source.project else []))
    if refs_in(spec):
        await check_vault(request, SessionSpec.model_validate(spec), source.labels, project)
    for _ in range(_COPY_TRIES):
        taken = {p.name.lower() for p in await repo.list_presets(conn)}
        name = explicit if explicit is not None else _copy_name(source.name, taken)
        try:
            async with conn.transaction():
                await hold_project(conn, project)
                created = await repo.create(
                    conn, name, spec, source.superleader_manageable, source.labels, project
                )
                attached, _ = ref_diff({}, refs_in(spec))
                await record_ref_events(
                    conn,
                    created.id,
                    _actor(request, superleader) if request else "user",
                    attached,
                    [],
                )
            return _view(created)
        except repo.PresetExists:
            if explicit is not None:
                raise _exists(name) from None
    raise ApiError(409, "conflict", f"could not find a free copy name for {source.name!r}")


@router.post("/presets/{preset_id}/copy", status_code=201, response_model=schemas.PresetView)
async def copy_preset(
    preset_id: uuid.UUID, request: Request, pool: Pool, body: schemas.CopyPreset | None = None
) -> schemas.PresetView:
    async with pool.connection() as conn:
        body = body or schemas.CopyPreset()
        return await copy(conn, preset_id, body.name, request=request, project=body.project)


async def by_ref(conn, ref: str) -> repo.Preset:
    """A preset by id or, failing that, by name (names are unique ignoring case)."""
    try:
        found = await repo.get(conn, uuid.UUID(ref))
    except ValueError:
        found = next(
            (p for p in await repo.list_presets(conn) if p.name.lower() == ref.lower()), None
        )
    if found is None:
        raise not_found("preset")
    return found

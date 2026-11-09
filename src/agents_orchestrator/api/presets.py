"""Preset routes: named SessionSpec templates. The api is the only writer of the presets table."""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import Any

from fastapi import APIRouter, Request, Response
from fastapi.concurrency import run_in_threadpool
from pydantic import SecretStr, ValidationError

from agents_orchestrator.api import schemas, secret_store, vault_client
from agents_orchestrator.api.errors import ApiError, not_found, project_error
from agents_orchestrator.api.sessions import Cfg, Pool, check_spec, hold_project
from agents_orchestrator.api.spec_edit import (
    guard_agent_edit,
    human_only_refused,
    merged_spec_in,
    note_fields,
)
from agents_orchestrator.api.vault_refs import check_refs, record_ref_events, ref_diff, refs_in
from agents_orchestrator.db import presets as repo
from agents_orchestrator.db import projects
from agents_orchestrator.preset_policy import (
    HUMAN_ONLY_COLUMNS,
    changed_paths,
    privileged,
    strip_grants,
)
from agents_orchestrator.secret_view import EnvMeta, redact_spec_dict
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import Ref, SessionSpec, default_cache_key

SECRET_MAX_BYTES = 1024 * 1024  # F1.9: a Kubernetes Secret holds at most 1 MiB

router = APIRouter(prefix="/api/v1")

_COPY = " copy"
_COPY_TRIES = 5
_PATCH_TRIES = 3


def _view(
    preset: repo.Preset, secrets: list[secret_store.SecretMeta] | None = None
) -> schemas.PresetView:
    metas = {m.name: EnvMeta(m.version, m.updated_at) for m in secrets or []}
    return schemas.PresetView(
        id=preset.id,
        name=preset.name,
        is_default=preset.is_default,
        superleader_manageable=preset.superleader_manageable,
        labels=preset.labels,
        project=preset.project,
        privileged=privileged(preset.spec),
        vault_reattach=preset.vault_reattach,
        spec=redact_spec_dict(preset.spec, metas),
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
) -> tuple[dict[str, Any], dict[str, SecretStr], SessionSpec]:
    """Check name and spec; return the spec as the caller sent it (no defaults filled), the
    protected values it carried, which never go into the spec, and the parsed spec.

    With `human_only_base` (the stored spec, `{}` on create) the human-only fields may only move
    toward safe, else 403 before anything is written."""
    _checked_name(body.name)
    spec, values = body.spec.split()
    if human_only_base is not None:
        dumped = spec.model_dump(mode="json", exclude_unset=True)
        guard_agent_edit(human_only_base, dumped, values, request, privileged=True)
    return await validated_spec(spec, settings, stored, preset_id), values, spec


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


def check_secret_size(value: SecretStr) -> None:
    """413 when a value does not fit one Kubernetes Secret."""
    if len(value.get_secret_value().encode()) > SECRET_MAX_BYTES:
        raise ApiError(413, "secret_too_large", f"a secret holds at most {SECRET_MAX_BYTES} bytes")


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


def store_failed() -> ApiError:
    return ApiError(502, "secret_store_failed", "could not store a protected variable")


async def forget(conn, kube, namespace: str, versions: list[tuple[uuid.UUID, str, int]]) -> None:
    """Best effort: delete the (preset id, name, version) Secrets written for a save that did not
    stick, never one a concurrent writer has made live since (`forget_unsaved`)."""
    if versions:
        await secret_store.forget_unsaved(conn, kube, namespace, versions)


async def _save(
    request: Request,
    conn,
    settings: Settings,
    spec: dict[str, Any],
    values: dict[str, SecretStr],
    project: str,
    save: Callable[[], Awaitable[repo.Preset]],
    actor: str = "user",
    old_refs: dict[str, Ref] | None = None,
) -> tuple[repo.Preset, list[secret_store.SecretMeta]]:
    """Save the preset row and its secrets as one step; a failure leaves the old state.

    In the same transaction: the names the new spec defines leave the preset's re-attach list,
    and the vault refs attached or detached against `old_refs` are recorded as events by `actor`.

    The preset's `project` is checked and held first (`hold_project`). The row save, the new
    Secret versions and the retirement of dropped variables share one transaction. The new
    versions are written to Kubernetes before it commits, so when anything fails the transaction
    rolls back and the versions written so far are deleted. The previous versions are collected
    only after the commit. A protected variable sent without a value is left as it is.
    """
    declared = sorted(n for n, e in spec.get("env", {}).items() if e.get("protected"))
    created: list[tuple[uuid.UUID, str, int]] = []
    kube = None
    try:
        async with conn.transaction():
            await hold_project(conn, project)
            preset = await save()
            preset = await _after_save(conn, preset, spec, actor, old_refs or {})
            if values or await secret_store.has_rows(conn, preset.id):
                kube = secret_store.kube_of(request)
                for name, value in values.items():
                    meta = await secret_store.set_secret(
                        conn, kube, settings.namespace, preset.id, name, value, collect=False
                    )
                    created.append((preset.id, name, meta.version))
                await secret_store.sync_declared(
                    conn, kube, settings.namespace, preset.id, declared, collect=False
                )
    except secret_store.SecretStoreError as exc:
        await forget(conn, kube, settings.namespace, created)
        if isinstance(exc, secret_store.PresetNotFound):
            raise not_found("preset") from None
        raise store_failed() from None
    except BaseException:
        if kube is not None:
            await forget(conn, kube, settings.namespace, created)
        raise
    if kube is not None:
        await secret_store.gc(conn, kube, settings.namespace, preset_id=preset.id)
    return preset, (await secret_store.list_metas(conn, [preset.id])).get(preset.id, [])


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
    metas = await secret_store.list_metas(conn, [p.id for p in presets])
    return [_view(p, metas.get(p.id)) for p in presets]


async def create(
    request: Request,
    conn,
    settings: Settings,
    body: schemas.PresetIn,
    superleader: bool = False,
) -> schemas.PresetView:
    if superleader and body.superleader_manageable is False:
        raise manageable_main_port_only()
    spec, values, model = await _validated(
        body, settings, human_only_base={} if superleader else None, request=request
    )
    manageable = superleader or bool(body.superleader_manageable)
    project = projects.DEFAULT if body.project is None else body.project
    await check_vault(request, model, body.labels or [], project)
    try:
        preset, metas = await _save(
            request,
            conn,
            settings,
            spec,
            values,
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
    return _view(preset, metas)


async def get_one(conn, preset_id: uuid.UUID) -> schemas.PresetView:
    preset = await repo.get(conn, preset_id)
    if preset is None:
        raise not_found("preset")
    secrets = (await secret_store.list_metas(conn, [preset_id])).get(preset_id)
    return _view(preset, secrets)


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
    spec, values, model = await _validated(
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
        preset, metas = await _save(
            request,
            conn,
            settings,
            spec,
            values,
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
    return _view(preset, metas)


def _merged_body(stored: repo.Preset, body: schemas.PresetPatch) -> schemas.PresetIn:
    """The stored spec with the patch merged in, as the body a PUT would carry."""
    _, spec = merged_spec_in(stored.spec, body.spec or {})
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
    what it does not name (a stored protected variable, a grant) is kept; a concurrent edit makes
    it re-read and retry."""
    for _ in range(_PATCH_TRIES):
        stored = await repo.get(conn, preset_id)
        if stored is None or not stored.superleader_manageable:
            raise not_found("preset")
        merged = _merged_body(stored, body)
        _refuse_column_moves(request, stored, merged.model_dump())
        spec, values, model = await _validated(
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
            preset, metas = await _save(
                request,
                conn,
                settings,
                spec,
                values,
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
        return _view(preset, metas)
    raise ApiError(409, "conflict", "the preset changed since it was read; retry")


def privileged_delete_refused(flags: list[str]) -> ApiError:
    return ApiError(
        403,
        "privileged_target",
        "only a human can delete a preset with kube_cluster_access, kube_namespace_access, "
        "session_messaging or docker on",
        extra={"fields": flags},
    )


async def remove(
    request: Request,
    conn,
    settings: Settings,
    preset_id: uuid.UUID,
    superleader: bool = False,
) -> None:
    """Delete a preset and retire its Secret rows.

    The superleader may not delete a privileged preset (403 `privileged_target`, `fields` the
    flags that are on). The check runs on the spec read under the row lock, before the delete
    and before any Secret cleanup, so a human turning a flag on concurrently cannot be raced."""

    def recheck(spec: dict) -> None:
        if superleader and (flags := privileged(spec)):
            note_fields(request, flags)
            raise privileged_delete_refused(flags)

    try:
        async with conn.transaction():
            await repo.delete(conn, preset_id, recheck=recheck)
            # no FK: the rows outlive the preset, so retire them here; gc then treats them
            # as dead and deletes the versions no live session names
            collect = await secret_store.has_rows(conn, preset_id)
            if collect:
                kube = secret_store.kube_of(request)
                await secret_store.sync_declared(
                    conn, kube, settings.namespace, preset_id, [], collect=False
                )
        if collect:
            await secret_store.gc(conn, kube, settings.namespace, preset_id=preset_id)
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
async def delete_preset(
    preset_id: uuid.UUID, request: Request, pool: Pool, settings: Cfg
) -> Response:
    async with pool.connection() as conn:
        await remove(request, conn, settings, preset_id)
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


async def put_secret(
    request: Request,
    conn,
    settings: Settings,
    preset: str,
    name: str,
    body: schemas.SecretIn,
    superleader: bool = False,
) -> schemas.SecretView:
    """Set the value of a protected variable (a new version). The value is never echoed.

    The superleader may not set one on a privileged preset (403 `privileged_target`)."""
    check_secret_size(body.value)
    found = await by_ref(conn, preset)

    def refuse_privileged(spec: dict) -> None:
        guard_agent_edit(spec, spec, [name], request, privileged=True)

    def refuse_undeclared(spec: dict) -> None:
        entry = spec.get("env", {}).get(name)
        if not (isinstance(entry, dict) and entry.get("protected")):
            raise ApiError(
                422, "invalid_request", f"{name!r} is not a protected variable of this preset"
            )

    def recheck(spec: dict) -> None:
        # runs on the spec read under the preset row lock, so a concurrent unprotect or removal
        # of the entry (or a grant turned on) cannot slip a write through
        if superleader:
            refuse_privileged(spec)
        refuse_undeclared(spec)

    recheck(found.spec)
    kube = secret_store.kube_of(request)
    try:
        meta = await secret_store.set_secret(
            conn,
            kube,
            settings.namespace,
            found.id,
            name,
            body.value,
            recheck=recheck,
        )
    except secret_store.PresetNotFound:
        raise not_found("preset") from None
    except secret_store.SecretStoreError:
        raise store_failed() from None
    return schemas.SecretView(
        name=meta.name, set=meta.set, version=meta.version, updated_at=meta.updated_at
    )


@router.put("/presets/{preset}/secrets/{name}", response_model=schemas.SecretView)
async def set_preset_secret(
    preset: str,
    name: str,
    body: schemas.SecretIn,
    request: Request,
    pool: Pool,
    settings: Cfg,
) -> schemas.SecretView:
    async with pool.connection() as conn:
        return await put_secret(request, conn, settings, preset, name, body)

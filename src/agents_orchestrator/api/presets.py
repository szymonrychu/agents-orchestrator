"""Preset routes: named SessionSpec templates. The api is the only writer of the presets table."""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import APIRouter, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.exceptions import RequestValidationError
from pydantic import SecretStr, ValidationError

from agents_orchestrator.api import schemas, secret_store
from agents_orchestrator.api.errors import ApiError, not_found
from agents_orchestrator.api.sessions import Cfg, Pool, check_spec
from agents_orchestrator.db import presets as repo
from agents_orchestrator.preset_policy import (
    changed_paths,
    human_only_changes,
    merge_patch,
    strip_grants,
)
from agents_orchestrator.secret_view import EnvMeta, redact_spec_dict
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec, SessionSpecIn, default_cache_key

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
        spec=redact_spec_dict(preset.spec, metas),
        created_at=preset.created_at,
        updated_at=preset.updated_at,
    )


def _checked_name(name: str) -> str:
    try:
        return repo.validate_preset_name(name)
    except ValueError as exc:
        raise ApiError(422, "invalid_request", str(exc)) from None


def human_only_refused(paths: list[str]) -> ApiError:
    return ApiError(
        403,
        "human_only_field",
        f"only a human can change: {', '.join(paths)}",
        extra={"fields": paths},
    )


def _note_fields(request: Request, paths: list[str]) -> None:
    """Hand the changed (or refused) paths to the admin audit; paths only, never values."""
    request.state.preset_fields = sorted(paths)


async def _validated(
    body: schemas.PresetIn,
    settings: Settings,
    stored: dict[str, Any] | None = None,
    preset_id: uuid.UUID | None = None,
    human_only_base: dict[str, Any] | None = None,
    request: Request | None = None,
) -> tuple[dict[str, Any], dict[str, SecretStr]]:
    """Check name and spec; return the spec as the caller sent it (no defaults filled) and the
    protected values it carried, which never go into the spec.

    With `human_only_base` (the stored spec, `{}` on create) the human-only fields may only move
    toward safe, else 403 before anything is written."""
    _checked_name(body.name)
    spec, values = body.spec.split()
    if human_only_base is not None:
        refused = human_only_changes(
            human_only_base, spec.model_dump(mode="json", exclude_unset=True)
        )
        if refused:
            if request is not None:
                _note_fields(request, refused)
            raise human_only_refused(refused)
    stored_spec = None
    if stored is not None:
        try:
            stored_spec = SessionSpec.model_validate(stored)
        except ValidationError:
            stored_spec = None
    derived = default_cache_key(preset_id) if preset_id is not None else None
    await run_in_threadpool(check_spec, spec, settings, stored_spec, derived_cache_key=derived)
    return spec.model_dump(mode="json", exclude_unset=True), values


def _store_failed() -> ApiError:
    return ApiError(502, "secret_store_failed", "could not store a protected variable")


async def _forget(kube, namespace: str, names: list[str]) -> None:
    """Best effort: delete Secret versions written for a save that did not stick."""
    for name in names:
        with contextlib.suppress(Exception):
            await kube.delete("Secret", namespace, name)


async def _save(
    request: Request,
    conn,
    settings: Settings,
    spec: dict[str, Any],
    values: dict[str, SecretStr],
    save: Callable[[], Awaitable[repo.Preset]],
) -> tuple[repo.Preset, list[secret_store.SecretMeta]]:
    """Save the preset row and its secrets as one step; a failure leaves the old state.

    The row save, the new Secret versions and the retirement of dropped variables share one
    transaction. The new versions are written to Kubernetes before it commits, so when anything
    fails the transaction rolls back and the versions written so far are deleted. The previous
    versions are collected only after the commit. A protected variable sent without a value is
    left as it is.
    """
    declared = sorted(n for n, e in spec.get("env", {}).items() if e.get("protected"))
    created: list[str] = []
    kube = None
    try:
        async with conn.transaction():
            preset = await save()
            if values or await secret_store.has_rows(conn, preset.id):
                kube = secret_store.kube_of(request)
                for name, value in values.items():
                    meta = await secret_store.set_secret(
                        conn, kube, settings.namespace, preset.id, name, value, collect=False
                    )
                    created.append(secret_store.secret_name(preset.id, name, meta.version))
                await secret_store.sync_declared(
                    conn, kube, settings.namespace, preset.id, declared, collect=False
                )
    except secret_store.SecretStoreError as exc:
        await _forget(kube, settings.namespace, created)
        if isinstance(exc, secret_store.PresetNotFound):
            raise not_found("preset") from None
        raise _store_failed() from None
    except BaseException:
        if kube is not None:
            await _forget(kube, settings.namespace, created)
        raise
    if kube is not None:
        await secret_store.gc(conn, kube, settings.namespace, preset_id=preset.id)
    return preset, (await secret_store.list_metas(conn, [preset.id])).get(preset.id, [])


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


async def list_all(conn, only_manageable: bool = False) -> list[schemas.PresetView]:
    presets = await repo.list_presets(conn)
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
    spec, values = await _validated(
        body, settings, human_only_base={} if superleader else None, request=request
    )
    manageable = superleader or bool(body.superleader_manageable)
    try:
        preset, metas = await _save(
            request,
            conn,
            settings,
            spec,
            values,
            lambda: repo.create(conn, body.name, spec, manageable, body.labels),
        )
    except repo.PresetExists:
        raise _exists(body.name) from None
    if superleader:
        _note_fields(
            request, changed_paths({}, spec) + ["name"] + (["labels"] if body.labels else [])
        )
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
    if stored is None:
        raise not_found("preset")
    spec, values = await _validated(
        body,
        settings,
        stored.spec,
        preset_id,
        human_only_base=stored.spec if superleader else None,
        request=request,
    )
    expected = stored.updated_at if superleader else None
    try:
        preset, metas = await _save(
            request,
            conn,
            settings,
            spec,
            values,
            lambda: repo.update(conn, preset_id, body.name, spec, flag, body.labels, expected),
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
        _note_fields(request, paths)
    return _view(preset, metas)


def _secret_in_patch(patch: dict[str, Any]) -> None:
    env = patch.get("env")
    for name, entry in (env if isinstance(env, dict) else {}).items():
        if isinstance(entry, dict) and entry.get("protected") and entry.get("value") is not None:
            raise ApiError(
                422,
                "secret_in_spec",
                f"protected variable {name} carries a value; set it through the secret route",
            )


def _merged_body(stored: repo.Preset, body: schemas.PresetPatch) -> schemas.PresetIn:
    """The stored spec with the patch merged in, as the body a PUT would carry."""
    merged = merge_patch(stored.spec, body.spec or {})
    try:
        spec = SessionSpecIn.model_validate(merged)
    except ValidationError as exc:
        errors = [{**e, "loc": ("body", "spec", *e["loc"])} for e in exc.errors()]
        raise RequestValidationError(errors) from None
    return schemas.PresetIn(name=body.name or stored.name, spec=spec, labels=body.labels)


async def patch(
    request: Request, conn, settings: Settings, preset_id: uuid.UUID, body: schemas.PresetPatch
) -> schemas.PresetView:
    """Merge-patch a preset for the superleader: the patch is applied to the stored spec, so
    what it does not name (a stored protected variable, a grant) is kept; a concurrent edit makes
    it re-read and retry."""
    _secret_in_patch(body.spec or {})
    for _ in range(_PATCH_TRIES):
        stored = await repo.get(conn, preset_id)
        if stored is None:
            raise not_found("preset")
        merged = _merged_body(stored, body)
        spec, values = await _validated(
            merged,
            settings,
            stored.spec,
            preset_id,
            human_only_base=stored.spec,
            request=request,
        )
        try:
            preset, metas = await _save(
                request,
                conn,
                settings,
                spec,
                values,
                lambda m=merged, sp=spec, at=stored.updated_at: repo.update(
                    conn, preset_id, m.name, sp, None, m.labels, at
                ),
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
        _note_fields(request, paths)
        return _view(preset, metas)
    raise ApiError(409, "conflict", "the preset changed since it was read; retry")


async def remove(request: Request, conn, settings: Settings, preset_id: uuid.UUID) -> None:
    try:
        async with conn.transaction():
            await repo.delete(conn, preset_id)
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
async def list_presets(pool: Pool) -> list[schemas.PresetView]:
    async with pool.connection() as conn:
        return await list_all(conn)


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
) -> schemas.PresetView:
    if explicit is not None:
        _checked_name(explicit)
    source = await repo.get(conn, preset_id)
    if source is None:
        raise not_found("preset")
    spec = _without_derived_key(source.spec, preset_id)
    if superleader:
        spec, removed = strip_grants(spec)
        if request is not None:
            _note_fields(request, removed)
    for _ in range(_COPY_TRIES):
        taken = {p.name.lower() for p in await repo.list_presets(conn)}
        name = explicit if explicit is not None else _copy_name(source.name, taken)
        try:
            return _view(
                await repo.create(conn, name, spec, source.superleader_manageable, source.labels)
            )
        except repo.PresetExists:
            if explicit is not None:
                raise _exists(name) from None
    raise ApiError(409, "conflict", f"could not find a free copy name for {source.name!r}")


@router.post("/presets/{preset_id}/copy", status_code=201, response_model=schemas.PresetView)
async def copy_preset(
    preset_id: uuid.UUID, pool: Pool, body: schemas.CopyPreset | None = None
) -> schemas.PresetView:
    async with pool.connection() as conn:
        return await copy(conn, preset_id, body.name if body else None)


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
    request: Request, conn, settings: Settings, preset: str, name: str, body: schemas.SecretIn
) -> schemas.SecretView:
    """Set the value of a protected variable (a new version). The value is never echoed."""
    value = body.value.get_secret_value()
    if len(value.encode()) > SECRET_MAX_BYTES:
        raise ApiError(413, "secret_too_large", f"a secret holds at most {SECRET_MAX_BYTES} bytes")
    found = await by_ref(conn, preset)
    entry = found.spec.get("env", {}).get(name)
    if not (isinstance(entry, dict) and entry.get("protected")):
        raise ApiError(
            422, "invalid_request", f"{name!r} is not a protected variable of this preset"
        )
    kube = secret_store.kube_of(request)
    try:
        meta = await secret_store.set_secret(
            conn, kube, settings.namespace, found.id, name, body.value
        )
    except secret_store.PresetNotFound:
        raise not_found("preset") from None
    except secret_store.SecretStoreError:
        raise _store_failed() from None
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

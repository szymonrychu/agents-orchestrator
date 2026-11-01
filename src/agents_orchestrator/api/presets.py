"""Preset routes: named SessionSpec templates. The api is the only writer of the presets table."""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Request, Response
from fastapi.concurrency import run_in_threadpool
from pydantic import SecretStr, ValidationError

from agents_orchestrator.api import schemas, secret_store
from agents_orchestrator.api.errors import ApiError, not_found
from agents_orchestrator.api.sessions import Cfg, Pool, check_spec
from agents_orchestrator.db import presets as repo
from agents_orchestrator.secret_view import EnvMeta, redact_spec_dict
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec, default_cache_key

SECRET_MAX_BYTES = 1024 * 1024  # F1.9: a Kubernetes Secret holds at most 1 MiB

router = APIRouter(prefix="/api/v1")

_COPY = " copy"
_COPY_TRIES = 5


def _view(
    preset: repo.Preset, secrets: list[secret_store.SecretMeta] | None = None
) -> schemas.PresetView:
    metas = {m.name: EnvMeta(m.version, m.updated_at) for m in secrets or []}
    return schemas.PresetView(
        id=preset.id,
        name=preset.name,
        is_default=preset.is_default,
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
) -> tuple[dict[str, Any], dict[str, SecretStr]]:
    """Check name and spec; return the spec as the caller sent it (no defaults filled) and the
    protected values it carried, which never go into the spec."""
    _checked_name(body.name)
    spec, values = body.spec.split()
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


async def _apply_secrets(
    request: Request,
    conn,
    settings: Settings,
    preset_id: uuid.UUID,
    spec: dict[str, Any],
    values: dict[str, SecretStr],
) -> list[secret_store.SecretMeta]:
    """Write the sent values as new versions, retire the variables the spec dropped and return
    the preset's live secrets. A protected variable sent without a value is left as it is."""
    declared = sorted(n for n, e in spec.get("env", {}).items() if e.get("protected"))
    if values or await secret_store.has_rows(conn, preset_id):
        kube = secret_store.kube_of(request)
        try:
            for name, value in values.items():
                await secret_store.set_secret(
                    conn, kube, settings.namespace, preset_id, name, value
                )
            await secret_store.sync_declared(conn, kube, settings.namespace, preset_id, declared)
        except secret_store.PresetNotFound:
            raise not_found("preset") from None
        except secret_store.SecretStoreError:
            raise _store_failed() from None
    return (await secret_store.list_metas(conn, [preset_id])).get(preset_id, [])


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


@router.get("/presets", response_model=list[schemas.PresetView])
async def list_presets(pool: Pool) -> list[schemas.PresetView]:
    async with pool.connection() as conn:
        presets = await repo.list_presets(conn)
        metas = await secret_store.list_metas(conn, [p.id for p in presets])
        return [_view(p, metas.get(p.id)) for p in presets]


@router.post("/presets", status_code=201, response_model=schemas.PresetView)
async def create_preset(
    body: schemas.PresetIn, request: Request, pool: Pool, settings: Cfg
) -> schemas.PresetView:
    spec, values = await _validated(body, settings)
    async with pool.connection() as conn:
        try:
            preset = await repo.create(conn, body.name, spec)
        except repo.PresetExists:
            raise _exists(body.name) from None
        return _view(preset, await _apply_secrets(request, conn, settings, preset.id, spec, values))


@router.get("/presets/{preset_id}", response_model=schemas.PresetView)
async def get_preset(preset_id: uuid.UUID, pool: Pool) -> schemas.PresetView:
    async with pool.connection() as conn:
        preset = await repo.get(conn, preset_id)
        if preset is None:
            raise not_found("preset")
        secrets = (await secret_store.list_metas(conn, [preset_id])).get(preset_id)
    return _view(preset, secrets)


@router.put("/presets/{preset_id}", response_model=schemas.PresetView)
async def replace_preset(
    preset_id: uuid.UUID, body: schemas.PresetIn, request: Request, pool: Pool, settings: Cfg
) -> schemas.PresetView:
    async with pool.connection() as conn:
        stored = await repo.get(conn, preset_id)
        if stored is None:
            raise not_found("preset")
        spec, values = await _validated(body, settings, stored.spec, preset_id)
        try:
            preset = await repo.update(conn, preset_id, body.name, spec)
            return _view(
                preset, await _apply_secrets(request, conn, settings, preset_id, spec, values)
            )
        except repo.PresetNotFound:
            raise not_found("preset") from None
        except repo.PresetDefault as exc:
            raise _default_error(exc) from None
        except repo.PresetExists:
            raise _exists(body.name) from None


@router.delete("/presets/{preset_id}", status_code=204, response_model=None)
async def delete_preset(
    preset_id: uuid.UUID, request: Request, pool: Pool, settings: Cfg
) -> Response:
    async with pool.connection() as conn:
        try:
            await repo.delete(conn, preset_id)
            await _apply_secrets(request, conn, settings, preset_id, {}, {})
        except repo.PresetNotFound:
            raise not_found("preset") from None
        except repo.PresetDefault as exc:
            raise _default_error(exc) from None
        except repo.LastPreset:
            raise ApiError(409, "last_preset", "the only preset cannot be deleted") from None
    return Response(status_code=204)


def _without_derived_key(spec: dict[str, Any], preset_id: uuid.UUID) -> dict[str, Any]:
    """The copy derives its own keys, so keys equal to the source's derived key are dropped."""
    cache = spec.get("cache")
    derived = default_cache_key(preset_id)
    if isinstance(cache, dict) and derived in (cache.get("restore_key"), cache.get("save_key")):
        keys = ("restore_key", "save_key")
        return {**spec, "cache": {k: v for k, v in cache.items() if k not in keys}}
    return spec


@router.post("/presets/{preset_id}/copy", status_code=201, response_model=schemas.PresetView)
async def copy_preset(
    preset_id: uuid.UUID, pool: Pool, body: schemas.CopyPreset | None = None
) -> schemas.PresetView:
    explicit = body.name if body else None
    if explicit is not None:
        _checked_name(explicit)
    async with pool.connection() as conn:
        source = await repo.get(conn, preset_id)
        if source is None:
            raise not_found("preset")
        spec = _without_derived_key(source.spec, preset_id)
        for _ in range(_COPY_TRIES):
            taken = {p.name.lower() for p in await repo.list_presets(conn)}
            name = explicit if explicit is not None else _copy_name(source.name, taken)
            try:
                return _view(await repo.create(conn, name, spec))
            except repo.PresetExists:
                if explicit is not None:
                    raise _exists(name) from None
        raise ApiError(409, "conflict", f"could not find a free copy name for {source.name!r}")


async def _by_ref(conn, ref: str) -> repo.Preset:
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


@router.put("/presets/{preset}/secrets/{name}", response_model=schemas.SecretView)
async def set_preset_secret(
    preset: str,
    name: str,
    body: schemas.SecretIn,
    request: Request,
    pool: Pool,
    settings: Cfg,
) -> schemas.SecretView:
    """Set the value of a protected variable (a new version). The value is never echoed."""
    value = body.value.get_secret_value()
    if len(value.encode()) > SECRET_MAX_BYTES:
        raise ApiError(413, "secret_too_large", f"a secret holds at most {SECRET_MAX_BYTES} bytes")
    async with pool.connection() as conn:
        found = await _by_ref(conn, preset)
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

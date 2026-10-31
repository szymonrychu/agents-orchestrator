"""Preset routes: named SessionSpec templates. The api is the only writer of the presets table."""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Response
from fastapi.concurrency import run_in_threadpool
from pydantic import ValidationError

from agents_orchestrator.api import schemas
from agents_orchestrator.api.errors import ApiError, not_found
from agents_orchestrator.api.sessions import Cfg, Pool, check_spec
from agents_orchestrator.db import presets as repo
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec, default_cache_key

router = APIRouter(prefix="/api/v1")

_COPY = " copy"
_COPY_TRIES = 5


def _view(preset: repo.Preset) -> schemas.PresetView:
    return schemas.PresetView(
        id=preset.id,
        name=preset.name,
        is_default=preset.is_default,
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
) -> dict[str, Any]:
    """Check name and spec; return the spec as the caller sent it (no defaults filled)."""
    _checked_name(body.name)
    stored_spec = None
    if stored is not None:
        try:
            stored_spec = SessionSpec.model_validate(stored)
        except ValidationError:
            stored_spec = None
    derived = default_cache_key(preset_id) if preset_id is not None else None
    await run_in_threadpool(check_spec, body.spec, settings, stored_spec, derived_cache_key=derived)
    return body.spec.model_dump(mode="json", exclude_unset=True)


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
        return [_view(p) for p in await repo.list_presets(conn)]


@router.post("/presets", status_code=201, response_model=schemas.PresetView)
async def create_preset(body: schemas.PresetIn, pool: Pool, settings: Cfg) -> schemas.PresetView:
    spec = await _validated(body, settings)
    async with pool.connection() as conn:
        try:
            return _view(await repo.create(conn, body.name, spec))
        except repo.PresetExists:
            raise _exists(body.name) from None


@router.get("/presets/{preset_id}", response_model=schemas.PresetView)
async def get_preset(preset_id: uuid.UUID, pool: Pool) -> schemas.PresetView:
    async with pool.connection() as conn:
        preset = await repo.get(conn, preset_id)
    if preset is None:
        raise not_found("preset")
    return _view(preset)


@router.put("/presets/{preset_id}", response_model=schemas.PresetView)
async def replace_preset(
    preset_id: uuid.UUID, body: schemas.PresetIn, pool: Pool, settings: Cfg
) -> schemas.PresetView:
    async with pool.connection() as conn:
        stored = await repo.get(conn, preset_id)
        if stored is None:
            raise not_found("preset")
        spec = await _validated(body, settings, stored.spec, preset_id)
        try:
            return _view(await repo.update(conn, preset_id, body.name, spec))
        except repo.PresetNotFound:
            raise not_found("preset") from None
        except repo.PresetDefault as exc:
            raise _default_error(exc) from None
        except repo.PresetExists:
            raise _exists(body.name) from None


@router.delete("/presets/{preset_id}", status_code=204, response_model=None)
async def delete_preset(preset_id: uuid.UUID, pool: Pool) -> Response:
    async with pool.connection() as conn:
        try:
            await repo.delete(conn, preset_id)
        except repo.PresetNotFound:
            raise not_found("preset") from None
        except repo.PresetDefault as exc:
            raise _default_error(exc) from None
        except repo.LastPreset:
            raise ApiError(409, "last_preset", "the only preset cannot be deleted") from None
    return Response(status_code=204)


def _without_derived_key(spec: dict[str, Any], preset_id: uuid.UUID) -> dict[str, Any]:
    """The copy derives its own key, so a key equal to the source's derived key is dropped."""
    cache = spec.get("cache")
    if isinstance(cache, dict) and cache.get("key") == default_cache_key(preset_id):
        return {**spec, "cache": {k: v for k, v in cache.items() if k != "key"}}
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

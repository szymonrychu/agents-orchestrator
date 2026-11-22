"""Volumes api (user, main port): project-labelled shared RWX volumes.

Rows live in `volumes`, `volume_mounts` and `volume_events` (`db.volumes` is their only writer);
the reconciler applies the PVC, the grow, the delete and the restore and save Jobs. The routes
only record the request and answer 202 for the asynchronous ones. Answers carry names, ids,
sizes, states, modes and slugs, never file names or content; description and labels are never
logged.

HARD RULE: a volume that a session holds an unreleased claim on is never restored: the explicit
restore answers 409 `volume_mounted` (the guard and the reconciler's pod check are in
`db.volumes.begin_explicit_restore` and `reconciler.volumes`).
"""

from __future__ import annotations

import logging
import uuid
from typing import Annotated, Any

import psycopg
from fastapi import APIRouter, Query, Request

from agents_orchestrator.api import schemas
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.service_auth import calling_service
from agents_orchestrator.api.sessions import Cfg, Pool
from agents_orchestrator.db import backups as backups_repo
from agents_orchestrator.db import volumes as repo

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1")

VERSIONS_SHOWN = 20
_Before = Annotated[int | None, Query(ge=1)]
_Limit = Annotated[int, Query(ge=1, le=repo.MAX_LIMIT)]

_ERRORS: dict[int | str, dict[str, Any]] = {404: {"description": "Unknown or deleted volume"}}


def _actor(request: Request) -> str:
    """Who a user action is recorded as: the main port has no user identity, so a fixed label
    (or the calling service)."""
    return calling_service(request) or "user"


def _not_found() -> ApiError:
    return ApiError(404, "volume_not_found", "unknown volume")


def _backup_name(name: str) -> str:
    return f"{backups_repo.RESERVED_PREFIX}{name}"


async def _view(conn: psycopg.AsyncConnection, v: repo.Volume) -> schemas.VolumeView:
    current = await backups_repo.current(conn, _backup_name(v.name))
    return schemas.VolumeView(
        id=v.id,
        name=v.name,
        projects=v.projects,
        description=v.description,
        labels=v.labels,
        exclude=v.exclude,
        size=v.size,
        storage_class=v.storage_class,
        state=v.state,
        state_reason=v.state_reason,
        attachments=len(await repo.attaching_presets(conn, v.id)),
        mounted=len(await repo.unreleased_claims(conn, v.id)),
        current_version=current.id if current else None,
        content_version=v.content_version,
        last_save_at=current.created_at if current else None,
        created_at=v.created_at,
    )


async def _require(conn: psycopg.AsyncConnection, volume_id: uuid.UUID) -> repo.Volume:
    found = await repo.get(conn, volume_id)
    if found is None:
        raise _not_found()
    return found


def _bad(exc: ValueError) -> ApiError:
    return ApiError(422, "invalid_request", str(exc))


@router.get("/volumes", response_model=list[schemas.VolumeView])
async def list_volumes(pool: Pool, project: str | None = None) -> list[schemas.VolumeView]:
    async with pool.connection() as conn:
        return [await _view(conn, v) for v in await repo.list(conn, project=project)]


@router.post(
    "/volumes",
    status_code=201,
    response_model=schemas.VolumeView,
    responses={409: {"description": "volume_exists: a live volume has this name"}},
)
async def create_volume(
    request: Request, pool: Pool, settings: Cfg, body: schemas.VolumeCreateIn
) -> schemas.VolumeView:
    actor = _actor(request)
    try:
        async with pool.connection() as conn, conn.transaction():
            created = await repo.create(
                conn,
                name=body.name,
                projects=body.projects,
                size=body.size or settings.volume_default_size,
                description=body.description,
                labels=body.labels,
                exclude=body.exclude,
                storage_class=settings.volume_storage_class,
                created_by=actor,
                actor_kind="user",
                actor=actor,
                max_size=settings.volume_max_size,
            )
            view = await _view(conn, created)
    except repo.VolumeExists:
        raise ApiError(409, "volume_exists", f"volume {body.name!r} already exists") from None
    except repo.OverCap as exc:
        raise ApiError(422, "invalid_request", str(exc)) from None
    except ValueError as exc:
        raise _bad(exc) from None
    log.info("audit: volume created name=%s by=%s", body.name, actor)
    return view


@router.get("/volumes/{volume_id}", response_model=schemas.VolumeDetail, responses=_ERRORS)
async def get_volume(pool: Pool, volume_id: uuid.UUID) -> schemas.VolumeDetail:
    async with pool.connection() as conn:
        v = await _require(conn, volume_id)
        base = await _view(conn, v)
        presets = [
            schemas.VolumeAttachmentView(
                preset_id=a.preset_id,
                preset_name=a.name,
                mode=a.attachment.get("mode", "ro"),
                mount_path=_mount_path(a.attachment, v.name),
                backup_restore=bool(a.attachment.get("backup_restore", False)),
            )
            for a in await repo.attaching_presets(conn, v.id)
        ]
        claims = [
            schemas.VolumeClaimView(session_slug=slug, mode=c.mode)
            for c, slug in await repo.claim_slugs(conn, v.id)
        ]
        page = await backups_repo.versions(
            conn, _backup_name(v.name), limit=VERSIONS_SHOWN, cursor=None
        )
        versions = [
            schemas.VolumeVersionView(
                id=x.id,
                created_at=x.created_at,
                origin=x.origin,
                live=bool((x.manifest or {}).get("live")),
            )
            for x in page.items
        ]
    return schemas.VolumeDetail(
        **base.model_dump(), presets=presets, claims=claims, versions=versions
    )


def _mount_path(attachment: dict[str, Any], name: str) -> str:
    return attachment.get("mount_path") or f"/volumes/{name}"


@router.patch(
    "/volumes/{volume_id}",
    response_model=schemas.VolumeView,
    responses={
        **_ERRORS,
        409: {
            "description": "shrink_refused, project_in_use (an attaching preset would lose its"
            " scope) or volume_busy (the volume is not ready to grow, or is being deleted)"
        },
    },
)
async def patch_volume(
    request: Request, pool: Pool, settings: Cfg, volume_id: uuid.UUID, body: schemas.VolumePatchIn
) -> schemas.VolumeView:
    actor = _actor(request)
    try:
        async with pool.connection() as conn, conn.transaction():
            await repo.lock(conn, volume_id)  # the check and the write see one state
            if body.projects is not None:
                keep = {p.strip() for p in body.projects}
                for a in await repo.attaching_presets(conn, volume_id):
                    if not a.scope & keep:
                        raise ApiError(
                            409,
                            "project_in_use",
                            f"preset {a.name!r} attaches this volume and depends on a removed"
                            " project",
                        )
            updated = await repo.update(
                conn,
                volume_id,
                size=body.size,
                description=body.description,
                labels=body.labels,
                projects=body.projects,
                exclude=body.exclude,
                actor_kind="user",
                actor=actor,
                max_size=settings.volume_max_size,
            )
            return await _view(conn, updated)
    except repo.VolumeNotFound:
        raise _not_found() from None
    except repo.ShrinkRefused as exc:
        raise ApiError(409, "shrink_refused", str(exc)) from None
    except repo.VolumeBusy as exc:
        raise ApiError(409, "volume_busy", str(exc)) from None
    except repo.OverCap as exc:
        raise ApiError(422, "invalid_request", str(exc)) from None
    except ValueError as exc:
        raise _bad(exc) from None


class _Refused(Exception):
    def __init__(self, code: str, message: str):
        self.code, self.message = code, message


@router.delete(
    "/volumes/{volume_id}",
    status_code=202,
    response_model=schemas.VolumeView,
    responses={
        **_ERRORS,
        409: {
            "description": "volume_attached (a preset attaches it), volume_mounted or volume_busy"
            " (a restore or save is running)"
        },
    },
)
async def delete_volume(request: Request, pool: Pool, volume_id: uuid.UUID) -> schemas.VolumeView:
    """Mark the volume `deleting`; the reconciler removes the PVC. The checks run under the
    row lock the state change takes, so an attach or claim cannot slip in between; a refusal
    rolls the state change back and records `delete_refused`."""
    actor = _actor(request)
    try:
        async with pool.connection() as conn, conn.transaction():
            v = await _require(conn, volume_id)
            await repo.mark_deleting(conn, volume_id)
            if await repo.attaching_presets(conn, volume_id):
                raise _Refused("volume_attached", "a preset attaches this volume")
            if await repo.unreleased_claims(conn, volume_id):
                raise _Refused("volume_mounted", "a session has this volume mounted")
            view = await _view(conn, v)
            view.state = "deleting"
    except _Refused as refused:
        async with pool.connection() as conn, conn.transaction():
            await repo.record_event(
                conn,
                volume_id,
                "delete_refused",
                actor_kind="user",
                actor=actor,
                detail={"reason": refused.code},
            )
        raise ApiError(409, refused.code, refused.message) from None
    except repo.VolumeNotFound:
        raise _not_found() from None
    except repo.VolumeBusy as exc:
        raise ApiError(409, "volume_busy", str(exc)) from None
    log.info("audit: volume delete requested id=%s by=%s", volume_id, actor)
    return view


@router.get(
    "/volumes/{volume_id}/events", response_model=schemas.VolumeEventPage, responses=_ERRORS
)
async def volume_events(
    pool: Pool, volume_id: uuid.UUID, before: _Before = None, limit: _Limit = 50
) -> schemas.VolumeEventPage:
    async with pool.connection() as conn:
        await _require(conn, volume_id)
        page = await repo.events(conn, volume_id, before=before, limit=limit)
    return schemas.VolumeEventPage(
        items=[
            schemas.VolumeEventView(
                id=e.id,
                at=e.at,
                actor_kind=e.actor_kind,
                actor=e.actor,
                kind=e.kind,
                detail=e.detail,
            )
            for e in page.items
        ],
        next_cursor=page.next_cursor,
    )


@router.post(
    "/volumes/{volume_id}/save",
    status_code=202,
    response_model=schemas.VolumeView,
    responses={**_ERRORS, 409: {"description": "volume_busy: the volume is not ready"}},
)
async def save_volume(request: Request, pool: Pool, volume_id: uuid.UUID) -> schemas.VolumeView:
    try:
        async with pool.connection() as conn:
            await _require(conn, volume_id)
            saved = await repo.request_save(
                conn, volume_id, trigger="user", session_slug=None, actor=_actor(request)
            )
            assert saved is not None
            return await _view(conn, saved)
    except repo.VolumeNotFound:
        raise _not_found() from None
    except repo.VolumeBusy as exc:
        raise ApiError(409, "volume_busy", str(exc)) from None


@router.post(
    "/volumes/{volume_id}/restore",
    status_code=202,
    response_model=schemas.VolumeView,
    responses={
        404: {
            "description": "volume_not_found, or version_not_found (not a version of volume-<name>)"
        },
        409: {"description": "volume_mounted (HARD RULE: a session has it mounted) or volume_busy"},
    },
)
async def restore_volume(
    request: Request, pool: Pool, volume_id: uuid.UUID, body: schemas.VolumeRestoreIn
) -> schemas.VolumeView:
    try:
        async with pool.connection() as conn:
            v = await _require(conn, volume_id)
            if await backups_repo.get_version(conn, _backup_name(v.name), body.version) is None:
                raise ApiError(404, "version_not_found", "not a version of this volume")
            restoring = await repo.begin_explicit_restore(
                conn, volume_id, version=body.version, actor=_actor(request)
            )
            return await _view(conn, restoring)
    except repo.VolumeNotFound:
        raise _not_found() from None
    except repo.VolumeMounted:
        raise ApiError(409, "volume_mounted", "a session has this volume mounted") from None
    except repo.VolumeBusy as exc:
        raise ApiError(409, "volume_busy", str(exc)) from None

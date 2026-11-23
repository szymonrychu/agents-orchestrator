"""Admin routes of the agent port for volumes (S-77 steps 9, spec 9.3, 12). Names, ids, sizes,
states, modes and counts only: never descriptions, labels, file names, manifests or content.

Reads (`reader_router`: a superleader) list the volumes whose
`projects` intersect the caller's own projects. Writes (superleader):

- attach `POST /presets/{ref}/volumes/{volume_id}` and detach `DELETE` on a manageable preset,
  saved like a preset patch. An attach that the cross-trust rule (`volume_attach_is_security`)
  judges security is not applied: it is filed as a preset proposal the user reviews (202). The
  same rule runs inside `prepare_volumes` for every other superleader save of `spec.volumes`.
- create `POST /volumes` never creates: it files a `volume_create` proposal (202). Resize, delete
  and restore have no admin route.
"""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import Request, Response

from agents_orchestrator.api import ack, proposals, schemas
from agents_orchestrator.api import presets as presets_api
from agents_orchestrator.api import volumes as volumes_api
from agents_orchestrator.api.admin import (
    FORBIDDEN,
    WRITE_FORBIDDEN,
    caller_of,
    note_detail,
    reader_router,
    router,
    writable_preset,
)
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.sessions import Cfg, Pool
from agents_orchestrator.db import volumes as repo

_NOT_FOUND = {"description": "volume_not_found: no live volume of the caller's projects"}


def _unknown() -> ApiError:
    return ApiError(404, "volume_not_found", "unknown volume")


async def _view(conn, v: repo.Volume) -> schemas.AdminVolumeView:
    full = await volumes_api._view(conn, v)
    return schemas.AdminVolumeView(
        **full.model_dump(include=set(schemas.AdminVolumeView.model_fields))
    )


@reader_router.get(
    "/volumes", response_model=list[schemas.AdminVolumeView], responses={403: FORBIDDEN}
)
async def list_volumes(request: Request, pool: Pool) -> list[schemas.AdminVolumeView]:
    """The volumes that share a project with the caller, name order."""
    _, caller = caller_of(request)
    scope = set(caller.projects)
    async with pool.connection() as conn:
        return [await _view(conn, v) for v in await repo.list(conn) if scope & set(v.projects)]


@reader_router.get(
    "/volumes/{volume_id}",
    response_model=schemas.AdminVolumeDetail,
    responses={403: FORBIDDEN, 404: _NOT_FOUND},
)
async def get_volume(
    volume_id: uuid.UUID, request: Request, pool: Pool
) -> schemas.AdminVolumeDetail:
    """One volume of the caller's projects with the presets that attach it."""
    _, caller = caller_of(request)
    async with pool.connection() as conn:
        v = await repo.get(conn, volume_id)
        if v is None or not set(caller.projects) & set(v.projects):
            raise _unknown()
        base = await _view(conn, v)
        presets = [
            schemas.VolumeAttachmentView(
                preset_id=a.preset_id,
                preset_name=a.name,
                mode=a.attachment.get("mode", "ro"),
                mount_path=volumes_api._mount_path(a.attachment, v.name),
                backup_restore=bool(a.attachment.get("backup_restore", False)),
            )
            for a in await repo.attaching_presets(conn, v.id)
            if set(caller.projects) & {a.project, *a.extra_projects}
        ]
    return schemas.AdminVolumeDetail(**base.model_dump(), presets=presets)


@router.post(
    "/presets/{ref}/volumes/{volume_id}",
    response_model=schemas.VolumeAttachResult,
    responses={
        202: {
            "model": schemas.VolumeAttachResult,
            "description": "a security attach: filed as a preset proposal for the user, the "
            "preset is unchanged",
        },
        403: {
            "description": "not_superleader; not_manageable: the preset is not "
            "superleader_manageable; volume_cross_trust: the rule judged it security only after "
            "a concurrent attach (retry files the proposal)"
        },
        404: {"description": "not_found: unknown preset; volume_not_found: unknown volume"},
        409: {
            "description": "conflict: changed meanwhile; open_proposals: too_many_proposals: "
            "the caller's open-proposal cap is reached"
        },
        422: {"description": "volumes_invalid: scope, path or duplicate problems"},
    },
)
async def attach_volume(
    ref: str,
    volume_id: uuid.UUID,
    body: schemas.VolumeAttachIn,
    request: Request,
    response: Response,
    pool: Pool,
    settings: Cfg,
) -> schemas.VolumeAttachResult:
    """Attach a volume to a manageable preset (replacing an earlier attachment of it). Free
    unless the volume would be shared with a privileged preset or attached read-write to one;
    then the preset is unchanged and the answer is 202 with the filed proposal."""
    _, caller = caller_of(request)
    note_detail(request, volume=str(volume_id), mode=body.mode)
    attachment: dict[str, Any] = {
        "volume": str(volume_id),
        "mode": body.mode,
        "mount_path": body.mount_path,
        "backup_restore": body.backup_restore,
    }
    async with pool.connection() as conn:
        found = await writable_preset(conn, ref)

        async def file(new_atts: list[dict[str, Any]]) -> schemas.ProposalView:
            view, _ = await proposals.create(
                request,
                conn,
                settings,
                schemas.ProposalIn(
                    kind="preset",
                    target=str(found.id),
                    patch={"volumes": new_atts},
                    reason=f"attach volume {volume_id} {body.mode}: shared with a privileged "
                    "preset or read-write on one",
                ),
                caller=caller,
                superleader=True,
            )
            return view

        result = await presets_api.edit_volumes(
            request,
            conn,
            settings,
            found.id,
            volume_id,
            attachment,
            caller_projects=caller.projects,
            file_proposal=file,
        )
    if isinstance(result, schemas.ProposalView):
        response.status_code = 202
        return schemas.VolumeAttachResult(proposal=result)
    return schemas.VolumeAttachResult(preset=result)


@router.delete(
    "/presets/{ref}/volumes/{volume_id}",
    response_model=schemas.WriteAck,
    responses={
        403: WRITE_FORBIDDEN,
        404: {"description": "not_found, volume_not_found, or not_attached: nothing to remove"},
        409: {"description": "conflict: the preset changed meanwhile; retry"},
    },
)
async def detach_volume(
    ref: str, volume_id: uuid.UUID, request: Request, pool: Pool, settings: Cfg
) -> schemas.WriteAck:
    """Detach a volume from a manageable preset at once (toward safe); running sessions keep
    their mount."""
    _, caller = caller_of(request)
    note_detail(request, volume=str(volume_id))
    async with pool.connection() as conn:
        found = await writable_preset(conn, ref)
        result = await presets_api.edit_volumes(
            request,
            conn,
            settings,
            found.id,
            volume_id,
            None,
            caller_projects=caller.projects,
            file_proposal=_never,
        )
    assert isinstance(result, schemas.PresetView)
    return ack.preset_ack(result)


async def _never(new_atts: list[dict[str, Any]]) -> schemas.ProposalView:
    raise RuntimeError("a detach never files a proposal")


@router.post(
    "/volumes",
    status_code=202,
    response_model=schemas.WriteAck,
    responses={
        403: {"description": "not_superleader; project_out_of_scope: a project the caller lacks"},
        409: {
            "description": "volume_exists: a live volume has this name; too_many_proposals: "
            "the caller's open-proposal cap is reached"
        },
    },
)
async def create_volume(
    body: schemas.VolumeProposalIn,
    request: Request,
    response: Response,
    pool: Pool,
    settings: Cfg,
) -> schemas.WriteAck:
    """File a request to create a volume as a `volume_create` proposal; nothing is created until
    the user approves it. 200 with the first proposal on a `client_key` replay."""
    _, caller = caller_of(request)
    note_detail(request, name=body.name)
    async with pool.connection() as conn:
        view, created = await proposals.create_volume(
            request, conn, settings, body=body, caller=caller
        )
    if not created:
        response.status_code = 200
    return ack.proposal_ack(view)

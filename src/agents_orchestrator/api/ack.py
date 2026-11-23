"""The agent-port write ack (decision 926): `WriteAck` from a view or from a relayed JSON body."""

from __future__ import annotations

import json
from typing import Any

from agents_orchestrator.api import schemas

# the upstream fields that name the written object, in order; the first present one is the id
_ID_FIELDS = ("key", "id", "path", "slug", "project")
_VERSION_FIELDS = ("version", "revision", "steps_version", "generation")


def preset_ack(view: schemas.PresetView) -> schemas.WriteAck:
    return schemas.WriteAck(id=str(view.id))


def session_ack(view: schemas.SessionView) -> schemas.WriteAck:
    return schemas.WriteAck(id=view.slug, version=view.generation, status=view.desired)


def proposal_ack(view: schemas.ProposalView) -> schemas.WriteAck:
    return schemas.WriteAck(id=view.id, version=view.revision, status=view.status)


def project_ack(view: schemas.ProjectView) -> schemas.WriteAck:
    return schemas.WriteAck(id=view.slug, status="archived" if view.archived else "active")


def _version(payload: dict[str, Any]) -> int | None:
    for field in _VERSION_FIELDS:
        value = payload.get(field)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return None


def json_ack(content: bytes, fallback_id: str) -> bytes | None:
    """The ack body for a relayed 2xx JSON object, or None to pass the upstream body through.

    A body that names no object, no version and no status (a list, `{"keys": [...]}`) is kept as
    it is: the agent needs what it carries. `fallback_id` (the request path) names the target when
    the body has no id field, as a plan or a step order has none. The id keeps its upstream JSON
    type (a number stays a number)."""
    try:
        payload = json.loads(content)
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    named = next((payload[f] for f in _ID_FIELDS if isinstance(payload.get(f), str | int)), None)
    version = _version(payload)
    status = payload.get("status")
    status = status if isinstance(status, str) else None
    if named is None and version is None and status is None:
        return None
    ack = schemas.WriteAck(
        id=fallback_id if named is None else named, version=version, status=status
    )
    return ack.model_dump_json().encode()

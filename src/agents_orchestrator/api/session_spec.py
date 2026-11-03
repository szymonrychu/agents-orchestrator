"""Merge-patch of a live session's spec by an agent: the preset PATCH guards, the fields a live
session cannot change, the leader's tighten-only limits, and the write under the row lock with
the same guards re-run on the locked row. Paths only, never values, in errors and the audit."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import psycopg
from fastapi import Request
from fastapi.concurrency import run_in_threadpool

from agents_orchestrator.api import schemas
from agents_orchestrator.api.agent import _leads
from agents_orchestrator.api.errors import ApiError, not_found
from agents_orchestrator.api.sessions import _secret_unset, check_spec, one_view
from agents_orchestrator.api.spec_edit import (
    guard_agent_edit,
    human_only_refused,
    merged_spec_in,
    note_fields,
)
from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import sessions as repo
from agents_orchestrator.db.queue import SessionBeingDeleted
from agents_orchestrator.preset_policy import changed_paths, fixed_changes, tighten_changes
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import default_cache_key

# A concurrent spec write (or a deadlock with a preset delete) makes the patch re-read and retry.
_TRIES = 3


def _fixed_refused(paths: list[str]) -> ApiError:
    return ApiError(
        422,
        "session_fixed_field",
        f"cannot change on a live session: {', '.join(paths)}",
        extra={"fields": paths},
    )


async def patch(
    request: Request,
    conn: psycopg.AsyncConnection,
    settings: Settings,
    target: repo.Session,
    body: schemas.SessionSpecPatch,
    *,
    caller: repo.Session,
    caller_agent: agents_repo.Agent,
    superleader: bool,
) -> schemas.SessionView:
    """Apply `body.spec` (RFC 7396) to the target's stored spec. The caller passed the gate:
    a superleader on its own or a manageable session, or a regular leader on its own. The
    source preset is never written; a changed spec bumps the generation once."""
    own = target.id == caller.id

    def guards(
        base: dict[str, Any], new: dict[str, Any], values: Iterable[str], versions: dict[str, str]
    ) -> None:
        """Plan 3.3 steps 3 to 7 on `base` (the stored spec); each refusal notes its paths."""
        guard_agent_edit(base, new, values, request, privileged=not own)
        if not superleader and (
            looser := tighten_changes(
                base,
                new,
                quota_default=settings.usage_quota_default,
                reminder_default=settings.inactivity_reminder_seconds,
                close_default=settings.inactivity_close_seconds,
            )
        ):
            note_fields(request, looser)
            raise human_only_refused(looser)
        if fixed := fixed_changes(base, new):
            note_fields(request, fixed)
            raise _fixed_refused(fixed)
        protected = {n for n, e in new.get("env", {}).items() if e.get("protected")}
        if unset := sorted(protected - set(versions)):
            note_fields(request, [f"env.{n}" for n in unset])
            raise _secret_unset(unset)

    for _ in range(_TRIES):
        found = await repo.get_stored(conn, target.id)
        if found is None:
            raise not_found("session")
        session, stored = found
        _, model = merged_spec_in(stored, body.spec)
        spec, values = model.split()
        new = spec.model_dump(mode="json", exclude_unset=True)
        guards(stored, new, values, session.secret_versions)
        derived = (
            default_cache_key(session.preset_id)
            if session.preset_id is not None
            else (session.spec.cache.restore_key if session.spec.cache is not None else None)
        )
        filled = await run_in_threadpool(
            check_spec, spec, settings, session.spec, derived_cache_key=derived
        )

        async def recheck(
            locked: repo.LockedSession,
            new: dict[str, Any] = new,
            values: Iterable[str] = values,
        ) -> None:
            """The same guards on the fresh locked row, then who may still edit it: the
            preset is held FOR SHARE (lock order sessions, then presets)."""
            guards(locked.spec, new, values, locked.secret_versions)
            if superleader and not own:
                preset = (
                    await presets_repo.get_for_share(conn, locked.preset_id)
                    if locked.preset_id is not None
                    else None
                )
                if preset is None or not preset.superleader_manageable:
                    raise not_found("session")
            elif not superleader and not await _leads(conn, caller_agent, target):
                raise ApiError(403, "not_leader", "only the session's leader may edit its spec")

        try:
            saved, _ = await repo.replace_spec(conn, target.id, stored, filled, recheck)
        except (repo.SpecChanged, psycopg.errors.DeadlockDetected):
            continue
        except SessionBeingDeleted:
            raise ApiError(409, "session_deleting", "the session is being discarded") from None
        except repo.SessionNotFound:
            raise not_found("session") from None
        note_fields(request, changed_paths(stored, new))
        return await one_view(conn, saved, settings)
    raise ApiError(409, "conflict", "the session spec changed while it was edited; retry")

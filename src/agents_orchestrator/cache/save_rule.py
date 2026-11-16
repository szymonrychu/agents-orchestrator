"""When the discard saves: the one rule the reconciler records and the api reports.

`save_plan` answers per backup entry whether the discard saves it, drops its directory or skips the
save, from the entry list and `status.backup` alone (the api applies it without importing the
reconciler). The Claude transcripts target has its own rule (`transcripts_skip`), read from
`status.backup.transcripts` (deploy-safe-agent-sessions D7, D8).

Saves run at the discard only (decision 765): no turn-end and no periodic save.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from agents_orchestrator.spec import backups, restores

if TYPE_CHECKING:
    from agents_orchestrator.db.sessions import Session

# A sessions restore skipped because the session's pods existed when the feature arrived leaves
# the transcripts the pods wrote, so they are saved as after a successful restore.
PODS_EXIST = "pods_exist"
# an entry that is restored but not saved: its directory goes with the session
RESTORE_ONLY = "restore-only"
# no entry in the backup direction names the workspace: the workspace is discarded
WORKSPACE_NOT_LISTED = "workspace_not_listed"
# index of the plan line for the workspace when no backup entry lists it
WORKSPACE_INDEX = -1
# No restore record and no converge ever finished: the session was discarded before its first
# converge, so no pod ever wrote /sessions or /claude and a save Job would wait on a missing PVC.
NEVER_CONVERGED = "never_converged"
NOT_CONFIGURED = "cache_not_configured"
# the entry's restore did not finish: its directory is not what the session started from
RESTORE_PENDING = "restore_pending"
# the entry's restore failed: the save is stored as an orphan version, never current
ORPHAN = "orphan"

# A workdir an init is still writing, or one a failed init left half built (a half-cloned
# submodule), must not replace a backup: every later restore would inherit it.
INIT_BLOCKS_SAVE = frozenset({"running", "failed"})

# transcripts restore states after which the transcripts save may run: it unpacked the archive,
# found none (first use) or found one it could not read (the save replaces the bad object)
SAVE_OK_RESTORE_STATES = frozenset({"restored", "miss", "unreadable"})

# the Job of an on-demand save (the agent backup tool); the reconciler names it from this suffix
SAVE_NOW_JOB_SUFFIX = "backup-save-now"


def save_now_job_name(slug: str) -> str:
    """That Job's name, as `reconciler.cache.job_name` renders it (the api may not import k8s;
    tests/test_api_backup_agent.py pins both to the same value)."""
    return f"agent-{slug}-{SAVE_NOW_JOB_SUFFIX}"


Action = Literal["save", "discard", "skip"]


@dataclass(frozen=True)
class EntryPlan:
    """What the discard does with one backup entry (`index` -1: the workspace root itself)."""

    index: int
    name_template: str
    action: Action
    reason: str | None = None


def backup_state(status: dict[str, Any]) -> dict[str, Any]:
    value = status.get("backup")
    return value if isinstance(value, dict) else {}


def entry_status(status: dict[str, Any], index: int) -> dict[str, Any]:
    """The `status.backup.entries[index]` record, or an empty one."""
    entries = backup_state(status).get("entries")
    if isinstance(entries, list) and 0 <= index < len(entries) and isinstance(entries[index], dict):
        return entries[index]
    return {}


def transcripts_run(status: dict[str, Any], action: Literal["restore", "save"]) -> dict | None:
    """The transcripts restore or save run record, or None."""
    transcripts = backup_state(status).get("transcripts")
    run = transcripts.get(action) if isinstance(transcripts, dict) else None
    return run if isinstance(run, dict) else None


def has_converged(status: dict[str, Any]) -> bool:
    """A converge ran to its closing status write, which records `replicas`.

    That write follows the apply of the workspace PVC and the StatefulSet, so a status without it
    is a session discarded before its first converge (or one a foreign object kept from applying).
    """
    return "replicas" in status


def _entry_skip(status: dict[str, Any], index: int, root: str, in_restore: bool) -> str | None:
    if root == "workspace":
        init_state = (status.get("init") or {}).get("state")
        if init_state in INIT_BLOCKS_SAVE:
            return f"init_{init_state}"
    if in_restore:
        state = (entry_status(status, index).get("restore") or {}).get("state")
        if state in (None, "running"):
            return RESTORE_PENDING
    return None


def save_plan(session: Session, cache_available: bool = True) -> list[EntryPlan]:
    """One line per entry of the session, plus the workspace line when no entry saves it.

    `save`: the discard saves the entry (reason `orphan` when its restore failed: the version is
    kept but never becomes current). `skip`: nothing is saved (cache not configured, never
    converged, the init left the workspace unfit, the restore not done). `discard`: the directory
    goes with the session (a restore-only entry, or the workspace when no entry saves it).
    """
    spec, status = session.spec, session.status
    saved = {i for i, _ in backups(spec)}
    restored = {i for i, _ in restores(spec)}
    plan: list[EntryPlan] = []
    for index, entry in enumerate(spec.backup or []):
        if index not in saved:
            plan.append(EntryPlan(index, entry.name, "discard", RESTORE_ONLY))
            continue
        if not cache_available:
            skip: str | None = NOT_CONFIGURED
        elif not has_converged(status):
            skip = NEVER_CONVERGED
        else:
            skip = _entry_skip(status, index, entry.root, index in restored)
        if skip is not None:
            plan.append(EntryPlan(index, entry.name, "skip", skip))
            continue
        failed = (entry_status(status, index).get("restore") or {}).get("state") == "failed"
        plan.append(EntryPlan(index, entry.name, "save", ORPHAN if failed else None))
    if not any(spec.backup[i].root == "workspace" for i in saved):  # type: ignore[index]
        plan.append(EntryPlan(WORKSPACE_INDEX, "", "discard", WORKSPACE_NOT_LISTED))
    return plan


def _pods_wrote_sessions(restore: dict[str, Any] | None) -> bool:
    """No sessions restore ran: the session predates it, or its pods existed (`pods_exist`)."""
    return restore is None or (
        restore.get("state") == "skipped" and restore.get("reason") == PODS_EXIST
    )


def _restore_skip(restore: dict[str, Any] | None) -> str | None:
    """Why a save may not follow this restore run, or None when it may."""
    state = (restore or {}).get("state")
    if restore is None or state == "running":
        return "restore_running"
    if state in SAVE_OK_RESTORE_STATES:
        return None
    if state == "failed":
        head = str(restore.get("reason") or "").split(":")[0].split(" ")[0]
        return head or "restore_failed"
    return f"restore_{state}"


def transcripts_skip(cache_available: bool, status: dict[str, Any]) -> str | None:
    """Why the discard skips the transcripts save (a session's `status`), or None to save.

    Without a sessions restore record the session either predates the restore (its pods ran) or
    was discarded before its first converge (`never_converged`: no PVC, nothing to save).
    """
    if not cache_available:
        return "not_configured"
    restore = transcripts_run(status, "restore")
    if _pods_wrote_sessions(restore):
        return None if restore is not None or has_converged(status) else NEVER_CONVERGED
    return _restore_skip(restore)

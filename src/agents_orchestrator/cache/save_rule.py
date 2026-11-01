"""When the discard saves a cache target: the one rule the reconciler records and the api reports.

The reconciler's `CacheRunner.save_step` writes `skipped` with this reason; the session view's
`backup` shows the same answer before a discard (deploy-safe-agent-sessions D7, D8). It reads
only `status.cache`, so the api can apply it without importing the reconciler.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agents_orchestrator.cache.presign import CacheTarget

# A save runs only after a restore that left the workdir trustworthy: it unpacked the archive,
# found none (first use), or emptied the workdir because the archive was unreadable (`reset`, so
# the save overwrites the bad object). Every other outcome, present or future, skips the save.
SAVE_OK_RESTORE_STATES = frozenset({"restored", "miss", "reset"})
# A sessions restore skipped because the session's pods existed when the feature arrived leaves
# the transcripts the pods wrote, so they are saved as after a successful restore.
PODS_EXIST = "pods_exist"
# `spec.cache.save_key` is empty: the cache is restore-only and a discard saves nothing.
RESTORE_ONLY = "restore-only"
# No sessions restore record and no converge ever finished: the session was discarded before its
# first converge, so no pod ever wrote /sessions and the save Job would wait on a missing PVC.
NEVER_CONVERGED = "never_converged"


def restore_key(target: CacheTarget) -> str:
    """The `status.cache` key of the restore run of `target`."""
    return "sessions_restore" if target == "sessions" else "restore"


def _pods_wrote_sessions(restore: dict[str, Any] | None, target: CacheTarget) -> bool:
    """No sessions restore ran: the session predates it, or its pods existed (`pods_exist`)."""
    if target != "sessions":
        return False
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


def has_converged(status: dict[str, Any]) -> bool:
    """A converge ran to its closing status write, which records `replicas`.

    That write follows the apply of the workspace PVC and the StatefulSet, so a status without it
    is a session discarded before its first converge (or one a foreign object kept from applying).
    """
    return "replicas" in status


def save_skip(
    cache_available: bool,
    status: dict[str, Any],
    target: CacheTarget,
    *,
    restore_only: bool = False,
) -> str | None:
    """Why the discard skips the save of `target` (a session's `status`), or None to save.

    `restore_only` (an empty `spec.cache.save_key`) skips the workdir save with `restore-only`.

    Without a sessions restore record the session either predates the restore (its pods ran) or
    was discarded before its first converge (`never_converged`: no PVC, nothing to save).
    """
    if not cache_available:
        return "not_configured"
    if restore_only and target == "workdir":
        return RESTORE_ONLY
    restore = (status.get("cache") or {}).get(restore_key(target))
    if _pods_wrote_sessions(restore, target):
        return None if restore is not None or has_converged(status) else NEVER_CONVERGED
    return _restore_skip(restore)

"""When the discard saves the Claude transcripts: the one rule the reconciler records and the api
reports (`transcripts_skip`), read from `status.backup.transcripts` (deploy-safe-agent-sessions
D7, D8). Entry backups are gone (S-77): volumes save through their own reconciler.

Saves run at the discard only (decision 765): no turn-end and no periodic save.
"""

from __future__ import annotations

from typing import Any, Literal

# A sessions restore skipped because the session's pods existed when the feature arrived leaves
# the transcripts the pods wrote, so they are saved as after a successful restore.
PODS_EXIST = "pods_exist"
# No restore record and no converge ever finished: the session was discarded before its first
# converge, so no pod ever wrote /sessions or /claude and a save Job would wait on a missing PVC.
NEVER_CONVERGED = "never_converged"
NOT_CONFIGURED = "cache_not_configured"
# A workdir an init is still writing, or one a failed init left half built (a half-cloned
# submodule), must not replace a backup: every later restore would inherit it.
INIT_BLOCKS_SAVE = frozenset({"running", "failed"})

# transcripts restore states after which the transcripts save may run: it unpacked the archive,
# found none (first use) or found one it could not read (the save replaces the bad object)
SAVE_OK_RESTORE_STATES = frozenset({"restored", "miss", "unreadable"})


def backup_state(status: dict[str, Any]) -> dict[str, Any]:
    value = status.get("backup")
    return value if isinstance(value, dict) else {}


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

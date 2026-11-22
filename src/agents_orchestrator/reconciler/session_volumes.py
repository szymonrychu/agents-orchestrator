"""The session side of shared volumes (spec 5.4, 5.5, 8.2, 8.3): resolve, claim, gate, release.

Before any pod of a session starts, `claim_step` claims every attached volume through
`db.volumes.claim_all` (the HARD RULE guard) and returns a gate. The StatefulSet and the init
Job wait while the gate holds: a volume not settled for a first mount (provisioning, resizing,
restoring, saving), a restore this claim started, or a deleting, missing or owner-exhausted volume
(degraded). A session that already holds a claim is never held back again except by a restore,
so a resize or an on-demand save never scales running pods away. `release` runs in the discard,
after every pod of the session is gone, and asks for the last-release save.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field

import psycopg
from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.db import backups, volumes
from agents_orchestrator.db.sessions import Session
from agents_orchestrator.k8s.render import VolumeAttachments


@dataclass
class VolumeGate:
    """Same shape as the cache and init gates: `hold` keeps the pods at zero."""

    hold: bool = False
    degraded: bool = False
    message: str = ""
    waited_restore: list[str] = field(default_factory=list)  # volumes restored before start


def backup_name(volume: volumes.Volume) -> str:
    return f"volume-{volume.name}"


async def resolve(
    conn: psycopg.AsyncConnection, session: Session
) -> tuple[VolumeAttachments, list[str]]:
    """(attachment, volume) pairs of `spec.volumes` in spec order, and the ids of attached
    volumes that no longer exist."""
    pairs, missing = [], []
    for att in session.spec.volumes or []:
        vol = await volumes.get(conn, att.volume)
        if vol is None:
            missing.append(str(att.volume))
        else:
            pairs.append((att, vol))
    return pairs, missing


def _reader(name: str) -> volumes.VersionReader:
    async def read(conn: psycopg.AsyncConnection) -> uuid.UUID | None:
        current = await backups.current(conn, name)
        return current.id if current else None

    return read


async def claim_step(
    pool: AsyncConnectionPool, session: Session, pairs: VolumeAttachments, missing: list[str]
) -> VolumeGate:
    """Claim every resolved attachment (`resolve`) and gate the session's pods on the result."""
    if not pairs and not missing:
        return VolumeGate()
    async with pool.connection() as conn:
        requests = []
        for att, vol in pairs:
            requests.append(
                volumes.ClaimRequest(
                    volume_id=vol.id,
                    session_id=session.id,
                    preset_id=session.preset_id,
                    mode=att.mode,
                    backup_restore=att.backup_restore,
                    session_slug=session.slug,
                    current_version=_reader(backup_name(vol)),
                )
            )
        async with conn.transaction():
            decisions = await volumes.claim_all(conn, requests) if requests else []
            mine = await volumes.claimed_volume_ids(conn, session.id)
    waiting, refused, restored = [], [f"volume {m} not found" for m in missing], []
    for d in decisions:
        vol = d.volume
        if d.decision == "proceed":
            continue
        if vol.state == "restoring":
            waiting.append(vol.name)
            restored.append(vol.name)
        elif vol.id in mine:
            continue  # running claimant: a resize, a live save or a refused delete
        elif d.decision == "refuse" and vol.state == "deleting":
            refused.append(f"volume {vol.name} is being deleted")
        elif d.decision == "refuse":
            refused.append(
                f"volume {vol.name} failed: its root could not be handed to the agent user"
                f" after {vol.owner_attempts} attempts; delete and recreate it"
            )
        else:
            waiting.append(vol.name)
    message = "; ".join([*(f"waiting for volume {n}" for n in waiting), *refused])
    return VolumeGate(
        hold=bool(waiting or refused),
        degraded=bool(refused),
        message=message,
        waited_restore=restored,
    )


async def release(conn: psycopg.AsyncConnection, session: Session) -> None:
    """Release the claims of a session whose pods are gone and request the release save of each
    volume it was the last mounter of (`volumes.request_release_saves`). The caller holds the
    transaction."""
    released = await volumes.release_claims(
        conn, session.id, actor_kind="system", actor="reconciler"
    )
    await volumes.request_release_saves(conn, released, session_slug=session.slug)

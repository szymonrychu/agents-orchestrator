"""The preset secret store (superleader-crud D5, D7).

One immutable Kubernetes Secret per (preset, declared name, version), named
`ps-<preset id8>-<hash6 of name>-v<version>`, single key `value`. A new value writes version N+1
under a new name, so a failed write leaves the old version live and a partial write never needs
to read another secret (the Role has no get). Metadata lives in `preset_secrets`.

A value is only ever held as a `SecretStr` until it goes into `stringData`; the apply response
is dropped, nothing here logs a value or an exception body, and errors are generic.

Change proposals stage a value (preset-change-proposals 3.6): it is written once, directly as the
version it would become, and that number is reserved in `change_proposal_secrets` (`staged`)
until the proposal is approved (`promote_staged` makes it current, `bind_staged` lets one session
name it) or released. Every writer allocates above the reservations, and `gc` never deletes a
reserved version.
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row
from pydantic import SecretStr

from agents_orchestrator.k8s.kube import KubeApi, KubeError
from agents_orchestrator.k8s.labels import MANAGED_BY, MANAGED_BY_LABEL

log = logging.getLogger(__name__)

VALUE_KEY = "value"
PROPOSAL_LABEL = "orchestrator.proposal"


class SecretStoreError(Exception):
    """A secret write failed; the message never carries the value or a Kubernetes body."""


class SecretUnset(SecretStoreError):
    """Protected variables with no stored value; `names` lists them."""

    def __init__(self, names: list[str]) -> None:
        super().__init__("protected variables with no value: " + ", ".join(names))
        self.names = names


class PresetNotFound(SecretStoreError):
    """The preset a secret is set on does not exist; nothing was written."""


class StagedStale(SecretStoreError):
    """No `staged` reservation of this proposal revision, or it is not above the live version."""


@dataclass(frozen=True)
class SecretMeta:
    name: str
    set: bool
    version: int
    updated_at: datetime


def secret_name(preset_id: uuid.UUID, name: str, version: int) -> str:
    digest = hashlib.sha256(name.encode()).hexdigest()[:6]
    return f"ps-{str(preset_id)[:8]}-{digest}-v{version}"


def _meta(row: dict) -> SecretMeta:
    return SecretMeta(row["name"], True, row["version"], row["updated_at"])


async def _lock(conn: psycopg.AsyncConnection, preset_id: uuid.UUID, name: str) -> None:
    """Serialise writers and session snapshots of one variable until the transaction ends."""
    await conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"{preset_id}/{name}",)
    )


async def _next_version(conn: psycopg.AsyncConnection, preset_id: uuid.UUID, name: str) -> int:
    """The next free version: above the stored one and every reservation. Call under `_lock`."""
    cur = await conn.execute(
        "SELECT COALESCE(GREATEST("
        " (SELECT max(version) FROM preset_secrets WHERE preset_id = %s AND name = %s),"
        " (SELECT max(version) FROM change_proposal_secrets WHERE preset_id = %s AND name = %s)"
        "), 0) + 1",
        (preset_id, name, preset_id, name),
    )
    return (await cur.fetchone())[0]


async def _write(
    kube: KubeApi, namespace: str, full: str, value: SecretStr, labels: dict[str, str]
) -> None:
    """Apply the immutable Secret `full`, first deleting a leftover of that name.

    The name is never live when this runs (its version is above every row), so a leftover is a
    write whose row never committed. Raises KubeError, which callers turn into a generic error.
    """
    await kube.delete("Secret", namespace, full)
    await kube.apply(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": full, "namespace": namespace, "labels": labels},
            "type": "Opaque",
            "immutable": True,
            "stringData": {VALUE_KEY: value.get_secret_value()},
        }
    )


async def snapshot_versions(
    conn: psycopg.AsyncConnection, preset_id: uuid.UUID, names: list[str]
) -> dict[str, str]:
    """The Secret names a new session must reference, one per protected variable.

    Call it inside the transaction that inserts the session. It takes the variable's lock, so a
    rotation or removal waits until the session row is committed and its collection then sees the
    reference (otherwise it could delete the version this session just read). Raises SecretUnset
    for a name with no live value.
    """
    found: dict[str, str] = {}
    missing: list[str] = []
    for name in sorted(names):
        await _lock(conn, preset_id, name)
        cur = await conn.execute(
            "SELECT version FROM preset_secrets"
            " WHERE preset_id = %s AND name = %s AND retired_at IS NULL",
            (preset_id, name),
        )
        row = await cur.fetchone()
        if row is None:
            missing.append(name)
        else:
            found[name] = secret_name(preset_id, name, row[0])
    if missing:
        raise SecretUnset(missing)
    return found


async def list_secrets(conn: psycopg.AsyncConnection, preset_id: uuid.UUID) -> list[SecretMeta]:
    return (await list_metas(conn, [preset_id])).get(preset_id, [])


async def list_metas(
    conn: psycopg.AsyncConnection, preset_ids: list[uuid.UUID]
) -> dict[uuid.UUID, list[SecretMeta]]:
    """The live (not retired) secrets of each preset, by name."""
    out: dict[uuid.UUID, list[SecretMeta]] = {}
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT preset_id, name, version, updated_at FROM preset_secrets"
            " WHERE preset_id = ANY(%s) AND retired_at IS NULL ORDER BY preset_id, name",
            (preset_ids,),
        )
        for r in await cur.fetchall():
            out.setdefault(r["preset_id"], []).append(_meta(r))
    return out


async def has_rows(conn: psycopg.AsyncConnection, preset_id: uuid.UUID) -> bool:
    """True while any row of the preset remains, live or retired and not yet collected."""
    cur = await conn.execute("SELECT 1 FROM preset_secrets WHERE preset_id = %s", (preset_id,))
    return await cur.fetchone() is not None


async def set_secret(
    conn: psycopg.AsyncConnection,
    kube: KubeApi,
    namespace: str,
    preset_id: uuid.UUID,
    name: str,
    value: SecretStr,
    *,
    collect: bool = True,
    recheck: Callable[[dict[str, Any]], None] | None = None,
) -> SecretMeta:
    """Store `value` as the next version of the preset's protected variable `name`.

    Returns metadata only. The preset must exist: garbage collection treats a row with no preset
    as dead and would delete the Secret just written. A caller whose transaction may still roll
    back passes `collect=False` and calls `gc` after its commit, so the previous version is never
    deleted for a write that did not stick.

    The preset row is locked first (the order every writer of a preset and its secrets uses), and
    `recheck`, when given, sees the locked row's spec before anything is written: it raises to
    refuse a write the caller decided on a spec that has since changed.
    """
    async with conn.transaction():
        cur = await conn.execute("SELECT spec FROM presets WHERE id = %s FOR UPDATE", (preset_id,))
        row = await cur.fetchone()
        if row is None:
            raise PresetNotFound(f"preset not found for secret {name}")
        if recheck is not None:
            recheck(row[0])
        await _lock(conn, preset_id, name)
        version = await _next_version(conn, preset_id, name)
        full = secret_name(preset_id, name, version)
        try:
            await _write(kube, namespace, full, value, {MANAGED_BY_LABEL: MANAGED_BY})
        except KubeError as exc:
            log.warning("secret write failed: %s v%d (%s)", name, version, type(exc).__name__)
            raise SecretStoreError(f"could not store secret {name}") from None
        cur = await conn.execute(
            "INSERT INTO preset_secrets (preset_id, name, version) VALUES (%s, %s, %s)"
            " ON CONFLICT (preset_id, name) DO UPDATE"
            " SET version = excluded.version, retired_at = NULL, updated_at = now()"
            " RETURNING updated_at",
            (preset_id, name, version),
        )
        updated_at = (await cur.fetchone())[0]
    log.info("secret set: preset=%s name=%s version=%d", str(preset_id)[:8], name, version)
    if collect:
        await gc(conn, kube, namespace, preset_id=preset_id, name=name)
    return SecretMeta(name, True, version, updated_at)


async def sync_declared(
    conn: psycopg.AsyncConnection,
    kube: KubeApi,
    namespace: str,
    preset_id: uuid.UUID,
    declared: list[str],
    *,
    collect: bool = True,
) -> None:
    """Retire the secrets whose name left `declared`, the preset's protected variable names.

    Then delete what no session still names.
    """
    async with conn.transaction():
        cur = await conn.execute(
            "SELECT name FROM preset_secrets WHERE preset_id = %s AND retired_at IS NULL"
            " AND NOT (name = ANY(%s)) ORDER BY name",
            (preset_id, declared),
        )
        for (name,) in await cur.fetchall():
            await _lock(conn, preset_id, name)
        await conn.execute(
            "UPDATE preset_secrets SET retired_at = now()"
            " WHERE preset_id = %s AND retired_at IS NULL AND NOT (name = ANY(%s))",
            (preset_id, declared),
        )
    if collect:
        await gc(conn, kube, namespace, preset_id=preset_id)


async def _session_referenced(conn: psycopg.AsyncConnection) -> set[str]:
    cur = await conn.execute(
        "SELECT DISTINCT e.value FROM sessions s, jsonb_each_text(s.secret_versions) AS e"
        " WHERE s.secret_versions IS NOT NULL"
    )
    return {r[0] for r in await cur.fetchall()}


async def _referenced(conn: psycopg.AsyncConnection) -> set[str]:
    """Secret names `gc` keeps: those a session names, and every reserved (staged, bound) one."""
    cur = await conn.execute("SELECT preset_id, name, version FROM change_proposal_secrets")
    reserved = {secret_name(*r) for r in await cur.fetchall()}
    return reserved | await _session_referenced(conn)


async def gc(
    conn: psycopg.AsyncConnection,
    kube: KubeApi,
    namespace: str,
    *,
    preset_id: uuid.UUID | None = None,
    name: str | None = None,
) -> None:
    """Delete, by name, every Secret version that is neither current nor named by a stored session.

    Reads only our database, the current versions first and the session references after them
    (the order closes the race with a session being created). A retired row, or one whose
    preset is gone, has no current version; its row is dropped once every version is deleted.
    """
    where, params = "", []
    if preset_id is not None:
        where += " AND ps.preset_id = %s"
        params.append(preset_id)
    if name is not None:
        where += " AND ps.name = %s"
        params.append(name)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT ps.preset_id, ps.name, ps.version, ps.swept,"
            " (ps.retired_at IS NOT NULL OR p.id IS NULL) AS dead"
            " FROM preset_secrets ps LEFT JOIN presets p ON p.id = ps.preset_id"
            f" WHERE true{where}",  # noqa: S608
            params,
        )
        found = await cur.fetchall()
    # Read after the current versions: a create that snapshotted a version committed before the
    # rotation that made it old, so a reference list read now always includes it.
    referenced = await _referenced(conn)
    for row in found:
        await _sweep(conn, kube, namespace, row, referenced)


async def _sweep(conn, kube: KubeApi, namespace: str, row: dict, referenced: set[str]) -> None:
    pid, name, current = row["preset_id"], row["name"], row["version"]
    upto = current if row["dead"] else current - 1
    stuck: int | None = None  # the lowest version still present
    for version in range(row["swept"] + 1, upto + 1):
        full = secret_name(pid, name, version)
        if full in referenced:
            stuck = stuck or version
            continue
        try:
            await kube.delete("Secret", namespace, full)
        except KubeError as exc:
            log.warning("secret gc failed: %s v%d (%s)", name, version, type(exc).__name__)
            stuck = stuck or version
    if row["dead"] and stuck is None:
        await conn.execute(
            "DELETE FROM preset_secrets WHERE preset_id = %s AND name = %s AND version = %s",
            (pid, name, current),
        )
        return
    swept = (stuck - 1) if stuck is not None else upto
    if swept > row["swept"]:
        await conn.execute(
            "UPDATE preset_secrets SET swept = %s WHERE preset_id = %s AND name = %s",
            (swept, pid, name),
        )


# ------------------------------------------- staged versions of change proposals (Q13 B, 3.6)


async def stage_secret(
    conn: psycopg.AsyncConnection,
    kube: KubeApi,
    namespace: str,
    preset_id: uuid.UUID,
    name: str,
    value: SecretStr,
    *,
    proposal_id: int,
    revision: int,
) -> str:
    """Write `value` as the next version of `name` and reserve it for the proposal revision.

    Runs in the caller's transaction: the reservation commits with the proposal, and a caller
    whose transaction rolls back deletes the returned Secret itself. The version does not go
    live; `promote_staged` or `bind_staged` use it on approval, `release` drops it. Returns the
    Secret name. Raises SecretStoreError, with a generic message, when the write fails.
    """
    async with conn.transaction():
        await _lock(conn, preset_id, name)
        version = await _next_version(conn, preset_id, name)
        full = secret_name(preset_id, name, version)
        labels = {MANAGED_BY_LABEL: MANAGED_BY, PROPOSAL_LABEL: str(proposal_id)}
        try:
            await _write(kube, namespace, full, value, labels)
        except KubeError as exc:
            log.warning(
                "staged secret write failed: %s v%d proposal=%d (%s)",
                name,
                version,
                proposal_id,
                type(exc).__name__,
            )
            raise SecretStoreError(f"could not stage secret {name}") from None
        await conn.execute(
            "INSERT INTO change_proposal_secrets"
            " (preset_id, name, version, proposal_id, revision, state)"
            " VALUES (%s, %s, %s, %s, %s, 'staged')",
            (preset_id, name, version, proposal_id, revision),
        )
    log.info(
        "secret staged: preset=%s name=%s version=%d proposal=%d",
        str(preset_id)[:8],
        name,
        version,
        proposal_id,
    )
    return full


async def _take_staged(
    conn: psycopg.AsyncConnection,
    preset_id: uuid.UUID,
    name: str,
    proposal_id: int,
    revision: int,
) -> int:
    """Lock the variable and return the version this revision staged; StagedStale if none."""
    await _lock(conn, preset_id, name)
    cur = await conn.execute(
        "SELECT version FROM change_proposal_secrets WHERE preset_id = %s AND name = %s"
        " AND proposal_id = %s AND revision = %s AND state = 'staged'",
        (preset_id, name, proposal_id, revision),
    )
    row = await cur.fetchone()
    if row is None:
        raise StagedStale(f"no staged value of {name} for this proposal revision")
    return row[0]


async def promote_staged(
    conn: psycopg.AsyncConnection,
    preset_id: uuid.UUID,
    name: str,
    *,
    proposal_id: int,
    revision: int,
) -> int:
    """Make the version the proposal revision staged the current one; returns it.

    Call inside the apply transaction, with the preset row already locked FOR UPDATE. The
    reservation must be `staged` for this proposal revision and above the stored version (a
    rotation since staging makes the proposal stale): otherwise StagedStale and nothing is
    written. The previous version is collected by `gc` after the commit.
    """
    version = await _take_staged(conn, preset_id, name, proposal_id, revision)
    cur = await conn.execute(
        "SELECT version FROM preset_secrets WHERE preset_id = %s AND name = %s",
        (preset_id, name),
    )
    row = await cur.fetchone()
    if row is not None and row[0] >= version:
        raise StagedStale(f"staged value of {name} is older than the stored one")
    await conn.execute(
        "INSERT INTO preset_secrets (preset_id, name, version) VALUES (%s, %s, %s)"
        " ON CONFLICT (preset_id, name) DO UPDATE"
        " SET version = excluded.version, retired_at = NULL, updated_at = now()",
        (preset_id, name, version),
    )
    await conn.execute(
        "DELETE FROM change_proposal_secrets WHERE preset_id = %s AND name = %s AND version = %s",
        (preset_id, name, version),
    )
    log.info(
        "staged secret promoted: preset=%s name=%s version=%d proposal=%d",
        str(preset_id)[:8],
        name,
        version,
        proposal_id,
    )
    return version


async def bind_staged(
    conn: psycopg.AsyncConnection,
    preset_id: uuid.UUID,
    name: str,
    *,
    proposal_id: int,
    revision: int,
) -> str:
    """Turn the revision's `staged` reservation `bound`; returns its Secret name.

    For a session apply: the caller writes the name into that session's `secret_versions` in the
    same transaction. A bound version keeps its number until `collect_bound` sees no session
    naming it. Raises StagedStale when the revision staged no value of `name`.
    """
    version = await _take_staged(conn, preset_id, name, proposal_id, revision)
    await conn.execute(
        "UPDATE change_proposal_secrets SET state = 'bound'"
        " WHERE preset_id = %s AND name = %s AND version = %s",
        (preset_id, name, version),
    )
    log.info(
        "staged secret bound: preset=%s name=%s version=%d proposal=%d",
        str(preset_id)[:8],
        name,
        version,
        proposal_id,
    )
    return secret_name(preset_id, name, version)


async def _drop_reservation(
    conn: psycopg.AsyncConnection, kube: KubeApi, namespace: str, row: tuple, state: str
) -> str | None:
    """Delete the reserved Secret, then its row (still in `state`); None when nothing was dropped.

    The row was read before the lock, so it is read again under it: a concurrent promote or bind
    may have made the version live or bound, and a bound version may be named by a session again;
    then the Secret is kept. The Secret goes first, so a stage cannot reuse the number for a
    Secret this then deletes. A Secret already gone is fine; a failed delete keeps the row.
    """
    preset_id, name, version, proposal_id = row
    full = secret_name(preset_id, name, version)
    async with conn.transaction():
        await _lock(conn, preset_id, name)
        cur = await conn.execute(
            "SELECT 1 FROM change_proposal_secrets WHERE preset_id = %s AND name = %s"
            " AND version = %s AND proposal_id = %s AND state = %s",
            (preset_id, name, version, proposal_id, state),
        )
        if await cur.fetchone() is None or (
            state == "bound" and full in await _session_referenced(conn)
        ):
            log.info("reserved secret kept, changed meanwhile: %s v%d", name, version)
            return None
        try:
            await kube.delete("Secret", namespace, full)
        except KubeError as exc:
            log.warning(
                "reserved secret delete failed: %s v%d proposal=%d (%s)",
                name,
                version,
                proposal_id,
                type(exc).__name__,
            )
            return None
        await conn.execute(
            "DELETE FROM change_proposal_secrets"
            " WHERE preset_id = %s AND name = %s AND version = %s AND state = %s",
            (preset_id, name, version, state),
        )
    log.info("reserved secret %s dropped: %s v%d proposal=%d", state, name, version, proposal_id)
    return full


async def release(
    conn: psycopg.AsyncConnection,
    kube: KubeApi,
    namespace: str,
    *,
    proposal_id: int,
    revision: int | None = None,
) -> list[str]:
    """Drop the `staged` reservations of the proposal (one revision, or all) and their Secrets.

    A failed delete keeps its row for the sweeper to retry. Returns the Secret names dropped.
    """
    cur = await conn.execute(
        "SELECT preset_id, name, version, proposal_id FROM change_proposal_secrets"
        " WHERE proposal_id = %s AND (%s::int IS NULL OR revision = %s) AND state = 'staged'"
        " ORDER BY preset_id, name, version",
        (proposal_id, revision, revision),
    )
    dropped = []
    for row in await cur.fetchall():
        full = await _drop_reservation(conn, kube, namespace, row, "staged")
        if full is not None:
            dropped.append(full)
    return dropped


async def collect_bound(conn: psycopg.AsyncConnection, kube: KubeApi, namespace: str) -> int:
    """Drop each `bound` reservation no session names any more, Secret first; returns the count.

    The bound rows are read before the session references: a reservation turns bound in the
    transaction that writes the session's reference, so a reference list read after it includes
    every committed one.
    """
    cur = await conn.execute(
        "SELECT preset_id, name, version, proposal_id FROM change_proposal_secrets"
        " WHERE state = 'bound' ORDER BY preset_id, name, version"
    )
    bound = await cur.fetchall()
    referenced = await _session_referenced(conn)
    dropped = 0
    for row in bound:
        if secret_name(row[0], row[1], row[2]) in referenced:
            continue
        if await _drop_reservation(conn, kube, namespace, row, "bound") is not None:
            dropped += 1
    return dropped


async def releasable(conn: psycopg.AsyncConnection) -> list[tuple[int, int]]:
    """(proposal id, revision) of every `staged` reservation that must go: its proposal is no
    longer `open` (or gone), or its revision is no longer the current one. Neither ever becomes
    open and current again, so releasing them never races an apply."""
    cur = await conn.execute(
        "SELECT DISTINCT s.proposal_id, s.revision FROM change_proposal_secrets s"
        " LEFT JOIN change_proposals p ON p.id = s.proposal_id"
        " WHERE s.state = 'staged'"
        " AND (p.id IS NULL OR p.status <> 'open' OR s.revision <> p.revision)"
        " ORDER BY s.proposal_id, s.revision"
    )
    return [(r[0], r[1]) for r in await cur.fetchall()]


ORPHAN_DEPTH = 3  # versions probed above the highest one a row holds


async def reap_unreserved(
    conn: psycopg.AsyncConnection, kube: KubeApi, namespace: str, *, depth: int = ORPHAN_DEPTH
) -> list[str]:
    """Delete staged Secrets whose reservation never committed; returns the names deleted.

    A create or revise that rolls back after `stage_secret` wrote its Secret, and could not
    delete it (or died first), leaves an immutable Secret no row names. The Role cannot list
    Secrets, so they are found by their deterministic names: for each variable a row or a spec
    knows, under its lock (a stage in flight holds it until its row commits), the `depth`
    versions above the highest stored or reserved one are deleted. Under the lock no row names
    them, so they are neither current, reserved, nor promoted (a promoted Secret keeps its
    proposal label but is current); a name a session references is skipped all the same. A
    missing Secret is fine; a failed delete is logged and retried on the next reap.
    """
    cur = await conn.execute(
        "SELECT preset_id, name FROM preset_secrets"
        " UNION SELECT preset_id, name FROM change_proposal_secrets"
        " UNION SELECT p.id, e.key FROM presets p, jsonb_each(CASE"
        " WHEN jsonb_typeof(p.spec->'env') = 'object' THEN p.spec->'env' ELSE '{}'::jsonb END) e"
        " WHERE e.value->'protected' = 'true'::jsonb"
        " UNION SELECT c.target_id::uuid, substr(i.path, 5) FROM change_proposal_items i"
        " JOIN change_proposals c ON c.id = i.proposal_id"
        " WHERE c.kind = 'preset' AND i.secret IS NOT NULL AND i.path LIKE 'env.%'"
        " ORDER BY 1, 2"
    )
    dropped: list[str] = []
    for preset_id, name in await cur.fetchall():
        async with conn.transaction():
            await _lock(conn, preset_id, name)
            top = await _next_version(conn, preset_id, name) - 1
            referenced = await _session_referenced(conn)
            for version in range(top + 1, top + 1 + depth):
                full = secret_name(preset_id, name, version)
                if full in referenced:
                    continue
                try:
                    gone = await kube.delete("Secret", namespace, full)
                except KubeError as exc:
                    log.warning(
                        "orphan secret reap failed: %s v%d (%s)", name, version, type(exc).__name__
                    )
                    continue
                if gone:
                    log.info("orphan staged secret reaped: %s v%d", name, version)
                    dropped.append(full)
    return dropped


# ----------------------------------------------------- webhook credentials (superleader-crud D9)


def webhook_secret_name(endpoint_id: int) -> str:
    return f"webhook-{endpoint_id}-password"


async def put_webhook_credential(
    kube: KubeApi, namespace: str, endpoint_id: int, key: str, value: SecretStr
) -> str:
    """Store the credential an agents-webhook endpoint issued once, as `stringData[key]`.

    `key` is `password` (basic mode) or `secret` (header, bearer, hmac). The Secret is replaced
    whole, so a rotate that changes the mode leaves no stale key behind. The apply response is
    dropped and errors carry no value. Returns the Secret name.
    """
    name = webhook_secret_name(endpoint_id)
    try:
        await kube.delete("Secret", namespace, name)
        await kube.apply(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": name,
                    "namespace": namespace,
                    "labels": {MANAGED_BY_LABEL: MANAGED_BY},
                },
                "type": "Opaque",
                "stringData": {key: value.get_secret_value()},
            }
        )
    except KubeError as exc:
        log.warning("webhook credential write failed: %s (%s)", name, type(exc).__name__)
        raise SecretStoreError(f"could not store the credential of webhook {endpoint_id}") from None
    log.info("webhook credential set: %s key=%s", name, key)
    return name


async def delete_webhook_credential(kube: KubeApi, namespace: str, endpoint_id: int) -> None:
    """Remove the endpoint's credential Secret; a failure is logged, never raised."""
    name = webhook_secret_name(endpoint_id)
    try:
        await kube.delete("Secret", namespace, name)
    except KubeError as exc:
        log.warning("webhook credential delete failed: %s (%s)", name, type(exc).__name__)

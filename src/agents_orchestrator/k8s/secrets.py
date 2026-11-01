"""The preset secret store (superleader-crud D5, D7).

One immutable Kubernetes Secret per (preset, declared name, version), named
`ps-<preset id8>-<hash6 of name>-v<version>`, single key `value`. A new value writes version N+1
under a new name, so a failed write leaves the old version live and a partial write never needs
to read another secret (the Role has no get). Metadata lives in `preset_secrets`.

A value is only ever held as a `SecretStr` until it goes into `stringData`; the apply response
is dropped, nothing here logs a value or an exception body, and errors are generic.
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime

import psycopg
from psycopg.rows import dict_row
from pydantic import SecretStr

from agents_orchestrator.k8s.kube import KubeApi, KubeError
from agents_orchestrator.k8s.labels import MANAGED_BY, MANAGED_BY_LABEL

log = logging.getLogger(__name__)

VALUE_KEY = "value"


class SecretStoreError(Exception):
    """A secret write failed; the message never carries the value or a Kubernetes body."""


class PresetNotFound(SecretStoreError):
    """The preset a secret is set on does not exist; nothing was written."""


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


async def list_secrets(conn: psycopg.AsyncConnection, preset_id: uuid.UUID) -> list[SecretMeta]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT name, version, updated_at FROM preset_secrets"
            " WHERE preset_id = %s AND retired_at IS NULL ORDER BY name",
            (preset_id,),
        )
        return [_meta(r) for r in await cur.fetchall()]


async def set_secret(
    conn: psycopg.AsyncConnection,
    kube: KubeApi,
    namespace: str,
    preset_id: uuid.UUID,
    name: str,
    value: SecretStr,
) -> SecretMeta:
    """Store `value` as the next version of the preset's protected variable `name`.

    Returns metadata only. The preset must exist: garbage collection treats a row with no preset
    as dead and would delete the Secret just written.
    """
    async with conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"{preset_id}/{name}",)
        )
        cur = await conn.execute("SELECT 1 FROM presets WHERE id = %s", (preset_id,))
        if await cur.fetchone() is None:
            raise PresetNotFound(f"preset not found for secret {name}")
        cur = await conn.execute(
            "SELECT version FROM preset_secrets WHERE preset_id = %s AND name = %s",
            (preset_id, name),
        )
        row = await cur.fetchone()
        version = (row[0] if row else 0) + 1
        full = secret_name(preset_id, name, version)
        try:
            # a leftover of a write whose row never committed: the name is not live, replace it
            await kube.delete("Secret", namespace, full)
            await kube.apply(
                {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": {
                        "name": full,
                        "namespace": namespace,
                        "labels": {MANAGED_BY_LABEL: MANAGED_BY},
                    },
                    "type": "Opaque",
                    "immutable": True,
                    "stringData": {VALUE_KEY: value.get_secret_value()},
                }
            )
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
    await gc(conn, kube, namespace, preset_id=preset_id, name=name)
    return SecretMeta(name, True, version, updated_at)


async def sync_declared(
    conn: psycopg.AsyncConnection,
    kube: KubeApi,
    namespace: str,
    preset_id: uuid.UUID,
    declared: list[str],
) -> None:
    """Retire the secrets whose name left `declared`, the preset's protected variable names.

    Then delete what no session still names.
    """
    async with conn.transaction():
        await conn.execute(
            "UPDATE preset_secrets SET retired_at = now()"
            " WHERE preset_id = %s AND retired_at IS NULL AND NOT (name = ANY(%s))",
            (preset_id, declared),
        )
    await gc(conn, kube, namespace, preset_id=preset_id)


async def _referenced(conn: psycopg.AsyncConnection) -> set[str]:
    cur = await conn.execute(
        "SELECT DISTINCT e.value FROM sessions s, jsonb_each_text(s.secret_versions) AS e"
        " WHERE s.secret_versions IS NOT NULL"
    )
    return {r[0] for r in await cur.fetchall()}


async def gc(
    conn: psycopg.AsyncConnection,
    kube: KubeApi,
    namespace: str,
    *,
    preset_id: uuid.UUID | None = None,
    name: str | None = None,
) -> None:
    """Delete, by name, every Secret version that is neither current nor named by a stored session.

    Reads only our database. A retired row, or one whose preset is gone, has no current version;
    its row is dropped once every version is deleted.
    """
    referenced = await _referenced(conn)
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

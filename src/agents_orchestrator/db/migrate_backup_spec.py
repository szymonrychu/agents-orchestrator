"""Rewrite stored cache specs and statuses to the backup model (S-68, T-523; D9.1).

`spec.cache` {restore_key, save_key, exclude} became `spec.backup`, a list of entries, and
`sessions.status.cache` became `sessions.status.backup`. Stored specs parse under the strict
`SessionSpec`, so `db/migrate.py` runs `apply` after the SQL migrations and before the api with
the new model serves; it is idempotent (a row without `cache` is left alone) and runs in the
runner's transaction. `report` is the read-only dry run behind `orch backup migrate --dry-run`:
it also validates every stored spec after the rewrite, so a non-empty set of unmapped rows means
the deploy must wait.

The mapping is pure (`map_cache`, `map_status`); the old `preset-<hash>` key derived from a preset
id (`legacy_cache_key`) is mapped through its preset to the literal name `<slug(name)>/workspace`.
Names the old orchestrator wrote as `x/<now>` groups become versions of the name `x`
(`migrate_backup_objects`).
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass, field
from typing import Any, Protocol

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import ValidationError

from agents_orchestrator import rendering
from agents_orchestrator.cache.keys import ArchiveNameError, validate_name
from agents_orchestrator.spec import SessionSpec, legacy_context, without_legacy_backup

HASH_KEY = re.compile(r"preset-[0-9a-f]{16}")
NOW_ONLY = re.compile(r"\{\{\s*now\s*\}\}")
ERROR_MAX = 300
WITHDRAWN_NOTE = (
    "withdrawn by the backup data migration: spec.cache was replaced by spec.backup;"
    " re-file the change against backup"
)


class Unmapped(ValueError):
    """A stored cache block that has no backup equivalent."""


class MigrationError(RuntimeError):
    """The rewrite refuses to run: at least one stored row does not map."""


class PresetLike(Protocol):
    id: uuid.UUID | None
    name: str


def legacy_cache_key(preset_id: uuid.UUID) -> str:
    """The key the old orchestrator derived from an immutable preset id."""
    digest = hashlib.sha256(b"workspace-cache:" + preset_id.bytes).hexdigest()
    return "preset-" + digest[:16]


def preset_backup_name(preset: PresetLike) -> str:
    """`<slug(preset name)>/workspace`: the literal name of a preset's default workspace backup."""
    name = f"{rendering.slug(preset.name)}/workspace"
    try:
        return validate_name(name, allow_prefix=False)
    except ArchiveNameError as exc:
        raise Unmapped(f"preset name {preset.name!r} gives no backup name: {exc}") from None


def _resolve(key: str, preset: PresetLike | None) -> str:
    """A key that is the old derived `preset-<hash>` becomes the preset's backup name.

    When the preset is gone (no id) the hash cannot be matched to a live preset, and the object
    migration keeps an archive of an unknown hash under its own name: the literal key stays, so
    spec and archive still meet.
    """
    if not HASH_KEY.fullmatch(key):
        return key
    if preset is None:
        raise Unmapped(f"derived key {key} without a preset to name it")
    if preset.id is None:
        return key
    if key != legacy_cache_key(preset.id):
        raise Unmapped(f"derived key {key} is not the derived key of preset {preset.name!r}")
    return preset_backup_name(preset)


def _entry(mode: str, name: str, exclude: list[str]) -> dict[str, Any]:
    entry: dict[str, Any] = {"root": "workspace", "path": ".", "mode": mode, "name": name}
    if exclude:
        entry["exclude"] = list(exclude)
    return entry


def map_cache(
    cache: dict[str, Any] | None, preset: PresetLike | None, *, is_preset: bool
) -> list[dict[str, Any]] | None:
    """The `backup` entries of one stored `cache` block, or None for no backup at all.

    `preset` is the preset itself (`is_preset`) or the session's preset, if it still has one.
    Raises `Unmapped` when no name can be derived.
    """
    if cache is None:
        return None
    exclude = list(cache.get("exclude") or [])
    restore, save = cache.get("restore_key"), cache.get("save_key")
    who = "preset" if is_preset else "session"
    if restore is None and save is None:
        if preset is None:
            raise Unmapped(f"{who} with unset keys and no preset to name the workspace backup")
        return [_entry("both", preset_backup_name(preset), exclude)]
    restore, save = _resolve(restore or "", preset), _resolve(save or "", preset)
    if not restore and not save:
        return None
    prefix_form = restore.endswith("/")
    restore_name = restore[:-1] if prefix_form else restore
    if not save:
        return [_entry("restore", restore_name, exclude)]
    if not restore:
        return [_entry("backup", save, exclude)]
    if save == restore or (
        prefix_form and save.startswith(restore) and NOW_ONLY.fullmatch(save[len(restore) :])
    ):
        return [_entry("both", restore_name, exclude)]
    return [_entry("restore", restore_name, exclude), _entry("backup", save, exclude)]


# old workdir restore outcome -> the entry restore state the save rule reads; a `skipped` or
# absent restore has none, so the entry stays restore-pending (the old rule skipped that save too)
RESTORE_STATES = {
    "restored": "restored",
    "miss": "missing",
    "reset": "unreadable",
    "failed": "failed",
    "running": "running",
}


def _entry_records(
    restore: dict[str, Any] | None, backup: list[dict[str, Any]] | None
) -> list[dict[str, Any]]:
    """One record per entry, shaped as the reconciler writes them (`_entry_record`).

    A workspace entry that restores takes the old workdir restore outcome: a session live across
    the deploy has already restored it, so its discard save is not held as restore-pending.
    """
    run = restore or {}
    state = RESTORE_STATES.get(run.get("state"))
    out = []
    for index, entry in enumerate(backup or []):
        record: dict[str, Any] = {
            "index": index,
            "root": entry.get("root"),
            "path": entry.get("path"),
            "version": None,
            "base_version": None,
            "restore": None,
            "save": None,
        }
        if state and entry.get("root") == "workspace" and entry.get("mode") in ("restore", "both"):
            record["restore"] = {
                "state": state,
                "name": entry.get("name"),
                "version": None,
                "at": run.get("finished_at") or run.get("started_at"),
            }
            if run.get("reason"):
                record["restore"]["reason"] = str(run["reason"])[:ERROR_MAX]
        out.append(record)
    return out


def map_status(
    status_cache: dict[str, Any] | None, backup: list[dict[str, Any]] | None = None
) -> dict[str, Any] | None:
    """`status.cache` as `status.backup`: the old run records move to their new keys.

    The workdir restore is the `restore_pre` run, the workdir save is the `save` run, the
    transcripts runs keep their place under `transcripts`. `backup` is the session's rewritten
    entry list: each restoring workspace entry records the old workdir restore outcome.
    """
    if status_cache is None:
        return None
    runs = {
        new: status_cache[old]
        for old, new in (("restore", "restore_pre"), ("save", "save"))
        if status_cache.get(old) is not None
    }
    transcripts = {
        new: status_cache[old]
        for old, new in (("sessions_restore", "restore"), ("sessions_save", "save"))
        if status_cache.get(old) is not None
    }
    entries = _entry_records(status_cache.get("restore"), backup)
    return {"entries": entries, "runs": runs, "transcripts": transcripts}


# ---------------------------------------------------------------------- stored rows


@dataclass
class Finding:
    kind: str  # preset | session
    id: uuid.UUID
    old: Any = None
    new: Any = None
    spec: dict[str, Any] | None = None
    status: dict[str, Any] | None = None
    changed: bool = False
    error: str | None = None


@dataclass
class Report:
    findings: list[Finding] = field(default_factory=list)

    @property
    def unmapped(self) -> list[Finding]:
        return [f for f in self.findings if f.error]


def _compact(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _problem(spec: dict[str, Any], pins: dict[str, Any] | None) -> str | None:
    """Why the strict model refuses `spec` (minus its legacy `backup`), or None."""
    try:
        SessionSpec.model_validate(without_legacy_backup(spec), context=legacy_context(pins))
    except ValidationError as exc:
        return "; ".join(f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors())[
            :ERROR_MAX
        ]
    return None


def _rewrite(spec: dict[str, Any], preset: PresetLike | None, is_preset: bool):
    """(new spec, old cache, new backup, changed) of one stored spec."""
    if "cache" not in spec:
        return spec, None, None, False
    if "backup" in spec:
        raise Unmapped("spec carries both cache and backup")
    old = spec["cache"]
    entries = map_cache(old, preset, is_preset=is_preset)
    new = {k: v for k, v in spec.items() if k != "cache"}
    if entries is not None:
        new["backup"] = entries
    return new, old, entries, True


def collect(conn: psycopg.Connection, *, only_cache: bool) -> Report:
    """Map every stored preset and session; `only_cache` skips rows with no cache to rewrite."""
    report = Report()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT id, name, spec FROM presets ORDER BY name")
        presets = cur.fetchall()
        cur.execute(
            "SELECT id, spec, status, preset_id, preset_name, secret_versions FROM sessions"
            " ORDER BY created_at, id"
        )
        sessions = cur.fetchall()
    by_id = {p["id"]: SimpleNamed(p["id"], p["name"]) for p in presets}
    defaults: dict[str, uuid.UUID] = {}
    for row in presets:
        finding = Finding("preset", row["id"])
        try:
            spec, finding.old, finding.new, finding.changed = _rewrite(
                row["spec"], by_id[row["id"]], True
            )
            if finding.changed:
                finding.spec = spec
                _claim_default(row["spec"]["cache"], by_id[row["id"]], defaults, row["id"])
            if finding.changed or not only_cache:
                finding.error = _problem(spec, None)
        except Unmapped as exc:
            finding.error = str(exc)
        if finding.changed or finding.error:
            report.findings.append(finding)
    for row in sessions:
        finding = Finding("session", row["id"])
        status = row["status"] or {}
        preset = by_id.get(row["preset_id"]) or (
            SimpleNamed(None, row["preset_name"]) if row["preset_name"] else None
        )
        try:
            spec, finding.old, finding.new, finding.changed = _rewrite(row["spec"], preset, False)
            if finding.changed:
                finding.spec = spec
            if "cache" in status:
                finding.status = {k: v for k, v in status.items() if k != "cache"}
                mapped = map_status(status["cache"], spec.get("backup"))
                if mapped is not None:
                    finding.status["backup"] = mapped
                finding.changed = True
            if finding.spec is not None or not only_cache:
                finding.error = _problem(spec, row["secret_versions"])
        except Unmapped as exc:
            finding.error = str(exc)
        if finding.changed or finding.error:
            report.findings.append(finding)
    return report


@dataclass(frozen=True)
class SimpleNamed:
    id: uuid.UUID | None
    name: str


def _claim_default(cache, preset: SimpleNamed, defaults: dict[str, uuid.UUID], pid) -> None:
    """Two presets must not share one derived default name: their workspaces would mix."""
    keys = (cache or {}).get("restore_key"), (cache or {}).get("save_key")
    derived = keys == (None, None) or any(k and HASH_KEY.fullmatch(k) for k in keys)
    if not derived:
        return
    name = preset_backup_name(preset)
    other = defaults.setdefault(name, pid)
    if other != pid:
        raise Unmapped(f"derived name {name} is also derived for preset {other}")


def format_line(f: Finding) -> str:
    if f.error:
        return f"UNMAPPED {f.kind} {f.id} {f.error}"
    return f"{f.kind} {f.id} {_compact(f.old)} -> {_compact(f.new)}"


def report(conn: psycopg.Connection) -> Report:
    """The dry run: every stored spec is validated after the rewrite; nothing is written."""
    return collect(conn, only_cache=False)


def apply(conn: psycopg.Connection) -> int:
    """Rewrite every row that still carries `cache`; the caller's transaction commits it.

    Raises MigrationError (nothing written) when a row to rewrite does not map. Returns the
    number of rows rewritten.
    """
    found = collect(conn, only_cache=True)
    if found.unmapped:
        lines = "\n".join(format_line(f) for f in found.unmapped)
        raise MigrationError(f"{len(found.unmapped)} stored spec(s) unmapped:\n{lines}")
    for f in found.findings:
        if f.kind == "preset":
            conn.execute("UPDATE presets SET spec = %s WHERE id = %s", (Jsonb(f.spec), f.id))
        else:
            conn.execute(
                "UPDATE sessions SET spec = COALESCE(%s, spec), status = COALESCE(%s, status)"
                " WHERE id = %s",
                (
                    Jsonb(f.spec) if f.spec is not None else None,
                    Jsonb(f.status) if f.status is not None else None,
                    f.id,
                ),
            )
    # A proposal that patches `cache` can no longer apply: close it as migration 0041 does
    # (status and the revision outcome) and leave the proposer a note on the item.
    conn.execute(
        "WITH closed AS ("
        " UPDATE change_proposals p SET status = 'withdrawn', closed_at = now(), updated_at = now()"
        " WHERE p.status IN ('open', 'rework', 'stale') AND EXISTS ("
        "   SELECT 1 FROM change_proposal_items i WHERE i.proposal_id = p.id"
        "   AND i.revision = p.revision AND i.path = 'cache')"
        " RETURNING p.id, p.revision"
        "), outcome AS ("
        " UPDATE change_proposal_revisions r SET outcome = 'withdrawn' FROM closed"
        " WHERE r.proposal_id = closed.id AND r.revision = closed.revision AND r.outcome IS NULL"
        ")"
        " UPDATE change_proposal_items i SET note = %s FROM closed"
        " WHERE i.proposal_id = closed.id AND i.revision = closed.revision AND i.path = 'cache'",
        (WITHDRAWN_NOTE,),
    )
    return len(found.findings)


# ---------------------------------------------------------------------- explicit entries

KINDS = ("skills", "rules", "scripts")


def _entries_for(project: str, root: str) -> list[dict[str, Any]]:
    """The skills, rules and scripts entries of one project (decisions 767, 768)."""
    out = []
    for kind in KINDS:
        entry: dict[str, Any] = {
            "root": root,
            "path": kind if root == "claude" else f"{project}/{kind}",
            "mode": "both",
            "name": validate_name(f"{project}/{kind}", allow_prefix=False),
        }
        if root == "claude":
            entry["live"] = True
        out.append(entry)
    return out


def add_entries(
    conn: psycopg.Connection, pairs: list[tuple[str, str]], root: str = "claude"
) -> list[str]:
    """Add the skills, rules and scripts entries of each (preset ref, project) pair.

    `ref` is a preset id or name. An entry whose root and path a preset already lists is
    skipped. The result must pass the strict model and the cross-entry rules, else
    MigrationError (nothing is written by the caller's transaction). Returns one line per preset.
    """
    lines = []
    for ref, project in pairs:
        with conn.cursor(row_factory=dict_row) as cur:
            try:
                key = uuid.UUID(ref)
                cur.execute("SELECT id, name, spec FROM presets WHERE id = %s FOR UPDATE", (key,))
            except ValueError:
                cur.execute(
                    "SELECT id, name, spec FROM presets WHERE lower(name) = lower(%s) FOR UPDATE",
                    (ref,),
                )
            row = cur.fetchone()
        if row is None:
            raise MigrationError(f"preset {ref!r} not found")
        try:
            wanted = _entries_for(project, root)
        except ArchiveNameError as exc:
            raise MigrationError(f"project {project!r}: {exc}") from None
        spec = dict(row["spec"])
        have = list(spec.get("backup") or [])
        taken = {(e.get("root"), e.get("path")) for e in have}
        added = [e for e in wanted if (e["root"], e["path"]) not in taken]
        spec["backup"] = have + added
        problem = _problem(spec, None)
        if problem:
            raise MigrationError(f"preset {row['name']!r} would not validate: {problem}")
        if added:
            conn.execute(
                "UPDATE presets SET spec = %s, updated_at = now() WHERE id = %s",
                (Jsonb(spec), row["id"]),
            )
        lines.append(
            f"preset {row['id']} {project}: added {len(added)}, existing {len(wanted) - len(added)}"
        )
    return lines

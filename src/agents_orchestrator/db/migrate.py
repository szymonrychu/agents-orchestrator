"""Forward-only SQL migration runner, safe under concurrent initContainers."""

from __future__ import annotations

import re
from pathlib import Path

import psycopg

from agents_orchestrator.db import migrate_backup_spec

MIGRATIONS_DIR = Path(__file__).parent / "migrations"
LOCK_KEY = 0x6F72636853636D61  # fixed advisory-lock key ("orchScma")
_NAME = re.compile(r"^(\d{4})_.+\.sql$")


def discover(directory: Path = MIGRATIONS_DIR) -> list[tuple[str, Path]]:
    found = []
    for path in sorted(directory.glob("*.sql")):
        m = _NAME.match(path.name)
        if m:
            found.append((m.group(1), path))
    return found


def run_migrations(dsn: str, directory: Path = MIGRATIONS_DIR) -> list[str]:
    """Apply pending migrations in order, each in its own transaction.

    Holds a session advisory lock for the whole run so concurrent callers serialise.
    Returns the versions applied by this call.
    """
    applied: list[str] = []
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (LOCK_KEY,))
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                "version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
            )
            done = {r[0] for r in conn.execute("SELECT version FROM schema_migrations")}
            for version, path in discover(directory):
                if version in done:
                    continue
                with conn.transaction():
                    conn.execute(path.read_text())
                    conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (version,))
                applied.append(version)
            # Python step: the strict spec model needs the old cache blocks rewritten before any
            # service reads a spec (idempotent; an unmapped row raises and blocks the start).
            # Only on the complete schema: a partial run (tests) lacks the tables it reads.
            if {version for version, _ in discover()} <= done | set(applied):
                with conn.transaction():
                    migrate_backup_spec.apply(conn)
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))
    return applied

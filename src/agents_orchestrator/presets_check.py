"""`orch presets-check`: validate every stored preset against the current model.

Run before a deploy. A preset is checked as a session create would check it (defaults filled,
then the spec model and the settings rules). Output names the preset and the failing field, and
never a value: only pydantic's `loc` and `msg` are printed, and any env value that a rule message
repeats is masked.
"""

from __future__ import annotations

from typing import TextIO

import psycopg
from psycopg.rows import dict_row
from pydantic import ValidationError

from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec, SpecInvalid, fill_resource_defaults, validate_spec

_MASK_MIN = 4


def _plain_values(raw: dict) -> list[str]:
    env = raw.get("env")
    if not isinstance(env, dict):
        return []
    values = [e.get("value") if isinstance(e, dict) else e for e in env.values()]
    return [v for v in values if isinstance(v, str) and len(v) >= _MASK_MIN]


def problems(raw: dict, settings: Settings) -> list[str]:
    """Why a session create from this stored spec would be refused; empty when it would not."""
    raw = dict(raw)
    raw.setdefault("replicas", settings.default_replicas)
    try:
        spec = SessionSpec.model_validate(raw)
    except ValidationError as exc:
        found = [f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors()]
    else:
        try:
            validate_spec(fill_resource_defaults(spec, settings), settings)
        except SpecInvalid as exc:
            found = list(exc.errors)
        else:
            return []
    masked = []
    for line in found:
        for value in _plain_values(raw):
            line = line.replace(value, "***")
        masked.append(line)
    return masked


def run(dsn: str, settings: Settings, out: TextIO) -> int:
    """Print the presets that would fail; exit code 1 when any would."""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        rows = conn.execute("SELECT id, name, spec FROM presets ORDER BY name").fetchall()
    failing = 0
    for row in rows:
        found = problems(row["spec"], settings)
        if found:
            failing += 1
            print(f"preset {row['name']!r} ({row['id']}):", file=out)
            for line in found:
                print(f"  {line}", file=out)
    print(f"{len(rows)} presets checked, {failing} would fail a session create", file=out)
    return 1 if failing else 0

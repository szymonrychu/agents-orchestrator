"""Backup entry lists for tests that think in a restore name and a save name."""

from __future__ import annotations

from typing import Any


def workspace_entries(restore: str = "", save: str = "") -> list[dict[str, Any]]:
    """Workspace entries for a restore name and a save name; empty means no entry in that
    direction. Equal names make one `both` entry."""
    base: dict[str, Any] = {"root": "workspace", "path": "."}
    if restore and restore == save:
        return [{**base, "mode": "both", "name": restore}]
    out = []
    if restore:
        out.append({**base, "mode": "restore", "name": restore})
    if save:
        out.append({**base, "mode": "backup", "name": save})
    return out

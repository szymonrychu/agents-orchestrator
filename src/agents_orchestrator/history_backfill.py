"""`orch history-backfill-projects` (S-83 decision 943): post the `project` and `extra_projects`
of every session row the orchestrator still holds to agents-history
`POST /history/v1/ingest/projects`, in batches of 500, with the ingest bearer (the discard hook
token, `ORCH_DISCARD_HOOK_TOKEN`). The upstream fills only stored sessions whose project is still
NULL, so a rerun changes nothing. The URL is the discard hook URL (`ORCH_DISCARD_HOOK_URL`, the
history ingest route) plus `/projects`. Prints counts only, never the token.
"""

from __future__ import annotations

from typing import TextIO

import httpx
import psycopg

BATCH = 500


def projects_url(discard_hook_url: str) -> str:
    """The backfill route next to the history ingest route the discard hook posts to."""
    return discard_hook_url.rstrip("/") + "/projects"


async def run(
    dsn: str,
    url: str,
    token: str,
    out: TextIO,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> int:
    """Post every sessions row; exit code 1 on the first refused batch (nothing after it)."""
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        cur = await conn.execute(
            "SELECT id, project, extra_projects FROM sessions ORDER BY created_at, id"
        )
        rows = await cur.fetchall()
    items = [
        {"session_id": str(sid), "project": project, "extra_projects": list(extra)}
        for sid, project, extra in rows
    ]
    updated = 0
    headers = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient(transport=transport, timeout=30) as http:
        for start in range(0, len(items), BATCH):
            r = await http.post(url, json={"items": items[start : start + BATCH]}, headers=headers)
            if r.status_code != 200:
                print(f"batch at {start} refused: HTTP {r.status_code}", file=out)
                return 1
            updated += int(r.json().get("updated", 0))
    print(f"{len(items)} sessions sent, {updated} history sessions updated", file=out)
    return 0

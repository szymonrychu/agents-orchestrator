"""Test helper: read the full view behind an agent-port write ack (S-83 step 10).

An admin preset or session write answers a `WriteAck`; tests that check what the write stored
call `expand` on the response, which reads the view from the admin GET route and answers a
response lookalike with the write's status and the view as its body."""

from __future__ import annotations

import re
from typing import Any

_WRITES = {"POST", "PUT", "PATCH", "DELETE"}
_ACK = {"id", "version", "status"}
_PATH = re.compile(r"^(.*/admin)/(presets|sessions)(/[^/]+)?(/copy|/vault/[^/]+|/volumes/[^/]+)?$")


class Full:
    def __init__(self, write: Any, body: Any) -> None:
        self._write = write
        self._body = body
        self.status_code = write.status_code
        self.headers = write.headers
        self.text = write.text

    def json(self) -> Any:
        return self._body


# Preset sub-writes that answer their full result on purpose (step 10 MEMORY: the vault attach
# marks `attachment_privileged`, the volume attach answers a preset or a proposal).
_FULL_ON_PURPOSE = re.compile(r"/vault/[^/]+|/volumes/[^/]+")


async def expand(method: str, path: str, response: Any, get: Any, app: Any = None) -> Any:
    """`get(path)` is the same caller's GET (a session is read through it, which the admin audit
    logs); a preset is read on the unaudited main port of `app`. A non-write, a refused write, a
    204 and a write that answers in full on purpose come back unchanged; any other 2xx preset or
    session write must answer a `WriteAck` whose object reads back, else the helper fails the
    test instead of handing the ack on as if it were the view."""
    path = path.split("?")[0]
    found = _PATH.match(path)
    if method not in _WRITES or found is None or not 200 <= response.status_code < 300:
        return response
    if response.status_code == 204 or _FULL_ON_PURPOSE.fullmatch(found.group(4) or ""):
        return response
    body = response.json()
    assert isinstance(body, dict) and set(body) == _ACK, (
        f"{method} {path} answered {response.status_code} without a WriteAck: {body}"
    )
    if found.group(2) == "presets" and app is not None:
        from test_api_sessions import client_for

        async with client_for(app) as main:
            read = await main.get(f"/api/v1/presets/{body['id']}")
    else:
        read = await get(f"{found.group(1)}/{found.group(2)}/{body['id']}")
    assert read.status_code == 200, f"the ack of {method} {path} does not read back: {read.text}"
    return Full(response, read.json())

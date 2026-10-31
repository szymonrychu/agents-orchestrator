"""Template preview: render a template against a fixed sample context for an editor."""

from __future__ import annotations

import asyncio
from typing import Any, Literal

from agents_common import templating
from fastapi import APIRouter
from pydantic import BaseModel, ConfigDict

router = APIRouter(prefix="/api/v1")

Surface = Literal["web_chat_start", "preset_text", "init_script"]

_COMMON: dict[str, Any] = {"session_slug": "sample-session", "preset_name": "Default"}
_ENV: dict[str, str] = {"TZ": "UTC", "LANG": "C.UTF-8"}
_NOW = "2026-01-01T00:00:00Z"

# Keys per surface follow plan.md "Context variables per surface"; `user` is a plain string.
SAMPLES: dict[str, dict[str, Any]] = {
    "web_chat_start": {**_COMMON, "user": "sample-user", "now": _NOW},
    "preset_text": {**_COMMON, "env": _ENV, "now": _NOW},
    "init_script": {**_COMMON, "env": _ENV},
}


class PreviewIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    template: str
    surface: Surface
    context: dict[str, Any] | None = None


class PreviewError(BaseModel):
    line: int | None
    message: str


class PreviewOut(BaseModel):
    rendered: str | None
    error: PreviewError | None
    variables: list[str]


@router.post("/templates/preview", response_model=PreviewOut)
async def preview(body: PreviewIn) -> dict[str, Any]:
    """Render `template` with the surface's sample context, `context` overriding top-level keys."""
    context = {**SAMPLES[body.surface], **(body.context or {})}
    return await asyncio.to_thread(templating.preview, body.template, context)

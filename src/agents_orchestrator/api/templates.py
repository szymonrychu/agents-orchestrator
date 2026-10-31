"""Template preview: render a template against a fixed sample context for an editor."""

from __future__ import annotations

import asyncio
from typing import Any, Literal

from agents_common import templating
from fastapi import APIRouter
from pydantic import BaseModel, ConfigDict

router = APIRouter(prefix="/api/v1")

Surface = Literal["web_chat_start", "preset_text", "init_script"]

_SESSION: dict[str, Any] = {
    "id": "00000000-0000-0000-0000-0000000000aa",
    "name": "sample-session",
    "slug": "sample-session",
    "namespace": "agents",
    "replicas": 1,
}

SAMPLES: dict[str, dict[str, Any]] = {
    "web_chat_start": {"session": _SESSION, "user": {"name": "sample-user"}},
    "preset_text": {"session": _SESSION, "preset": {"name": "Default"}},
    "init_script": {"session": _SESSION, "workdir": "/workspace"},
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

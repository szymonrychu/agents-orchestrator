"""Template preview: render a template against a fixed sample context for an editor."""

from __future__ import annotations

import asyncio
from typing import Any

from agents_common import templating
from fastapi import APIRouter
from pydantic import BaseModel, ConfigDict

from agents_orchestrator.rendering import SAMPLES, Surface

router = APIRouter(prefix="/api/v1")


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

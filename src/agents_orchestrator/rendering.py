"""Jinja rendering of the spec's text fields (sandbox and preview live in agents-common).

One context per surface, with the keys of the preview samples below, so what an editor previews
is what renders. `SAMPLES` is the single definition; `POST /templates/preview` serves it.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

from agents_common import templating
from agents_common.templating import TemplateError

from agents_orchestrator.cache.keys import ArchiveNameError, validate_name

if TYPE_CHECKING:
    from agents_orchestrator.db.sessions import Session

Surface = Literal["web_chat_start", "preset_text", "init_script", "archive_name"]

_COMMON: dict[str, Any] = {"session_slug": "sample-session", "preset_name": "Default"}
_ENV: dict[str, str] = {"TZ": "UTC", "LANG": "C.UTF-8"}
_NOW = "2026-01-01T00:00:00Z"

# Keys per surface follow plan.md "Context variables per surface"; `user` is a plain string.
SAMPLES: dict[str, dict[str, Any]] = {
    "web_chat_start": {**_COMMON, "user": "sample-user", "now": _NOW},
    "preset_text": {**_COMMON, "env": _ENV, "now": _NOW},
    "init_script": {**_COMMON, "env": _ENV},
    # Values are already in the archive segment alphabet; `now` is fixed width so names sort.
    "archive_name": {
        "session_slug": "sample-session",
        "preset_name": "default",
        "now": "20260101t000000z",
        "date": "2026-01-01",
        "user": "sample-user",
    },
}

MARKERS = ("{{", "{%", "{#")


_NOT_SEGMENT = re.compile(r"[^a-z0-9._-]+")


def format_now(moment: datetime) -> str:
    """Compact fixed-width UTC time (`20261031t120000z`): chronological as a string, and in the
    archive segment alphabet (lowercase)."""
    return moment.astimezone(UTC).strftime("%Y%m%dt%H%M%Sz")


def _segment(value: str) -> str:
    """`value` squeezed into the archive segment alphabet (may come out empty)."""
    return _NOT_SEGMENT.sub("-", value.lower()).strip("-._")[:63].rstrip("-._")


def archive_context(
    slug: str, preset_name: str | None, user: str | None, now: datetime
) -> dict[str, Any]:
    """The `archive_name` context: every value squeezed into the archive segment alphabet."""
    return {
        "session_slug": _segment(slug),
        "preset_name": _segment(preset_name or ""),
        "now": format_now(now),
        "date": now.astimezone(UTC).strftime("%Y-%m-%d"),
        "user": _segment(user or ""),
    }


def session_context(
    surface: Surface, session: Session, *, now: datetime | None = None
) -> dict[str, Any]:
    """The render context of `surface` for a stored session.

    `now` is the session's creation time, not the wall clock: the rendered wrapper config is
    hashed into the pod template, so a clock value would roll the pods on every reconcile.
    The `archive_name` surface alone takes a `now` override: a save name is rendered at save time.
    """
    if surface == "archive_name":
        return archive_context(
            session.slug, session.preset_name, session.created_by_service, now or session.created_at
        )
    ctx: dict[str, Any] = {
        "session_slug": session.slug,
        "preset_name": session.preset_name or "",
        "user": session.created_by_service or "",  # the api has no end-user identity
        "env": dict(session.spec.env),
        "now": session.created_at.isoformat(),
    }
    return {k: ctx[k] for k in SAMPLES[surface]}


def render_name(template: str, context: dict[str, Any], *, allow_prefix: bool) -> str:
    """Render an archive name template, then validate the result against the name grammar.

    Raises ArchiveNameError for a template that does not render (unknown variable, syntax, sandbox
    violation; `line` set when known) and for output that breaks the grammar. Never rewrites.
    """
    try:
        rendered = render_text(template, context)
    except TemplateError as exc:
        raise ArchiveNameError(exc.message, exc.line) from exc
    return validate_name(rendered, allow_prefix=allow_prefix)


def render_text(template: str, context: dict[str, Any]) -> str:
    """Render `template`; text without Jinja markers is returned as is (no child process)."""
    if not any(m in template for m in MARKERS):
        return template
    return templating.render(template, context)


ESCAPE_HINT = "write a literal '{{' as {{ '{{' }}"


def render_init_scripts(session: Session) -> list[str]:
    """The session's init scripts rendered with the `init_script` context.

    Raises TemplateError naming the script; a script written before templating existed that
    contains `{{` or `{%` surfaces here (and at save time) with the escape rule in the message.
    """
    context = session_context("init_script", session)
    rendered: list[str] = []
    for i, script in enumerate(session.spec.init_scripts):
        try:
            rendered.append(render_text(script, context))
        except TemplateError as exc:
            raise TemplateError(
                f"init_scripts[{i}]: {exc.message} ({ESCAPE_HINT})", exc.line
            ) from exc
    return rendered


def problem(template: str, surface: Surface, env: dict[str, str]) -> tuple[str | None, str | None]:
    """(error, rendered) for a template as a spec field: allowed variables, syntax, a trial render.

    The trial uses the surface sample with the spec's own `env`, so an `env.NAME` that the spec
    does not define fails at save time instead of at the reconciler.
    """
    allowed = set(SAMPLES[surface])
    try:
        unknown = sorted(set(templating.undeclared_variables(template)) - allowed)
        if unknown:
            names, ok = ", ".join(unknown), ", ".join(sorted(allowed))
            return f"undeclared variable(s) {names}; allowed: {ok}", None
        context = {**SAMPLES[surface], **({"env": dict(env)} if "env" in allowed else {})}
        return None, render_text(template, context)
    except TemplateError as exc:
        return exc.message, None

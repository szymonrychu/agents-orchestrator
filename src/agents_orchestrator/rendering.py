"""Jinja rendering of the spec's text fields (sandbox and preview live in agents-common).

One context per surface, with the keys of the preview samples below, so what an editor previews
is what renders. `SAMPLES` is the single definition; `POST /templates/preview` serves it.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

from agents_common import templating
from agents_common.templating import TemplateError
from jinja2.sandbox import SecurityError

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
        "project": "sample-project",
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
    slug: str, preset_name: str | None, user: str | None, now: datetime, project: str
) -> dict[str, Any]:
    """The `archive_name` context: every value squeezed into the archive segment alphabet."""
    return {
        "project": _segment(project),
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
            session.slug,
            session.preset_name,
            session.created_by_service,
            now or session.created_at,
            session.project,
        )
    ctx: dict[str, Any] = {
        "session_slug": session.slug,
        "preset_name": session.preset_name or "",
        "user": session.created_by_service or "",  # the api has no end-user identity
        "env": session.spec.plain_env,
        "now": session.created_at.isoformat(),
    }
    return {k: ctx[k] for k in SAMPLES[surface]}


class NeedsDigests(Exception):
    """A name template calls `sha256` and no digests were given: `calls` are the (path, length)
    pairs to resolve (by the resolve Job), in first-use order."""

    def __init__(self, calls: list[tuple[str, int]]) -> None:
        super().__init__(
            "name template needs sha256 digests: " + ", ".join(f"{p}:{n}" for p, n in calls)
        )
        self.calls = calls


SHA256_MIN, SHA256_MAX, SHA256_DEFAULT = 8, 62, 16


def digest_key(path: str, length: int) -> str:
    """The key of one `sha256(path, length)` call in a digests mapping."""
    return f"{path}:{length}"


def slug(value: object) -> str:
    """The `slug` filter: lowercase, runs outside `a-z0-9._-` squeezed to `-`, at most 63 chars."""
    return _segment(str(value))


def _name_child(conn, template, context, digests, memory_mb) -> None:  # noqa: ANN001
    """Render `template` in the sandbox of this child: ({rendered}, calls) or an error."""
    from agents_common.templating import _BoundedEnvironment, _cap_memory, _describe
    from jinja2 import StrictUndefined

    calls: list[tuple[str, int]] = []

    def sha256(path: str, length: int = SHA256_DEFAULT) -> str:
        if not isinstance(path, str) or not isinstance(length, int) or isinstance(length, bool):
            raise ValueError("sha256(path, length): path is a string and length an integer")
        if not SHA256_MIN <= length <= SHA256_MAX:
            raise ValueError(f"sha256 length must be {SHA256_MIN} to {SHA256_MAX}")
        if path.startswith("/") or ".." in path.split("/"):
            raise ValueError("sha256 path must be relative with no '..' segment")
        if (path, length) not in calls:
            calls.append((path, length))
        if digests is None:
            return "0" * length  # dry render: the value is never used
        try:
            return digests[digest_key(path, length)]
        except KeyError:
            raise ValueError(f"no digest for sha256({path!r}, {length})") from None

    try:
        _cap_memory(memory_mb)
        env = _BoundedEnvironment(autoescape=False, undefined=StrictUndefined)
        env.filters["slug"] = slug
        env.globals["sha256"] = sha256
        rendered = env.from_string(template).render(**context)
        conn.send((True, (rendered, calls), None))
    except SecurityError as exc:
        conn.send((False, f"SecurityError: {exc}", None))
    except BaseException as exc:
        message, line = _describe(exc)
        conn.send((False, message, line))
    finally:
        conn.close()


def _run_name(
    template: str, context: dict[str, Any], digests: Mapping[str, str] | None
) -> tuple[str, list[tuple[str, int]]]:
    """One killable, memory-bounded sandbox render of a name template, with `slug` and `sha256`."""
    templating._check_size(template)  # noqa: SLF001
    try:
        mp = templating._mp_context()  # noqa: SLF001
        parent, child = mp.Pipe(duplex=False)
        proc = mp.Process(
            target=_name_child,
            args=(child, template, context, None if digests is None else dict(digests), 256),
            daemon=True,
        )
        proc.start()
    except Exception as exc:
        raise TemplateError(f"template render failed to start: {exc}") from exc
    child.close()
    try:
        if not parent.poll(templating.DEFAULT_TIMEOUT_SECONDS):
            raise TemplateError("template render timed out")
        try:
            ok, value, line = parent.recv()
        except EOFError as exc:
            raise TemplateError("template render failed: worker died") from exc
    finally:
        parent.close()
        if proc.is_alive():
            proc.kill()
        proc.join()
    if not ok:
        raise TemplateError(value, line)
    return value


def sha256_calls(template: str) -> list[tuple[str, int]]:
    """The (path, length) pairs a name template requests, found by a sandboxed dry render with the
    sample context (the output is discarded)."""
    if not any(m in template for m in MARKERS):
        return []
    try:
        return _run_name(template, SAMPLES["archive_name"], None)[1]
    except TemplateError as exc:
        raise ArchiveNameError(exc.message, exc.line) from exc


def render_name(
    template: str,
    context: dict[str, Any],
    digests: Mapping[str, str] | None = None,
    *,
    dry: bool = False,
) -> str:
    """Render a backup name template, then validate the result against the name grammar.

    The sandbox offers the `slug` filter and `sha256(path, length=16)`. With `digests` None a
    template that calls `sha256` raises NeedsDigests (unless `dry`, which substitutes a
    placeholder: used to validate a spec); with digests each call is replaced by
    `digests["<path>:<length>"]`. Raises ArchiveNameError for a template that does not render and
    for output that breaks the grammar. Never rewrites.
    """
    if not any(m in template for m in MARKERS):
        return validate_name(template, allow_prefix=False)
    try:
        rendered, calls = _run_name(template, context, None if dry else digests)
    except TemplateError as exc:
        raise ArchiveNameError(exc.message, exc.line) from exc
    if digests is None and not dry and calls:
        raise NeedsDigests(calls)
    return validate_name(rendered, allow_prefix=False)


def _name_variables(template: str) -> list[str]:
    """Context names a name template reads (`sha256` is a function of the sandbox, not one)."""
    import jinja2.meta
    from agents_common.templating import _BoundedEnvironment, _describe  # noqa: PLC2701
    from jinja2 import StrictUndefined
    from jinja2 import TemplateError as Jinja2Error

    templating._check_size(template)  # noqa: SLF001
    env = _BoundedEnvironment(autoescape=False, undefined=StrictUndefined)
    env.filters["slug"] = slug
    env.globals["sha256"] = lambda *a, **k: ""
    try:
        found = jinja2.meta.find_undeclared_variables(env.parse(template))
    except Jinja2Error as exc:
        message, line = _describe(exc)
        raise TemplateError(f"invalid template: {message}", line) from exc
    return sorted(found)


def preview_name(template: str, context: dict[str, Any]) -> dict[str, Any]:
    """Editor preview of a name template (`{rendered, error{line,message}, variables}`); a
    `sha256` call previews as zeros. Never raises."""
    try:
        variables = _name_variables(template)
        rendered = _run_name(template, context, None)[0]
    except TemplateError as exc:
        return {
            "rendered": None,
            "error": {"line": exc.line, "message": exc.message},
            "variables": [],
        }
    return {"rendered": rendered, "error": None, "variables": variables}


def name_problem(template: str) -> tuple[str | None, str | None]:
    """(error, rendered) for a name template as a spec field: allowed variables, syntax, a dry
    trial render against the sample context."""
    allowed = set(SAMPLES["archive_name"])
    try:
        unknown = sorted(set(_name_variables(template)) - allowed)
        if unknown:
            names, ok = ", ".join(unknown), ", ".join(sorted(allowed))
            return f"undeclared variable(s) {names}; allowed: {ok}", None
        if not any(m in template for m in MARKERS):
            return None, template
        return None, _run_name(template, SAMPLES["archive_name"], None)[0]
    except TemplateError as exc:
        return exc.message, None


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

"""Spec-level edit helpers shared by the preset admin routes and the session spec routes: the
agent guard (human-only, privileged), the RFC 7396 merge with the secret check, and the
audit note. Paths only, never values."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.preset_policy import (
    human_only_changes,
    merge_patch,
    privileged_target_changes,
)
from agents_orchestrator.preset_policy import privileged as privileged_flags
from agents_orchestrator.spec import SessionSpecIn

_PROPOSE = "; send it as a proposal: proposal_create"


def human_only_refused(paths: list[str]) -> ApiError:
    return ApiError(
        403,
        "human_only_field",
        f"only a human can change: {', '.join(paths)}{_PROPOSE}",
        extra={"fields": paths, "proposable": True},
    )


def privileged_edit_refused(paths: list[str]) -> ApiError:
    return ApiError(
        403,
        "privileged_target",
        "only a human can change these fields of a preset with kube_cluster_access, "
        f"kube_namespace_access, session_messaging or docker on: {', '.join(paths)}{_PROPOSE}",
        extra={"fields": paths, "proposable": True},
    )


def note_fields(request: Request, paths: list[str]) -> None:
    """Hand the changed (or refused) paths to the admin audit; paths only, never values."""
    request.state.preset_fields = sorted(paths)


def guard_agent_edit(
    base: dict[str, Any],
    new: dict[str, Any],
    values: Iterable[str],
    request: Request | None,
    *,
    privileged: bool,
    plain_to_ref: bool = False,
) -> None:
    """403 when an agent edit of `base` into `new` leaves the human-only rules, and, when
    `privileged`, the allowlist of a privileged target (`values` names the protected variables
    the call gives a value). The refused paths go to the audit note before the raise."""
    refused = human_only_changes(base, new)
    if refused:
        raise _noted(request, refused, human_only_refused)
    if privileged:
        refused = privileged_target_changes(base, new, plain_to_ref=plain_to_ref)
        if privileged_flags(base):
            # a value for a protected variable would swap a credential the granted pod runs with
            refused = sorted({*refused, *(f"env.{n}" for n in values)})
        if refused:
            raise _noted(request, refused, privileged_edit_refused)


def _noted(
    request: Request | None, paths: list[str], refusal: Callable[[list[str]], ApiError]
) -> ApiError:
    """The refusal for `paths`, with the paths noted for the audit first."""
    if request is not None:
        note_fields(request, paths)
    return refusal(paths)


def secret_in_patch(patch: dict[str, Any], merged: dict[str, Any]) -> None:
    """422 when the patch gives a value to a variable that is protected after the merge (the
    merge fills `protected` from the stored entry, so the patch need not repeat it)."""
    env, merged_env = patch.get("env"), merged.get("env")
    for name, entry in (env if isinstance(env, dict) else {}).items():
        kept = merged_env.get(name) if isinstance(merged_env, dict) else None
        if (
            isinstance(entry, dict)
            and entry.get("value") is not None
            and isinstance(kept, dict)
            and kept.get("protected")
        ):
            raise ApiError(
                422,
                "secret_in_spec",
                f"protected variable {name} carries a value; set it through the secret route",
            )


def merged_spec_in(
    stored: dict[str, Any], patch: dict[str, Any]
) -> tuple[dict[str, Any], SessionSpecIn]:
    """The stored spec with the patch merged in, validated as a PUT body would be; pydantic
    errors are re-raised with a `body.spec` loc prefix so the text equals PUT's."""
    merged = merge_patch(stored, patch)
    secret_in_patch(patch, merged)
    try:
        spec = SessionSpecIn.model_validate(merged)
    except ValidationError as exc:
        errors = [{**e, "loc": ("body", "spec", *e["loc"])} for e in exc.errors()]
        raise RequestValidationError(errors) from None
    return merged, spec

"""Project routes. A project groups presets and sessions; the api is the only writer."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import Any

import psycopg
from fastapi import APIRouter, Request, Response

from agents_orchestrator.api import schemas, secret_store
from agents_orchestrator.api.errors import ApiError, not_found, project_error
from agents_orchestrator.api.presets import check_secret_size, store_failed
from agents_orchestrator.api.service_auth import calling_service
from agents_orchestrator.api.sessions import Cfg, Pool
from agents_orchestrator.db import projects as repo
from agents_orchestrator.spec import ENV_NAME_RULE, env_name_allowed

# Each project secret becomes an env var of every pod of the project's presets (plan 3.16).
PROJECT_SECRETS_MAX = 100

router = APIRouter(prefix="/api/v1")


def view(project: repo.Project) -> schemas.ProjectView:
    return schemas.ProjectView(
        slug=project.slug,
        name=project.name,
        description=project.description,
        archived=project.archived_at is not None,
        created_at=project.created_at,
        updated_at=project.updated_at,
        presets=project.presets,
        sessions=project.sessions,
    )


def _invalid(exc: ValueError) -> ApiError:
    return ApiError(422, "invalid_request", str(exc))


@router.get("/projects", response_model=list[schemas.ProjectView])
async def list_projects(
    request: Request, pool: Pool, archived: bool = False
) -> list[schemas.ProjectView]:
    """Active projects, or only the archived ones with `archived=true`. Counts are left out on
    the service port."""
    async with pool.connection() as conn:
        rows = await repo.list_projects(
            conn, archived=archived, counts=calling_service(request) is None
        )
    return [view(p) for p in rows]


@router.post("/projects", response_model=schemas.ProjectView, status_code=201)
async def create_project(body: schemas.ProjectIn, pool: Pool) -> schemas.ProjectView:
    try:
        async with pool.connection() as conn:
            project = await repo.create_project(conn, body.slug, body.name, body.description)
    except ValueError as exc:
        raise _invalid(exc) from None
    except repo.ProjectExists as exc:
        raise project_error(exc) from None
    return view(project)


@router.get("/projects/{slug}", response_model=schemas.ProjectView)
async def get_project(slug: str, request: Request, pool: Pool) -> schemas.ProjectView:
    async with pool.connection() as conn:
        project = await repo.get_project(conn, slug)
        if project is None:
            raise not_found("project")
        if calling_service(request) is None:
            counts = await repo.project_counts(conn, slug)
            project = replace(project, **counts)
    return view(project)


@router.patch("/projects/{slug}", response_model=schemas.ProjectView)
async def patch_project(slug: str, body: schemas.ProjectPatch, pool: Pool) -> schemas.ProjectView:
    try:
        async with pool.connection() as conn:
            project = await repo.update_project(conn, slug, **body.model_dump(exclude_unset=True))
            counts = await repo.project_counts(conn, slug)
    except ValueError as exc:
        raise _invalid(exc) from None
    except repo.ProjectNotFound:
        raise not_found("project") from None
    except repo.DefaultProject as exc:
        raise project_error(exc) from None
    return view(replace(project, **counts))


# ------------------------------------------------- project secrets (plan 3.16), main port only
# The checks and the locked write below are shared with the admin routes (preflight F8).


def check_secret_name(name: str) -> None:
    """422 `invalid_request` for a name outside the `spec.env` name rule."""
    if not env_name_allowed(name):
        raise ApiError(422, "invalid_request", f"{ENV_NAME_RULE}: {name}")


def _refuse_archived(row: dict[str, Any]) -> None:
    if row["archived_at"] is not None:
        raise ApiError(422, "project_archived", f"project {row['slug']} is archived")


def _outside_transaction(conn: psycopg.AsyncConnection) -> None:
    """Refuse a connection inside a transaction: `conn.transaction()` would then be a savepoint
    and the `gc` meant to run after the commit would run before it."""
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        raise RuntimeError("project secret writes need a connection outside a transaction")


async def _hold_writable(
    conn: psycopg.AsyncConnection, slug: str
) -> tuple[dict[str, Any], list[str]]:
    """Lock the project row FOR UPDATE (the order every project secret writer uses) and return
    it (`slug`, `archived_at`, as `set_secret` hands it to a recheck) with its live secret names;
    404 for an unknown project, 422 `project_archived`."""
    try:
        project = await repo.require_active(conn, slug, lock="update")
    except repo.UnknownProject:
        raise not_found("project") from None
    except repo.ProjectArchived as exc:
        raise project_error(exc) from None
    metas = await secret_store.list_metas(conn, secret_store.Owner.project(slug))
    return {"slug": project.slug, "archived_at": project.archived_at}, [m.name for m in metas]


async def write_project_secret(
    conn: psycopg.AsyncConnection,
    request: Request,
    namespace: str,
    slug: str,
    name: str,
    body: schemas.SecretIn,
    recheck: Callable[[dict[str, Any]], None] | None = None,
) -> schemas.ProjectSecretSet:
    """Store the next version of the project secret `name`. The value is never echoed.

    The cap is counted under the project row lock, so two writes of new names cannot both pass
    it. `recheck` sees the locked project row before anything is written (the archived check
    runs first)."""
    _outside_transaction(conn)
    check_secret_name(name)
    check_secret_size(body.value)
    owner = secret_store.Owner.project(slug)

    def checks(row: dict[str, Any]) -> None:
        _refuse_archived(row)
        if recheck is not None:
            recheck(row)

    kube = secret_store.kube_of(request)
    try:
        async with conn.transaction():
            _, live = await _hold_writable(conn, slug)
            if name not in live and len(live) >= PROJECT_SECRETS_MAX:
                raise ApiError(
                    422,
                    "too_many_secrets",
                    f"a project holds at most {PROJECT_SECRETS_MAX} secrets",
                )
            meta = await secret_store.set_secret(
                conn, kube, namespace, owner, name, body.value, collect=False, recheck=checks
            )
        await secret_store.gc(conn, kube, namespace, owner=owner, name=name)
    except secret_store.ProjectNotFound:
        raise not_found("project") from None
    except secret_store.SecretStoreError:
        raise store_failed() from None
    return schemas.ProjectSecretSet(name=meta.name, version=meta.version)


async def retire_project_secret(
    conn: psycopg.AsyncConnection,
    request: Request,
    namespace: str,
    slug: str,
    name: str,
    recheck: Callable[[dict[str, Any]], None] | None = None,
) -> None:
    """Retire the project secret `name`: new sessions no longer get it; running ones keep their
    snapshot. 404 for an unknown project or a name with no live secret (`retire` itself would
    silently do nothing)."""
    _outside_transaction(conn)
    owner = secret_store.Owner.project(slug)
    async with conn.transaction():
        row, live = await _hold_writable(conn, slug)
        if name not in live:
            raise not_found("project secret")
        if recheck is not None:
            recheck(row)
        await secret_store.retire(conn, owner, name)
    await secret_store.gc(conn, secret_store.kube_of(request), namespace, owner=owner, name=name)


@router.get("/projects/{slug}/secrets", response_model=list[schemas.ProjectSecretView])
async def list_project_secrets(slug: str, pool: Pool) -> list[schemas.ProjectSecretView]:
    """The project's live secrets, metadata only, with the active sessions still on an older
    version. Archived projects keep answering."""
    async with pool.connection() as conn:
        if await repo.get_project(conn, slug) is None:
            raise not_found("project")
        rows = await repo.secrets_overview(conn, slug)
    return [schemas.ProjectSecretView.model_validate(r) for r in rows]


@router.put("/projects/{slug}/secrets/{name}", response_model=schemas.ProjectSecretSet)
async def set_project_secret(
    slug: str, name: str, body: schemas.SecretIn, request: Request, pool: Pool, settings: Cfg
) -> schemas.ProjectSecretSet:
    async with pool.connection() as conn:
        return await write_project_secret(conn, request, settings.namespace, slug, name, body)


@router.delete("/projects/{slug}/secrets/{name}", status_code=204)
async def delete_project_secret(
    slug: str, name: str, request: Request, pool: Pool, settings: Cfg
) -> Response:
    async with pool.connection() as conn:
        await retire_project_secret(conn, request, settings.namespace, slug, name)
    return Response(status_code=204)

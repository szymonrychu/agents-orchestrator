"""Project routes. A project groups presets and sessions; the api is the only writer."""

from __future__ import annotations

from dataclasses import replace

from fastapi import APIRouter, Request

from agents_orchestrator.api import schemas
from agents_orchestrator.api.errors import ApiError, not_found, project_error
from agents_orchestrator.api.service_auth import calling_service
from agents_orchestrator.api.sessions import Pool
from agents_orchestrator.db import projects as repo

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

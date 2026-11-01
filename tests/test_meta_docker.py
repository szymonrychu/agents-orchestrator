"""`/meta` reports whether the docker sidecar is available and its caps."""

from __future__ import annotations

from test_spec_session_options import app_client


async def test_meta_docker_off_by_default(pool, dsn):
    async with app_client(pool, dsn) as http:
        docker = (await http.get("/api/v1/meta")).json()["docker"]
    assert docker == {
        "available": False,
        "storage": "30Gi",
        "max_storage": "100Gi",
        "max_cpu": "8",
        "max_memory": "16Gi",
    }


async def test_meta_docker_available_with_image_and_caps(pool, dsn):
    async with app_client(
        pool,
        dsn,
        docker_image="docker:27.3.1-dind",
        docker_storage="20Gi",
        max_docker_storage="50Gi",
        max_docker_cpu="4",
        max_docker_memory="12Gi",
    ) as http:
        docker = (await http.get("/api/v1/meta")).json()["docker"]
    assert docker == {
        "available": True,
        "storage": "20Gi",
        "max_storage": "50Gi",
        "max_cpu": "4",
        "max_memory": "12Gi",
    }

"""Agent pods and Jobs run as ORCH_AGENT_SERVICE_ACCOUNT with no mounted token (G3, D4)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from agents_orchestrator.db import queue
from agents_orchestrator.db.sessions import Session
from agents_orchestrator.k8s import render
from agents_orchestrator.reconciler.cache import render_cache_job
from agents_orchestrator.reconciler.checks import render_job
from agents_orchestrator.reconciler.init import render_init_job
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec
from agents_orchestrator.tokens import SessionTokens

TOKENS = SessionTokens(auth="a" * 32, peer="p" * 32, agent="g" * 32)
URL = "https://s3.example/bucket/workspace-cache/k.tar.zst?X-Amz-Signature=abc"
WHEN = datetime(2026, 10, 26, tzinfo=UTC)


def make_settings(**over: Any) -> Settings:
    base: dict[str, Any] = {
        "database_url": "postgresql://u:p@db/orch",
        "master_key": "m" * 32,
        "agent_image": "harbor.example/containers/claude-code-wrapper:ca2ddf3",
        "default_model": "sonnet",
        "models": ["sonnet"],
        "cache_s3_endpoint": "http://rgw.example:80",
        "cache_s3_bucket": "agents-workspace-cache",
        "cache_s3_access_key_id": "TESTIDEXAMPLEACCESSKEY",
        "cache_s3_secret_access_key": "s3cr3t-example-secret-access-key",
        "cache_image": "harbor.example/containers/agents-orchestrator:abc1234",
        "kube_access_service_account": "orch-kube-access",
    }
    base.update(over)
    return Settings(**base)


def make_session(**spec: Any) -> Session:
    return Session(
        id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        slug="alpha",
        spec=SessionSpec.model_validate(
            {
                "init_scripts": ["echo hi"],
                "cache": {
                    "restore_key": "preset-0123456789abcdef",
                    "save_key": "preset-0123456789abcdef",
                },
                **spec,
            }
        ),
        generation=1,
        observed_generation=0,
        desired="running",
        status={},
        created_at=WHEN,
    )


def assert_explicit_sa_no_token(pod: dict[str, Any], account: str) -> None:
    assert pod["serviceAccountName"] == account
    assert pod["automountServiceAccountToken"] is False
    assert "serviceAccount" not in pod


SETTINGS = [({}, "default"), ({"agent_service_account": "orch-agent"}, "orch-agent")]


@pytest.mark.parametrize(("over", "account"), SETTINGS)
def test_agent_pod_explicit_sa_no_token(over: dict[str, Any], account: str) -> None:
    objs = render.render_session(make_session(), make_settings(**over), TOKENS)
    (sts,) = (o for o in objs if o["kind"] == "StatefulSet")
    assert_explicit_sa_no_token(sts["spec"]["template"]["spec"], account)


@pytest.mark.parametrize("kube_access", [None, False, True])
@pytest.mark.parametrize(("over", "account"), SETTINGS)
def test_jobs_explicit_sa_no_token(
    over: dict[str, Any], account: str, kube_access: bool | None
) -> None:
    """Every Job runs unprivileged, a kube_access session's included."""
    settings = make_settings(**over)
    session = make_session(kube_access=kube_access)
    check = queue.Check(
        id=7,
        session_id=session.id,
        command="pytest -q",
        status="queued",
        job_name=None,
        created_at=WHEN,
        started_at=WHEN,
    )
    jobs = [
        render_init_job(session, settings),
        render_job(session, check, settings),
        render_cache_job(session, settings, "restore", URL),
        render_cache_job(session, settings, "save", URL),
    ]
    for job in jobs:
        assert_explicit_sa_no_token(job["spec"]["template"]["spec"], account)

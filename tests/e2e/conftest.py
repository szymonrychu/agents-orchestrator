"""tests/e2e: run through scripts/e2e.sh, or on its own against an environment it brings up.

`mise run test:e2e` (scripts/e2e.sh) creates the kind cluster, installs the chart, port-forwards
the api and then runs `pytest -m e2e tests/e2e` with ORCH_E2E_API_URL set. A test named on its
own (`uv run pytest tests/e2e/test_e2e_kind.py::<name>`) gets the same environment: the default
`-m 'not integration and not e2e'` from pyproject.toml is lifted for a run that names tests/e2e,
and when ORCH_E2E_API_URL is unset the session fixture runs scripts/e2e.sh in hold mode
(E2E_HOLD_FILE), points this process at the kind cluster it reports, and removes the hold file
at the end so the script deletes the kind cluster.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

E2E_DIR = Path(__file__).resolve().parent
ROOT = E2E_DIR.parents[1]
BRINGUP_TIMEOUT = 3600  # kind, two image builds, the chart and helm test
TEARDOWN_TIMEOUT = 600


def _explicit_markexpr(config: pytest.Config) -> bool:
    return any(
        arg == "-m" or (arg.startswith("-m") and not arg.startswith("--"))
        for arg in config.invocation_params.args
    )


def names_e2e(args: list[str] | tuple[str, ...], invocation_dir: Path) -> bool:
    """Whether a command line names tests/e2e (the directory, a file in it, or a node id)."""
    for arg in args:
        if arg.startswith("-"):
            continue
        path = (invocation_dir / arg.split("::", 1)[0]).resolve()
        if path == E2E_DIR or E2E_DIR in path.parents:
            return True
    return False


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Keep the e2e tests a run names explicitly; an explicit -m still wins."""
    if _explicit_markexpr(config):
        return
    if names_e2e(config.invocation_params.args, config.invocation_params.dir):
        config.option.markexpr = "not integration"


def _tail(path: Path, lines: int = 80) -> str:
    return "\n".join(path.read_text(errors="replace").splitlines()[-lines:])


@pytest.fixture(scope="session", autouse=True)
def kind_environment() -> Iterator[None]:  # noqa: C901
    """Bring up the kind environment with scripts/e2e.sh unless the script is running us."""
    if os.environ.get("ORCH_E2E_API_URL"):
        yield
        return
    work = Path(tempfile.mkdtemp(prefix="orch-e2e-hold."))
    hold, log = work / "env", work / "e2e.log"
    env = {**os.environ, "E2E_HOLD_FILE": str(hold), "E2E_HOLD_PID": str(os.getpid())}
    with log.open("w") as out:
        proc = subprocess.Popen(
            [str(ROOT / "scripts" / "e2e.sh")],
            cwd=ROOT,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=subprocess.STDOUT,
        )
    saved: dict[str, str | None] = {}
    try:
        deadline = time.monotonic() + BRINGUP_TIMEOUT
        while not hold.exists():
            if proc.poll() is not None:
                pytest.fail(
                    f"scripts/e2e.sh exited {proc.returncode} before the environment was up "
                    f"(log {log}):\n{_tail(log)}"
                )
            if time.monotonic() > deadline:
                pytest.fail(f"scripts/e2e.sh not up after {BRINGUP_TIMEOUT}s:\n{_tail(log)}")
            time.sleep(2)
        values = dict(line.split("=", 1) for line in hold.read_text().splitlines() if "=" in line)
        for key, value in values.items():
            saved[key] = os.environ.get(key)
            os.environ[key] = value
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        hold.unlink(missing_ok=True)
        if proc.poll() is None:
            try:
                proc.wait(timeout=TEARDOWN_TIMEOUT)
            except subprocess.TimeoutExpired:
                proc.terminate()
                proc.wait(timeout=120)
        if proc.returncode == 0:
            shutil.rmtree(work, ignore_errors=True)  # a failed run keeps its log

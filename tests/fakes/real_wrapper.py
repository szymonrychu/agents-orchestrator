"""Start the real claude-code-wrapper test image (ORCH_TEST_WRAPPER_IMAGE) for the contract suite.

Build the image with e2e/agent-test.Dockerfile. The container runs e2e/agent-test.config.yaml,
which points claude.binary at the wrapper's scripted fake claude, so no credentials are needed.
"""

from __future__ import annotations

import subprocess
import time
import uuid
from pathlib import Path

import httpx

E2E_DIR = Path(__file__).resolve().parents[2] / "e2e"
CONFIG = E2E_DIR / "agent-test.config.yaml"
INTERNAL_PORT = 8080


class RealWrapper:
    def __init__(self, image: str, token: str = "ci-token", ready_timeout: float = 120.0) -> None:
        self.image = image
        self.token = token
        self.ready_timeout = ready_timeout
        self.name = f"orch-wrapper-{uuid.uuid4().hex[:8]}"
        self.base_url = ""

    def _docker(self, *args: str) -> str:
        done = subprocess.run(["docker", *args], capture_output=True, text=True, check=False)
        if done.returncode != 0:
            raise RuntimeError(f"docker {' '.join(args)} failed: {done.stderr.strip()}")
        return done.stdout.strip()

    def start(self) -> None:
        # create + cp instead of a bind mount: the docker daemon may not share this filesystem.
        self._docker(
            "create", "--name", self.name,
            "-p", f"127.0.0.1::{INTERNAL_PORT}",
            self.image,
        )  # fmt: skip
        try:
            self._docker("cp", str(CONFIG), f"{self.name}:/etc/ccw/config.yaml")
            self._docker("start", self.name)
            port = self._docker("port", self.name, f"{INTERNAL_PORT}/tcp").splitlines()[0]
            self.base_url = f"http://127.0.0.1:{port.rsplit(':', 1)[1]}"
            self._wait_ready()
        except BaseException:
            self.stop()
            raise

    def _wait_ready(self) -> None:
        end = time.monotonic() + self.ready_timeout
        while time.monotonic() < end:
            if self._docker("inspect", "-f", "{{.State.Running}}", self.name) != "true":
                break
            try:
                if httpx.get(f"{self.base_url}/readyz", timeout=2).status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.5)
        logs = subprocess.run(
            ["docker", "logs", "--tail", "40", self.name], capture_output=True, text=True
        )
        raise RuntimeError(f"wrapper not ready:\n{logs.stdout}{logs.stderr}")

    def stop(self) -> None:
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True, check=False)

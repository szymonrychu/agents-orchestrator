"""Image CI and pin consistency (D16, D17, D22)."""

import re
import subprocess
import tomllib
from pathlib import Path

import yaml

from agents_orchestrator.settings import Settings

ROOT = Path(__file__).resolve().parent.parent
IMAGE = "harbor.szymonrichert.pl/containers/agents-orchestrator"


def _ci() -> dict:
    return yaml.safe_load((ROOT / ".github/workflows/ci.yaml").read_text())


def _pyproject() -> dict:
    return tomllib.loads((ROOT / "pyproject.toml").read_text())


def test_ci_pushes_orchestrator_image_on_main_only():
    text = (ROOT / ".github/workflows/ci.yaml").read_text()
    job = _ci()["jobs"]["images"]
    steps = job["steps"]
    runs = [s.get("run", "") for s in steps]
    assert any("docker build" in r for r in runs)
    assert any("docker run --rm" in r and "--help" in r for r in runs)
    pushing = [s for s in steps if "regctl image import" in s.get("run", "")]
    assert len(pushing) == 1
    push = pushing[0]
    assert "github.ref == 'refs/heads/main'" in push["if"]
    assert "github.event_name == 'push'" in push["if"]
    assert job["env"]["IMAGE"] == IMAGE
    assert '"$IMAGE:$SHORTSHA"' in push["run"]
    assert any("${GITHUB_SHA::7}" in r and "SHORTSHA" in r for r in runs)
    logins = [s for s in steps if "registry login" in s.get("run", "")]
    assert len(logins) == 1
    assert "github.ref == 'refs/heads/main'" in logins[0]["if"]
    assert "HARBOR_USERNAME" in str(logins[0]) and "HARBOR_PASSWORD" in str(logins[0])
    # exactly one image reference is ever pushed
    assert set(re.findall(r"harbor\.szymonrichert\.pl/[\w./-]+", text)) == {
        IMAGE,
        "harbor.szymonrichert.pl/charts",
    }
    assert "pull_request" in _ci()[True]
    # layers go up in 50MB chunks (Cloudflare answers 413 above 100MB)
    assert "--blob-chunk 52428800 --blob-max 52428800" in push["run"]
    # nothing reaches Harbor before lint, test and e2e pass
    assert {"lint", "test", "e2e"} <= set(job["needs"])


def test_no_derived_agent_image():
    files = subprocess.run(
        ["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.splitlines()
    assert not [f for f in files if f.startswith("agent-image/")]
    ci = (ROOT / ".github/workflows/ci.yaml").read_text()
    assert "agent-image" not in ci
    # CI builds and pushes no agent image; the e2e job may still pull the wrapper image.
    assert "containers/agents-orchestrator-agent" not in ci
    imports = [
        s["run"]
        for j in _ci()["jobs"].values()
        for s in j["steps"]
        if "regctl image import" in s.get("run", "")
    ]
    assert len(imports) == 1 and "agents-orchestrator-agent" not in imports[0]
    # No production Dockerfile derives from the wrapper image. The only derived image is the
    # test-only e2e/agent-test.Dockerfile, which is never pushed.
    derived = []
    for f in files:
        if f.rsplit("/", 1)[-1].lower().endswith("dockerfile"):
            text = (ROOT / f).read_text()
            froms = re.findall(r"^FROM\s+(\S+)", text, re.M | re.I)
            args = dict(re.findall(r"^ARG\s+(\w+)=(\S+)", text, re.M))
            for ref in froms:
                ref = re.sub(r"\$\{(\w+)\}", lambda m, a=args: a.get(m.group(1), ""), ref)
                if "claude-code-wrapper" in ref:
                    derived.append(f)
    assert derived == ["e2e/agent-test.Dockerfile"]


def test_ccw_dev_dependency_matches_agent_image_sha():
    dev = _pyproject()["dependency-groups"]["dev"]
    pin = next(d for d in dev if d.startswith("claude-code-wrapper"))
    sha = re.search(r"claude-code-wrapper@([0-9a-f]{40})$", pin).group(1)
    readme = (ROOT / "README.md").read_text()
    images = readme.split("## Images", 1)[1].split("\n## ", 1)[0]
    ref = r"harbor\.szymonrichert\.pl/containers/claude-code-wrapper:(\d+\.\d+\.\d+)\b"
    m = re.search(ref, images)
    assert m, "README ## Images must name the agent image"
    dockerfile = (ROOT / "e2e/agent-test.Dockerfile").read_text()
    assert f"claude-code-wrapper:{m.group(1)}" in dockerfile  # semver tag, same in both places
    lock = (ROOT / "uv.lock").read_text()
    assert f"claude-code-wrapper?rev={sha}#{sha}" in lock


def test_agent_test_dockerfile_matches_dev_dependency():
    dev = _pyproject()["dependency-groups"]["dev"]
    pin = next(d for d in dev if d.startswith("claude-code-wrapper"))
    sha = re.search(r"claude-code-wrapper@([0-9a-f]{40})$", pin).group(1)
    text = (ROOT / "e2e/agent-test.Dockerfile").read_text()
    args = dict(re.findall(r"^ARG\s+(\w+)=(\S+)", text, re.M))
    assert re.fullmatch(
        r"harbor\.szymonrichert\.pl/containers/claude-code-wrapper:\d+\.\d+\.\d+",
        args["BASE_IMAGE"],
    )
    assert args["WRAPPER_SHA"] == sha


def test_pyproject_any_mcp_pin_matches_settings_ref():
    deps = _pyproject()["project"]["dependencies"]
    pin = next(d for d in deps if d.startswith("any-mcp"))
    tag = re.search(r"any-mcp@(v[\w.]+)$", pin).group(1)
    assert tag == Settings.model_fields["any_mcp_ref"].default
    # uv refuses the plugins workspace's own any-mcp source, so an override keeps one any-mcp.
    assert _pyproject()["tool"]["uv"]["override-dependencies"] == [pin]
    lock = (ROOT / "uv.lock").read_text()
    assert f"any-mcp?rev={tag}#" in lock
    assert lock.count('\nname = "any-mcp"\n') == 1


def test_plugins_dev_pin_matches_settings_ref():
    dev = _pyproject()["dependency-groups"]["dev"]
    pin = next(d for d in dev if d.startswith("any-mcp-tatara-orchestrator"))
    m = re.search(r"git\+(\S+?)@(v[\w.]+)#subdirectory=", pin)
    assert m, pin
    repo, tag = m.groups()
    assert repo == Settings.model_fields["any_mcp_plugins_repo"].default
    assert tag == Settings.model_fields["any_mcp_plugins_ref"].default
    lock = (ROOT / "uv.lock").read_text()
    assert f"any-mcp-tatara-plugins?subdirectory=orchestration%2Forchestrator&rev={tag}#" in lock


MAIN_PUSH = "github.event_name == 'push' && github.ref == 'refs/heads/main'"
RELEASED = "steps.version.outputs.version != ''"


def _release() -> dict:
    return _ci()["jobs"]["release"]


def _step_index(steps: list[dict], needle: str) -> list[int]:
    return [i for i, s in enumerate(steps) if needle in s.get("run", "")]


def test_release_job_runs_last_on_main_push():
    jobs = _ci()["jobs"]
    job = jobs["release"]
    assert job["if"] == MAIN_PUSH
    assert set(job["needs"]) == set(jobs) - {"release"}
    assert set(job["needs"]) == {"lint", "test", "images", "chart", "e2e", "chart-publish"}
    assert job["permissions"] == {"contents": "write"}
    assert job["concurrency"] == {"group": "release", "cancel-in-progress": False}


def test_release_job_fetches_full_history_and_tags():
    checkout = _release()["steps"][0]
    assert checkout["uses"].startswith("actions/checkout@")
    assert checkout["with"]["fetch-depth"] == 0
    assert checkout["with"]["fetch-tags"] is True


def test_release_job_computes_version_with_script():
    steps = _release()["steps"]
    version = next(i for i, s in enumerate(steps) if s.get("id") == "version")
    # A bare assignment fails the step (bash -e) when the script fails; inside echo it would not.
    assert "shell" not in steps[version]
    assert steps[version]["run"].strip().splitlines() == [
        'version="$(python3 scripts/next_version.py)"',
        'echo "version=$version" >> "$GITHUB_OUTPUT"',
    ]
    later = steps[version + 1 :]
    assert later
    assert all(s.get("if") == RELEASED for s in later)


def test_release_job_retags_manifest_only():
    job = _release()
    steps = job["steps"]
    assert job["env"]["IMAGE"] == IMAGE
    assert job["env"]["HARBOR_URL"] == "harbor.szymonrichert.pl"
    copies = [
        line.strip()
        for s in steps
        for line in s.get("run", "").splitlines()
        if "regctl image copy" in line
    ]
    assert copies == [
        'regctl image copy "$IMAGE:$SHORTSHA" "$IMAGE:$VERSION"',
        'regctl image copy "$IMAGE:$SHORTSHA" "$IMAGE:$VERSION_MINOR"',
        'regctl image copy "$IMAGE:$SHORTSHA" "$IMAGE:$VERSION_MAJOR"',
    ]
    tag = steps[_step_index(steps, "regctl image copy")[0]]["run"]
    assert 'SHORTSHA="${GITHUB_SHA::7}"' in tag
    assert 'VERSION_MINOR="${VERSION%.*}"' in tag
    assert 'VERSION_MAJOR="${VERSION%%.*}"' in tag
    installers = [s["uses"] for s in steps if "regctl-installer" in s.get("uses", "")]
    assert len(installers) == 1
    assert re.fullmatch(r"regclient/actions/regctl-installer@[0-9a-f]{40}", installers[0])
    logins = [s for s in steps if "regctl registry login" in s.get("run", "")]
    assert len(logins) == 1
    assert logins[0]["env"] == {
        "HARBOR_USERNAME": "${{ secrets.HARBOR_USERNAME }}",
        "HARBOR_PASSWORD": "${{ secrets.HARBOR_PASSWORD }}",
    }
    text = yaml.safe_dump(job)
    for banned in ("docker", "regctl image import", "--blob-chunk"):
        assert banned not in text, banned


def test_release_job_pushes_git_tag_last():
    steps = _release()["steps"]
    last = steps[-1]["run"].strip().splitlines()
    assert [line.strip() for line in last] == [
        'git tag "v$VERSION" "$GITHUB_SHA"',
        'git push origin "refs/tags/v$VERSION"',
    ]
    assert max(_step_index(steps, "regctl image copy")) < len(steps) - 1
    assert steps[-1]["env"]["VERSION"] == "${{ steps.version.outputs.version }}"
    for s in steps:
        assert not re.search(r"(^|\s)(--force|-f)(\s|$)", s.get("run", "")), s
    assert "secrets.GITHUB_TOKEN" not in (ROOT / ".github/workflows/ci.yaml").read_text()

"""kind end-to-end: the chart installed into namespace `agents` (D9, D19, D22, D26, D28).

Run by `scripts/e2e.sh` (`mise run test:e2e`), which creates the kind cluster, installs the
chart with e2e/values-kind.yaml, runs `helm test`, port-forwards the api Service to
ORCH_E2E_API_URL and then runs this module. The tests share one agent session and run in
file order: the session is scheduled from a preset created through POST /presets, the
round-robin fixture sends the messages the event-stream test reads, the preset tests run after
them, and the discard fixture runs last, so the shared-namespace test compares against a
baseline taken before the session existed. Every fixture is lazy, so a single test can also run
alone:
`uv run pytest tests/e2e/test_e2e_kind.py::<name>` brings the same environment up itself
(tests/e2e/conftest.py runs scripts/e2e.sh in hold mode and deletes the kind cluster afterwards).

The test (never the orchestrator) uses kubectl exec to look inside agent pods.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import subprocess
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from websockets.exceptions import InvalidStatus
from websockets.sync.client import ClientConnection, connect

from agents_orchestrator.settings import Settings

pytestmark = pytest.mark.e2e

NAMESPACE = os.environ.get("ORCH_E2E_NAMESPACE", "agents")
RELEASE = os.environ.get("ORCH_E2E_RELEASE", "agents-orchestrator")
# not a secret: its sha256 is serviceApi.tokenHashes.e2e in e2e/values-kind.yaml
E2E_SERVICE_TOKEN = "e2e-service-token-not-a-secret"
API_URL = os.environ.get("ORCH_E2E_API_URL", "http://127.0.0.1:18080")
KUBE_CONTEXT = os.environ.get("ORCH_E2E_KUBE_CONTEXT", "kind-agents-orchestrator-e2e")
SLUG = "e2e-rr"
PRESET_NAME = "E2E two replicas"
GENERATED_SLUG = re.compile(r"^default-[a-z]{4}$")
SESSION_LABEL = "agents.szymonrichert.pl/session"
COMPONENTS = ("api", "dispatcher", "ingester", "reconciler")
RECONCILER_SA = f"system:serviceaccount:{NAMESPACE}:{RELEASE}-reconciler"

# The test agent image (e2e/agent-test.Dockerfile) ships the wrapper's scripted fake claude at
# /opt/agent-test/claude and a scenario whose "slow" turns take a fixed 4 s. spec.env puts the
# fake first on PATH (so the rendered config's `claude` binary resolves to it) and selects
# that scenario; the renderer passes both through to the claude child (claude.passthrough_env).
FAKE_TURN_ENV = {
    "PATH": "/opt/agent-test:/opt/mise/shims:/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin"
    ":/sbin:/bin",
    "FAKE_CLAUDE_SCENARIO": "/opt/agent-test/scenario.json",
}
SLOW_PROMPT = "slow task {n}: report when done"
DISPATCH_STAGGER = 1.5  # seconds between dispatching message 1 and sending message 2

# Run inside an agent pod: every session's claude config (.claude.json, settings.json).
SESSION_CONFIGS = (
    "import glob, json, os\n"
    "out = []\n"
    "for d in sorted(glob.glob('/sessions/*/config')):\n"
    "    docs = [json.load(open(os.path.join(d, f))) for f in ('.claude.json', 'settings.json')]\n"
    "    out.append(docs)\n"
    "print(json.dumps(out))\n"
)

READY_TIMEOUT = 900  # onStart pip-installs any-mcp from GitHub in every pod
TURN_TIMEOUT = 300
DISCARD_TIMEOUT = 180
LEADER_TIMEOUT = 90  # election and handover are applied within the dispatcher poll
HIVE_PUSH_TIMEOUT = 150  # hive push pass every 10 s
HIVE_QUIET_WINDOW = 30  # three push passes in which no unkeyed note may arrive
ALONE_SLUG = "e2e-alone"
SHELL_TIMEOUT = 60
EVIL_ORIGIN = "http://evil.example"


# ------------------------------------------------------------------ helpers


def run(*argv: str, check: bool = True, timeout: float = 300) -> subprocess.CompletedProcess:
    proc = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    if check and proc.returncode != 0:
        raise AssertionError(
            f"{' '.join(argv)} exited {proc.returncode}\nstdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    return proc


def kubectl(*args: str, check: bool = True, timeout: float = 300) -> subprocess.CompletedProcess:
    return run("kubectl", "-n", NAMESPACE, *args, check=check, timeout=timeout)


def kube_json(*args: str) -> dict[str, Any]:
    return json.loads(kubectl(*args, "-o", "json").stdout)


def wait_for(what: str, probe: Callable[[], Any], timeout: float, interval: float = 2.0) -> Any:
    """Poll `probe` until it returns a truthy value; fail naming `what` and the last value."""
    deadline = time.monotonic() + timeout
    last: Any = None
    while True:
        last = probe()
        if last:
            return last
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out after {timeout:.0f}s waiting for {what}: {last!r}")
        time.sleep(interval)


def labelled_objects(slug: str) -> list[str]:
    out = kubectl(
        "get",
        "statefulsets,services,configmaps,secrets,persistentvolumeclaims,pods",
        "-l",
        f"{SESSION_LABEL}={slug}",
        "-o",
        "name",
    ).stdout
    return [line for line in out.splitlines() if line.strip()]


def fingerprint(kind: str, name: str) -> dict[str, Any]:
    """What must not change on an orchestrator object: identity, spec generation and data."""
    obj = kube_json("get", kind, name)
    meta = obj["metadata"]
    out: dict[str, Any] = {"uid": meta["uid"], "generation": meta.get("generation")}
    if kind in ("configmap", "secret"):
        out["resourceVersion"] = meta["resourceVersion"]
        out["data"] = obj.get("data")
    if kind == "service":
        out["spec"] = obj["spec"]
    return out


def orchestrator_objects() -> list[tuple[str, str]]:
    """The chart's own Deployments, Service, ConfigMap and Secrets, plus the e2e Postgres."""
    names: list[tuple[str, str]] = []
    for kind in ("deployment", "service", "configmap", "secret", "serviceaccount", "role"):
        out = kubectl(
            "get", kind, "-l", f"app.kubernetes.io/instance={RELEASE}", "-o", "name"
        ).stdout
        names += [(kind, line.split("/", 1)[1]) for line in out.splitlines() if line.strip()]
    names += [("deployment", "postgres"), ("service", "postgres")]
    return sorted(set(names))


def release_setting(env: str, field: str) -> str:
    data = kube_json("get", "configmap", f"{RELEASE}-settings").get("data") or {}
    return data.get(env) or Settings.model_fields[field].default


def any_mcp_ref() -> str:
    return release_setting("ORCH_ANY_MCP_REF", "any_mcp_ref")


def orchestrator_source() -> str:
    """The reserved entry's source: the plugins repo at the release's pinned ref (D7)."""
    repo = release_setting("ORCH_ANY_MCP_PLUGINS_REPO", "any_mcp_plugins_repo")
    ref = release_setting("ORCH_ANY_MCP_PLUGINS_REF", "any_mcp_plugins_ref")
    return f"git::{repo}//orchestration/orchestrator?ref={ref}"


def agents_of(api: httpx.Client, session_id: str) -> list[dict[str, Any]]:
    resp = api.get(f"/sessions/{session_id}/agents")
    resp.raise_for_status()
    return resp.json()


def all_events(api: httpx.Client, session_id: str) -> list[list[dict[str, Any]]]:
    """Every page of the merged timeline, newest page first, each page as returned."""
    pages: list[list[dict[str, Any]]] = []
    params: dict[str, Any] = {"limit": 500}
    while True:
        resp = api.get(f"/sessions/{session_id}/events", params=params)
        resp.raise_for_status()
        body = resp.json()
        pages.append(body["events"])
        if not body["has_more_before"] or not body["events"]:
            return pages
        params = {"limit": 500, "before_id": body["events"][0]["id"]}


def leader_view(api: httpx.Client, session_id: str) -> tuple[dict[str, Any] | None, int | None]:
    """(the leader's agent view, the epoch) from the public session view; (None, None) before
    the first election."""
    view = api.get(f"/sessions/{session_id}").json()
    leader_id = view.get("leader_agent_id")
    if not leader_id:
        return None, None
    leader = next((a for a in agents_of(api, session_id) if a["id"] == leader_id), None)
    return leader, view["leader_epoch"]


def wait_leader(
    api: httpx.Client, session_id: str, epoch: int, agent_id: str | None = None
) -> dict[str, Any]:
    """Wait (at most LEADER_TIMEOUT) until the session view shows `epoch` (and `agent_id`)."""

    def probe() -> dict[str, Any] | None:
        leader, current = leader_view(api, session_id)
        ok = leader and current == epoch and (agent_id is None or leader["id"] == agent_id)
        return leader if ok else None

    return wait_for(f"leader at epoch {epoch}", probe, LEADER_TIMEOUT)


# Run inside an agent pod: one agent-API call with the pod's own URL, token and session id.
# argv: method, path, JSON body or "", "token"/"notoken". Prints `RESULT [status, body]`.
AGENT_CALL = (
    "import json, os, sys, urllib.request, urllib.error\n"
    "method, path, body, auth = sys.argv[1:5]\n"
    "headers = {'X-Orch-Session': os.environ['ORCH_SESSION_ID']}\n"
    "if auth == 'token':\n"
    "    headers['Authorization'] = 'Bearer ' + os.environ['ORCH_AGENT_TOKEN']\n"
    "data = None\n"
    "if body:\n"
    "    data = body.encode()\n"
    "    headers['Content-Type'] = 'application/json'\n"
    "url = os.environ['ORCH_AGENT_URL'] + path\n"
    "req = urllib.request.Request(url, data, headers, method=method)\n"
    "try:\n"
    "    r = urllib.request.urlopen(req, timeout=15)\n"
    "    out = [r.status, r.read().decode()]\n"
    "except urllib.error.HTTPError as e:\n"
    "    out = [e.code, e.read().decode()]\n"
    "print('RESULT ' + json.dumps(out))\n"
)


def pod_exec(pod: str, *argv: str) -> str:
    return kubectl("exec", pod, "-c", "ccw", "--", *argv, timeout=60).stdout


def agent_call(
    pod: str, method: str, path: str, body: dict[str, Any] | None = None, token: bool = True
) -> tuple[int, Any]:
    """Call the agent API from inside `pod` (kubectl exec + python3 urllib); (status, JSON)."""
    out = pod_exec(
        pod,
        "python3",
        "-c",
        AGENT_CALL,
        method,
        path,
        json.dumps(body) if body is not None else "",
        "token" if token else "notoken",
    )
    line = next(ln for ln in out.splitlines() if ln.startswith("RESULT "))
    status, text = json.loads(line.removeprefix("RESULT "))
    try:
        return status, json.loads(text)
    except ValueError:
        return status, text


def pod_name_of(agent: dict[str, Any]) -> str:
    return agent["pod_name"]


def on_start_any_mcp(configmap: dict[str, Any]) -> dict[str, Any]:
    """The any-mcp JSON a session's on-start.sh writes (the heredoc body)."""
    script = configmap["data"]["on-start.sh"]
    match = re.search(r"<<'(\w+)'\n(.*?)\n\1\n", script, re.DOTALL)
    assert match, script
    return json.loads(match.group(2))


HIVE_ONLY_TOOLS = {
    "me",
    "activity",
    "leadership_handover",
    "lock_claim",
    "lock_release",
    "lock_list",
    "lock_reassign",
    "scale",
}


ALONE_SESSION_TOOLS = [
    "sessions_list",
    "session_send",
    "init_scripts_get",
    "init_scripts_edit",
    "chat_rooms",
    "chat_send",
    "chat_react",
    "chat_status",
]


def assert_alone_session_tools(configmap: dict[str, Any]) -> None:
    """An alone session created by a user renders the orchestrator entry with exactly the session,
    init-scripts and chat tools, as a literal list so a code change cannot pass by agreeing with
    itself."""
    plugins = on_start_any_mcp(configmap).get("plugins", [])
    entry = next((p for p in plugins if p.get("name") == "orchestrator"), None)
    assert entry is not None, plugins
    assert entry["source"] == orchestrator_source() and "plugin" not in entry, entry
    assert entry["allow"] == ALONE_SESSION_TOOLS, entry
    assert not set(entry["allow"]) & HIVE_ONLY_TOOLS, entry
    assert entry["config"]["hive"] is False, entry


def ws_url(api: httpx.Client, agent_id: str, terminal_id: str) -> str:
    """The attach WebSocket URL on the same host and port as the REST API port-forward."""
    base = str(api.base_url).rstrip("/").replace("http", "ws", 1)
    return f"{base}/agents/{agent_id}/terminals/{terminal_id}/attach"


def attach(api: httpx.Client, agent_id: str, terminal_id: str, **kwargs: Any) -> ClientConnection:
    """A headless attach: no Origin unless given, and never through an environment proxy."""
    return connect(ws_url(api, agent_id, terminal_id), proxy=None, **kwargs)


def read_until(ws: ClientConnection, pattern: str, timeout: float = SHELL_TIMEOUT) -> str:
    """Collect PTY output (binary frames; text frames are control JSON) until `pattern` matches.

    The pattern is matched per line against the output with every CR, CRLF or LF turned into one
    newline: a pty echoes the typed command ending in CRLF and then prints the command's output
    after a bare CR, so `^ok$` finds the output line and never the echoed `echo ok`.
    """
    deadline = time.monotonic() + timeout
    seen = ""
    while not re.search(pattern, re.sub(r"\r\n?", "\n", seen), re.MULTILINE):
        left = deadline - time.monotonic()
        if left <= 0:
            raise AssertionError(f"no match for {pattern!r} within {timeout:.0f}s in {seen!r}")
        try:
            frame = ws.recv(timeout=left)
        except TimeoutError:
            continue
        if isinstance(frame, bytes):
            seen += frame.decode(errors="replace")
    return seen


def terminals_of(api: httpx.Client, session_id: str) -> list[dict[str, Any]]:
    resp = api.get(f"/sessions/{session_id}/terminals")
    assert resp.status_code == 200, resp.text
    return resp.json()["terminals"]


@contextlib.contextmanager
def shell(api: httpx.Client, agent_id: str) -> Iterator[dict[str, Any]]:
    """A terminal created through POST /agents/{id}/terminals; deleted on exit if still there."""
    made = api.post(f"/agents/{agent_id}/terminals", json={"rows": 24, "cols": 80})
    assert made.status_code == 201, made.text
    term = made.json()
    try:
        yield term
    finally:
        api.delete(f"/agents/{agent_id}/terminals/{term['terminal_id']}")


# ------------------------------------------------------------------ fixtures


@pytest.fixture(scope="module", autouse=True)
def kind_context() -> None:
    """Refuse to touch anything but the e2e kind cluster (kubectl and helm follow KUBECONFIG)."""
    current = run("kubectl", "config", "current-context", check=False).stdout.strip()
    if current != KUBE_CONTEXT:
        pytest.fail(
            f"kube context is {current!r}, not {KUBE_CONTEXT!r}: run through scripts/e2e.sh "
            "(mise run test:e2e), which creates the kind cluster and points KUBECONFIG at it"
        )


# uvicorn closes an idle keep-alive connection after 5 s (timeout_keep_alive, not set by
# cli.build_uvicorn_config) and httpx keeps one for 5 s by default. Through kubectl
# port-forward a test idle for about that long sent its next request on a connection the
# server was closing and got "Server disconnected without sending a response" (CI run
# 40565058706). A 1 s window leaves the server's close well out of reach.
API_KEEPALIVE_EXPIRY = 1.0


def api_client(base: str, client_class: type[httpx.Client] = httpx.Client) -> httpx.Client:
    return client_class(
        base_url=f"{base}/api/v1",
        timeout=30,
        limits=httpx.Limits(keepalive_expiry=API_KEEPALIVE_EXPIRY),
    )


@pytest.fixture(scope="module")
def api() -> Iterator[httpx.Client]:
    # read at fixture time: tests/e2e/conftest.py may have brought the environment up
    base = os.environ.get("ORCH_E2E_API_URL", API_URL)
    with api_client(base) as client:
        yield client


@pytest.fixture(scope="module")
def baseline() -> dict[tuple[str, str], dict[str, Any]]:
    """Fingerprints of the orchestrator's own objects before any agent session exists."""
    objs = orchestrator_objects()
    assert ("deployment", f"{RELEASE}-reconciler") in objs, objs
    return {o: fingerprint(*o) for o in objs}


@pytest.fixture(scope="module")
def preset(api: httpx.Client) -> Iterator[dict[str, Any]]:
    """The 2-replica, peers-on preset the shared session is scheduled from (POST /presets)."""
    resp = api.post(
        "/presets",
        json={
            "name": PRESET_NAME,
            "spec": {"replicas": 2, "peers": True, "env": FAKE_TURN_ENV},
        },
    )
    assert resp.status_code == 201, resp.text
    created = resp.json()
    try:
        yield created
    finally:  # the edit-and-delete test already removed it; a lone run may not have
        api.delete(f"/presets/{created['id']}")


@pytest.fixture(scope="module")
def session(api: httpx.Client, baseline: dict, preset: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """A 2-replica, peers-on session scheduled from a preset, with 2 idle agents."""
    resp = api.post("/sessions", json={"preset_id": preset["id"], "slug": SLUG})
    assert resp.status_code == 201, resp.text
    created = resp.json()
    cid = created["id"]
    assert created["slug"] == SLUG, created
    assert created["preset_id"] == preset["id"], created
    assert created["preset_name"] == PRESET_NAME, created

    def two_idle_agents() -> list[dict[str, Any]] | None:
        view = api.get(f"/sessions/{cid}").json()
        if view["status"].get("error"):
            raise AssertionError(f"session reconcile error: {view['status']}")
        agents = [a for a in agents_of(api, cid) if not a["gone"]]
        ready = [a for a in agents if a["ready"] and a["state"] == "idle" and a["free_since"]]
        return ready if len(ready) == 2 else None

    try:
        agents = wait_for("2 ready idle agents with sessions", two_idle_agents, READY_TIMEOUT)
    except AssertionError:
        print(kubectl("get", "pods,pvc", "-l", f"{SESSION_LABEL}={SLUG}", check=False).stdout)
        print(kubectl("describe", "pods", "-l", f"{SESSION_LABEL}={SLUG}", check=False).stdout)
        print(kubectl("logs", "-l", f"{SESSION_LABEL}={SLUG}", "--tail=80", check=False).stdout)
        raise
    try:
        yield {**created, "agents": agents}
    finally:  # the discard fixture normally removed it already (404 then); never leave it behind
        api.delete(f"/sessions/{cid}")


def message(api: httpx.Client, mid: int) -> dict[str, Any]:
    view = api.get(f"/messages/{mid}").json()
    if view["status"] == "failed":
        raise AssertionError(f"message {mid} failed: {view['error']}")
    return view


def first_idle_after(events: list[dict[str, Any]], agent_id: str, after_id: int) -> int:
    """Id of the agent's first session_state `ready` (idle) event newer than `after_id`."""
    return next(
        e["id"]
        for e in events
        if e["agent_id"] == agent_id
        and e["id"] > after_id
        and e["type"] == "session_state"
        and (e["data"] or {}).get("state") == "ready"
    )


@pytest.fixture(scope="module")
def round_robin(api: httpx.Client, session: dict[str, Any]) -> dict[str, Any]:
    """Three slow (4 s) messages; each awaited until its turn completed.

    The session is a hive, so untargeted messages go to the leader: the first is untargeted
    (the leader), the second targets the other agent explicitly (an explicit target wins) and
    the third is untargeted again and waits for the leader. The second is sent DISPATCH_STAGGER
    seconds after the first was dispatched and the third right after the second was dispatched,
    so both agents are busy when the third arrives and the leader frees well before the other.
    """
    cid = session["id"]
    leader = wait_for("a leader", lambda: leader_view(api, cid)[0], LEADER_TIMEOUT)
    other = next(a for a in agents_of(api, cid) if a["id"] != leader["id"])

    def send(n: int, target: str | None = None) -> int:
        body: dict[str, Any] = {"text": SLOW_PROMPT.format(n=n)}
        if target:
            body["target_agent_id"] = target
        resp = api.post(f"/sessions/{cid}/messages", json=body)
        assert resp.status_code == 202, resp.text
        return resp.json()["message_id"]

    def dispatched(mid: int) -> Callable[[], dict[str, Any] | None]:
        def probe() -> dict[str, Any] | None:
            view = message(api, mid)
            return view if view["status"] == "sent" and view["agent_id"] else None

        return probe

    ids = [send(1)]
    wait_for("message 1 dispatched", dispatched(ids[0]), TURN_TIMEOUT, interval=0.2)
    time.sleep(DISPATCH_STAGGER)
    ids.append(send(2, other["id"]))
    wait_for("message 2 dispatched", dispatched(ids[1]), TURN_TIMEOUT, interval=0.2)

    def both_busy() -> dict[str, str] | None:
        states = {a["id"]: a["state"] for a in agents_of(api, cid)}
        return states if set(states.values()) == {"busy"} else None

    busy = wait_for("both agents busy", both_busy, 30, interval=0.2)
    ids.append(send(3))

    def done() -> list[dict[str, Any]] | None:
        views = [message(api, mid) for mid in ids]
        finished = all(
            v["status"] == "sent" and v["turn"] and v["turn"]["completed_at"] for v in views
        )
        return views if finished else None

    messages = wait_for("3 completed turns", done, TURN_TIMEOUT, interval=1)
    events = sorted((e for page in all_events(api, cid) for e in page), key=lambda e: e["id"])
    started = {
        e["turn_id"]: e["id"] for e in events if e["type"] == "turn_started" and e["turn_id"]
    }
    both_started = max(started[m["turn_id"]] for m in messages[:2])
    freed = {
        m["agent_id"]: first_idle_after(events, m["agent_id"], both_started) for m in messages[:2]
    }
    return {
        "leader": leader,
        "other": other,
        "busy_before_third": busy,
        "messages": messages,
        "freed_event_id": freed,
    }


@pytest.fixture(scope="module")
def discarded(
    api: httpx.Client, session: dict[str, Any], round_robin: dict[str, Any]
) -> dict[str, Any]:
    """DELETE the session and wait until no labelled object remains (at most 180 s)."""
    cid = session["id"]
    resp = api.delete(f"/sessions/{cid}")
    assert resp.status_code == 202, resp.text
    started = time.monotonic()
    remaining: list[str] = []

    def gone() -> bool:
        nonlocal remaining
        remaining = labelled_objects(SLUG)
        return not remaining and api.get(f"/sessions/{cid}").status_code == 404

    wait_for(f"session {SLUG} objects to be deleted ({remaining})", gone, DISCARD_TIMEOUT)
    return {"session": session, "elapsed": time.monotonic() - started}


# ------------------------------------------------------------------ tests


def test_chart_installed_and_helm_test_passes(api: httpx.Client) -> None:
    status = json.loads(run("helm", "status", RELEASE, "-n", NAMESPACE, "-o", "json").stdout)
    assert status["info"]["status"] == "deployed", status["info"]
    for component in COMPONENTS:
        dep = kube_json("get", "deployment", f"{RELEASE}-{component}")
        assert dep["status"].get("availableReplicas", 0) >= 1, (component, dep["status"])
        init = dep["spec"]["template"]["spec"]["initContainers"]
        assert [c["args"] for c in init] == [["migrate"]], init
    proc = run("helm", "test", RELEASE, "-n", NAMESPACE, "--timeout", "3m")
    assert "Phase:" in proc.stdout and "Succeeded" in proc.stdout, proc.stdout
    assert api.get("/meta").status_code == 200


def test_round_robin_by_free_since(round_robin: dict[str, Any]) -> None:
    leader, other = round_robin["leader"], round_robin["other"]
    m1, m2, m3 = round_robin["messages"]
    # untargeted goes to the leader of a hive, an explicit target wins
    assert m1["agent_id"] == leader["id"], (m1, leader)
    assert m2["agent_id"] == other["id"], (m2, other)
    # both were busy when the third arrived; it waited for the leader, which freed first
    assert set(round_robin["busy_before_third"].values()) == {"busy"}, round_robin
    freed = round_robin["freed_event_id"]
    first_freed = min(freed, key=freed.__getitem__)
    assert first_freed == m1["agent_id"], (freed, m1, m2)
    assert m3["agent_id"] == leader["id"] == first_freed, (freed, m1, m2, m3)
    for m in (m1, m2, m3):
        assert m["attempts"] == 0 and m["error"] is None, m  # sent first time, no retry
        assert m["turn_id"], m


def test_merged_event_stream(
    api: httpx.Client, session: dict[str, Any], round_robin: dict[str, Any]
) -> None:
    pages = all_events(api, session["id"])
    for page in pages:
        ids = [e["id"] for e in page]
        assert ids == sorted(ids) and len(set(ids)) == len(ids), ids
    events = sorted((e for page in pages for e in page), key=lambda e: e["id"])
    agent_ids = {a["id"] for a in session["agents"]}
    assert {e["agent_id"] for e in events} == agent_ids
    assert all(e["session_id"] == session["id"] for e in events)
    # interleaved: the timeline switches between the agents more than once
    sequence = [e["agent_id"] for e in events]
    switches = sum(1 for a, b in zip(sequence, sequence[1:], strict=False) if a != b)
    assert switches >= 2, sequence
    # each agent's own events keep their per-wrapper-session order inside the merged timeline
    for agent in agent_ids:
        own = [(e["wrapper_session_id"], e["seq"]) for e in events if e["agent_id"] == agent]
        for wrapper_session in {s for s, _ in own}:
            seqs = [q for s, q in own if s == wrapper_session]
            assert seqs == sorted(seqs), (agent, wrapper_session, seqs)
    turn_ids = {e["turn_id"] for e in events if e["turn_id"]}
    assert {m["turn_id"] for m in round_robin["messages"]} <= turn_ids


def test_any_mcp_installed_by_on_start(session: dict[str, Any]) -> None:
    ref = any_mcp_ref()
    cm = kube_json("get", "configmap", f"agent-{SLUG}-config")["data"]
    assert f"REF={ref}\n" in cm["on-start.sh"], cm["on-start.sh"]
    install = "/opt/any-mcp/bin/any-mcp plugins install --config /opt/any-mcp/etc/any-mcp.yaml\n"
    assert install in cm["on-start.sh"], cm["on-start.sh"]
    plugins = on_start_any_mcp({"data": cm})["plugins"]
    entry = next(p for p in plugins if p["name"] == "orchestrator")
    assert entry["source"] == orchestrator_source() and "plugin" not in entry, entry
    rendered = yaml.safe_load(cm["config.yaml"])
    claude = rendered["claude"]
    assert claude["mcp_servers"]["any-mcp"] == {
        "type": "stdio",
        "command": "/opt/any-mcp/bin/any-mcp",
        "args": ["--config", "/opt/any-mcp/etc/any-mcp.yaml"],
    }
    assert "AskUserQuestion" in claude["permissions"]["deny"]
    assert rendered["sessions"]["ask_user_question"] == "deny"
    assert rendered["hooks"]["onStart"][0]["run"] == "sh /etc/ccw/on-start.sh"

    pods = [a["pod_name"] for a in session["agents"]]
    assert len(pods) == 2
    for pod in pods:
        assert kubectl("exec", pod, "-c", "ccw", "--", "cat", "/opt/any-mcp/.ref").stdout == (
            ref + "\n"
        ), pod
        kubectl("exec", pod, "-c", "ccw", "--", "/opt/any-mcp/bin/any-mcp", "--help")
        kubectl("exec", pod, "-c", "ccw", "--", "test", "-s", "/opt/any-mcp/etc/any-mcp.yaml")
        # the reserved source was installed by `any-mcp plugins install` in onStart
        pip = "/opt/any-mcp/bin/pip"
        kubectl("exec", pod, "-c", "ccw", "--", pip, "show", "any-mcp-tatara-orchestrator")
        # the session the wrapper started registers the stdio server and denies the tool
        found = kubectl("exec", pod, "-c", "ccw", "--", "python3", "-c", SESSION_CONFIGS).stdout
        sessions = json.loads(found)
        assert sessions, (pod, "no session config under /sessions")
        for claude_json, settings in sessions:
            server = claude_json["mcpServers"]["any-mcp"]
            assert server["command"] == "/opt/any-mcp/bin/any-mcp", (pod, server)
            assert server["args"] == ["--config", "/opt/any-mcp/etc/any-mcp.yaml"], (pod, server)
            assert "AskUserQuestion" in settings["permissions"]["deny"], (pod, settings)


def test_reconciler_role_cannot_read_secrets() -> None:
    def can(verb: str, resource: str, *extra: str) -> bool:
        proc = kubectl("auth", "can-i", verb, resource, *extra, "--as", RECONCILER_SA, check=False)
        answer = proc.stdout.strip()
        assert answer in ("yes", "no"), (verb, resource, proc.stdout, proc.stderr)
        return answer == "yes"

    assert can("create", "secrets")
    assert can("patch", "secrets")
    assert can("delete", "secrets")
    assert not can("get", "secrets")
    assert not can("list", "secrets")
    assert not can("watch", "secrets")
    assert not can("create", "pods", "--subresource=exec")
    assert not can("get", "pods", "--subresource=exec")
    # what the reconciler does need, it has
    assert can("create", "statefulsets.apps")
    assert can("delete", "persistentvolumeclaims")
    assert can("get", "pods", "--subresource=log")
    role = kube_json("get", "role", f"{RELEASE}-reconciler")
    resources = {r for rule in role["rules"] for r in rule["resources"]}
    assert "pods/exec" not in resources, role["rules"]


def test_in_session_pod_refused_by_allowlist(api: httpx.Client) -> None:
    image = kube_json("get", "deployment", f"{RELEASE}-api")["spec"]["template"]["spec"][
        "containers"
    ][0]["image"]
    pod = f"allowlist-probe-{uuid.uuid4().hex[:6]}"
    service = f"http://{RELEASE}.{NAMESPACE}.svc"
    script = (
        "import json, urllib.request, urllib.error\n"
        "out = {}\n"
        "for path in ('/api/v1/sessions', '/healthz'):\n"
        "    try:\n"
        f"        r = urllib.request.urlopen('{service}' + path, timeout=10)\n"
        "        out[path] = [r.status, r.read().decode()]\n"
        "    except urllib.error.HTTPError as e:\n"
        "        out[path] = [e.code, e.read().decode()]\n"
        "print('RESULT ' + json.dumps(out))\n"
    )
    overrides = {
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "containers": [
                {
                    "name": "probe",
                    "image": image,
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["python", "-c", script],
                }
            ],
        }
    }
    kubectl(
        "run", pod, f"--image={image}", "--restart=Never", f"--overrides={json.dumps(overrides)}"
    )
    try:
        kubectl("wait", f"pod/{pod}", "--for=jsonpath={.status.phase}=Succeeded", "--timeout=180s")
        logs = kubectl("logs", pod).stdout
    finally:
        kubectl("delete", "pod", pod, "--wait=false", check=False)
    line = next(ln for ln in logs.splitlines() if ln.startswith("RESULT "))
    result = json.loads(line.removeprefix("RESULT "))
    code, body = result["/api/v1/sessions"]
    assert code == 403, result
    assert json.loads(body)["error"]["code"] == "forbidden", body
    assert result["/healthz"][0] == 200, result  # health routes are exempt
    # the port-forwarded caller arrives from loopback, inside ORCH_ALLOWED_CIDRS
    resp = api.get("/sessions")
    assert resp.status_code == 200, resp.text


def test_in_session_service_token(api: httpx.Client) -> None:
    image = kube_json("get", "deployment", f"{RELEASE}-api")["spec"]["template"]["spec"][
        "containers"
    ][0]["image"]
    pod = f"service-probe-{uuid.uuid4().hex[:6]}"
    services = f"http://{RELEASE}-services.{NAMESPACE}.svc:8002"
    main = f"http://{RELEASE}.{NAMESPACE}.svc"
    script = (
        "import json, urllib.request, urllib.error\n"
        "def call(method, url, token):\n"
        "    req = urllib.request.Request(url, method=method)\n"
        "    if token:\n"
        "        req.add_header('Authorization', 'Bearer ' + token)\n"
        "    try:\n"
        "        return urllib.request.urlopen(req, timeout=10).status\n"
        "    except urllib.error.HTTPError as e:\n"
        "        return e.code\n"
        f"tok = '{E2E_SERVICE_TOKEN}'\n"
        "out = {\n"
        f"    'with': call('GET', '{services}/api/v1/presets', tok),\n"
        f"    'without': call('GET', '{services}/api/v1/presets', None),\n"
        f"    'delete': call('DELETE', '{services}/api/v1/sessions/does-not-matter', tok),\n"
        f"    'main': call('GET', '{main}/api/v1/presets', tok),\n"
        "}\n"
        "print('RESULT ' + json.dumps(out))\n"
    )
    overrides = {
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "containers": [
                {
                    "name": "probe",
                    "image": image,
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["python", "-c", script],
                }
            ],
        }
    }
    kubectl(
        "run", pod, f"--image={image}", "--restart=Never", f"--overrides={json.dumps(overrides)}"
    )
    try:
        kubectl("wait", f"pod/{pod}", "--for=jsonpath={.status.phase}=Succeeded", "--timeout=180s")
        logs = kubectl("logs", pod).stdout
    finally:
        kubectl("delete", "pod", pod, "--wait=false", check=False)
    line = next(ln for ln in logs.splitlines() if ln.startswith("RESULT "))
    result = json.loads(line.removeprefix("RESULT "))
    assert result == {"with": 200, "without": 401, "delete": 403, "main": 403}, result


def test_files_list_and_read_through_api(api: httpx.Client, session: dict[str, Any]) -> None:
    cid = session["id"]
    agent_id = session["agents"][0]["id"]
    note = f"e2e-note-{uuid.uuid4().hex[:8]}.txt"
    text = f"hello-{uuid.uuid4().hex}"
    # the agent writes a file into the shared workdir through its own shell
    with shell(api, agent_id) as term, attach(api, agent_id, term["terminal_id"]) as ws:
        ws.send(f"printf '%s\\n' {text} > {note}\n".encode())
        ws.send(b"echo written-$((6*7))\n")
        read_until(ws, r"^written-42$")

    listing = api.get(f"/sessions/{cid}/files")
    assert listing.status_code == 200, listing.text
    body = listing.json()
    assert body["session_id"] == cid and body["path"] == "", body
    assert body["root"].startswith("/workdir"), body["root"]
    entry = next(e for e in body["entries"] if e["name"] == note)
    assert entry["type"] == "file" and entry["size"] == len(text) + 1, entry

    content = api.get(f"/sessions/{cid}/files/content", params={"path": note})
    assert content.status_code == 200, content.text
    got = content.json()
    assert got["path"] == note and got["kind"] == "text", got
    assert got["content"] == text + "\n", got

    # a path that leaves the workdir is refused, for the listing and for the content
    for route in ("files", "files/content"):
        for bad in ("..", "../etc/passwd", "/etc/passwd"):
            resp = api.get(f"/sessions/{cid}/{route}", params={"path": bad})
            assert resp.status_code == 400, (route, bad, resp.text)
            assert resp.json()["error"]["code"] == "invalid_path", (route, bad, resp.text)


def test_files_write_through_api(api: httpx.Client, session: dict[str, Any]) -> None:
    cid = session["id"]
    agent_id = session["agents"][0]["id"]
    note = f"e2e-write-{uuid.uuid4().hex[:8]}.txt"
    text = f"hello-{uuid.uuid4().hex}"
    # the agent writes a text file into the shared workdir through its own shell
    with shell(api, agent_id) as term, attach(api, agent_id, term["terminal_id"]) as ws:
        ws.send(f"printf '%s\\n' {text} > {note}\n".encode())
        ws.send(b"echo written-$((6*7))\n")
        read_until(ws, r"^written-42$")

    # the stamp comes from the content route, never computed here
    loaded = api.get(f"/sessions/{cid}/files/content", params={"path": note})
    assert loaded.status_code == 200, loaded.text
    stamp = {"mtime": loaded.json()["mtime"], "size": loaded.json()["size"]}

    updated = f"saved-{uuid.uuid4().hex}\n"
    saved = api.put(
        f"/sessions/{cid}/files/content",
        params={"path": note},
        json={"content": updated, "expected": stamp},
    )
    assert saved.status_code == 200, saved.text
    assert saved.json()["path"] == note and saved.json()["size"] == len(updated), saved.json()
    reread = api.get(f"/sessions/{cid}/files/content", params={"path": note})
    assert reread.status_code == 200, reread.text
    assert reread.json()["content"] == updated, reread.json()

    # a second save with the old stamp finds the file changed
    stale = api.put(
        f"/sessions/{cid}/files/content",
        params={"path": note},
        json={"content": "stale\n", "expected": stamp},
    )
    assert stale.status_code == 409, stale.text
    assert stale.json()["error"]["code"] == "file_changed", stale.text

    # `expected: null` overwrites regardless of the stamp
    forced = api.put(
        f"/sessions/{cid}/files/content",
        params={"path": note},
        json={"content": "forced\n", "expected": None},
    )
    assert forced.status_code == 200, forced.text
    reread = api.get(f"/sessions/{cid}/files/content", params={"path": note})
    assert reread.json()["content"] == "forced\n", reread.json()

    # a path that leaves the workdir is refused, a missing file is not created
    escaped = api.put(
        f"/sessions/{cid}/files/content",
        params={"path": "../etc/passwd"},
        json={"content": "x", "expected": None},
    )
    assert escaped.status_code == 400, escaped.text
    assert escaped.json()["error"]["code"] == "invalid_path", escaped.text
    missing = api.put(
        f"/sessions/{cid}/files/content",
        params={"path": f"missing-{uuid.uuid4().hex[:8]}.txt"},
        json={"content": "x", "expected": None},
    )
    assert missing.status_code == 404, missing.text
    assert missing.json()["error"]["code"] == "not_found", missing.text


def test_files_download_through_api(api: httpx.Client, session: dict[str, Any]) -> None:
    cid = session["id"]
    agent_id = session["agents"][0]["id"]
    suffix = uuid.uuid4().hex[:8]
    note = f"e2e-dl-{suffix}.txt"
    blob = f"e2e-dl-{suffix}.bin"
    text = f"download-{uuid.uuid4().hex}"
    # the agent writes a text file and a binary file (every byte 0-255) through its own shell
    with shell(api, agent_id) as term, attach(api, agent_id, term["terminal_id"]) as ws:
        ws.send(f"printf '%s\\n' {text} > {note}\n".encode())
        ws.send(f"python3 -c \"open('{blob}','wb').write(bytes(range(256)))\"\n".encode())
        ws.send(b"echo written-$((6*7))\n")
        read_until(ws, r"^written-42$")

    listing = api.get(f"/sessions/{cid}/files")
    assert listing.status_code == 200, listing.text
    for name, size in ((note, len(text) + 1), (blob, 256)):
        entry = next(e for e in listing.json()["entries"] if e["name"] == name)
        assert entry["type"] == "file" and entry["size"] == size, entry
        assert entry["target_type"] is None, entry

    for name, expected in ((note, (text + "\n").encode()), (blob, bytes(range(256)))):
        resp = api.get(f"/sessions/{cid}/files/download", params={"path": name})
        assert resp.status_code == 200, resp.text
        assert resp.content == expected, (name, resp.content)
        disposition = resp.headers["content-disposition"]
        assert disposition.startswith("attachment"), disposition
        assert name in disposition, disposition

    # a path that leaves the workdir is refused
    for bad in ("..", "../etc/passwd", "/etc/passwd"):
        resp = api.get(f"/sessions/{cid}/files/download", params={"path": bad})
        assert resp.status_code == 400, (bad, resp.text)
        assert resp.json()["error"]["code"] == "invalid_path", (bad, resp.text)


def test_terminal_lifecycle_over_websocket(api: httpx.Client, session: dict[str, Any]) -> None:
    cid = session["id"]
    agent_id = session["agents"][0]["id"]
    root = api.get(f"/sessions/{cid}/files").json()["root"]
    made = api.post(f"/agents/{agent_id}/terminals", json={"rows": 24, "cols": 80})
    assert made.status_code == 201, made.text
    term = made.json()
    tid = term["terminal_id"]
    assert re.fullmatch(r"term_[0-9a-f]{12}", tid), term
    assert term["agent_id"] == agent_id and term["session_id"] == cid, term
    assert term["alive"] is True and (term["rows"], term["cols"]) == (24, 80), term
    try:
        with attach(api, agent_id, tid) as ws:
            ws.send(b"pwd\n")
            read_until(ws, re.escape(root))
            ws.send(b"echo ok\n")
            read_until(ws, r"^ok$")
            ws.send(json.dumps({"type": "ping"}))
            listed = wait_for(
                "the terminal listed as attached",
                lambda: next(
                    (
                        t
                        for t in terminals_of(api, cid)
                        if t["terminal_id"] == tid and t["attached"]
                    ),
                    None,
                ),
                30,
            )
            assert listed["agent_id"] == agent_id and listed["alive"] is True, listed

        # reattach: the wrapper replays the shell's recent output before anything live
        with attach(api, agent_id, tid) as ws:
            replay = read_until(ws, r"^ok$")
            assert root in replay, replay
            ws.send(b"echo again\n")
            read_until(ws, r"^again$")

        deleted = api.delete(f"/agents/{agent_id}/terminals/{tid}")
        assert deleted.status_code == 204, deleted.text
        wait_for(
            "the terminal to be gone from the list",
            lambda: all(t["terminal_id"] != tid for t in terminals_of(api, cid)),
            30,
        )
        again = api.delete(f"/agents/{agent_id}/terminals/{tid}")
        assert again.status_code == 404, again.text
        assert again.json()["error"]["code"] == "terminal_not_found", again.text
    finally:
        api.delete(f"/agents/{agent_id}/terminals/{tid}")


def test_terminal_websocket_refuses_foreign_origin(
    api: httpx.Client, session: dict[str, Any]
) -> None:
    agent_id = session["agents"][0]["id"]
    with shell(api, agent_id) as term:
        with pytest.raises(InvalidStatus) as refused:
            attach(api, agent_id, term["terminal_id"], origin=EVIL_ORIGIN)
        assert refused.value.response.status_code == 403
        # the REST routes apply the same rule
        post = api.post(f"/agents/{agent_id}/terminals", headers={"Origin": EVIL_ORIGIN})
        assert post.status_code == 403, post.text
        assert post.json()["error"]["code"] == "forbidden_origin", post.text
        delete = api.delete(
            f"/agents/{agent_id}/terminals/{term['terminal_id']}", headers={"Origin": EVIL_ORIGIN}
        )
        assert delete.status_code == 403, delete.text
        # the refused attempts opened and closed nothing: the shell is still there and usable
        with attach(api, agent_id, term["terminal_id"]) as ws:
            ws.send(b"echo still-$((6*7))\n")
            read_until(ws, r"^still-42$")


# ------------------------------------------------------------------ hive tests (API only)

WRITE_PROMPT = (
    "write note file"  # the scenario turn that Writes NOTE_FILE (e2e/agent-test.scenario.json)
)
NOTE_FILE = "e2e-note.txt"


def test_agent_api_identity(api: httpx.Client, session: dict[str, Any]) -> None:
    pod0 = f"agent-{SLUG}-0"
    status, me = agent_call(pod0, "GET", "/agent/v1/me")
    assert status == 200, me
    assert me["pod_name"] == pod0 and me["session_id"] == session["id"], me
    assert {m["pod_name"] for m in me["members"]} == {pod0, f"agent-{SLUG}-1"}, me
    assert me["static"] is False and me["max_replicas"] == 2, me

    status, body = agent_call(pod0, "GET", "/agent/v1/me", token=False)
    assert status == 401, body

    # the agents port serves nothing but /agent/v1, /healthz and /readyz
    status, body = agent_call(pod0, "GET", "/api/v1/sessions")
    assert status == 404, body

    # a pod that is not an agent of the session is refused even with a valid token
    token = pod_exec(pod0, "printenv", "ORCH_AGENT_TOKEN").strip()
    url = pod_exec(pod0, "printenv", "ORCH_AGENT_URL").strip()
    image = kube_json("get", "deployment", f"{RELEASE}-api")["spec"]["template"]["spec"][
        "containers"
    ][0]["image"]
    script = (
        "import json, urllib.request, urllib.error\n"
        f"req = urllib.request.Request('{url}/agent/v1/me', headers={{\n"
        f"    'Authorization': 'Bearer {token}', 'X-Orch-Session': '{session['id']}'}})\n"
        "try:\n"
        "    r = urllib.request.urlopen(req, timeout=10)\n"
        "    out = [r.status, r.read().decode()]\n"
        "except urllib.error.HTTPError as e:\n"
        "    out = [e.code, e.read().decode()]\n"
        "print('RESULT ' + json.dumps(out))\n"
    )
    pod = f"identity-probe-{uuid.uuid4().hex[:6]}"
    overrides = {
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "containers": [
                {
                    "name": "probe",
                    "image": image,
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["python", "-c", script],
                }
            ],
        }
    }
    kubectl(
        "run", pod, f"--image={image}", "--restart=Never", f"--overrides={json.dumps(overrides)}"
    )
    try:
        kubectl("wait", f"pod/{pod}", "--for=jsonpath={.status.phase}=Succeeded", "--timeout=180s")
        logs = kubectl("logs", pod).stdout
    finally:
        kubectl("delete", "pod", pod, "--wait=false", check=False)
    line = next(ln for ln in logs.splitlines() if ln.startswith("RESULT "))
    code, text = json.loads(line.removeprefix("RESULT "))
    assert code == 403, (code, text)
    assert json.loads(text)["error"]["code"] == "forbidden", text


def test_leadership_handover_routes(api: httpx.Client, session: dict[str, Any]) -> None:
    cid = session["id"]
    first = wait_leader(api, cid, 1)
    other = next(a for a in agents_of(api, cid) if a["id"] != first["id"])
    status, me = agent_call(first["pod_name"], "GET", "/agent/v1/me")
    assert status == 200 and me["leader"] == first["pod_name"] and me["epoch"] == 1, me

    status, body = agent_call(
        first["pod_name"],
        "POST",
        "/agent/v1/leadership/handover",
        {"to": other["pod_name"], "reason": "e2e handover", "epoch": 1},
    )
    assert status == 202 and body["request_id"], body
    wait_leader(api, cid, 2, other["id"])

    resp = api.post(f"/sessions/{cid}/messages", json={"text": "hello after the handover"})
    assert resp.status_code == 202, resp.text
    mid = resp.json()["message_id"]

    def done() -> dict[str, Any] | None:
        view = message(api, mid)
        turn = view["turn"]
        return view if view["status"] == "sent" and turn and turn["completed_at"] else None

    view = wait_for("untargeted message completed", done, TURN_TIMEOUT, interval=1)
    assert view["agent_id"] == other["id"], (view, other)


def test_user_make_leader(api: httpx.Client, session: dict[str, Any]) -> None:
    cid = session["id"]
    leader, epoch = leader_view(api, cid)
    assert leader is not None and epoch is not None
    target = next(a for a in agents_of(api, cid) if a["id"] != leader["id"])
    resp = api.post(f"/sessions/{cid}/leader", json={"agent_id": target["id"]})
    assert resp.status_code == 202, resp.text
    assert resp.json()["request_id"], resp.text
    wait_leader(api, cid, epoch + 1, target["id"])
    assert next(a for a in agents_of(api, cid) if a["id"] == target["id"])["leader"] is True


def test_advisory_locks(api: httpx.Client, session: dict[str, Any]) -> None:
    cid = session["id"]
    leader, _ = leader_view(api, cid)
    assert leader is not None
    agents = sorted(agents_of(api, cid), key=lambda a: a["ordinal"])
    pod0, pod1 = (pod_name_of(a) for a in agents)
    member = next(pod for pod in (pod0, pod1) if pod != leader["pod_name"])
    path = f"e2e-lock-{uuid.uuid4().hex[:6]}.txt"

    status, body = agent_call(pod0, "POST", "/agent/v1/locks/claim", {"paths": [path]})
    assert status == 200 and body == {"claimed": [path], "held": []}, body

    status, body = agent_call(pod1, "GET", "/agent/v1/locks")
    assert status == 200, body
    assert [(x["path"], x["agent"]) for x in body["locks"] if x["path"] == path] == [(path, pod0)]

    status, body = agent_call(pod1, "POST", "/agent/v1/locks/claim", {"paths": [path]})
    assert status == 200 and body["claimed"] == [], body
    assert body["held"] == [{"path": path, "agent": pod0}], body

    hook = {"data": {"tool_name": "Write", "tool_input": {"file_path": f"/workdir/workdir/{path}"}}}
    status, body = agent_call(pod1, "POST", "/agent/v1/locks/check", hook)
    assert status == 200 and body["decision"] == "allow", body
    assert pod0 in body["additional_context"] and path in body["additional_context"], body

    status, body = agent_call(
        member, "POST", "/agent/v1/locks/reassign", {"path": path, "to": None}
    )
    assert status == 403, body
    assert body["error"]["code"] == "not_leader", body

    status, body = agent_call(
        leader["pod_name"], "POST", "/agent/v1/locks/reassign", {"path": path, "to": None}
    )
    assert status == 200 and body == {"path": path, "agent": None}, body
    status, body = agent_call(pod1, "GET", "/agent/v1/locks")
    assert all(x["path"] != path for x in body["locks"]), body


def test_hive_notes_delivered(api: httpx.Client, session: dict[str, Any]) -> None:
    cid = session["id"]
    writer, reader = sorted(agents_of(api, cid), key=lambda a: a["ordinal"])
    mid = post_message(api, cid, WRITE_PROMPT, writer["id"])
    wait_for(
        "the note-writing turn to complete",
        lambda: turn_completed(message(api, mid)),
        TURN_TIMEOUT,
        2,
    )

    def notes() -> list[dict[str, Any]]:
        events = (e for page in all_events(api, cid) for e in page)
        return [e for e in events if e["type"] == "note_received"]

    def hive_state_on_both() -> list[dict[str, Any]] | None:
        received = notes()
        for agent in (writer, reader):
            keyed = [
                e
                for e in received
                if e["agent_id"] == agent["id"] and e["data"].get("key") == "hive_state"
            ]
            if not keyed or "hive-state" not in keyed[-1]["data"]["text"]:
                return None
        return received

    wait_for("the hive_state note on both agents", hive_state_on_both, HIVE_PUSH_TIMEOUT, 3)

    def writer_line_on_reader() -> list[dict[str, Any]] | None:
        status, body = agent_call(
            pod_name_of(reader), "GET", f"/agent/v1/activity?agent={pod_name_of(writer)}"
        )
        assert status == 200, body
        lines = [
            ln
            for ln in body["lines"]
            if ln["agent"] == pod_name_of(writer) and NOTE_FILE in ln["text"]
        ]
        return lines or None

    pulled = wait_for("the writer's file write in the reader's activity", writer_line_on_reader, 60)
    assert all(ln["text"].startswith(f"{pod_name_of(writer)} wrote ") for ln in pulled), pulled

    time.sleep(HIVE_QUIET_WINDOW)
    unkeyed = [e for e in notes() if e["data"].get("key") is None]
    assert unkeyed == [], unkeyed


COMMAND_EVENT_TYPES = "command_queued,command_started,command_completed,command_failed"


def agent_state(api: httpx.Client, cid: str, agent_id: str) -> str:
    return next(a for a in agents_of(api, cid) if a["id"] == agent_id)["state"]


def command_records(
    api: httpx.Client, cid: str, agent_id: str, event_type: str = COMMAND_EVENT_TYPES
) -> list[dict[str, Any]]:
    """The wrapper command records of the agent's command_* events, oldest first."""
    resp = api.get(
        f"/sessions/{cid}/events",
        params={"agent_id": agent_id, "types": event_type, "limit": 500},
    )
    resp.raise_for_status()
    return [e["data"]["record"] for e in resp.json()["events"]]


def test_slash_command_from_chat_keeps_session_responsive(
    api: httpx.Client, session: dict[str, Any]
) -> None:
    """The reported case: a chat "/reload-plugins" must not leave the session busy (G3)."""
    cid = session["id"]
    agent = sorted(agents_of(api, cid), key=lambda a: a["ordinal"])[0]
    wait_for(
        "the agent to be idle", lambda: agent_state(api, cid, agent["id"]) == "idle", TURN_TIMEOUT
    )

    # the chat text runs as a wrapper command, never as a turn
    mid = post_message(api, cid, "/reload-plugins", agent["id"])

    def command_sent() -> dict[str, Any] | None:
        view = message(api, mid)
        return view if view["status"] == "sent" and view["wrapper_command_id"] else None

    sent = wait_for("the /reload-plugins command to be sent", command_sent, TURN_TIMEOUT, 1)
    assert sent["kind"] == "command" and sent["agent_id"] == agent["id"], sent
    command_id = sent["wrapper_command_id"]

    def completed() -> dict[str, Any] | None:
        records = command_records(api, cid, agent["id"], "command_completed")
        return next((r for r in records if r["id"] == command_id), None)

    record = wait_for("command_completed for /reload-plugins", completed, TURN_TIMEOUT, 1)
    assert record["name"] == "reload-plugins" and record["state"] == "completed", record

    # the command does not hold the agent busy
    wait_for(
        "the agent to return idle after the command",
        lambda: agent_state(api, cid, agent["id"]) == "idle",
        TURN_TIMEOUT,
        1,
    )

    # prose still runs a whole turn afterwards
    prose = post_message(api, cid, "say hello after the command", agent["id"])
    wait_for(
        "the prose message's turn to complete",
        lambda: turn_completed(message(api, prose)),
        TURN_TIMEOUT,
        1,
    )

    # a command that would end the session is refused with the wrapper's own error code
    exit_mid = post_message(api, cid, "/exit", agent["id"])

    def refused() -> dict[str, Any] | None:
        view = api.get(f"/messages/{exit_mid}").json()
        return view if view["status"] == "failed" else None

    failed = wait_for("the /exit message to fail", refused, TURN_TIMEOUT, 1)
    assert failed["kind"] == "command" and failed["error"] == "command_denied", failed
    assert [r for r in command_records(api, cid, agent["id"]) if r["name"] == "exit"] == []
    wait_for(
        "the agent to stay idle after the refused command",
        lambda: agent_state(api, cid, agent["id"]) == "idle",
        TURN_TIMEOUT,
        1,
    )


def test_alone_session_has_no_hive_tools(api: httpx.Client, session: dict[str, Any]) -> None:
    resp = api.post(
        "/sessions",
        json={
            "slug": ALONE_SLUG,
            "spec": {"replicas": 1, "static": True, "env": FAKE_TURN_ENV},
        },
    )
    assert resp.status_code == 201, resp.text
    alone = resp.json()
    try:
        assert alone["hive"] is False and alone["leader_agent_id"] is None, alone
        name = f"agent-{ALONE_SLUG}-config"

        def rendered() -> dict[str, Any] | None:
            proc = kubectl("get", "configmap", name, "-o", "json", check=False)
            return json.loads(proc.stdout) if proc.returncode == 0 else None

        configmap = wait_for("the alone session's ConfigMap", rendered, 120)
        config = yaml.safe_load(configmap["data"]["config.yaml"])
        assert config["notes"] == {"enabled": False}, config
        assert "peers" not in config or not config["peers"].get("enabled"), config
        assert "onAgentToolCall" not in config["hooks"], config["hooks"]
        assert_alone_session_tools(configmap)

        # the hive session of the same release renders the hive tools, so the parse is sound
        hive_map = kube_json("get", "configmap", f"agent-{SLUG}-config")
        hive_plugins = on_start_any_mcp(hive_map).get("plugins", [])
        hive_entry = next(p for p in hive_plugins if p.get("name") == "orchestrator")
        assert "me" in hive_entry["allow"], hive_entry
        assert hive_entry["source"] == orchestrator_source(), hive_entry
        assert yaml.safe_load(hive_map["data"]["config.yaml"])["notes"] == {"enabled": True}
    finally:
        api.delete(f"/sessions/{alone['id']}")

        def gone() -> bool:
            return (
                not labelled_objects(ALONE_SLUG)
                and api.get(f"/sessions/{alone['id']}").status_code == 404
            )

        wait_for(f"session {ALONE_SLUG} objects to be deleted", gone, DISCARD_TIMEOUT)


def pods_of(slug: str) -> dict[str, str]:
    """Pod name -> pod uid of a session, read with kubectl."""
    out = kubectl(
        "get",
        "pods",
        "-l",
        f"{SESSION_LABEL}={slug}",
        "-o",
        'jsonpath={range .items[*]}{.metadata.name}{" "}{.metadata.uid}{"\\n"}{end}',
    ).stdout
    return dict(line.split() for line in out.splitlines() if line.strip())


def presets_by_name(api: httpx.Client) -> dict[str, dict[str, Any]]:
    resp = api.get("/presets")
    assert resp.status_code == 200, resp.text
    return {p["name"]: p for p in resp.json()}


def default_preset(api: httpx.Client) -> dict[str, Any]:
    found = [p for p in api.get("/presets").json() if p["is_default"]]
    assert len(found) == 1, found
    return found[0]


def test_default_preset_listed(api: httpx.Client) -> None:
    listed = presets_by_name(api)
    assert "Default" in listed, listed
    default = listed["Default"]
    assert default["is_default"] is True, default
    assert default["spec"]["replicas"] == 1, default
    assert default_preset(api)["id"] == default["id"]


def test_session_scheduled_from_preset_keeps_provenance(
    api: httpx.Client, session: dict[str, Any], preset: dict[str, Any]
) -> None:
    view = api.get(f"/sessions/{session['id']}").json()
    assert view["slug"] == SLUG
    assert view["preset_id"] == preset["id"] and view["preset_name"] == PRESET_NAME, view
    assert view["spec"]["replicas"] == 2 and view["spec"]["peers"] is True, view["spec"]
    assert view["spec"]["env"] == FAKE_TURN_ENV, view["spec"]


def test_preset_edit_and_delete_leave_session_unchanged(
    api: httpx.Client,
    session: dict[str, Any],
    preset: dict[str, Any],
    round_robin: dict[str, Any],
) -> None:
    cid = session["id"]
    before = api.get(f"/sessions/{cid}").json()
    pods_before = pods_of(SLUG)
    assert len(pods_before) == 2, pods_before
    copy_id: str | None = None

    def assert_unchanged() -> None:
        # several reconcile polls (ORCH_RECONCILE_POLL_SECONDS=3) pass first
        time.sleep(10)
        view = api.get(f"/sessions/{cid}").json()
        assert view["spec"] == before["spec"], view["spec"]
        assert view["generation"] == before["generation"], view
        assert view["observed_generation"] == before["observed_generation"], view
        assert pods_of(SLUG) == pods_before

    try:
        edited = api.put(
            f"/presets/{preset['id']}", json={"name": PRESET_NAME, "spec": {"replicas": 1}}
        )
        assert edited.status_code == 200, edited.text
        assert edited.json()["spec"]["replicas"] == 1, edited.text
        copied = api.post(f"/presets/{preset['id']}/copy")
        assert copied.status_code == 201, copied.text
        copy = copied.json()
        copy_id = copy["id"]
        assert copy["id"] != preset["id"] and copy["name"] != PRESET_NAME, copy
        assert copy["is_default"] is False and copy["spec"]["replicas"] == 1, copy
        assert_unchanged()
        assert api.get(f"/sessions/{cid}").json()["preset_id"] == preset["id"]

        deleted = api.delete(f"/presets/{preset['id']}")
        assert deleted.status_code == 204, deleted.text
        assert api.get(f"/presets/{preset['id']}").status_code == 404
        assert_unchanged()
        after = api.get(f"/sessions/{cid}").json()
        assert after["preset_id"] is None, after
        assert after["preset_name"] == PRESET_NAME, after
    finally:
        if copy_id is not None:
            gone = api.delete(f"/presets/{copy_id}")
            assert gone.status_code in (204, 404), gone.text
    assert copy_id not in {p["id"] for p in api.get("/presets").json()}


def test_schedule_default_preset_generated_slug(api: httpx.Client) -> None:
    default = default_preset(api)
    resp = api.post("/sessions", json={"preset_id": default["id"]})
    assert resp.status_code == 201, resp.text
    created = resp.json()
    cid = created["id"]
    try:
        assert GENERATED_SLUG.fullmatch(created["slug"]), created["slug"]
        assert created["spec"]["replicas"] == 1, created["spec"]
        assert created["preset_id"] == default["id"], created
        assert created["preset_name"] == "Default", created
    finally:  # the session costs a reconcile for nothing: discard it at once
        api.delete(f"/sessions/{cid}")
    wait_for(
        f"session {created['slug']} row to be gone",
        lambda: api.get(f"/sessions/{cid}").status_code == 404,
        DISCARD_TIMEOUT,
    )


def test_patch_session_retired(api: httpx.Client, session: dict[str, Any]) -> None:
    resp = api.patch(f"/sessions/{session['id']}", json={"spec": {"replicas": 3}})
    assert resp.status_code == 405, resp.text


def test_delete_default_preset_refused(api: httpx.Client) -> None:
    default = default_preset(api)
    resp = api.delete(f"/presets/{default['id']}")
    assert resp.status_code == 409, resp.text
    assert resp.json()["error"]["code"] == "preset_default", resp.text
    assert api.get(f"/presets/{default['id']}").status_code == 200


# ------------------------------------------------------------------ scaling (D1-D4)

HASH_ANNOTATION = "agents.szymonrichert.pl/config-hash"
SCALE_SLUG_PREFIX = "e2e-scale"


@contextlib.contextmanager
def scale_session(api: httpx.Client, suffix: str, spec: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Create a session of its own through POST /sessions; always delete it and wait for it."""
    slug = f"{SCALE_SLUG_PREFIX}-{suffix}"
    resp = api.post("/sessions", json={"slug": slug, "spec": {**spec, "env": FAKE_TURN_ENV}})
    assert resp.status_code == 201, resp.text
    created = resp.json()
    cid = created["id"]
    try:
        yield created
    finally:
        api.delete(f"/sessions/{cid}")

        def gone() -> bool:
            return not labelled_objects(slug) and api.get(f"/sessions/{cid}").status_code == 404

        wait_for(f"session {slug} objects to be deleted", gone, DISCARD_TIMEOUT)


def pod_identity(slug: str) -> dict[str, tuple[str, str | None]]:
    """Pod name -> (uid, config-hash annotation) of a session, read with kubectl."""
    items = kube_json("get", "pods", "-l", f"{SESSION_LABEL}={slug}")["items"]
    return {
        p["metadata"]["name"]: (
            p["metadata"]["uid"],
            (p["metadata"].get("annotations") or {}).get(HASH_ANNOTATION),
        )
        for p in items
    }


def live_agents(api: httpx.Client, cid: str) -> list[dict[str, Any]]:
    return [a for a in agents_of(api, cid) if not a["gone"]]


def agent_by_pod(api: httpx.Client, cid: str, pod: str) -> dict[str, Any]:
    return next(a for a in agents_of(api, cid) if a["pod_name"] == pod)


def agent_me(pod: str) -> dict[str, Any]:
    status, me = agent_call(pod, "GET", "/agent/v1/me")
    assert status == 200, me
    return me


def wait_members(pod: str, expected: set[str]) -> None:
    def probe() -> bool:
        return {m["pod_name"] for m in agent_me(pod)["members"]} == expected

    wait_for(f"{pod} sees members {sorted(expected)}", probe, 120)


def wait_pod_gone(slug: str, pod: str, timeout: float = DISCARD_TIMEOUT) -> None:
    wait_for(f"pod {pod} to be gone", lambda: pod not in pod_identity(slug), timeout)


def scale_to(api: httpx.Client, cid: str, replicas: int) -> httpx.Response:
    return api.post(f"/sessions/{cid}/scale", json={"replicas": replicas})


def error_code(resp: httpx.Response) -> str:
    return resp.json()["error"]["code"]


def test_scale_up_and_down_keeps_surviving_pods(api: httpx.Client, baseline: dict) -> None:
    with scale_session(api, "updown", {"replicas": 1, "max_replicas": 3}) as created:
        cid, slug = created["id"], created["slug"]
        pods = [f"agent-{slug}-{i}" for i in range(3)]
        wait_sessions(api, cid, 1)
        before = pod_identity(slug)
        assert set(before) == {pods[0]}, before
        assert before[pods[0]][1], before
        leader = wait_for("a leader", lambda: leader_view(api, cid)[0], LEADER_TIMEOUT)
        assert leader["pod_name"] == pods[0], leader

        resp = scale_to(api, cid, 3)
        assert resp.status_code == 202, resp.text
        assert resp.json()["replicas"] == 3 and resp.json()["previous_replicas"] == 1, resp.text
        wait_sessions(api, cid, 3)
        grown = pod_identity(slug)
        assert set(grown) == set(pods), grown
        assert grown[pods[0]] == before[pods[0]], (before, grown)  # same uid, same config hash
        wait_members(pods[0], set(pods))
        assert agent_me(pods[0])["leader"] == pods[0]  # a joining agent never leads
        joined_leader, _ = leader_view(api, cid)
        assert joined_leader is not None and joined_leader["id"] == leader["id"], joined_leader

        resp = scale_to(api, cid, 2)
        assert resp.status_code == 202, resp.text
        wait_pod_gone(slug, pods[2])
        shrunk = pod_identity(slug)
        assert set(shrunk) == set(pods[:2]), shrunk
        assert shrunk[pods[0]] == before[pods[0]] and shrunk[pods[1]] == grown[pods[1]], shrunk
        wait_for("agent-2 marked gone", lambda: agent_by_pod(api, cid, pods[2])["gone"], 120)
        wait_members(pods[0], set(pods[:2]))
        final_leader, _ = leader_view(api, cid)
        assert final_leader is not None and final_leader["id"] == leader["id"], final_leader


def test_scale_down_below_leader_hands_over(api: httpx.Client, baseline: dict) -> None:
    with scale_session(api, "handover", {"replicas": 3, "max_replicas": 3}) as created:
        cid, slug = created["id"], created["slug"]
        wait_sessions(api, cid, 3)
        wait_for("the first election", lambda: leader_view(api, cid)[1] is not None, LEADER_TIMEOUT)
        current, epoch = leader_view(api, cid)
        assert current is not None and epoch is not None

        def ordinal(agent: dict[str, Any]) -> int:
            return int(agent["pod_name"].rsplit("-", 1)[1])

        # The highest ordinal is the pod a 3->2 scale removes; it must be the leader.
        top = max(live_agents(api, cid), key=ordinal)
        if top["id"] != current["id"]:
            resp = api.post(f"/sessions/{cid}/leader", json={"agent_id": top["id"]})
            assert resp.status_code == 202, resp.text
            wait_leader(api, cid, epoch + 1, top["id"])
            epoch += 1
        before = pod_identity(slug)
        assert len(before) == 3, before

        resp = scale_to(api, cid, 2)
        assert resp.status_code == 202, resp.text

        def moved() -> dict[str, Any] | None:
            view = api.get(f"/sessions/{cid}").json()
            leader, current_epoch = leader_view(api, cid)
            ok = (
                leader is not None
                and ordinal(leader) < ordinal(top)
                and current_epoch is not None
                and current_epoch > epoch
                and view["leader_reason"] == "scale_down"
            )
            return leader if ok else None

        wait_for("leadership to move to a staying agent (scale_down)", moved, LEADER_TIMEOUT)
        wait_pod_gone(slug, top["pod_name"])
        after = pod_identity(slug)
        assert set(after) == set(before) - {top["pod_name"]}, after
        for pod, (uid, _) in after.items():
            assert uid == before[pod][0], (pod, before, after)


def test_scale_refusals(api: httpx.Client, baseline: dict) -> None:
    with scale_session(api, "refuse", {"replicas": 3, "max_replicas": 3}) as created:
        cid = created["id"]
        wait_sessions(api, cid, 3)
        for bad in (0, 4):
            resp = scale_to(api, cid, bad)
            assert resp.status_code == 422, (bad, resp.text)
            assert error_code(resp) == "out_of_range", resp.text

    with scale_session(api, "static", {"replicas": 2, "max_replicas": 2, "static": True}) as s:
        resp = scale_to(api, s["id"], 1)
        assert resp.status_code == 409, resp.text
        assert error_code(resp) == "static_session", resp.text
        view = api.get(f"/sessions/{s['id']}").json()
        assert view["static"] is True and view["spec"]["replicas"] == 2, view


def test_agent_scale_leader_only(api: httpx.Client, session: dict[str, Any]) -> None:
    cid = session["id"]
    leader, _ = leader_view(api, cid)
    assert leader is not None
    follower = next(a for a in live_agents(api, cid) if a["id"] != leader["id"])
    replicas = api.get(f"/sessions/{cid}").json()["spec"]["replicas"]
    body = {"replicas": replicas}

    status, out = agent_call(leader["pod_name"], "POST", "/agent/v1/scale", body)
    assert status == 202, out
    assert out["replicas"] == replicas, out

    status, out = agent_call(follower["pod_name"], "POST", "/agent/v1/scale", body)
    assert status == 403, out
    assert out["error"]["code"] == "not_leader", out


def test_static_single_pod_is_alone(api: httpx.Client, baseline: dict) -> None:
    with scale_session(api, "single", {"replicas": 1, "max_replicas": 1, "static": True}) as c:
        assert c["hive"] is False and c["static"] is True and c["leader_agent_id"] is None, c
        name = f"agent-{c['slug']}-config"

        def rendered() -> dict[str, Any] | None:
            proc = kubectl("get", "configmap", name, "-o", "json", check=False)
            return json.loads(proc.stdout) if proc.returncode == 0 else None

        configmap = wait_for("the single static pod's ConfigMap", rendered, 120)
        config = yaml.safe_load(configmap["data"]["config.yaml"])
        assert config["notes"] == {"enabled": False}, config
        assert not (config.get("peers") or {}).get("enabled"), config
        assert_alone_session_tools(configmap)


def test_scale_down_waits_for_busy_agent(api: httpx.Client, baseline: dict) -> None:
    """The fake claude's "slow" turn lasts 4 s, so the window is short: the pod is checked
    right after the scale is posted, while the turn is still running."""
    with scale_session(api, "drain", {"replicas": 2, "max_replicas": 2}) as created:
        cid, slug = created["id"], created["slug"]
        top_pod = f"agent-{slug}-1"
        wait_sessions(api, cid, 2)
        wait_leader(api, cid, 1)
        top = agent_by_pod(api, cid, top_pod)
        uid = pod_identity(slug)[top_pod][0]

        body = {"text": SLOW_PROMPT.format(n=1), "target_agent_id": top["id"]}
        resp = api.post(f"/sessions/{cid}/messages", json=body)
        assert resp.status_code == 202, resp.text
        mid = resp.json()["message_id"]

        def busy() -> bool:
            sent = message(api, mid)["status"] == "sent"
            return sent and agent_by_pod(api, cid, top_pod)["state"] == "busy"

        wait_for("the top agent to be busy", busy, TURN_TIMEOUT, interval=0.2)
        resp = scale_to(api, cid, 1)
        assert resp.status_code == 202, resp.text

        def done() -> bool:
            turn = message(api, mid)["turn"]
            return bool(turn and turn["completed_at"])

        # sample the pod for the whole turn: it must stay until the turn is sent
        samples = 0
        deadline = time.monotonic() + TURN_TIMEOUT
        while not done():
            assert pod_identity(slug).get(top_pod, (None,))[0] == uid, "pod left mid-turn"
            samples += 1
            assert time.monotonic() < deadline, "the turn did not complete"
            time.sleep(0.5)
        assert samples >= 1
        wait_pod_gone(slug, top_pod)
        wait_for("the top agent marked gone", lambda: agent_by_pod(api, cid, top_pod)["gone"], 120)


def test_discard_removes_objects_and_history(
    api: httpx.Client, discarded: dict[str, Any], round_robin: dict[str, Any]
) -> None:
    cid = discarded["session"]["id"]
    assert discarded["elapsed"] <= DISCARD_TIMEOUT
    assert labelled_objects(SLUG) == []
    for kind in ("statefulset", "service", "configmap", "persistentvolumeclaim"):
        names = kubectl("get", kind, "-o", "name").stdout
        assert f"agent-{SLUG}" not in names, (kind, names)
    # the Secret is checked by name: it is deleted by its deterministic name
    assert kubectl("get", "secret", f"agent-{SLUG}-ccw", check=False).returncode != 0
    session_resp = api.get(f"/sessions/{cid}")
    assert session_resp.status_code == 404, session_resp.text
    assert session_resp.json()["error"]["code"] == "not_found"
    events_resp = api.get(f"/sessions/{cid}/events")
    assert events_resp.status_code == 404, events_resp.text
    for m in round_robin["messages"]:
        assert api.get(f"/messages/{m['id']}").status_code == 404, m
    assert all(c["id"] != cid for c in api.get("/sessions").json())


def test_shared_namespace_leaves_orchestrator_objects(
    baseline: dict[tuple[str, str], dict[str, Any]], discarded: dict[str, Any]
) -> None:
    assert orchestrator_objects() == sorted(baseline)
    for (kind, name), before in baseline.items():
        assert fingerprint(kind, name) == before, (kind, name)
    for component in COMPONENTS:
        dep = kube_json("get", "deployment", f"{RELEASE}-{component}")
        assert dep["status"].get("availableReplicas", 0) >= 1, (component, dep["status"])
    pg = kube_json("get", "deployment", "postgres")
    assert pg["status"].get("availableReplicas", 0) >= 1, pg["status"]


# ------------------------------------------------------------------ init scripts

INIT_TIMEOUT = 300
RECONCILE_INTERVALS = 10  # seconds: several reconcile polls (ORCH_RECONCILE_POLL_SECONDS=3)
WORKDIR = "/workdir/workdir"


@contextlib.contextmanager
def init_preset(api: httpx.Client, scripts: list[str], replicas: int) -> Iterator[dict[str, Any]]:
    resp = api.post(
        "/presets",
        json={
            "name": f"e2e-init-{uuid.uuid4().hex[:6]}",
            "spec": {"replicas": replicas, "env": FAKE_TURN_ENV, "init_scripts": scripts},
        },
    )
    assert resp.status_code == 201, resp.text
    created = resp.json()
    try:
        yield created
    finally:
        api.delete(f"/presets/{created['id']}")


@contextlib.contextmanager
def init_session(api: httpx.Client, body: dict[str, Any]) -> Iterator[dict[str, Any]]:
    resp = api.post("/sessions", json=body)
    assert resp.status_code == 201, resp.text
    created = resp.json()
    cid = created["id"]
    try:
        yield created
    finally:
        api.delete(f"/sessions/{cid}")
        wait_for(
            f"session {created['slug']} row to be gone",
            lambda: api.get(f"/sessions/{cid}").status_code == 404,
            DISCARD_TIMEOUT,
        )


def wait_init_state(api: httpx.Client, cid: str, states: tuple[str, ...]) -> dict[str, Any]:
    def probe() -> dict[str, Any] | None:
        view = api.get(f"/sessions/{cid}").json()
        if view["status"].get("error"):
            raise AssertionError(f"session reconcile error: {view['status']}")
        init = view["status"].get("init")
        return view if init and init.get("state") in states else None

    return wait_for(f"status.init.state in {states}", probe, INIT_TIMEOUT)


def wait_sessions(api: httpx.Client, cid: str, count: int) -> list[dict[str, Any]]:
    def probe() -> list[dict[str, Any]] | None:
        agents = [a for a in agents_of(api, cid) if not a["gone"]]
        ready = [a for a in agents if a["ready"] and a["free_since"]]
        return ready if len(ready) == count else None

    return wait_for(f"{count} agents with sessions", probe, READY_TIMEOUT)


def init_jobs(slug: str) -> list[str]:
    out = kubectl("get", "jobs", "-o", "name", check=False).stdout
    return [ln for ln in out.splitlines() if ln.endswith(f"agent-{slug}-init")]


def test_init_scripts_run_once(api: httpx.Client, baseline: dict) -> None:
    with (
        init_preset(api, ["echo e2e-init > e2e-init.txt"], replicas=2) as preset,
        init_session(api, {"preset_id": preset["id"]}) as created,
    ):
        cid, slug = created["id"], created["slug"]
        view = wait_init_state(api, cid, ("succeeded", "failed"))
        assert view["status"]["init"]["state"] == "succeeded", view["status"]
        agents = wait_sessions(api, cid, 2)
        for agent in agents:
            pod = pod_name_of(agent)
            assert pod_exec(pod, "cat", f"{WORKDIR}/e2e-init.txt").strip() == "e2e-init"
            pod_exec(pod, "test", "-s", f"{WORKDIR}/.agent-init/init.sh")
            pod_exec(pod, "test", "-e", f"{WORKDIR}/.agent-init/init.log")
        # run once: the Job is gone after the outcome and the run is never started again
        started = view["status"]["init"]["started_at"]
        time.sleep(2 * RECONCILE_INTERVALS)
        later = api.get(f"/sessions/{cid}").json()
        assert later["status"]["init"]["started_at"] == started, later["status"]
        assert later["status"]["init"]["state"] == "succeeded", later["status"]
        assert init_jobs(slug) == []


def test_init_scripts_edit_writes_session_and_preset_without_roll(
    api: httpx.Client, baseline: dict
) -> None:
    with (
        init_preset(api, ["echo first > first.txt"], replicas=1) as preset,
        init_session(api, {"preset_id": preset["id"]}) as created,
    ):
        cid, slug = created["id"], created["slug"]
        view = wait_init_state(api, cid, ("succeeded",))
        wait_sessions(api, cid, 1)
        started = view["status"]["init"]["started_at"]
        pods_before = pods_of(slug)
        assert len(pods_before) == 1, pods_before
        pod = f"agent-{slug}-0"

        def assert_no_roll() -> None:
            time.sleep(2 * RECONCILE_INTERVALS)
            now = api.get(f"/sessions/{cid}").json()
            assert now["status"]["init"]["started_at"] == started, now["status"]
            assert pods_of(slug) == pods_before
            assert init_jobs(slug) == []

        # public route
        public = ["echo second > second.txt"]
        put = api.put(f"/sessions/{cid}/init-scripts", json={"scripts": public})
        assert put.status_code == 200, put.text
        got = api.get(f"/sessions/{cid}/init-scripts").json()
        assert got["scripts"] == public and got["edited_since_run"] is True, got
        assert api.get(f"/presets/{preset['id']}").json()["spec"]["init_scripts"] == public
        assert_no_roll()

        # agent route from inside the pod
        agent_scripts = ["echo third > third.txt"]
        status, body = agent_call(pod, "PUT", "/agent/v1/init-scripts", {"scripts": agent_scripts})
        assert status == 200, body
        assert body["scripts"] == agent_scripts, body
        status, body = agent_call(pod, "GET", "/agent/v1/init-scripts")
        assert status == 200 and body["scripts"] == agent_scripts, body
        assert body["edited_since_run"] is True, body
        assert api.get(f"/sessions/{cid}/init-scripts").json()["scripts"] == agent_scripts
        assert api.get(f"/presets/{preset['id']}").json()["spec"]["init_scripts"] == agent_scripts
        assert_no_roll()


def test_init_scripts_failure_starts_sessions_degraded(api: httpx.Client, baseline: dict) -> None:
    spec = {"replicas": 1, "env": FAKE_TURN_ENV, "init_scripts": ["exit 3"]}
    with init_session(
        api, {"spec": spec, "slug": f"e2e-initfail-{uuid.uuid4().hex[:6]}"}
    ) as created:
        cid = created["id"]
        view = wait_init_state(api, cid, ("failed",))
        init = view["status"]["init"]
        assert init["state"] == "failed" and init["exit_code"] == 3, init

        def degraded() -> dict[str, Any] | None:
            now = api.get(f"/sessions/{cid}").json()
            st = now["status"]
            return now if st.get("phase") == "degraded" and "init failed" in st["message"] else None

        wait_for("phase degraded with init failed", degraded, INIT_TIMEOUT)
        assert len(wait_sessions(api, cid, 1)) == 1


def test_session_without_init_scripts_skipped(api: httpx.Client, baseline: dict) -> None:
    # own session: the shared `session` fixture is discarded by the discard test before this runs
    with (
        init_preset(api, [], replicas=1) as preset,
        init_session(api, {"preset_id": preset["id"]}) as created,
    ):
        view = wait_init_state(api, created["id"], ("skipped",))
        assert view["status"]["init"]["state"] == "skipped", view["status"]


# ------------------------------------------------------------------ configuration (A6)

HOOK_FILE = "hook-ran.txt"
HOOK_TIMEOUT = 120
COMPACT_TIMEOUT = 180
LEASE_TIMEOUT = 180  # lease 60 s plus the dispatcher poll and the turn-boundary wait
AUTOCLOSE_TIMEOUT = 240  # 30 s window plus the sweeper interval and the discard


@contextlib.contextmanager
def config_session(
    api: httpx.Client, suffix: str, spec: dict[str, Any]
) -> Iterator[dict[str, Any]]:
    """A session of its own through POST /sessions; always deleted and awaited in teardown."""
    slug = f"e2e-cfg-{suffix}"
    resp = api.post("/sessions", json={"slug": slug, "spec": {**spec, "env": FAKE_TURN_ENV}})
    assert resp.status_code == 201, resp.text
    created = resp.json()
    cid = created["id"]
    try:
        yield created
    finally:
        api.delete(f"/sessions/{cid}")
        wait_for(
            f"session {slug} row to be gone",
            lambda: api.get(f"/sessions/{cid}").status_code == 404,
            DISCARD_TIMEOUT,
        )


def ready_agents(api: httpx.Client, cid: str, count: int) -> list[dict[str, Any]]:
    def probe() -> list[dict[str, Any]] | None:
        view = api.get(f"/sessions/{cid}").json()
        if view["status"].get("error"):
            raise AssertionError(f"session reconcile error: {view['status']}")
        agents = [a for a in agents_of(api, cid) if not a["gone"]]
        ready = [a for a in agents if a["ready"] and a["free_since"]]
        return ready if len(ready) == count else None

    return wait_for(f"{count} ready agents", probe, READY_TIMEOUT)


def test_wrapper_hook_and_idle_compact(api: httpx.Client) -> None:
    spec = {
        "replicas": 1,
        "wrapper": {
            "hooks": {
                "onTurnComplete": [
                    {"type": "command", "run": f'echo "$POD_NAME" > {HOOK_FILE}'},
                ]
            },
            "auto_compact": {"idle_seconds": 5},
        },
    }
    with config_session(api, "hook", spec) as created:
        cid = created["id"]
        agent = ready_agents(api, cid, 1)[0]
        resp = api.post(f"/sessions/{cid}/messages", json={"text": "say hello"})
        assert resp.status_code == 202, resp.text
        mid = resp.json()["message_id"]

        def turn_done() -> bool:
            view = message(api, mid)
            return bool(view["status"] == "sent" and view["turn"] and view["turn"]["completed_at"])

        wait_for(f"message {mid} turn to complete", turn_done, TURN_TIMEOUT, interval=1)

        def hook_output() -> str | None:
            r = api.get(f"/sessions/{cid}/files/content", params={"path": HOOK_FILE})
            return r.json()["content"] if r.status_code == 200 else None

        content = wait_for(
            f"{HOOK_FILE} written by the onTurnComplete hook", hook_output, HOOK_TIMEOUT
        )
        assert content.strip() == agent["pod_name"], (content, agent["pod_name"])

        def compacted() -> list[dict[str, Any]] | None:
            r = api.get(f"/sessions/{cid}/events", params={"types": "compacted"})
            r.raise_for_status()
            return r.json()["events"] or None

        events = wait_for("a compacted event after 5 s idle", compacted, COMPACT_TIMEOUT)
        assert all(e["type"] == "compacted" for e in events), events


def test_leader_lease_hands_over(api: httpx.Client) -> None:
    spec = {"replicas": 2, "leader_lease_seconds": 60}
    with config_session(api, "lease", spec) as created:
        cid = created["id"]
        ready_agents(api, cid, 2)

        def first_leader() -> dict[str, Any] | None:
            view = api.get(f"/sessions/{cid}").json()
            return view if view["leader_agent_id"] and view["leader_epoch"] is not None else None

        first = wait_for("a first leader", first_leader, LEADER_TIMEOUT)

        def handed_over() -> dict[str, Any] | None:
            view = api.get(f"/sessions/{cid}").json()
            ok = (
                view["leader_agent_id"] not in (None, first["leader_agent_id"])
                and view["leader_epoch"] > first["leader_epoch"]
                and view["leader_reason"] == "lease_expired"
            )
            return view if ok else None

        wait_for(
            f"leader to change from {first['leader_agent_id']} (epoch {first['leader_epoch']}) "
            "with reason lease_expired",
            handed_over,
            LEASE_TIMEOUT,
        )


COMPACT_PROMPT = "compact mid turn: report when done"


def test_leader_compaction_hands_over(api: httpx.Client) -> None:
    with config_session(api, "compact", {"replicas": 2}) as created:
        cid = created["id"]
        ready_agents(api, cid, 2)
        wait_for("the first election", lambda: leader_view(api, cid)[1] is not None, LEADER_TIMEOUT)
        first, epoch = leader_view(api, cid)
        assert first is not None and epoch is not None

        resp = api.post(f"/sessions/{cid}/messages", json={"text": COMPACT_PROMPT})
        assert resp.status_code == 202, resp.text
        mid = resp.json()["message_id"]

        def turn_done() -> dict[str, Any] | None:
            view = message(api, mid)
            done = view["status"] == "sent" and view["turn"] and view["turn"]["completed_at"]
            return view if done else None

        sent = wait_for(f"message {mid} turn to complete", turn_done, TURN_TIMEOUT, interval=1)
        assert sent["agent_id"] == first["id"], (sent, first)
        events = [e for page in all_events(api, cid) for e in page]
        compacting = [e for e in events if e["type"] == "compacting"]
        assert any(e["agent_id"] == first["id"] for e in compacting), compacting

        def handed_over() -> dict[str, Any] | None:
            view = api.get(f"/sessions/{cid}").json()
            ok = (
                view["leader_agent_id"] not in (None, first["id"])
                and view["leader_epoch"] > epoch
                and view["leader_reason"] == "compaction"
            )
            return view if ok else None

        new = wait_for("leadership to hand over (compaction)", handed_over, LEADER_TIMEOUT)

        resp = api.post(f"/sessions/{cid}/messages", json={"text": "say hello"})
        assert resp.status_code == 202, resp.text
        second = wait_for(
            "the next message to be dispatched",
            lambda: (v := message(api, resp.json()["message_id"]))["agent_id"] and v,
            TURN_TIMEOUT,
            interval=0.5,
        )
        assert second["agent_id"] == new["leader_agent_id"], (second, new)


def test_autoclose_discards_session(api: httpx.Client) -> None:
    from datetime import datetime, timedelta

    with config_session(api, "autoclose", {"replicas": 1, "autoclose_after_seconds": 30}) as c:
        cid = c["id"]
        view = api.get(f"/sessions/{cid}").json()
        created_at = datetime.fromisoformat(view["created_at"])
        assert datetime.fromisoformat(view["closes_at"]) == created_at + timedelta(seconds=30), view

        def closing() -> bool:
            r = api.get(f"/sessions/{cid}")
            return r.status_code == 404 or r.json()["desired"] == "deleting"

        wait_for("the autoclose sweeper to mark the session deleting", closing, AUTOCLOSE_TIMEOUT)

        def gone() -> bool:
            return api.get(f"/sessions/{cid}").status_code == 404

        wait_for("the closed session to answer 404", gone, DISCARD_TIMEOUT)


# ------------------------------------------------------------------ rollout mid-turn (D1, D11)
#
# A helm upgrade to a second tag of the same agent image rolls every session's StatefulSet while
# a long ("marathon") turn runs. The pods roll by OnDelete and the reconciler's idle-only
# replacer (reconciler/rollout.py): the busy pod waits for its turn, the leader goes last and
# only after leadership moved, and no wrapper session is replaced. The upgrade also replaces the
# api pod, so these tests use ResilientClient, and scripts/e2e.sh keeps the api port-forward
# alive across it.

ROOT = Path(__file__).resolve().parents[2]
CHART_DIR = ROOT / "chart" / "agents-orchestrator"
SCENARIO = ROOT / "e2e" / "agent-test.scenario.json"
AGENT_IMAGE_NEXT = os.environ.get(
    "ORCH_E2E_AGENT_IMAGE_NEXT", "agents-orchestrator-agent-test:e2e-next"
)
MARATHON_PROMPT = "marathon task {n}: report when done"
REVISION_LABEL = "controller-revision-hash"
HELM_TIMEOUT = 660  # helm upgrade --timeout 10m plus the call itself
API_OUTAGE_WINDOW = 180  # the api pod is replaced and the port-forward reconnects within this
ROLL_SAMPLE_INTERVAL = 2.0
DEAD_DEADLINE = 600  # no agent may stay dead longer: a pod comes back well within READY_TIMEOUT
ROLLOUT_STATUS_TIMEOUT = 600
IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "DELETE"})


def marathon_seconds() -> float:
    """How long the scenario's marathon turn sleeps (e2e/agent-test.scenario.json)."""
    turns = json.loads(SCENARIO.read_text())["turns"]
    turn = next(t for t in turns if t["match"] == "marathon")
    return float(sum(step["sleep"] for step in turn["steps"] if "sleep" in step))


class ResilientClient(httpx.Client):
    """An httpx client that rides out the api pod being replaced by a helm upgrade.

    A request that fails at the transport (the port-forward reconnecting, the pod terminating)
    is sent again for up to API_OUTAGE_WINDOW seconds, then fails with an AssertionError. Only
    an idempotent request is resent after any transport error; any other (a message POST) only
    after a ConnectError, which means it was never sent, so an accepted message is never
    posted twice. Every other transport error of a non-idempotent request is raised as is.
    """

    def send(self, request: httpx.Request, **kwargs: Any) -> httpx.Response:
        deadline = time.monotonic() + API_OUTAGE_WINDOW
        while True:
            try:
                return super().send(request, **kwargs)
            except httpx.TransportError as exc:
                if request.method not in IDEMPOTENT_METHODS and not isinstance(
                    exc, httpx.ConnectError
                ):
                    raise
                if time.monotonic() > deadline:
                    raise AssertionError(
                        f"api unreachable for {API_OUTAGE_WINDOW}s: {request.method} "
                        f"{request.url}: {exc!r}"
                    ) from exc
                time.sleep(1)


@pytest.fixture(scope="module")
def roll_api() -> Iterator[httpx.Client]:
    base = os.environ.get("ORCH_E2E_API_URL", API_URL)
    with api_client(base, ResilientClient) as client:
        yield client


def upgrade_agent_image(image: str, wait: bool = False) -> None:
    """`helm upgrade --reuse-values --set agentImage=<image>`; the sessions' template follows."""
    argv = [
        "helm",
        "upgrade",
        RELEASE,
        str(CHART_DIR),
        "-n",
        NAMESPACE,
        "--reuse-values",
        "--set",
        f"agentImage={image}",
        "--timeout",
        "10m",
    ]
    if wait:
        argv.append("--wait")
    run(*argv, timeout=HELM_TIMEOUT)


def wait_orchestrator_rolled() -> None:
    """Every orchestrator Deployment finished rolling (`kubectl rollout status`)."""
    for component in COMPONENTS:
        kubectl(
            "rollout",
            "status",
            f"deployment/{RELEASE}-{component}",
            f"--timeout={ROLLOUT_STATUS_TIMEOUT}s",
            timeout=ROLLOUT_STATUS_TIMEOUT + 30,
        )


@contextlib.contextmanager
def restoring_agent_image(api: httpx.Client) -> Iterator[str]:
    """Yield the release's agent image; upgrade the release back to it on exit (and wait)."""
    first = api.get("/meta").json()["agent_image"]
    assert first != AGENT_IMAGE_NEXT, (first, AGENT_IMAGE_NEXT)
    try:
        yield first
    finally:
        upgrade_agent_image(first, wait=True)
        wait_orchestrator_rolled()
        api.get("/meta").raise_for_status()


RECONCILER_SELECTOR = f"app.kubernetes.io/instance={RELEASE},app.kubernetes.io/component=reconciler"


def reconciler_log() -> str:
    return kubectl("logs", "-l", RECONCILER_SELECTOR, "--tail=-1", check=False).stdout


def reconciler_pods() -> list[dict[str, Any]]:
    return kube_json("get", "pods", "-l", RECONCILER_SELECTOR)["items"]


def scale_reconciler(replicas: int) -> None:
    kubectl("scale", f"deployment/{RELEASE}-reconciler", f"--replicas={replicas}")


@dataclass(frozen=True)
class PodInfo:
    uid: str
    revision: str | None
    image: str | None
    terminating: bool
    ready: bool


def pod_infos(slug: str) -> dict[str, PodInfo]:
    """The session's agent pods by name: uid, controller revision, image, terminating, Ready."""
    out: dict[str, PodInfo] = {}
    for pod in kube_json("get", "pods", "-l", f"{SESSION_LABEL}={slug}")["items"]:
        meta, name = pod["metadata"], pod["metadata"]["name"]
        if not re.fullmatch(rf"agent-{re.escape(slug)}-\d+", name):
            continue
        container = next((c for c in pod["spec"]["containers"] if c["name"] == "ccw"), {})
        conditions = (pod.get("status") or {}).get("conditions") or []
        out[name] = PodInfo(
            uid=meta["uid"],
            revision=(meta.get("labels") or {}).get(REVISION_LABEL),
            image=container.get("image"),
            terminating=bool(meta.get("deletionTimestamp")),
            ready=any(c["type"] == "Ready" and c["status"] == "True" for c in conditions),
        )
    return out


def sts_info(slug: str) -> dict[str, Any]:
    """The session's StatefulSet: update revision, strategy, template image, generation."""
    sts = kube_json("get", "statefulset", f"agent-{slug}")
    containers = sts["spec"]["template"]["spec"]["containers"]
    return {
        "update_revision": (sts.get("status") or {}).get("updateRevision"),
        "strategy": sts["spec"].get("updateStrategy"),
        "image": next((c["image"] for c in containers if c["name"] == "ccw"), None),
        "generation": sts["metadata"].get("generation"),
        "observed_generation": (sts.get("status") or {}).get("observedGeneration"),
    }


def post_message(api: httpx.Client, cid: str, text: str, target: str | None = None) -> int:
    body: dict[str, Any] = {"text": text}
    if target:
        body["target_agent_id"] = target
    resp = api.post(f"/sessions/{cid}/messages", json=body)
    assert resp.status_code == 202, resp.text
    return resp.json()["message_id"]


def turn_completed(view: dict[str, Any]) -> bool:
    return bool(view["status"] == "sent" and view["turn"] and view["turn"]["completed_at"])


def wrapper_ids(api: httpx.Client, cid: str) -> dict[str, set[str]]:
    """Agent id -> every wrapper session id its events carry (the events API)."""
    out: dict[str, set[str]] = {}
    for page in all_events(api, cid):
        for event in page:
            out.setdefault(event["agent_id"], set()).add(event["wrapper_session_id"])
    return out


@dataclass
class Snap:
    """One look at a rolling session. Pods are read first and the message last, so a pod seen
    replaced while `turn_done` is False really was replaced before its turn completed."""

    t: float
    pods: dict[str, PodInfo]
    update_revision: str | None
    strategy: dict[str, Any] | None
    template_image: str | None
    leader_id: str | None
    epoch: int | None
    leader_reason: str | None
    phase: str | None
    rollout: dict[str, Any] | None
    agents: dict[str, dict[str, Any]]
    turn_done: bool


class RollWatch:
    """Samples a session during a roll; `turn_mid` is the long turn's message id."""

    def __init__(self, api: httpx.Client, cid: str, slug: str) -> None:
        self.api, self.cid, self.slug = api, cid, slug
        self.turn_mid: int | None = None
        self.snaps: list[Snap] = []
        self.started = time.monotonic()

    def sample(self) -> Snap:
        pods = pod_infos(self.slug)
        sts = sts_info(self.slug)
        view = self.api.get(f"/sessions/{self.cid}").json()
        agents = {
            a["id"]: {k: a[k] for k in ("ordinal", "pod_name", "state", "ready", "gone")}
            for a in agents_of(self.api, self.cid)
        }
        assert self.turn_mid is not None
        snap = Snap(
            t=time.monotonic() - self.started,
            pods=pods,
            update_revision=sts["update_revision"],
            strategy=sts["strategy"],
            template_image=sts["image"],
            leader_id=view.get("leader_agent_id"),
            epoch=view.get("leader_epoch"),
            leader_reason=view.get("leader_reason"),
            phase=view["status"].get("phase"),
            rollout=view["status"].get("rollout"),
            agents=agents,
            turn_done=turn_completed(message(self.api, self.turn_mid)),
        )
        self.snaps.append(snap)
        return snap

    def timeline(self, last: int = 60) -> str:
        lines = []
        for s in self.snaps[-last:]:
            pods = " ".join(
                f"{name.rsplit('-', 1)[1]}:{p.uid[:6]}@{(p.revision or '-')[-5:]}"
                f"{'+T' if p.terminating else ''}{'+R' if p.ready else ''}"
                for name, p in sorted(s.pods.items())
            )
            agents = ",".join(
                f"{a['ordinal']}={a['state']}{'' if a['ready'] else '!'}"
                for a in sorted(s.agents.values(), key=lambda a: a["ordinal"])
            )
            leader = (s.agents.get(s.leader_id or "") or {}).get("ordinal")
            lines.append(
                f"t={s.t:6.1f}s update={(s.update_revision or '-')[-5:]} "
                f"leader={leader}/{s.epoch} ({s.leader_reason}) phase={s.phase} "
                f"agents[{agents}] pods[{pods}] rollout={s.rollout} turn_done={s.turn_done}"
            )
        return "\n".join(lines)


def roll_diagnostics(api: httpx.Client, cid: str, slug: str, watch: RollWatch) -> str:
    """Everything a failed roll needs to be read from the CI log alone."""

    def section(title: str, read: Callable[[], str]) -> str:
        try:
            return f"--- {title}\n{read()}"
        except Exception as exc:  # a diagnostic never hides the failure it explains
            return f"--- {title}\n(unavailable: {exc!r})"

    return "\n".join(
        [
            section(
                "pods (uid, controller-revision-hash)",
                lambda: (
                    kubectl(
                        "get",
                        "pods",
                        "-l",
                        f"{SESSION_LABEL}={slug}",
                        "-L",
                        REVISION_LABEL,
                        "-o",
                        "wide",
                    ).stdout
                ),
            ),
            section("statefulset", lambda: json.dumps(sts_info(slug))),
            section(
                "session status (status.rollout)",
                lambda: json.dumps(api.get(f"/sessions/{cid}").json()["status"]),
            ),
            section("agents", lambda: json.dumps(agents_of(api, cid))),
            section("reconciler log tail", lambda: "\n".join(reconciler_log().splitlines()[-100:])),
            section("timeline", watch.timeline),
        ]
    )


@contextlib.contextmanager
def diagnosed(api: httpx.Client, cid: str, slug: str, watch: RollWatch) -> Iterator[None]:
    try:
        yield
    except BaseException:
        print(roll_diagnostics(api, cid, slug, watch))
        raise


def touched(snap: Snap, pod: str, original: PodInfo) -> bool:
    """The pod was deleted: gone, terminating or already a new pod (another uid)."""
    current = snap.pods.get(pod)
    return current is None or current.uid != original.uid or current.terminating


def first_touch(snaps: list[Snap], pod: str, original: PodInfo) -> int | None:
    return next((i for i, s in enumerate(snaps) if touched(s, pod, original)), None)


def rolled_to(snap: Snap, image: str, replicas: int) -> bool:
    """Every agent pod is Ready on the update revision and the image; nothing is pending."""
    return (
        len(snap.pods) == replicas
        and snap.rollout is None
        and snap.template_image == image
        and all(
            p.revision == snap.update_revision
            and p.image == image
            and p.ready
            and not p.terminating
            for p in snap.pods.values()
        )
        and all(a["ready"] and a["state"] != "dead" for a in snap.agents.values() if not a["gone"])
    )


def longest_dead(snaps: list[Snap]) -> dict[str, float]:
    """Agent id -> the longest stretch (seconds between samples) it was seen `dead`."""
    longest: dict[str, float] = {}
    since: dict[str, float] = {}
    for s in snaps:
        for aid, agent in s.agents.items():
            if agent["state"] == "dead":
                since.setdefault(aid, s.t)
                longest[aid] = max(longest.get(aid, 0.0), s.t - since[aid])
            else:
                since.pop(aid, None)
    return longest


def streamed_ids(api: httpx.Client, cid: str, agents: int) -> dict[str, set[str]] | None:
    ids = wrapper_ids(api, cid)
    return ids if len(ids) == agents and all(len(v) == 1 for v in ids.values()) else None


def replacement_order() -> list[str]:
    """Pod names in the order the reconciler logged replacing them.

    The reconciler logs one JSON object per line; the pod name ends its `msg`.
    """
    pattern = re.compile(r"replacing idle outdated pod (\S+)$")
    order = []
    for line in reconciler_log().splitlines():
        try:
            record = json.loads(line)
        except ValueError:
            continue
        msg = record.get("msg") if isinstance(record, dict) else None
        if isinstance(msg, str) and (m := pattern.search(msg)):
            order.append(m.group(1))
    return order


@pytest.fixture(scope="module")
def roll_session(roll_api: httpx.Client) -> Iterator[dict[str, Any]]:
    """A 2-replica hive session of its own; the release is back on its first image afterwards."""
    spec = {"replicas": 2, "max_replicas": 2}
    with restoring_agent_image(roll_api) as first, scale_session(roll_api, "roll", spec) as made:
        wait_sessions(roll_api, made["id"], 2)
        wait_leader(roll_api, made["id"], 1)
        yield {**made, "first_image": first}


@pytest.fixture(scope="module")
def image_roll(roll_api: httpx.Client, roll_session: dict[str, Any]) -> dict[str, Any]:
    """Run the roll once: a marathon turn on the non-leader, then the helm upgrade.

    Samples until every pod is on the new image and the posted messages are delivered.
    """
    api, cid, slug = roll_api, roll_session["id"], roll_session["slug"]
    first_image = roll_session["first_image"]
    watch = RollWatch(api, cid, slug)
    with diagnosed(api, cid, slug, watch):
        leader, epoch0 = leader_view(api, cid)
        assert leader is not None and epoch0 is not None
        follower = next(a for a in live_agents(api, cid) if a["id"] != leader["id"])
        leader_pod, follower_pod = leader["pod_name"], follower["pod_name"]
        original = pod_infos(slug)
        before = sts_info(slug)
        assert set(original) == {leader_pod, follower_pod}, original
        assert all(p.revision == before["update_revision"] for p in original.values()), (
            original,
            before,
        )
        assert all(p.image == first_image for p in original.values()), (original, first_image)
        ids_before = wait_for(
            "one wrapper session id per agent in the events", lambda: streamed_ids(api, cid, 2), 120
        )

        posted = {"marathon": post_message(api, cid, MARATHON_PROMPT.format(n=1), follower["id"])}
        watch.turn_mid = posted["marathon"]

        def follower_runs_marathon() -> bool:
            sent = message(api, posted["marathon"])["status"] == "sent"
            return sent and agent_by_pod(api, cid, follower_pod)["state"] == "busy"

        wait_for("the follower to run the marathon turn", follower_runs_marathon, TURN_TIMEOUT, 0.5)

        upgrade_agent_image(AGENT_IMAGE_NEXT)
        # a message POST is not resent after it may have reached the api (ResilientClient), so
        # post once the api pod is replaced; the busy follower still holds the agent roll
        wait_orchestrator_rolled()
        api.get("/meta").raise_for_status()
        # sent while the roll starts: one to the leader, one queued for the busy follower
        posted["to_leader"] = post_message(api, cid, SLOW_PROMPT.format(n=1))
        posted["queued_for_follower"] = post_message(
            api, cid, SLOW_PROMPT.format(n=2), follower["id"]
        )

        budget = marathon_seconds() + 2 * READY_TIMEOUT + 300
        deadline = time.monotonic() + budget
        stable = 0
        while True:
            snap = watch.sample()
            if "to_replaced_follower" not in posted and touched(
                snap, follower_pod, original[follower_pod]
            ):  # the busy pod's turn is over and its pod is rolling: queue one for the new pod
                posted["to_replaced_follower"] = post_message(
                    api, cid, SLOW_PROMPT.format(n=3), follower["id"]
                )
            delivered = all(turn_completed(message(api, mid)) for mid in posted.values())
            rolled = rolled_to(snap, AGENT_IMAGE_NEXT, 2) and delivered
            stable = stable + 1 if rolled and "to_replaced_follower" in posted else 0
            if stable >= 2:
                break
            if time.monotonic() > deadline:
                raise AssertionError(
                    f"the roll did not settle within {budget:.0f}s\n{watch.timeline()}"
                )
            time.sleep(ROLL_SAMPLE_INTERVAL)

        return {
            "cid": cid,
            "slug": slug,
            "first_image": first_image,
            "leader": leader,
            "follower": follower,
            "epoch0": epoch0,
            "original": original,
            "snaps": watch.snaps,
            "timeline": watch.timeline(len(watch.snaps)),
            "ids_before": ids_before,
            "ids_after": wrapper_ids(api, cid),
            "messages": {label: message(api, mid) for label, mid in posted.items()},
            "replacement_order": replacement_order(),
        }


def test_rolling_update_statefulset_moves_to_on_delete_without_a_roll(
    roll_api: httpx.Client,
) -> None:
    """The StatefulSet of a session created by the previous release carries RollingUpdate with a
    defaulted partition. With the reconciler stopped, patch the strategy back to RollingUpdate
    (as a kubectl patch does), then change the template (helm upgrade to the second image) and
    start the new reconciler: its apply must succeed, leave OnDelete without `rollingUpdate`,
    and the controller must roll nothing, so the busy pod keeps its uid until its turn is done.
    """
    api = roll_api
    with restoring_agent_image(api), scale_session(api, "ondelete", {"replicas": 1}) as made:
        cid, slug = made["id"], made["slug"]
        pod = f"agent-{slug}-0"
        wait_sessions(api, cid, 1)
        watch = RollWatch(api, cid, slug)
        with diagnosed(api, cid, slug, watch):
            original = pod_infos(slug)[pod]
            before = sts_info(slug)
            assert before["strategy"]["type"] == "OnDelete", before
            scale_reconciler(0)
            try:
                wait_for("the reconciler pods to be gone", lambda: not reconciler_pods(), 180)
                kubectl(
                    "patch",
                    "statefulset",
                    f"agent-{slug}",
                    "--type",
                    "merge",
                    "-p",
                    json.dumps({"spec": {"updateStrategy": {"type": "RollingUpdate"}}}),
                )
                patched = sts_info(slug)
                assert patched["strategy"]["type"] == "RollingUpdate", patched
                print(f"statefulset agent-{slug} strategy after the patch: {patched['strategy']}")
                mid = post_message(api, cid, MARATHON_PROMPT.format(n=3))
                watch.turn_mid = mid

                def busy() -> bool:
                    sent = message(api, mid)["status"] == "sent"
                    return sent and agent_by_pod(api, cid, pod)["state"] == "busy"

                wait_for("the agent to run the marathon turn", busy, TURN_TIMEOUT, 0.5)
                upgrade_agent_image(AGENT_IMAGE_NEXT)
            finally:
                scale_reconciler(1)
            wait_orchestrator_rolled()

            deadline = time.monotonic() + marathon_seconds() + READY_TIMEOUT
            while not watch.sample().turn_done:
                if time.monotonic() > deadline:
                    raise AssertionError(f"the marathon turn did not end\n{watch.timeline()}")
                time.sleep(ROLL_SAMPLE_INTERVAL)

            snaps = watch.snaps
            early = [s for s in snaps if touched(s, pod, original) and not s.turn_done]
            assert not early, f"the busy pod {pod} was replaced mid-turn\n{watch.timeline()}"
            switched = [
                s
                for s in snaps
                if not s.turn_done
                and s.strategy == {"type": "OnDelete"}
                and s.template_image == AGENT_IMAGE_NEXT
            ]
            assert switched, (
                "the StatefulSet never reached OnDelete (no rollingUpdate) on the new image "
                f"while the turn ran\n{watch.timeline()}"
            )
            waiting = [
                s for s in snaps if not s.turn_done and 0 in (s.rollout or {}).get("waiting", [])
            ]
            assert waiting, f"status.rollout never showed the busy pod waiting\n{watch.timeline()}"
            assert not [s for s in snaps if s.phase == "error"], watch.timeline()
            log = reconciler_log()
            assert f"statefulset agent-{slug}: moving to updateStrategy OnDelete" in log, log[
                -3000:
            ]
            assert f"reconcile of session {slug} failed" not in log, log[-3000:]
            assert sts_info(slug)["strategy"] == {"type": "OnDelete"}, sts_info(slug)

            def rolled_when_idle() -> bool:
                info = pod_infos(slug).get(pod)
                sts = sts_info(slug)
                return bool(
                    info
                    and info.uid != original.uid
                    and info.ready
                    and not info.terminating
                    and info.revision == sts["update_revision"]
                    and info.image == AGENT_IMAGE_NEXT
                )

            wait_for(f"pod {pod} to roll once its turn is done", rolled_when_idle, READY_TIMEOUT, 3)


def test_image_roll_keeps_the_busy_pod_until_its_turn_completed(
    image_roll: dict[str, Any],
) -> None:
    r = image_roll
    follower, pod = r["follower"], r["follower"]["pod_name"]
    original, snaps = r["original"][pod], r["snaps"]
    early = [s for s in snaps if touched(s, pod, original) and not s.turn_done]
    assert not early, f"the busy pod {pod} was replaced mid-turn\n{r['timeline']}"
    waiting = [
        s
        for s in snaps
        if not s.turn_done and follower["ordinal"] in (s.rollout or {}).get("waiting", [])
    ]
    assert waiting, (
        "status.rollout never showed the busy pod waiting while its marathon turn ran "
        f"(is the marathon turn too short for the roll?)\n{r['timeline']}"
    )
    marathon = r["messages"]["marathon"]
    assert turn_completed(marathon) and marathon["turn"]["status"] == "completed", marathon
    assert first_touch(snaps, pod, original) is not None, r["timeline"]
    final = snaps[-1].pods[pod]
    assert final.uid != original.uid, (final, original)
    assert final.revision == snaps[-1].update_revision and final.image == AGENT_IMAGE_NEXT, final


def test_image_roll_replaces_the_leader_last_after_leadership_moved(
    image_roll: dict[str, Any],
) -> None:
    r = image_roll
    leader, follower, snaps = r["leader"], r["follower"], r["snaps"]
    f_pod, l_pod = follower["pod_name"], leader["pod_name"]
    f_at = first_touch(snaps, f_pod, r["original"][f_pod])
    l_at = first_touch(snaps, l_pod, r["original"][l_pod])
    assert f_at is not None and l_at is not None, r["timeline"]
    assert f_at < l_at, f"the leader pod went before the follower pod\n{r['timeline']}"
    assert snaps[l_at].leader_id not in (None, leader["id"]), (
        f"the leader pod {l_pod} was deleted before leadership moved\n{r['timeline']}"
    )
    assert "handover: rollout" in {s.leader_reason for s in snaps}, r["timeline"]
    assert snaps[-1].leader_id == follower["id"], r["timeline"]
    assert snaps[-1].epoch is not None and snaps[-1].epoch > r["epoch0"], r["timeline"]
    order = r["replacement_order"]
    assert f_pod in order and l_pod in order, order
    assert order.index(f_pod) < order.index(l_pod), order


def test_image_roll_keeps_wrapper_session_ids_and_no_agent_stays_dead(
    image_roll: dict[str, Any],
) -> None:
    r = image_roll
    assert r["ids_after"] == r["ids_before"], (r["ids_before"], r["ids_after"])
    assert all(len(v) == 1 for v in r["ids_after"].values()), r["ids_after"]
    dead = longest_dead(r["snaps"])
    too_long = {aid: secs for aid, secs in dead.items() if secs > DEAD_DEADLINE}
    assert not too_long, f"agents stayed dead past {DEAD_DEADLINE}s: {too_long}\n{r['timeline']}"
    last = r["snaps"][-1]
    assert all(a["state"] != "dead" and a["ready"] for a in last.agents.values()), r["timeline"]


def test_messages_posted_during_the_roll_are_delivered(image_roll: dict[str, Any]) -> None:
    messages = image_roll["messages"]
    assert set(messages) == {"marathon", "to_leader", "queued_for_follower", "to_replaced_follower"}
    for label, view in messages.items():
        assert view["status"] == "sent", (label, view)
        assert turn_completed(view), (label, view)
        assert view["turn"]["status"] == "completed", (label, view)
        assert view["error"] is None, (label, view)


def test_busy_pod_deleted_by_hand_returns_on_the_new_revision(
    roll_api: httpx.Client, image_roll: dict[str, Any]
) -> None:
    """A second roll (back to the first image) with a marathon turn on the non-leader; the
    outdated busy pod is deleted by hand (a crash, accepted under D1)."""
    api, cid, slug = roll_api, image_roll["cid"], image_roll["slug"]
    first_image = image_roll["first_image"]
    watch = RollWatch(api, cid, slug)
    with diagnosed(api, cid, slug, watch):
        leader, _ = leader_view(api, cid)
        assert leader is not None
        target = next(a for a in live_agents(api, cid) if a["id"] != leader["id"])
        pod = target["pod_name"]
        ids_before = wait_for(
            "one wrapper session id per agent", lambda: streamed_ids(api, cid, 2), 120
        )
        assert ids_before == image_roll["ids_after"], (ids_before, image_roll["ids_after"])

        mid = post_message(api, cid, MARATHON_PROMPT.format(n=2), target["id"])
        watch.turn_mid = mid

        def running() -> bool:
            sent = message(api, mid)["status"] == "sent"
            return sent and agent_by_pod(api, cid, pod)["state"] == "busy"

        wait_for("the target to run the marathon turn", running, TURN_TIMEOUT, 0.5)
        turn_id = message(api, mid)["turn_id"]
        assert turn_id, message(api, mid)
        upgrade_agent_image(first_image)
        # the old reconciler (still applying the previous image) is gone before the pod is read
        wait_orchestrator_rolled()

        def outdated_and_held() -> PodInfo | None:
            live = [p for p in reconciler_pods() if not p["metadata"].get("deletionTimestamp")]
            info = pod_infos(slug).get(pod)
            sts = sts_info(slug)
            rollout = api.get(f"/sessions/{cid}").json()["status"].get("rollout") or {}
            held = target["ordinal"] in rollout.get("waiting", [])
            outdated = info is not None and info.revision != sts["update_revision"]
            ok = len(live) == 1 and sts["image"] == first_image and outdated and held
            return info if ok else None

        old = wait_for(
            "the busy target to be an outdated pod the roll holds", outdated_and_held, 600, 5
        )
        assert old.image == AGENT_IMAGE_NEXT, old
        kubectl("delete", "pod", pod, "--wait=false")

        def returned() -> PodInfo | None:
            info = pod_infos(slug).get(pod)
            ok = info is not None and info.uid != old.uid and info.ready and not info.terminating
            return info if ok else None

        new = wait_for(f"pod {pod} to return Ready", returned, READY_TIMEOUT, 3)
        sts = sts_info(slug)
        assert new.revision == sts["update_revision"] and new.image == first_image, (new, sts)
        assert new.revision != old.revision, (new, old)

        def turn_done() -> dict[str, Any] | None:
            view = message(api, mid)
            return view if turn_completed(view) else None

        done = wait_for("the interrupted turn to complete", turn_done, TURN_TIMEOUT, 2)
        assert done["turn_id"] == turn_id and done["turn"]["status"] == "completed", done

        def resumed() -> bool:
            page = api.get(f"/sessions/{cid}/events", params={"types": "turn_resumed"}).json()
            return any(
                turn_id in (e["turn_id"], (e["data"] or {}).get("turn_id")) for e in page["events"]
            )

        wait_for(f"a turn_resumed event for turn {turn_id}", resumed, 60)
        ids_after = wrapper_ids(api, cid)
        assert ids_after == ids_before, (ids_before, ids_after)

        def alive() -> bool:
            agent = agent_by_pod(api, cid, pod)
            return agent["state"] != "dead" and agent["ready"]

        wait_for(f"agent {pod} to be alive again", alive, DEAD_DEADLINE, 3)

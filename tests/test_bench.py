import io
import json
import random
from pathlib import Path

import httpx
import pytest

from agents_orchestrator import cli
from agents_orchestrator.bench.report import format_table, summarize
from agents_orchestrator.bench.runner import Bench, arm_slug
from agents_orchestrator.bench.spec import BenchSpecError, load_spec, parse_spec

SPEC = {
    "arms": [
        {"name": "one", "replicas": 1, "peers": False},
        {"name": "three", "replicas": 3, "peers": True},
    ],
    "session": {"model": "sonnet"},
    "tasks": [{"name": "t1", "prompt": "do it", "verify": "true"}],
    "repeats": 2,
    "timeout_seconds": 5,
}


class FakeApi:
    """Just enough of /api/v1 for the bench: one agent per replica, instant turns."""

    def __init__(
        self, *, verify_status="passed", stream_ok=True, turn_status="completed", view_ok=True
    ):
        self.view_ok = view_ok
        self.calls: list[tuple[str, str]] = []
        self.bodies: dict[str, dict] = {}
        self.sessions: dict[str, int] = {}
        self.next_msg = 0
        self.verify_status = verify_status
        self.stream_ok = stream_ok
        self.turn_status = turn_status

    def handler(self, request: httpx.Request) -> httpx.Response:  # noqa: C901, PLR0911
        m, path = request.method, request.url.path
        self.calls.append((m, path))
        body = json.loads(request.content) if request.content else None
        if (m, path) == ("POST", "/api/v1/sessions"):
            cid = f"c{len(self.sessions)}"
            self.sessions[cid] = body["spec"]["replicas"]
            self.bodies[cid] = body
            return httpx.Response(201, json={"id": cid, "slug": body["slug"]})
        if m == "GET" and path.startswith("/api/v1/sessions/") and path.count("/") == 4:
            if not self.view_ok:
                return httpx.Response(500, text="boom")
            return httpx.Response(200, json={"id": path.rsplit("/", 1)[1], "leader_epoch": 4})
        if path.endswith("/agents"):
            n = self.sessions[path.split("/")[4]]
            return httpx.Response(
                200,
                json=[
                    {"id": f"a{i}", "ready": True, "state": "busy", "gone": False} for i in range(n)
                ],
            )
        if m == "POST" and path.endswith("/messages"):
            self.next_msg += 1
            return httpx.Response(202, json={"message_id": self.next_msg, "status": "queued"})
        if m == "GET" and path.startswith("/api/v1/messages/"):
            mid = int(path.rsplit("/", 1)[1])
            ok = self.turn_status == "completed"
            return httpx.Response(
                200,
                json={
                    "id": mid,
                    "status": "sent" if ok else "failed",
                    "attempts": 1 if ok else 3,
                    "created_at": "2026-01-01T00:00:00Z",
                    "turn": {
                        "status": self.turn_status,
                        "completed_at": "2026-01-01T00:00:10Z",
                        "usage": {"input_tokens": 100, "output_tokens": 20},
                    },
                },
            )
        if m == "GET" and path == "/api/v1/stream":
            if not self.stream_ok:
                return httpx.Response(503)
            return httpx.Response(200, text='event: snapshot\ndata: {"sessions": []}\n\n')
        if m == "POST" and path.endswith("/checks"):
            return httpx.Response(202, json={"check_id": 9})
        if m == "GET" and path == "/api/v1/checks/9":
            ok = self.verify_status == "passed"
            return httpx.Response(
                200, json={"status": self.verify_status, "exit_code": 0 if ok else 1}
            )
        if m == "POST" and path.endswith("/restart"):
            return httpx.Response(202, json={"command_id": 1})
        if m == "DELETE":
            return httpx.Response(202, json={"id": path.rsplit("/", 1)[1]})
        return httpx.Response(404, json={"error": {"code": "not_found", "message": path}})


def run(api, spec_data=SPEC, **kw):
    spec = parse_spec(spec_data)
    client = httpx.Client(base_url="http://x", transport=httpx.MockTransport(api.handler))
    out = io.StringIO()
    bench = Bench(spec, client, out, run_id="abc123", poll_seconds=0.01, sleep=lambda s: None, **kw)
    return bench.run(), out.getvalue()


def test_spec_parsing_and_validation(tmp_path, capsys):
    spec = parse_spec(SPEC)
    assert [a.name for a in spec.arms] == ["one", "three"]
    assert spec.session.model == "sonnet"
    bad = [
        ({**SPEC, "arms": []}, "arms"),
        ({**SPEC, "tasks": [{"name": "t", "prompt": "", "verify": "x"}]}, "tasks.0.prompt"),
        ({**SPEC, "repeats": 0}, "repeats"),
        ({**SPEC, "chaos": {"after_seconds": -1}}, "chaos.after_seconds"),
        ({**SPEC, "arms": [SPEC["arms"][0]] * 2}, "duplicate"),
        ({**SPEC, "bogus": 1}, "bogus"),
    ]
    for data, field in bad:
        with pytest.raises(BenchSpecError) as exc:
            parse_spec(data)
        assert field in str(exc.value)
    path = tmp_path / "bad.yaml"
    path.write_text("arms: []\n")
    with pytest.raises(SystemExit) as exit_:
        cli.main(["bench", str(path), "--api", "http://x", "--out", str(tmp_path / "o")])
    assert exit_.value.code == 2
    assert "arms" in capsys.readouterr().err


def test_runs_each_arm_and_repeat():
    api = FakeApi()
    records, _ = run(api)
    assert [(r["arm"], r["repeat"]) for r in records] == [
        ("one", 1),
        ("one", 2),
        ("three", 1),
        ("three", 2),
    ]
    assert api.calls.count(("POST", "/api/v1/sessions")) == 2
    assert api.bodies["c1"]["spec"]["replicas"] == 3 and api.bodies["c1"]["spec"]["peers"] is True
    assert api.bodies["c0"]["spec"]["model"] == "sonnet"
    assert api.bodies["c0"]["slug"] == "bench-one-abc123"
    assert len(arm_slug("x" * 80, "abc123")) <= 40


def test_metrics_jsonl_fields():
    records, out = run(FakeApi())
    lines = [json.loads(line) for line in out.splitlines()]
    assert lines == records
    r = lines[0]
    assert r["wall_seconds"] == 10.0
    assert r["turn_status"] == "completed" and r["failed_turns"] == 0
    assert (r["input_tokens"], r["output_tokens"]) == (100, 20)
    assert r["verify_passed"] is True and r["exit_code"] == 0
    failed, _ = run(FakeApi(turn_status="failed", verify_status="failed", stream_ok=False))
    assert failed[0]["turn_status"] == "failed" and failed[0]["failed_turns"] == 3
    assert failed[0]["verify_passed"] is False and failed[0]["exit_code"] == 1


def test_summary_table():
    def rec(arm, wall, status, tin, tout, ok):
        return dict(
            arm=arm,
            wall_seconds=wall,
            turn_status=status,
            input_tokens=tin,
            output_tokens=tout,
            verify_passed=ok,
        )

    rows = summarize(
        [
            rec("a", 10, "completed", 80, 20, True),
            rec("a", 30, "completed", 180, 20, False),
            rec("a", 99, "failed", None, None, False),
            rec("b", 5, "completed", 10, 0, True),
        ]
    )
    a = rows[0]
    assert (a["runs"], a["mean_wall_s"], a["p50_wall_s"], a["failures"]) == (3, 20, 20, 1)
    assert a["mean_tokens"] == pytest.approx(100.0)
    assert a["verify_pass"] == pytest.approx(1 / 3)
    table = format_table(rows)
    assert table.splitlines()[0].split()[:3] == ["arm", "runs", "mean_wall_s"]
    assert "33%" in table and "100%" in table


def test_chaos_restart_called():
    api = FakeApi()
    run(api, {**SPEC, "chaos": {"after_seconds": 0}}, rng=random.Random(1))
    restarts = [c for c in api.calls if c[1].endswith("/restart")]
    assert len(restarts) == 2  # once per arm
    quiet = FakeApi()
    run(quiet)
    assert not [c for c in quiet.calls if c[1].endswith("/restart")]


def test_sessions_discarded_unless_keep():
    api = FakeApi()
    run(api)
    assert sorted(p for m, p in api.calls if m == "DELETE") == [
        "/api/v1/sessions/c0",
        "/api/v1/sessions/c1",
    ]
    kept = FakeApi()
    run(kept, keep=True)
    assert not [c for c in kept.calls if c[0] == "DELETE"]


def test_example_spec_parses():
    spec = load_spec("examples/bench.yaml")
    assert [(a.replicas, a.peers) for a in spec.arms] == [
        (1, False),
        (3, False),
        (3, True),
        (3, True),
    ]


def test_arm_cap_and_static_in_spec():
    data = {
        **SPEC,
        "arms": [
            {"name": "h", "replicas": 2, "peers": True, "max_replicas": 4},
            {"name": "s", "replicas": 3, "peers": True, "static": True},
            {"name": "plain", "replicas": 1, "peers": False},
        ],
    }
    api = FakeApi()
    run(api, data)
    h, s, plain = (api.bodies[c]["spec"] for c in ("c0", "c1", "c2"))
    assert h["max_replicas"] == 4 and "static" not in h
    assert s["static"] is True and "max_replicas" not in s
    assert "max_replicas" not in plain and "static" not in plain


def test_report_leader_epoch():
    recs, out = run(FakeApi())
    assert all(r["leader_epoch"] == 4 for r in recs)
    assert json.loads(out.splitlines()[0])["leader_epoch"] == 4
    rows = summarize(
        [
            dict(
                arm="a",
                wall_seconds=1,
                turn_status="completed",
                input_tokens=1,
                output_tokens=1,
                verify_passed=True,
                leader_epoch=2,
            ),
            dict(
                arm="a",
                wall_seconds=1,
                turn_status="completed",
                input_tokens=1,
                output_tokens=1,
                verify_passed=True,
                leader_epoch=5,
            ),
            dict(
                arm="b",
                wall_seconds=1,
                turn_status="completed",
                input_tokens=1,
                output_tokens=1,
                verify_passed=True,
            ),
        ]
    )
    assert [r["leader_epoch"] for r in rows] == [5, None]
    lines = format_table(rows).splitlines()
    assert "leader_epoch" in lines[0]
    assert lines[2].split()[-1] == "5" and lines[3].split()[-1] == "-"


def test_example_has_hive_arm():
    spec = load_spec("examples/bench.yaml")
    hive = next(a for a in spec.arms if a.name == "hive3")
    assert (hive.replicas, hive.peers) == (3, True)
    assert "hive" in Path("examples/bench.yaml").read_text().split("arms:")[0]


def test_leader_epoch_none_when_session_view_fails(caplog):
    with caplog.at_level("WARNING", logger="agents_orchestrator.bench"):
        recs, _ = run(FakeApi(view_ok=False))
    assert recs and all(r["leader_epoch"] is None for r in recs)
    assert "reading leader epoch" in caplog.text


def test_example_bench_arms_pass_api_spec_validation():
    from agents_orchestrator.settings import Settings
    from agents_orchestrator.spec import SessionSpec, validate_spec

    settings = Settings(database_url="postgresql://x/x", master_key="k" * 32)
    spec = load_spec("examples/bench.yaml")
    for arm in spec.arms:
        body = {
            **spec.session.model_dump(exclude_none=True, exclude={"max_replicas", "static"}),
            "replicas": arm.replicas,
            "peers": arm.peers,
            **arm.model_dump(include={"max_replicas", "static"}, exclude_none=True),
        }
        validate_spec(SessionSpec(**body), settings)

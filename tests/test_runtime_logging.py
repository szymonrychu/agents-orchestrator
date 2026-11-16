"""The JSON log formatter and the structured session close line."""

from __future__ import annotations

import json
import logging
import uuid

from agents_orchestrator.db import sessions as repo
from agents_orchestrator.runtime.run import JsonFormatter


def _record(**extra) -> logging.LogRecord:
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "hello", None, None)
    for key, value in extra.items():
        setattr(record, key, value)
    return record


def test_json_formatter_merges_fields():
    out = json.loads(JsonFormatter().format(_record(fields={"close_kind": "self"})))
    assert out["close_kind"] == "self"
    assert out["msg"] == "hello"


def test_json_formatter_keeps_reserved_keys():
    fields = {"msg": "x", "ts": "t", "level": "l", "logger": "g", "exc": "e", "step": "ask"}
    out = json.loads(JsonFormatter().format(_record(fields=fields)))
    assert out["msg"] == "hello"
    assert out["level"] == "INFO"
    assert out["logger"] == "x"
    assert out["ts"] != "t"
    assert "exc" not in out
    assert out["step"] == "ask"


def test_log_close_line(caplog):
    sid = uuid.uuid4()
    caplog.set_level(logging.INFO, logger="agents_orchestrator.db.sessions")
    repo.log_close(sid, close_kind="inactivity", reason="quiet", step="ask")
    [record] = [r for r in caplog.records if r.getMessage().startswith("session close: ")]
    assert record.getMessage() == (
        f"session close: {sid} close_kind=inactivity step=ask reason=quiet"
    )
    assert record.fields == {
        "event": "session_close",
        "session_id": str(sid),
        "close_kind": "inactivity",
        "close_reason": "quiet",
        "step": "ask",
    }


def test_configure_logging_holds_http_clients_at_warning():
    from agents_orchestrator.runtime.run import configure_logging

    root = logging.getLogger()
    saved = (root.handlers[:], root.level)
    levels = {n: logging.getLogger(n).level for n in ("httpx", "httpcore")}
    try:
        for name in ("httpx", "httpcore"):
            logging.getLogger(name).setLevel(logging.NOTSET)
        configure_logging("DEBUG")
        for name in ("httpx", "httpcore"):
            assert logging.getLogger(name).level == logging.WARNING
    finally:
        root.handlers[:], root.level = saved[0], saved[1]
        for name, level in levels.items():
            logging.getLogger(name).setLevel(level)


def test_access_log_filter_strips_query_string():
    import io

    from agents_orchestrator.runtime.run import quiet_access_log

    quiet_access_log()
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(message)s"))
    access = logging.getLogger("uvicorn.access")
    old_level, old_prop = access.level, access.propagate
    access.addHandler(handler)
    access.setLevel(logging.INFO)
    access.propagate = False
    try:
        access.info(
            '%s - "%s %s HTTP/%s" %d',
            "10.0.0.1:1",
            "GET",
            "/agent/v1/chat/history?from=CANARY",
            "1.1",
            200,
        )
    finally:
        access.removeHandler(handler)
        access.setLevel(old_level)
        access.propagate = old_prop
    out = stream.getvalue()
    assert "CANARY" not in out
    assert "GET /agent/v1/chat/history HTTP/1.1" in out and "200" in out

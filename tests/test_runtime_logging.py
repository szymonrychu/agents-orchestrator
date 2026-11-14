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

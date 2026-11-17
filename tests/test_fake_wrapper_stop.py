"""The fake wrapper emits assistant_text and the onStop payload in the real wrapper's format."""

from __future__ import annotations

from fakes.fake_wrapper import FakeWrapper
from test_api_leadership import hive
from test_envelope_stop import BAD, OK, stop


async def test_say_carries_turn_id_and_sidechain_and_feeds_the_stop_payload():
    async with FakeWrapper() as fw:
        sid = fw.add_persisted_session("/workdir/w")
        fw.sessions[sid].current_turn_id = "turn-1"
        main = fw.say(OK, sid)
        side = fw.say("sub agent text", sid, sidechain=True)
        assert main["data"] == {"text": OK, "turn_id": "turn-1", "sidechain": False}
        assert side["data"]["sidechain"] is True
        payload = fw.on_stop_payload(sid)
        assert payload["event"] == "onStop" and payload["turn_id"] == "turn-1"
        data = payload["data"]
        assert data["turn_id"] == "turn-1"
        assert [t["text"] for t in data["assistant_texts"]] == [OK]
        assert set(data["assistant_texts"][0]) == {"message_id", "text"}
        assert data["assistant_texts_truncated"] is False


async def test_the_fake_payload_is_accepted_by_the_stop_endpoint(dsn, pool):
    c, _a0, _a1 = await hive(pool)
    async with FakeWrapper() as fw:
        sid = fw.add_persisted_session("/workdir/w")
        fw.sessions[sid].current_turn_id = "turn-1"
        fw.say(OK, sid)
        fw.say(BAD, sid)
        out = await stop(dsn, pool, c, fw.on_stop_payload(sid))
    assert out["decision"] == "block"
    assert "of message " in out["reason"]

"""Async client for claude-code-wrapper's /v1 API."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Any, Literal

import httpx

from agents_orchestrator.wrapper.models import (
    Ambiguous,
    Busy,
    ConfigError,
    DialogOpen,
    Envelope,
    Gone,
    PeersView,
    Rejected,
    Snapshot,
    SubmitOutcome,
    TurnAccepted,
    TurnRecord,
    TurnSummary,
    Unavailable,
    WrapperError,
    WrapperUnreachable,
)
from agents_orchestrator.wrapper.sse import SSEParser

log = logging.getLogger(__name__)

NoteOutcome = Literal["ok", "disabled", "failed"]

DEFAULT_TIMEOUT = 30.0


def _error_parts(resp: httpx.Response) -> tuple[str, str]:
    try:
        err = resp.json()["error"]
        return str(err["code"]), str(err.get("message", ""))
    except (ValueError, KeyError, TypeError):
        return f"http_{resp.status_code}", resp.text[:200]


def _raise_for_error(resp: httpx.Response) -> None:
    if resp.status_code >= 300:
        code, message = _error_parts(resp)
        raise WrapperError(resp.status_code, code, message)


class WrapperClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        http: httpx.AsyncClient | None = None,
        send_timeout: float = 15.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.send_timeout = send_timeout
        self._http = http or httpx.AsyncClient()
        self._owns_http = http is None
        self._headers = {"Authorization": f"Bearer {token}"}

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    def _url(self, path: str) -> str:
        return f"{self.base_url}/v1{path}"

    async def _request(
        self, method: str, path: str, *, json: Any = None, timeout: float = DEFAULT_TIMEOUT
    ) -> httpx.Response:
        try:
            resp = await self._http.request(
                method, self._url(path), json=json, headers=self._headers, timeout=timeout
            )
        except httpx.HTTPError as exc:
            raise WrapperUnreachable(f"{type(exc).__name__}: {exc}") from exc
        _raise_for_error(resp)
        return resp

    async def request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> tuple[int, Any]:
        """Any wrapper `/v1` call: (status, body) for a 2xx, the parsed JSON (None when empty).

        A 3xx/4xx/5xx raises WrapperError with the wrapper's own code; a transport failure
        raises WrapperUnreachable. The bearer travels only in the Authorization header.
        """
        try:
            resp = await self._http.request(
                method,
                self._url(path),
                params=params,
                json=json,
                headers=self._headers,
                timeout=timeout,
            )
        except httpx.HTTPError as exc:
            raise WrapperUnreachable(f"{type(exc).__name__}: {exc}") from exc
        _raise_for_error(resp)
        if not resp.content:
            return resp.status_code, None
        try:
            return resp.status_code, resp.json()
        except ValueError:
            return resp.status_code, resp.text

    # sessions

    async def list_sessions(self) -> list[Snapshot]:
        resp = await self._request("GET", "/sessions")
        return [Snapshot.model_validate(s) for s in resp.json()["sessions"]]

    async def create_session(
        self, name: str, workdir: str, model: str | None = None, effort: str | None = None
    ) -> Snapshot:
        body = {"name": name, "workdir": workdir, "model": model, "effort": effort}
        body = {k: v for k, v in body.items() if v is not None}
        # creation blocks until claude is ready, so it gets the long default
        resp = await self._request("POST", "/sessions", json=body, timeout=120.0)
        return Snapshot.model_validate(resp.json())

    async def get_session(self, session_id: str) -> Snapshot:
        return Snapshot.model_validate(
            (await self._request("GET", f"/sessions/{session_id}")).json()
        )

    async def resume_session(self, session_id: str) -> Snapshot:
        resp = await self._request("POST", f"/sessions/{session_id}/resume", timeout=120.0)
        return Snapshot.model_validate(resp.json())

    # turns

    async def submit_turn(
        self,
        session_id: str,
        text: str,
        metadata: dict[str, Any] | None = None,
        lane: str = "regular",
    ) -> SubmitOutcome:
        body: dict[str, Any] = {"text": text, "lane": lane}
        if metadata is not None:
            body["metadata"] = metadata
        try:
            resp = await self._http.post(
                self._url(f"/sessions/{session_id}/turns"),
                json=body,
                headers=self._headers,
                timeout=self.send_timeout,
            )
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as exc:
            return Unavailable(f"{type(exc).__name__}: {exc}")
        except httpx.HTTPError as exc:  # read timeout, reset, write failure: may be delivered
            return Ambiguous(f"{type(exc).__name__}: {exc}")
        return self._map_submit(resp)

    @staticmethod
    def _map_submit(resp: httpx.Response) -> SubmitOutcome:
        status = resp.status_code
        if 200 <= status < 300:
            try:
                return TurnAccepted(str(resp.json()["turn_id"]))
            except (ValueError, KeyError, TypeError):
                return Ambiguous(f"{status} without a turn_id")
        code, message = _error_parts(resp)
        reason = f"{status} {code}: {message}"
        if status == 409:
            if code == "dialog_open":
                return DialogOpen(reason)
            if code == "dead":
                return Unavailable(reason)  # nothing delivered; the rebootstrap path recovers
            return Busy()  # busy, booting (or an unknown 409): requeue without an attempt
        if status in (404, 410):
            return Gone(reason)
        if status in (401, 403):
            return ConfigError(reason)
        if status in (500, 502):
            # the wrapper may have recorded the turn before failing: never resend blindly
            return Ambiguous(reason)
        if status >= 500:
            return Unavailable(reason)
        return Rejected(reason)

    async def list_turns(self, session_id: str) -> list[TurnSummary]:
        resp = await self._request("GET", f"/sessions/{session_id}/turns")
        return [TurnSummary.model_validate(t) for t in resp.json()["turns"]]

    async def get_turn(self, session_id: str, turn_id: str) -> TurnRecord:
        resp = await self._request("GET", f"/sessions/{session_id}/turns/{turn_id}")
        return TurnRecord.model_validate(resp.json())

    # control

    async def interrupt(self, session_id: str) -> dict[str, Any]:
        return (await self._request("POST", f"/sessions/{session_id}/interrupt")).json()

    async def command(self, session_id: str, name: str) -> dict[str, Any]:
        resp = await self._request("POST", f"/sessions/{session_id}/commands", json={"name": name})
        return resp.json()

    async def post_note(
        self,
        session_id: str,
        text: str,
        key: str | None = None,
        source: str = "orchestrator",
        timeout: float = 3.0,
    ) -> NoteOutcome:
        """Push a note into the session's inbox: keyed notes replace, the rest are appended.

        "ok" when the wrapper accepted it, "disabled" for 409 notes_disabled (the session's
        wrapper runs without a notes inbox: nothing to retry), "failed" for anything else
        (unreachable, unknown or stopped session, refusal). Never raises; every miss is logged.
        """
        body: dict[str, Any] = {"text": text, "source": source}
        if key is not None:
            body["key"] = key
        try:
            await self._request("POST", f"/sessions/{session_id}/notes", json=body, timeout=timeout)
        except WrapperError as exc:
            log.info("note to session %s not delivered: %s", session_id, exc)
            return "disabled" if exc.code == "notes_disabled" else "failed"
        except WrapperUnreachable as exc:
            log.info("note to session %s not delivered: %s", session_id, exc)
            return "failed"
        return "ok"

    async def peers(self) -> PeersView:
        return PeersView.model_validate((await self._request("GET", "/peers")).json())

    # events

    async def events(
        self, session_id: str, last_event_id: int | str | None = None
    ) -> AsyncIterator[Envelope]:
        headers = {**self._headers, "Accept": "text/event-stream"}
        if last_event_id is not None:
            headers["Last-Event-ID"] = str(last_event_id)
        parser = SSEParser()
        try:
            async with self._http.stream(
                "GET",
                self._url(f"/sessions/{session_id}/events"),
                headers=headers,
                timeout=httpx.Timeout(DEFAULT_TIMEOUT, read=None),
            ) as resp:
                if resp.status_code >= 300:
                    await resp.aread()
                    _raise_for_error(resp)
                async for chunk in resp.aiter_bytes():
                    for ev in parser.feed(chunk):
                        yield Envelope.model_validate_json(ev.data)
        except httpx.HTTPError as exc:
            raise WrapperUnreachable(f"{type(exc).__name__}: {exc}") from exc

    # file download

    async def open_download(self, session_id: str, path: str) -> httpx.Response:
        """Open the wrapper's raw file download; the caller relays aiter_raw() and aclose()s it.

        Status >= 300 is read, closed and raised as WrapperError before any byte is handed
        out. No total timeout (any size): connect and each read keep DEFAULT_TIMEOUT.
        """
        request = self._http.build_request(
            "GET",
            self._url(f"/sessions/{session_id}/files/download"),
            params={"path": path},
            headers=self._headers,
            timeout=httpx.Timeout(DEFAULT_TIMEOUT),
        )
        try:
            resp = await self._http.send(request, stream=True)
        except httpx.HTTPError as exc:
            raise WrapperUnreachable(f"{type(exc).__name__}: {exc}") from exc
        if resp.status_code < 300:
            return resp
        try:
            await resp.aread()
        except httpx.HTTPError as exc:
            raise WrapperUnreachable(f"{type(exc).__name__}: {exc}") from exc
        finally:
            await resp.aclose()
        _raise_for_error(resp)
        return resp

"""Error shape shared with the wrapper: {"error": {"code", "message"}}."""

from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

log = logging.getLogger(__name__)

_CODES = {
    400: "bad_request",
    403: "forbidden",
    404: "not_found",
    409: "conflict",
    422: "invalid_request",
}


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, retry_after: int | None = None):
        self.status = status
        self.code = code
        self.message = message
        self.retry_after = retry_after  # seconds; sent as the Retry-After header and in the body
        super().__init__(message)


def error_body(code: str, message: str, retry_after: int | None = None) -> dict:
    body = {"code": code, "message": message}
    if retry_after is not None:
        body["retry_after"] = retry_after
    return {"error": body}


def error_response(
    status: int,
    code: str,
    message: str,
    headers: dict[str, str] | None = None,
    retry_after: int | None = None,
) -> JSONResponse:
    if retry_after is not None:
        headers = {**(headers or {}), "Retry-After": str(retry_after)}
    return JSONResponse(error_body(code, message, retry_after), status_code=status, headers=headers)


def not_found(what: str) -> ApiError:
    return ApiError(404, "not_found", f"{what} not found")


def install(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def _api(_: Request, exc: ApiError):
        return error_response(exc.status, exc.code, exc.message, retry_after=exc.retry_after)

    @app.exception_handler(StarletteHTTPException)
    async def _http(_: Request, exc: StarletteHTTPException):
        code = _CODES.get(exc.status_code, "bad_request" if exc.status_code < 500 else "internal")
        return error_response(exc.status_code, code, str(exc.detail), exc.headers)

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError):
        parts = []
        for err in exc.errors():
            loc = ".".join(str(p) for p in err["loc"])
            parts.append(f"{loc}: {err['msg']}")
        return error_response(422, "invalid_request", "; ".join(parts) or "invalid request")

    @app.exception_handler(Exception)
    async def _internal(_: Request, exc: Exception):
        log.exception("unhandled error", exc_info=exc)
        return error_response(500, "internal", "internal error")

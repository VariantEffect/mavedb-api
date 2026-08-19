"""Middleware that converts an uncaught exception into a response the caller can read."""

import logging
import time

from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from mavedb.lib.logging.canonical import log_request
from mavedb.lib.logging.context import (
    correlation_id_for_context,
    format_raised_exception_info_as_dict,
    logging_context,
    save_to_logging_context,
)
from mavedb.lib.slack import send_slack_error

logger = logging.getLogger(__name__)


class CatchAllErrorMiddleware:
    """Turn an uncaught exception into a 500 that the browser is allowed to read.

    Starlette dispatches ``@app.exception_handler(Exception)`` from ``ServerErrorMiddleware``, which sits
    outside the user middleware stack. Its response therefore never passes through ``CORSMiddleware`` and
    carries no ``Access-Control-Allow-Origin``, so a browser rejects it before the client library sees the
    body and the caller is left with an opaque network error. Installing this middleware *inside* the CORS
    layer puts the 500 back under CORS, and inside the context middleware so the correlation id that
    identifies the failure in the logs can be returned to the caller.

    Implemented as pure ASGI rather than ``BaseHTTPMiddleware``: the latter interposes on the response body
    and has a history of breaking ``StreamingResponse``, which the NDJSON export endpoints rely on. Only
    exceptions raised before the response starts are converted — once bytes are on the wire the status is
    already committed, so the exception is re-raised and each stream is responsible for its own
    mid-flight error reporting.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        response_started = False

        async def send_wrapper(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception as err:
            # The status line is already committed; nothing can be salvaged into a 500 here.
            if response_started:
                raise

            save_to_logging_context(format_raised_exception_info_as_dict(err))
            request = Request(scope, receive)
            response = JSONResponse(
                status_code=500,
                content={"detail": "Internal server error", "correlation_id": correlation_id_for_context()},
            )

            try:
                logger.error(msg="Uncaught exception.", extra=logging_context(), exc_info=err)
                send_slack_error(err=err, request=request)
            finally:
                log_request(request, response, time.time_ns())

            await response(scope, receive, send)

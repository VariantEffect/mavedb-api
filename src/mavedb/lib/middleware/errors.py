"""Middleware that converts an uncaught exception into a response the caller can read."""

import logging
import time

from sqlalchemy.exc import DBAPIError
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from mavedb.lib.exception_handlers.handlers import REPORTED_SCOPE_KEY, alerts, unwrap_started_response_error
from mavedb.lib.logging.canonical import log_request
from mavedb.lib.logging.context import (
    correlation_id_for_context,
    logging_context,
    record_exception,
)
from mavedb.lib.slack import send_slack_error

logger = logging.getLogger(__name__)

CONNECTION_LOST_RETRY_AFTER_SECONDS = 5
"""The ``Retry-After`` on a 503 for a request whose database connection the server dropped."""

_CONNECTION_LOST = "The database connection was lost. Please try again shortly."
"""The detail of a 503 for a request whose database connection the server dropped."""


def _uncaught_response(err: Exception) -> JSONResponse:
    """A 503 a client can retry when the server dropped the connection, and a 500 for anything else."""
    correlation_id = correlation_id_for_context()
    # A restart, failover or the idle-in-transaction limit ends the session, often with no SQLSTATE, so the flag marks it.
    if isinstance(err, DBAPIError) and err.connection_invalidated:
        return JSONResponse(
            status_code=503,
            content={"detail": _CONNECTION_LOST, "correlation_id": correlation_id},
            headers={"Retry-After": str(CONNECTION_LOST_RETRY_AFTER_SECONDS)},
        )
    return JSONResponse(status_code=500, content={"detail": "Internal server error", "correlation_id": correlation_id})


class CatchAllErrorMiddleware:
    """Turn an exception no handler in ``ERROR_RESPONSES`` answers into a 500 the browser is allowed to read.

    A database connection the server dropped becomes a 503 with ``Retry-After`` instead, since a retry gets a
    fresh connection.

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
    mid-flight error reporting. It is still logged and alerted here, while the request's logging context
    and correlation id are open: the app-level handler that sees the exception next runs outside them.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        response_started = False
        started_status = 500

        async def send_wrapper(message: Message) -> None:
            nonlocal response_started, started_status
            if message["type"] == "http.response.start":
                response_started = True
                started_status = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception as err:
            # The status line is already committed; nothing can be salvaged into a 500 here.
            if response_started:
                original = unwrap_started_response_error(err)
                record_exception(original)
                request = Request(scope, receive)
                try:
                    if alerts(original):
                        logger.error(msg="Exception after the response started.", extra=logging_context(), exc_info=err)
                        send_slack_error(err=original, request=request)
                    else:
                        logger.warning(
                            msg="Exception after the response started.", extra=logging_context(), exc_info=err
                        )
                finally:
                    log_request(request, Response(status_code=started_status), time.time_ns())
                    scope[REPORTED_SCOPE_KEY] = True
                raise

            record_exception(err)
            request = Request(scope, receive)
            response = _uncaught_response(err)

            # A dropped connection still alerts, unlike the database timeouts: it can also mean an outage.
            try:
                logger.error(msg="Uncaught exception.", extra=logging_context(), exc_info=err)
                send_slack_error(err=err, request=request)
            finally:
                log_request(request, response, time.time_ns())

            await response(scope, receive, send)

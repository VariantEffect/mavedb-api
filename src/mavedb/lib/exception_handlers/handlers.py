"""Serve every domain's error table through one handler, and register them on the app."""

import logging
import time
from typing import Optional

from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from starlette.requests import Request
from starlette.responses import JSONResponse

from mavedb.lib.exception_handlers.database import DATABASE_ERROR_RESPONSES
from mavedb.lib.exception_handlers.external_services import EXTERNAL_SERVICE_ERROR_RESPONSES
from mavedb.lib.exception_handlers.permissions import PERMISSION_ERROR_RESPONSES
from mavedb.lib.exception_handlers.responses import ErrorResponse, ErrorResponses, merge_error_responses
from mavedb.lib.exception_handlers.score_sets import SCORE_SET_ERROR_RESPONSES
from mavedb.lib.exception_handlers.validation import VALIDATION_ERROR_RESPONSES
from mavedb.lib.logging.canonical import log_request
from mavedb.lib.logging.context import logging_context, record_exception
from mavedb.lib.slack import send_slack_error

logger = logging.getLogger(__name__)

ERROR_RESPONSES: ErrorResponses = merge_error_responses(
    PERMISSION_ERROR_RESPONSES,
    VALIDATION_ERROR_RESPONSES,
    DATABASE_ERROR_RESPONSES,
    EXTERNAL_SERVICE_ERROR_RESPONSES,
    SCORE_SET_ERROR_RESPONSES,
)
"""How the api answers each exception it doesn't leave to the 500 path, from every domain's table."""

REPORTED_SCOPE_KEY = "mavedb.error_reported"
"""Set on the ASGI scope once ``CatchAllErrorMiddleware`` has logged and alerted a request's exception."""

# Starlette raises this, from the original, when an exception with a handler is raised after the response
# started, as when a stream fails partway. The message is the only thing that marks it.
_STARTED_RESPONSE_MESSAGE = "Caught handled exception, but response already started."


def _entry_for(exc: BaseException) -> Optional[ErrorResponse]:
    return next((ERROR_RESPONSES[cls] for cls in type(exc).__mro__ if cls in ERROR_RESPONSES), None)


def error_response_for(exc: Exception) -> ErrorResponse:
    """The entry for ``exc``'s most specific class in the table, as Starlette picks its handler."""
    entry = _entry_for(exc)
    if entry is None:
        raise KeyError(f"{type(exc).__name__} has no entry in ERROR_RESPONSES.")
    return entry


def unwrap_started_response_error(err: BaseException) -> BaseException:
    """The exception Starlette wrapped because it was raised after the response started, or ``err`` itself."""
    if isinstance(err, RuntimeError) and err.__cause__ is not None and str(err) == _STARTED_RESPONSE_MESSAGE:
        return err.__cause__
    return err


def alerts(exc: BaseException) -> bool:
    """Whether ``exc`` alerts Slack: as its table entry says if it's mapped, and always if it isn't."""
    entry = _entry_for(exc)
    return entry is None or entry.alert


def handle_mapped_exception(request: Request, exc: Exception) -> JSONResponse:
    entry = error_response_for(exc)
    record_exception(exc)
    response = JSONResponse(
        status_code=entry.status_for(exc), content=jsonable_encoder(entry.body(exc)), headers=entry.headers
    )

    try:
        if entry.alert:
            logger.error(msg="Alerting exception.", extra=logging_context(), exc_info=exc)
            send_slack_error(err=exc, request=request)
    finally:
        log_request(request, response, time.time_ns())

    return response


def handle_uncaught_exception(request: Request, err: Exception) -> JSONResponse:
    """The 500 for an exception raised outside ``CatchAllErrorMiddleware``, which handles the rest."""
    response = JSONResponse(status_code=500, content={"message": "Internal server error"})
    # The middleware already reported it from inside the request's logging context, with its correlation id.
    if request.scope.get(REPORTED_SCOPE_KEY):
        return response

    record_exception(err)

    try:
        logger.error(msg="Uncaught exception.", extra=logging_context(), exc_info=err)
        send_slack_error(err=err, request=request)
    finally:
        log_request(request, response, time.time_ns())

    return response


def install_exception_handlers(app: FastAPI) -> None:
    for exc_type in ERROR_RESPONSES:
        app.add_exception_handler(exc_type, handle_mapped_exception)
    app.add_exception_handler(Exception, handle_uncaught_exception)

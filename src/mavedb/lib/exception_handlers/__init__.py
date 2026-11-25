"""Answer exceptions with HTTP responses, from per-domain tables served by one handler.

To answer an exception with anything but a plain 500, map it in its domain's table, or add a module with a
new table and merge it into ``ERROR_RESPONSES`` in ``handlers.py``. ``install_exception_handlers``
registers them all on the app.
"""

from mavedb.lib.exception_handlers.handlers import (
    ERROR_RESPONSES,
    error_response_for,
    handle_mapped_exception,
    handle_uncaught_exception,
    install_exception_handlers,
)
from mavedb.lib.exception_handlers.responses import ErrorResponse, merge_error_responses

__all__ = [
    "ERROR_RESPONSES",
    "ErrorResponse",
    "error_response_for",
    "handle_mapped_exception",
    "handle_uncaught_exception",
    "install_exception_handlers",
    "merge_error_responses",
]

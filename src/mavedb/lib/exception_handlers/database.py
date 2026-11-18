"""How the api answers database errors."""

from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.orm.exc import StaleDataError

from mavedb.db.errors import DatabaseBusy, MissingGrant, RowSecurityViolation
from mavedb.lib.exception_handlers.responses import ErrorResponse, ErrorResponses, attributable

# Shared by the statement, lock and pool timeouts, which a caller can't tell apart or act on differently.
_BUSY = "The database is busy. Please try again shortly."
"""The detail of a 503 for a database too busy to answer."""

DATABASE_ERROR_RESPONSES: ErrorResponses = {
    # Python allowed this write, so the caller can see the row and a 403 reveals nothing. An upsert onto a hidden
    # row also lands here and would reveal it, so map that form to 404 before a route upserts under row security.
    RowSecurityViolation: ErrorResponse(
        status=403, body=attributable("You don't have permission to make this change."), alert=True
    ),
    MissingGrant: ErrorResponse(status=500, body=attributable("Internal server error"), alert=True),
    # A write Python allowed but a policy hid matches no row and lands here, the same bug a rejected write alerts on.
    StaleDataError: ErrorResponse(
        status=404,
        body=attributable("The record was not found. It may have been changed or deleted."),
        alert=True,
    ),
    # These arrive in bursts under load, and the request log counts them, so they don't alert.
    DatabaseBusy: ErrorResponse(status=503, body=attributable(_BUSY), alert=False),
    PoolTimeoutError: ErrorResponse(status=503, body=attributable(_BUSY), alert=False),
}
"""How the api answers each database error it doesn't leave to the 500 path."""

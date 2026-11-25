"""How the api answers a request the caller isn't allowed to make."""

from mavedb.lib.exception_handlers.responses import ErrorResponse, ErrorResponses
from mavedb.lib.permissions.exceptions import PermissionException

PERMISSION_ERROR_RESPONSES: ErrorResponses = {
    PermissionException: ErrorResponse(
        status=lambda exc: exc.http_code, body=lambda exc: {"detail": exc.message}, alert=False
    ),
}
"""How the api answers a failed permission check, with the status the check chose."""

"""How the api answers errors in score set requests."""

from mavedb.lib.exception_handlers.responses import ErrorResponse, ErrorResponses, message
from mavedb.lib.exceptions import MixedTargetError

SCORE_SET_ERROR_RESPONSES: ErrorResponses = {
    MixedTargetError: ErrorResponse(status=400, body=message, alert=False),
}
"""How the api answers a score set request it can't carry out."""

"""How the api answers a request whose parameters or body fail validation."""

from typing import Any

from fastapi.exceptions import RequestValidationError

from mavedb.lib.exception_handlers.responses import ErrorResponse, ErrorResponses


def _customize_validation_error(error: dict[str, Any]) -> dict[str, Any]:
    # surface custom validation loc context
    if error.get("ctx", {}).get("custom_loc"):
        error = {"loc": error["ctx"]["custom_loc"], "msg": error["msg"], "type": error["type"]}

    if error["type"] == "type_error.none.not_allowed":
        return {"loc": error["loc"], "msg": "Required", "type": error["type"]}
    return error


def _validation_errors(exc: RequestValidationError) -> dict[str, Any]:
    return {"detail": [_customize_validation_error(error) for error in exc.errors()]}


VALIDATION_ERROR_RESPONSES: ErrorResponses = {
    RequestValidationError: ErrorResponse(status=422, body=_validation_errors, alert=False),
}
"""How the api answers a request that fails validation."""

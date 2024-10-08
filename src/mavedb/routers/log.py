from typing import Any

from fastapi import APIRouter

from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import logging_context, save_to_logging_context

router = APIRouter(
    prefix="/api/v1/log",
    tags=["log"],
    responses={404: {"description": "Not found"}},
    route_class=LoggedRoute,
)


# NOTE: Despite not containing any calls to a logger, this route will log posted context
#       by nature of its inheritance from LoggedRoute.
@router.post("/", status_code=200, response_model=str, responses={404: {}})
def log_it(log_context: dict) -> Any:
    """
    Log an interaction.
    """
    # Overwrites middleware generated context with context from POST request. It
    # may be that the posted log context contains some colliding state (which we
    # should treat preferentially, so as to retain any previously generated state)
    # to that generated by the middleware.
    ctx = {**logging_context(), **log_context}
    ctx = save_to_logging_context(ctx)

    return ctx.get("X-Correlation-ID")

from typing import Any

from fastapi import APIRouter

from mavedb import __project__, __version__
from mavedb.view_models.log_request import LogRequest

import logging

router = APIRouter(prefix="/api/v1/log", tags=["log"], responses={404: {"description": "Not found"}})

logger = logging.getLogger(__name__)


@router.post("/log", status_code=200, responses={400: {"description": "Your problem!"}, 500: {"description": "My problem!"}})
def log(*, request: LogRequest) -> Any:
    """
    Log message body.
    """
    logger.error(request.content)

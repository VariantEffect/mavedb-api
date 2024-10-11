from typing import Any

from fastapi import APIRouter

from mavedb import __project__, __version__
from mavedb.view_models import api_version

router = APIRouter(prefix="/api/v1/api", tags=["api information"], responses={404: {"description": "Not found"}})


@router.get("/version", status_code=200, response_model=api_version.ApiVersion, responses={404: {}})
def show_version() -> Any:
    """
    Describe the API version.
    """

    return api_version.ApiVersion(name=__project__, version=__version__)

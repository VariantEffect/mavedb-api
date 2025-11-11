from typing import Any

from fastapi import APIRouter

from mavedb import __project__, __version__
from mavedb.routers.shared import PUBLIC_ERROR_RESPONSES, ROUTER_BASE_PREFIX
from mavedb.view_models import api_version

TAG_NAME = "API Information"

router = APIRouter(
    prefix=f"{ROUTER_BASE_PREFIX}/api",
    tags=[TAG_NAME],
    responses={**PUBLIC_ERROR_RESPONSES},
)

metadata = {
    "name": TAG_NAME,
    "description": "Retrieve information about the MaveDB API.",
}


@router.get("/version", status_code=200, response_model=api_version.ApiVersion, summary="Show API version")
def show_version() -> Any:
    """
    Describe the API version and project.
    """

    return api_version.ApiVersion(name=__project__, version=__version__)

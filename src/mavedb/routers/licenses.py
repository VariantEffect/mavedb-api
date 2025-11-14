from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.models.license import License
from mavedb.routers.shared import PUBLIC_ERROR_RESPONSES, ROUTER_BASE_PREFIX
from mavedb.view_models import license

TAG_NAME = "Licenses"

router = APIRouter(
    prefix=f"{ROUTER_BASE_PREFIX}/licenses",
    tags=[TAG_NAME],
    responses={**PUBLIC_ERROR_RESPONSES},
)

metadata = {
    "name": TAG_NAME,
    "description": "Retrieve information about licenses supported by MaveDB.",
    "externalDocs": {
        "description": "Licenses Documentation",
        "url": "https://mavedb.org/docs/mavedb/data_licensing.html",
    },
}


@router.get("/", status_code=200, response_model=List[license.ShortLicense], summary="List all licenses")
def list_licenses(
    *,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    List all supported licenses.
    """

    items = db.query(License).order_by(License.short_name).all()
    return items


@router.get("/active", status_code=200, response_model=List[license.ShortLicense], summary="List active licenses")
def list_active_licenses(
    *,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    List all active licenses.
    """

    items = db.query(License).where(License.active.is_(True)).order_by(License.short_name).all()
    return items


@router.get("/{item_id}", status_code=200, response_model=license.License, summary="Fetch license by ID")
def fetch_license(
    *,
    item_id: int,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    Fetch a single license by ID.
    """

    item = db.query(License).filter(License.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail=f"License with ID {item_id} not found")
    return item

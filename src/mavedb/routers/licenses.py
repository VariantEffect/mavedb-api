from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.models.license import License
from mavedb.view_models import license

router = APIRouter(prefix="/api/v1/licenses", tags=["licenses"], responses={404: {"description": "Not found"}})


@router.get("/", status_code=200, response_model=List[license.ShortLicense], responses={404: {}})
def list_licenses(
    *,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    List licenses.
    """

    items = db.query(License).order_by(License.short_name).all()
    return items


@router.get("/active", status_code=200, response_model=List[license.ShortLicense], responses={404: {}})
def list_active_licenses(
    *,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    List active licenses.
    """

    items = db.query(License).where(License.active.is_(True)).order_by(License.short_name).all()
    return items


@router.get("/{item_id}", status_code=200, response_model=license.License, responses={404: {}})
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

from fastapi import APIRouter, Depends
from fastapi.exceptions import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func

from mavedb import deps

from mavedb.models.controlled_keyword import ControlledKeyword
from mavedb.view_models import keyword


router = APIRouter(
    prefix="/api/v1/controlled-keywords", tags=["controlled-keywords"], responses={404: {"description": "Not found"}}
)

@router.get(
    "/{key}",
    status_code=200,
    response_model=list[keyword.Keyword],
    responses={404: {}},
    response_model_exclude_none=True,
)
def fetch_keywords_by_key(
    *,
    key: str,
    db: Session = Depends(deps.get_db),
) -> ControlledKeyword:
    """
    Fetch keywords by category.
    """
    lower_key = key.lower()
    items = db.query(ControlledKeyword).filter(func.lower(ControlledKeyword.key) == lower_key).order_by(ControlledKeyword.value).all()
    if not items:
        raise HTTPException(status_code=404, detail=f"Controlled keywords with key {key} not found")
    return items
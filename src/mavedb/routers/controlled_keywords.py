from fastapi import APIRouter, Depends
from fastapi.exceptions import HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.keywords import search_keyword as _search_keyword
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
) -> list[ControlledKeyword]:
    """
    Fetch keywords by category.
    """
    lower_key = key.lower()
    items = (
        db.query(ControlledKeyword)
        .filter(func.lower(ControlledKeyword.key) == lower_key)
        .order_by(ControlledKeyword.value)
        .all()
    )
    if not items:
        raise HTTPException(status_code=404, detail=f"Controlled keywords with key {key} not found")
    return items


@router.post("/search/{key}/{value}", status_code=200, response_model=keyword.Keyword)
def search_keyword_by_key_and_value(key: str, value: str, db: Session = Depends(deps.get_db)) -> ControlledKeyword:
    """
    Search keywords.
    """
    return _search_keyword(db, key, value)

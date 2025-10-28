from fastapi import APIRouter, Depends
from fastapi.exceptions import HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.keywords import search_keyword as _search_keyword
from mavedb.models.controlled_keyword import ControlledKeyword
from mavedb.routers.shared import PUBLIC_ERROR_RESPONSES, ROUTER_BASE_PREFIX
from mavedb.view_models import keyword

router = APIRouter(
    prefix=f"{ROUTER_BASE_PREFIX}/controlled-keywords",
    tags=["Controlled Keywords"],
    responses={**PUBLIC_ERROR_RESPONSES},
)


@router.get(
    "/{key}",
    status_code=200,
    response_model=list[keyword.Keyword],
    response_model_exclude_none=True,
    summary="Fetch keywords by category",
)
def fetch_keywords_by_key(
    *,
    key: str,
    db: Session = Depends(deps.get_db),
) -> list[ControlledKeyword]:
    """
    Fetch the controlled keywords for a given key.
    """
    lower_key = key.lower()
    items = (
        db.query(ControlledKeyword)
        .filter(func.lower(ControlledKeyword.key) == lower_key)
        .order_by(ControlledKeyword.label)
        .all()
    )
    if not items:
        raise HTTPException(status_code=404, detail=f"Controlled keywords with key {key} not found")
    return items


@router.post(
    "/search/{key}/{value}", status_code=200, response_model=keyword.Keyword, summary="Search keyword by key and value"
)
def search_keyword_by_key_and_value(key: str, label: str, db: Session = Depends(deps.get_db)) -> ControlledKeyword:
    """
    Search controlled keywords by key and label.
    """
    return _search_keyword(db, key, label)

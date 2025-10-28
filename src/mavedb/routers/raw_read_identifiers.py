from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.models.raw_read_identifier import RawReadIdentifier
from mavedb.routers.shared import BASE_400_RESPONSE, PUBLIC_ERROR_RESPONSES, ROUTER_BASE_PREFIX
from mavedb.view_models import raw_read_identifier
from mavedb.view_models.search import TextSearch

router = APIRouter(
    prefix=f"{ROUTER_BASE_PREFIX}/raw-read-identifiers",
    tags=["Raw Read Identifiers"],
    responses={**PUBLIC_ERROR_RESPONSES},
)


@router.post(
    "/search",
    status_code=200,
    response_model=List[raw_read_identifier.RawReadIdentifier],
    responses={**BASE_400_RESPONSE},
    summary="Search Raw Read identifiers",
)
def search_raw_read_identifiers(search: TextSearch, db: Session = Depends(deps.get_db)) -> Any:
    """
    Search Raw Read identifiers.
    """

    query = db.query(RawReadIdentifier)

    if search.text and len(search.text.strip()) > 0:
        lower_search_text = search.text.strip().lower()
        query = query.filter(func.lower(RawReadIdentifier.identifier).contains(lower_search_text))
    else:
        raise HTTPException(status_code=400, detail="Search text is required")

    items = query.order_by(RawReadIdentifier.identifier).limit(50).all()
    if not items:
        items = []
    return items

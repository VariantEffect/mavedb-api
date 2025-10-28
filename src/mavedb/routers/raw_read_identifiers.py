from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.models.raw_read_identifier import RawReadIdentifier
from mavedb.view_models import raw_read_identifier
from mavedb.view_models.search import TextSearch

router = APIRouter(
    prefix="/api/v1/raw-read-identifiers",
    tags=["Raw Read Identifiers"],
    responses={
        404: {"description": "Not found"},
        500: {"description": "Internal server error"},
    },
)


@router.post(
    "/search",
    status_code=200,
    response_model=List[raw_read_identifier.RawReadIdentifier],
    responses={
        400: {"description": "Bad request"},
    },
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

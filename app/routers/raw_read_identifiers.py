from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

import app.view_models.raw_read_identifier
from app import deps
from app.models.raw_read_identifier import RawReadIdentifier
from app.view_models.search import TextSearch

router = APIRouter(
    prefix="/api/v1/rawReadIdentifiers",
    tags=["Raw read identifiers"],
    responses={404: {"description": "Not found"}}
)


@router.post(
    '/search',
    status_code=200,
    response_model=List[app.view_models.raw_read_identifier.RawReadIdentifier]
)
def search_raw_read_identifiers(
        search: TextSearch,
        db: Session = Depends(deps.get_db)
) -> Any:
    """
    Search Raw Read identifiers.
    """

    query = db.query(RawReadIdentifier)

    if search.text and len(search.text.strip()) > 0:
        lower_search_text = search.text.strip().lower()
        query = query.filter(func.lower(RawReadIdentifier.identifier).contains(lower_search_text))
    else:
        raise HTTPException(status_code=500, detail='Search text is required')

    items = query.order_by(RawReadIdentifier.identifier) \
        .limit(50) \
        .all()
    if not items:
        items = []
    return items

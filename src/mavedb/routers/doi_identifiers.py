from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.models.doi_identifier import DoiIdentifier
from mavedb.view_models import doi_identifier
from mavedb.view_models.search import TextSearch

router = APIRouter(
    prefix="/api/v1/doi-identifiers", tags=["DOI identifiers"], responses={404: {"description": "Not found"}}
)


@router.post("/search", status_code=200, response_model=List[doi_identifier.DoiIdentifier])
def search_doi_identifiers(search: TextSearch, db: Session = Depends(deps.get_db)) -> Any:
    """
    Search DOI identifiers.
    """

    query = db.query(DoiIdentifier)

    if search.text and len(search.text.strip()) > 0:
        lower_search_text = search.text.strip().lower()
        query = query.filter(func.lower(DoiIdentifier.identifier).contains(lower_search_text))
    else:
        raise HTTPException(status_code=500, detail="Search text is required")

    items = query.order_by(DoiIdentifier.identifier).limit(50).all()
    if not items:
        items = []
    return items

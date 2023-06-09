from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.models.publication_identifier import PublicationIdentifier
from mavedb.view_models import publication_identifier
from mavedb.view_models.search import TextSearch

router = APIRouter(
    prefix="/api/v1/publicationIdentifiers", tags=["PubMed identifiers"], responses={404: {"description": "Not found"}}
)


@router.post("/search", status_code=200, response_model=List[publication_identifier.PublicationIdentifier])
def search_publication_identifiers(search: TextSearch, db: Session = Depends(deps.get_db)) -> Any:
    """
    Search PubMed identifiers.
    """

    query = db.query(PublicationIdentifier)

    if search.text and len(search.text.strip()) > 0:
        lower_search_text = search.text.strip().lower()
        query = query.filter(func.lower(PublicationIdentifier.identifier).contains(lower_search_text))
    else:
        raise HTTPException(status_code=500, detail="Search text is required")

    items = query.order_by(PublicationIdentifier.identifier).limit(50).all()
    if not items:
        items = []
    return items

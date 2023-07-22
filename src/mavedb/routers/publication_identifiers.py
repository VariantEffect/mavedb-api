from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.identifiers import find_generic_article
from mavedb.models.publication_identifier import PublicationIdentifier
from mavedb.models.user import User
from mavedb.view_models import publication_identifier
from mavedb.view_models.search import TextSearch

router = APIRouter(
    prefix="/api/v1/publication-identifiers",
    tags=["publication identifiers"],
    responses={404: {"description": "Not found"}},
)


@router.post("/search", status_code=200, response_model=list[publication_identifier.PublicationIdentifier])
def search_publication_identifiers(search: TextSearch, db: Session = Depends(deps.get_db)) -> Any:
    """
    Search publication identifiers.
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


@router.post(
    "/search-external", status_code=200, response_model=List[publication_identifier.ExternalPublicationIdentifier]
)
async def search_external_publication_identifiers(search: TextSearch, db: Session = Depends(deps.get_db)) -> Any:
    """
    Search external publication identifiers.
    """

    if search.text and len(search.text.strip()) > 0:
        lower_search_text = search.text.strip().lower()
        items = await find_generic_article(db, lower_search_text)
    else:
        raise HTTPException(status_code=500, detail="Search text is required")

    if not any(items.values()):
        return []
    return list(items.values())

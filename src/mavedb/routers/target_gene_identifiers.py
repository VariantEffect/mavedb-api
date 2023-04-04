from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.view_models.search import TextSearch
from mavedb.view_models import external_gene_identifier
from mavedb.lib.identifiers import EXTERNAL_GENE_IDENTIFIER_CLASSES

router = APIRouter(
    prefix="/api/v1/targetGeneIdentifiers",
    tags=["target gene identifiers"],
    responses={404: {"description": "Not found"}},
)


@router.post("/search", status_code=200, response_model=List[external_gene_identifier.ExternalGeneIdentifier])
def search_target_gene_identifiers(db_name: str, search: TextSearch, db: Session = Depends(deps.get_db)) -> Any:
    """
    Search target gene identifiers.
    """

    identifier_class = EXTERNAL_GENE_IDENTIFIER_CLASSES[db_name]
    query = db.query(identifier_class)

    if search.text and len(search.text.strip()) > 0:
        lower_search_text = search.text.strip().lower()
        query = query.filter(func.lower(identifier_class.identifier).contains(lower_search_text))
    else:
        raise HTTPException(status_code=500, detail="Search text is required")

    items = query.order_by(identifier_class.identifier).limit(50).all()
    if not items:
        items = []
    return items

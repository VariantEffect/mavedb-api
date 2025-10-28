from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.identifiers import EXTERNAL_GENE_IDENTIFIER_CLASSES
from mavedb.routers.shared import BASE_400_RESPONSE, PUBLIC_ERROR_RESPONSES, ROUTER_BASE_PREFIX
from mavedb.view_models import external_gene_identifier
from mavedb.view_models.search import TextSearch

router = APIRouter(
    prefix=f"{ROUTER_BASE_PREFIX}/target-gene-identifiers",
    tags=["Target Gene Identifiers"],
    responses={**PUBLIC_ERROR_RESPONSES},
)


@router.post(
    "/search",
    status_code=200,
    response_model=List[external_gene_identifier.ExternalGeneIdentifier],
    summary="Search target gene identifiers",
    responses={**BASE_400_RESPONSE},
)
def search_target_gene_identifiers(db_name: str, search: TextSearch, db: Session = Depends(deps.get_db)) -> Any:
    """
    Search target gene identifiers.
    """
    if db_name not in EXTERNAL_GENE_IDENTIFIER_CLASSES:
        raise HTTPException(
            status_code=422,
            detail=f"Unexpected db_name: {db_name}. Expected one of: {list(EXTERNAL_GENE_IDENTIFIER_CLASSES.keys())}",
        )

    identifier_class = EXTERNAL_GENE_IDENTIFIER_CLASSES[db_name]
    assert hasattr(identifier_class, "identifier")

    query = db.query(identifier_class)

    if search.text and len(search.text.strip()) > 0:
        lower_search_text = search.text.strip().lower()
        query = query.filter(func.lower(identifier_class.identifier).contains(lower_search_text))
    else:
        raise HTTPException(status_code=400, detail="Search text is required")

    items = query.order_by(identifier_class.identifier).limit(50).all()
    if not items:
        items = []
    return items

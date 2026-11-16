from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.authorization import get_principal
from mavedb.lib.identifier_search import readable_record_loads, search_identifiers_on_readable_records
from mavedb.lib.permissions.principal import Principal
from mavedb.lib.permissions.score_set import ScoreSetViewer
from mavedb.lib.validation.constants.identifier import valid_dbnames
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_gene_external_identifier import TargetGeneExternalIdentifier
from mavedb.routers.shared import BASE_400_RESPONSE, PUBLIC_ERROR_RESPONSES, ROUTER_BASE_PREFIX
from mavedb.view_models import external_gene_identifier
from mavedb.view_models.search import TextSearch

TAG_NAME = "Target Gene Identifiers"

router = APIRouter(
    prefix=f"{ROUTER_BASE_PREFIX}/target-gene-identifiers",
    tags=[TAG_NAME],
    responses={**PUBLIC_ERROR_RESPONSES},
)

metadata = {
    "name": TAG_NAME,
    "description": "Search and retrieve target gene identifiers associated with MaveDB records.",
}


@router.post(
    "/search",
    status_code=200,
    response_model=List[external_gene_identifier.ExternalGeneIdentifier],
    summary="Search target gene identifiers",
    responses={**BASE_400_RESPONSE},
)
def search_target_gene_identifiers(
    db_name: str,
    search: TextSearch,
    db: Session = Depends(deps.get_db),
    principal: Principal = Depends(get_principal),
) -> Any:
    """
    Search the target gene identifiers of score sets you can read.
    """
    if db_name not in valid_dbnames:
        raise HTTPException(
            status_code=422,
            detail=f"Unexpected db_name: {db_name}. Expected one of: {valid_dbnames}",
        )

    if not search.text or not search.text.strip():
        raise HTTPException(status_code=400, detail="Search text is required")

    return search_identifiers_on_readable_records(
        db,
        principal,
        search.text.strip(),
        [
            (
                TargetGeneExternalIdentifier,
                ScoreSetViewer,
                select(TargetGeneExternalIdentifier, ScoreSet)
                .join(TargetGeneExternalIdentifier.target_gene)
                .join(TargetGene.score_set)
                .where(TargetGeneExternalIdentifier.db_name == db_name)
                .options(*readable_record_loads(ScoreSet)),
            )
        ],
    )

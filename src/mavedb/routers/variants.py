import logging

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.logging import LoggedRoute
from mavedb.models.variant import Variant
from mavedb.view_models.variant import ClingenAlleleIdVariantLookupsRequest, VariantWithShortScoreSet

router = APIRouter(
    prefix="/api/v1", tags=["access keys"], responses={404: {"description": "Not found"}}, route_class=LoggedRoute
)

logger = logging.getLogger(__name__)


@router.post("/variants/clingen-allele-id-lookups", response_model=list[list[VariantWithShortScoreSet]])
def get_variants(*, request: ClingenAlleleIdVariantLookupsRequest, db: Session = Depends(deps.get_db)):
    variants = db.query(Variant).filter(Variant.clingen_allele_id.in_(request.clingen_allele_ids)).all()
    
    variants_by_allele_id: dict[str, list[Variant]] = {allele_id: [] for allele_id in request.clingen_allele_ids}
    for variant in variants:
        variants_by_allele_id[variant.clingen_allele_id].append(variant)
    
    return [variants_by_allele_id[allele_id] for allele_id in request.clingen_allele_ids]

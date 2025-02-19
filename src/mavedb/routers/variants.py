import logging

from fastapi import APIRouter, Depends
from fastapi.exceptions import HTTPException
from mavedb.lib.authentication import UserData, get_current_user
from mavedb.lib.permissions import Action, assert_permission, has_permission
from sqlalchemy import select
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.orm import Session, joinedload

from mavedb import deps
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.models.score_set import ScoreSet
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.variant import Variant
from mavedb.view_models.variant import (
    ClingenAlleleIdVariantLookupsRequest,
    VariantWithScoreSet,
    VariantWithShortScoreSet,
)

router = APIRouter(
    prefix="/api/v1", tags=["access keys"], responses={404: {"description": "Not found"}}, route_class=LoggedRoute
)

logger = logging.getLogger(__name__)


@router.post("/variants/clingen-allele-id-lookups", response_model=list[list[VariantWithShortScoreSet]])
def lookup_variants(
    *,
    request: ClingenAlleleIdVariantLookupsRequest,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(get_current_user),
):
    variants = db.execute(
        select(Variant, MappedVariant.clingen_allele_id)
        .join(MappedVariant)
        .options(joinedload(Variant.score_set).joinedload(ScoreSet.experiment))
        .where(MappedVariant.clingen_allele_id.in_(request.clingen_allele_ids))
    ).all()

    variants_by_allele_id: dict[str, list[Variant]] = {allele_id: [] for allele_id in request.clingen_allele_ids}

    for variant, allele_id in variants:
        if has_permission(user_data, variant.score_set, Action.READ).permitted:
            variants_by_allele_id[allele_id].append(variant)

    return [variants_by_allele_id[allele_id] for allele_id in request.clingen_allele_ids]


@router.get(
    "/variants/{urn}",
    status_code=200,
    response_model=VariantWithScoreSet,
    responses={404: {}, 500: {}},
    response_model_exclude_none=True,
)
def get_variant(*, urn: str, db: Session = Depends(deps.get_db), user_data: UserData = Depends(get_current_user)):
    """
    Fetch a single variant by URN.
    """
    save_to_logging_context({"requested_resource": urn})
    try:
        query = db.query(Variant).filter(Variant.urn == urn)
        variant = query.one_or_none()
    except MultipleResultsFound:
        logger.info(
            msg="Could not fetch the requested score set; Multiple such variants exist.", extra=logging_context()
        )
        raise HTTPException(status_code=500, detail=f"multiple variants with URN '{urn}' were found")

    if not variant:
        logger.info(msg="Could not fetch the requested variant; No such variant exists.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"variant with URN '{urn}' not found")

    assert_permission(user_data, variant.score_set, Action.READ)
    return variant

import logging
from typing import Annotated, Any, Optional

from fastapi import APIRouter, Depends, Path
from fastapi.exceptions import HTTPException
from ga4gh.core.identifiers import GA4GH_IR_REGEXP
from ga4gh.va_spec.base.core import ExperimentalVariantFunctionalImpactStudyResult, Statement
from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
from sqlalchemy import or_, select
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.annotation.annotate import (
    variant_study_result,
    variant_functional_impact_statement,
    variant_pathogenicity_evidence,
)
from mavedb.lib.annotation.exceptions import MappingDataDoesntExistException
from mavedb.lib.authentication import UserData
from mavedb.lib.authorization import get_current_user
from mavedb.lib.permissions import has_permission
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import (
    logging_context,
    save_to_logging_context,
)
from mavedb.lib.permissions import Action, assert_permission
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.variant import Variant
from mavedb.view_models import mapped_variant

logger = logging.getLogger(__name__)


async def fetch_mapped_variant_by_variant_urn(db: Session, user: Optional[UserData], urn: str) -> MappedVariant:
    """
    We may combine this function back to show_mapped_variant if none of any new function call it.
    Fetch one mapped variant by variant URN.

    :param db: An active database session.
    :param urn: The variant URN.
    :return: The mapped variant.

    :raises HTTPException: If the mapped variant is not found or if multiple variants are found.
    """
    try:
        item = (
            db.execute(select(MappedVariant).join(Variant).where(Variant.urn == urn, MappedVariant.current.is_(True)))
            .scalars()
            .one_or_none()
        )
    except MultipleResultsFound:
        logger.info(
            msg="Could not fetch the requested mapped variant; Multiple such variants exist.", extra=logging_context()
        )
        raise HTTPException(status_code=500, detail=f"Multiple variants with URN {urn} were found.")

    if not item:
        logger.info(
            msg="Could not fetch the requested mapped variant; No such mapped variants exist.", extra=logging_context()
        )

        raise HTTPException(status_code=404, detail=f"Mapped variant with URN {urn} not found")

    # Base mapped variant read permission on the score set in which it is contained.
    assert_permission(user, item.variant.score_set, Action.READ)
    return item


router = APIRouter(
    prefix="/api/v1/mapped-variants",
    tags=["mapped variants"],
    responses={404: {"description": "Not found"}},
    route_class=LoggedRoute,
)


@router.get("/{urn}", status_code=200, response_model=mapped_variant.MappedVariant, responses={404: {}, 500: {}})
async def show_mapped_variant(
    *, urn: str, db: Session = Depends(deps.get_db), user: Optional[UserData] = Depends(get_current_user)
) -> Any:
    """
    Fetch a mapped variant by URN.
    """
    save_to_logging_context({"requested_resource": urn})

    return await fetch_mapped_variant_by_variant_urn(db, user, urn)


@router.get(
    "/{urn}/va/study-result",
    status_code=200,
    response_model=ExperimentalVariantFunctionalImpactStudyResult,
    responses={404: {}, 500: {}},
)
async def show_mapped_variant_study_result(
    *, urn: str, db: Session = Depends(deps.get_db), user: Optional[UserData] = Depends(get_current_user)
) -> ExperimentalVariantFunctionalImpactStudyResult:
    """
    Construct a VA-Spec StudyResult from a mapped variant.
    """
    save_to_logging_context({"requested_resource": urn})

    mapped_variant = await fetch_mapped_variant_by_variant_urn(db, user, urn)

    try:
        return variant_study_result(mapped_variant)
    except MappingDataDoesntExistException as e:
        logger.info(
            msg=f"Could not construct a study result for mapped variant {urn}: {e}",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"Could not construct a study result for mapped variant {urn}: {e}")


# TODO#416: For now, this route supports only one statement per mapped variant. Eventually, we should support the possibility of multiple statements.
@router.get("/{urn}/va/functional-impact", status_code=200, response_model=Statement, responses={404: {}, 500: {}})
async def show_mapped_variant_functional_impact_statement(
    *, urn: str, db: Session = Depends(deps.get_db), user: Optional[UserData] = Depends(get_current_user)
) -> Statement:
    """
    Construct a VA-Spec Statement from a mapped variant.
    """
    save_to_logging_context({"requested_resource": urn})

    mapped_variant = await fetch_mapped_variant_by_variant_urn(db, user, urn)

    try:
        functional_impact = variant_functional_impact_statement(mapped_variant)
    except MappingDataDoesntExistException as e:
        logger.info(
            msg="Could not construct a functional impact statement for this mapped variant; No mapping data exists for this score set.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=404, detail=f"Could not construct a functional impact statement for mapped variant {urn}: {e}"
        )

    if not functional_impact:
        logger.info(
            msg="Could not construct a functional impact statement for this mapped variant. Variant does not have sufficient evidence to evaluate its functional impact.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=404,
            detail=f"Could not construct a functional impact statement for mapped variant {urn}. Variant does not have sufficient evidence to evaluate its functional impact.",
        )

    return functional_impact


# TODO#416: For now, this route supports only one evidence line per mapped variant. Eventually, we should support the possibility of multiple evidence lines.
@router.get(
    "/{urn}/va/clinical-evidence",
    status_code=200,
    response_model=VariantPathogenicityEvidenceLine,
    responses={404: {}, 500: {}},
)
async def show_mapped_variant_acmg_evidence_line(
    *, urn: str, db: Session = Depends(deps.get_db), user: Optional[UserData] = Depends(get_current_user)
) -> VariantPathogenicityEvidenceLine:
    """
    Construct a list of VA-Spec EvidenceLine(s) from a mapped variant.
    """
    save_to_logging_context({"requested_resource": urn})

    mapped_variant = await fetch_mapped_variant_by_variant_urn(db, user, urn)

    try:
        pathogenicity_evidence = variant_pathogenicity_evidence(mapped_variant)
    except MappingDataDoesntExistException as e:
        logger.info(
            msg="Could not construct a pathogenicity evidence line for this mapped variant; No mapping data exists for this score set.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=404, detail=f"Could not construct a pathogenicity evidence line for mapped variant {urn}: {e}"
        )

    if not pathogenicity_evidence:
        logger.info(
            msg="Could not construct a pathogenicity evidence line for this mapped variant; Variant does not have sufficient evidence to evaluate its pathogenicity.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=404,
            detail=f"Could not construct a pathogenicity evidence line for mapped variant {urn}; Variant does not have sufficient evidence to evaluate its pathogenicity.",
        )

    return pathogenicity_evidence


@router.get(
    "/vrs/{identifier}",
    status_code=200,
    response_model=list[mapped_variant.MappedVariant],
    responses={404: {}, 500: {}},
)
async def show_mapped_variants_by_identifier(
    *,
    identifier: Annotated[
        str,
        Path(
            description="String, a valid GA4GH digest based identifier.",
            json_schema_extra={"example": "ga4gh:SQ.0123abcd"},
            regex=GA4GH_IR_REGEXP,
        ),
    ],
    only_current: bool = True,
    db: Session = Depends(deps.get_db),
    user: Optional[UserData] = Depends(get_current_user),
) -> list[MappedVariant]:
    """
    Fetch a mapped variant by GA4GH identifier.
    """
    query = select(MappedVariant).where(
        or_(MappedVariant.pre_mapped["id"].astext == identifier, MappedVariant.post_mapped["id"].astext == identifier)
    )

    if only_current:
        query = query.where(MappedVariant.current.is_(True))

    items = db.scalars(query).all()

    permitted_items = [item for item in items if has_permission(user, item.variant.score_set, Action.READ).permitted]
    if not permitted_items:
        raise HTTPException(status_code=404, detail=f"No mapped variants with identifier {identifier} were found")

    return permitted_items


# for testing only
# @router.post("/map/{urn}", status_code=200, responses={404: {}, 500: {}})
# async def map_score_set(*, urn: str, worker: ArqRedis = Depends(deps.get_worker)) -> Any:
#     await worker.lpush(MAPPING_QUEUE_NAME, urn)  # type: ignore
#     await worker.enqueue_job(
#         "variant_mapper_manager",
#         None,
#         None,
#         None
#     )

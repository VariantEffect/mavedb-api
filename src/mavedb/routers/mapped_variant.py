from typing import Any

from fastapi import APIRouter, Depends
from fastapi.exceptions import HTTPException
from ga4gh.va_spec.base.core import ExperimentalVariantFunctionalImpactStudyResult, Statement
from ga4gh.va_spec.acmg_2015 import VariantPathogenicityFunctionalImpactEvidenceLine
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.annotation.annotate import (
    variant_study_result,
    variant_functional_impact_statement,
    variant_pathogenicity_evidence,
)
from mavedb.lib.annotation.exceptions import MappingDataDoesntExistException
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.variant import Variant
from mavedb.view_models import mapped_variant


async def fetch_mapped_variant_by_variant_urn(db, urn: str) -> MappedVariant:
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
            db.query(MappedVariant)
            .filter(Variant.urn == urn)
            .filter(MappedVariant.variant_id == Variant.id)
            .filter(MappedVariant.current)  # filter current is true
            .one_or_none()
        )
    except MultipleResultsFound:
        raise HTTPException(status_code=500, detail=f"Multiple variants with URN {urn} were found.")
    if not item:
        raise HTTPException(status_code=404, detail=f"Mapped variant with URN {urn} not found")
    return item


router = APIRouter(
    prefix="/api/v1/mapped-variants", tags=["mapped variants"], responses={404: {"description": "Not found"}}
)


@router.get("/{urn}", status_code=200, response_model=mapped_variant.MappedVariant, responses={404: {}, 500: {}})
async def show_mapped_variant(*, urn: str, db: Session = Depends(deps.get_db)) -> Any:
    """
    Fetch a mapped variant by URN.
    """

    return await fetch_mapped_variant_by_variant_urn(db, urn)


@router.get(
    "/{urn}/va/study-result",
    status_code=200,
    response_model=ExperimentalVariantFunctionalImpactStudyResult,
    responses={404: {}, 500: {}},
)
async def show_mapped_variant_study_result(
    *, urn: str, db: Session = Depends(deps.get_db)
) -> ExperimentalVariantFunctionalImpactStudyResult:
    """
    Construct a VA-Spec StudyResult from a mapped variant.
    """

    mapped_variant = await fetch_mapped_variant_by_variant_urn(db, urn)

    try:
        return variant_study_result(mapped_variant)
    except MappingDataDoesntExistException as e:
        raise HTTPException(status_code=404, detail=f"Could not construct a study result for mapped variant {urn}: {e}")


# TODO#416: For now, this route supports only one statement per mapped variant. Eventually, we should support the possibility of multiple statements.
@router.get("/{urn}/va/functional-impact", status_code=200, response_model=Statement, responses={404: {}, 500: {}})
async def show_mapped_variant_functional_impact_statement(*, urn: str, db: Session = Depends(deps.get_db)) -> Statement:
    """
    Construct a VA-Spec Statement from a mapped variant.
    """

    mapped_variant = await fetch_mapped_variant_by_variant_urn(db, urn)

    try:
        functional_impact = variant_functional_impact_statement(mapped_variant)
    except MappingDataDoesntExistException as e:
        raise HTTPException(
            status_code=404, detail=f"Could not construct a functional impact statement for mapped variant {urn}: {e}"
        )

    if not functional_impact:
        raise HTTPException(
            status_code=404,
            detail=f"Could not construct a functional impact statement for mapped variant {urn}: No evidence found",
        )

    return functional_impact


# TODO#416: For now, this route supports only one evidence line per mapped variant. Eventually, we should support the possibility of multiple evidence lines.
@router.get(
    "/{urn}/va/clinial-evidence",
    status_code=200,
    response_model=VariantPathogenicityFunctionalImpactEvidenceLine,
    responses={404: {}, 500: {}},
)
async def show_mapped_variant_acmg_evidence_line(
    *, urn: str, db: Session = Depends(deps.get_db)
) -> VariantPathogenicityFunctionalImpactEvidenceLine:
    """
    Construct a list of VA-Spec EvidenceLine(s) from a mapped variant.
    """

    mapped_variant = await fetch_mapped_variant_by_variant_urn(db, urn)

    try:
        pathogenicity_evidence = variant_pathogenicity_evidence(mapped_variant)
    except MappingDataDoesntExistException as e:
        raise HTTPException(
            status_code=404, detail=f"Could not construct a pathogenicity evidence line for mapped variant {urn}: {e}"
        )

    if not pathogenicity_evidence:
        raise HTTPException(
            status_code=404,
            detail=f"Could not construct a pathogenicity evidence line for mapped variant {urn}: No evidence found",
        )

    return pathogenicity_evidence


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

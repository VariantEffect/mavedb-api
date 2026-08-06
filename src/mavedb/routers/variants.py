import itertools
import logging
import re
from typing import Any, List, Optional

from fastapi import APIRouter, Depends, Query
from fastapi.exceptions import HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.sql import or_

from mavedb import deps
from mavedb.lib.authentication import get_current_user
from mavedb.lib.authorization import get_principal
from mavedb.lib.csv.namespaces import CSV_NAMESPACES_PARAM_DESCRIPTION, CsvNamespaceStr
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.permissions.principal import Principal
from mavedb.lib.permissions.score_calibration import ScoreCalibrationViewer
from mavedb.lib.permissions import Action, assert_permission, has_permission
from mavedb.lib.types.authentication import UserData
from mavedb.lib.csv.variant import available_variant_csv_namespaces, get_variant_csv
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.variant_translation import VariantTranslation
from mavedb.routers.shared import (
    ACCESS_CONTROL_ERROR_RESPONSES,
    BASE_400_RESPONSE,
    PUBLIC_ERROR_RESPONSES,
    ROUTER_BASE_PREFIX,
)
from mavedb.view_models.csv_namespace import AvailableCsvNamespace
from mavedb.view_models.variant import (
    ClingenAlleleIdVariantLookupResponse,
    ClingenAlleleIdVariantLookupsRequest,
    VariantEffectMeasurementWithScoreSet,
)

TAG_NAME = "Variants"

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix=f"{ROUTER_BASE_PREFIX}",
    tags=[TAG_NAME],
    responses={**PUBLIC_ERROR_RESPONSES},
    route_class=LoggedRoute,
)

metadata = {
    "name": TAG_NAME,
    "description": "Search and retrieve variants associated with MaveDB records.",
}


@router.post(
    "/variants/clingen-allele-id-lookups",
    status_code=200,
    response_model=list[ClingenAlleleIdVariantLookupResponse],
    responses={
        **BASE_400_RESPONSE,
        **ACCESS_CONTROL_ERROR_RESPONSES,
    },
    summary="Lookup variants by ClinGen Allele IDs",
)
def lookup_variants(
    *,
    request: ClingenAlleleIdVariantLookupsRequest,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(get_current_user),
):
    """
    Lookup variants by ClinGen Allele IDs.
    """
    save_to_logging_context({"requested_resource": "clingen-allele-id-lookups"})
    save_to_logging_context({"clingen_allele_ids_to_lookup": request.clingen_allele_ids})
    logger.debug(msg="Looking up variants by Clingen Allele IDs", extra=logging_context())

    # sort multi-variant components lexicographically, as they are in the database
    request.clingen_allele_ids = [",".join(sorted(allele_id.split(","))) for allele_id in request.clingen_allele_ids]
    exact_match_variants = db.execute(
        select(Variant, MappedVariant.clingen_allele_id)
        .join(MappedVariant)
        .options(joinedload(Variant.score_set).joinedload(ScoreSet.experiment))
        .where(MappedVariant.clingen_allele_id.in_(request.clingen_allele_ids))
        .where(MappedVariant.current == True)  # noqa: E712
    ).all()

    save_to_logging_context({"num_variants_matching_clingen_allele_ids": len(exact_match_variants)})
    logger.debug(msg="Found variants with exactly matching ClinGen Allele IDs", extra=logging_context())

    num_variants_matching_clingen_allele_ids_and_permitted = 0

    variants_by_allele_id: dict[str, dict] = {
        allele_id: {
            "clingen_allele_id": allele_id,
            "exact_match": {"clingen_allele_id": allele_id, "variant_effect_measurements": []},
            "equivalent_nt": [],
            "equivalent_aa": [],
        }
        for allele_id in request.clingen_allele_ids
    }
    for variant, allele_id in exact_match_variants:
        if has_permission(user_data, variant.score_set, Action.READ).permitted:
            num_variants_matching_clingen_allele_ids_and_permitted += 1
            variants_by_allele_id[allele_id]["exact_match"]["variant_effect_measurements"].append(variant)

    save_to_logging_context(
        {"clingen_allele_ids_with_permitted_variants": num_variants_matching_clingen_allele_ids_and_permitted}
    )

    for allele_id in request.clingen_allele_ids:
        if not variants_by_allele_id[allele_id]["exact_match"]["variant_effect_measurements"]:
            variants_by_allele_id[allele_id]["exact_match"] = None

    for allele_id in request.clingen_allele_ids:
        # validate and determine whether multi-variant
        # NOTE: assuming we never have more than 2 components in a multi-variant
        single_clingen_allele_id_re = r"^[CP]A\d+$"
        multi_clingen_allele_id_re = r"^[CP]A\d+,[CP]A\d+$"
        single_or_multi_clingen_allele_id_re = r"^[CP]A\d+(,[CP]A\d+)?$"

        if re.fullmatch(single_or_multi_clingen_allele_id_re, allele_id) is None:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid Clingen Allele ID '{allele_id}'",
            )

        if re.fullmatch(single_clingen_allele_id_re, allele_id):
            if allele_id.startswith("PA"):
                subquery = (
                    db.execute(
                        select(VariantTranslation.nt_clingen_id).where(VariantTranslation.aa_clingen_id == allele_id)
                    )
                    .scalars()
                    .all()
                )
                related_clingen_ids = (
                    db.execute(
                        select(VariantTranslation).where(
                            or_(
                                VariantTranslation.aa_clingen_id == allele_id,
                                VariantTranslation.nt_clingen_id.in_(subquery),
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                # exclude requested clingen allele id from "related_clingen_ids" to avoid duplicates
                related_aa_clingen_ids = [
                    var_translation.aa_clingen_id
                    for var_translation in related_clingen_ids
                    if var_translation.aa_clingen_id != allele_id
                ]
                related_aa_variants = db.execute(
                    select(Variant, MappedVariant.clingen_allele_id)
                    .join(MappedVariant)
                    .options(joinedload(Variant.score_set).joinedload(ScoreSet.experiment))
                    .where(MappedVariant.clingen_allele_id.in_(related_aa_clingen_ids))
                    .where(MappedVariant.current == True)  # noqa: E712
                ).all()
                related_nt_clingen_ids = [var_translation.nt_clingen_id for var_translation in related_clingen_ids]
                related_nt_variants = db.execute(
                    select(Variant, MappedVariant.clingen_allele_id)
                    .join(MappedVariant)
                    .options(joinedload(Variant.score_set).joinedload(ScoreSet.experiment))
                    .where(MappedVariant.clingen_allele_id.in_(related_nt_clingen_ids))
                    .where(MappedVariant.current == True)  # noqa: E712
                ).all()
            elif allele_id.startswith("CA"):
                subquery = (
                    db.execute(
                        select(VariantTranslation.aa_clingen_id).where(VariantTranslation.nt_clingen_id == allele_id)
                    )
                    .scalars()
                    .all()
                )
                related_clingen_ids = (
                    db.execute(
                        select(VariantTranslation).where(
                            or_(
                                VariantTranslation.nt_clingen_id == allele_id,
                                VariantTranslation.aa_clingen_id.in_(subquery),
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                related_aa_clingen_ids = [var_translation.aa_clingen_id for var_translation in related_clingen_ids]
                related_aa_variants = db.execute(
                    select(Variant, MappedVariant.clingen_allele_id)
                    .join(MappedVariant)
                    .options(joinedload(Variant.score_set).joinedload(ScoreSet.experiment))
                    .where(MappedVariant.clingen_allele_id.in_(related_aa_clingen_ids))
                    .where(MappedVariant.current == True)  # noqa: E712
                ).all()
                # exclude requested clingen allele id from "related_clingen_ids" to avoid duplicates
                related_nt_clingen_ids = [
                    var_translation.nt_clingen_id
                    for var_translation in related_clingen_ids
                    if var_translation.nt_clingen_id != allele_id
                ]
                related_nt_variants = db.execute(
                    select(Variant, MappedVariant.clingen_allele_id)
                    .join(MappedVariant)
                    .options(joinedload(Variant.score_set).joinedload(ScoreSet.experiment))
                    .where(MappedVariant.clingen_allele_id.in_(related_nt_clingen_ids))
                    .where(MappedVariant.current == True)  # noqa: E712
                ).all()

            num_variants_matching_clingen_allele_ids_and_permitted = 0
            equivalent_aa_variants = {}
            for variant, related_allele_id in related_aa_variants:
                if has_permission(user_data, variant.score_set, Action.READ).permitted:
                    if related_allele_id not in equivalent_aa_variants:
                        equivalent_aa_variants[related_allele_id] = {
                            "clingen_allele_id": related_allele_id,
                            "variant_effect_measurements": [],
                        }
                    equivalent_aa_variants[related_allele_id]["variant_effect_measurements"].append(variant)

            variants_by_allele_id[allele_id]["equivalent_aa"] = [
                equivalent_aa_variants[related_allele_id] for related_allele_id in equivalent_aa_variants
            ]

            equivalent_nt_variants = {}
            for variant, related_allele_id in related_nt_variants:
                if has_permission(user_data, variant.score_set, Action.READ).permitted:
                    if related_allele_id not in equivalent_nt_variants:
                        equivalent_nt_variants[related_allele_id] = {
                            "clingen_allele_id": related_allele_id,
                            "variant_effect_measurements": [],
                        }
                    equivalent_nt_variants[related_allele_id]["variant_effect_measurements"].append(variant)

            variants_by_allele_id[allele_id]["equivalent_nt"] = [
                equivalent_nt_variants[related_allele_id] for related_allele_id in equivalent_nt_variants
            ]

        elif re.fullmatch(multi_clingen_allele_id_re, allele_id):
            # validate each component allele id - already done via re
            # allow more than 2 components?
            # full matches are determined the same exact way as single variant (done above already)
            # equivalent aa/nt: if all components of variant are represented by equivalent or exact match, but not all exact matches because those are already represented above
            # so basically go through variant translations table and create every possible combo of components except for full exact match,
            # then search the db for those combos

            allele_id_components = allele_id.split(",")
            if all(component.startswith("PA") for component in allele_id_components):
                related_clingen_id_components = []  # list of lists, one list for each component
                for component in allele_id_components:
                    subquery = (
                        db.execute(
                            select(VariantTranslation.nt_clingen_id).where(
                                VariantTranslation.aa_clingen_id == component
                            )
                        )
                        .scalars()
                        .all()
                    )
                    related_clingen_id_components.append(
                        db.execute(
                            select(VariantTranslation).where(
                                or_(
                                    VariantTranslation.aa_clingen_id == component,
                                    VariantTranslation.nt_clingen_id.in_(subquery),
                                )
                            )
                        )
                        .scalars()
                        .all()
                    )
                # create every possible combination of those two, including the original components, except for the version with both original components
                # assuming that aa variants should always be paired with aa variants, and nt variants with nt variants,
                # create lists of just the aa variants, and lists of just the nt variants.
                # assuming only 2 components for any multivariant in db.
                aa_clingen_id_first_allele = [
                    translation.aa_clingen_id for translation in related_clingen_id_components[0]
                ]
                # original component should also be included, even if it is not in the variant translations table
                aa_clingen_id_first_allele.append(allele_id_components[0])
                aa_clingen_id_second_allele = [
                    translation.aa_clingen_id for translation in related_clingen_id_components[1]
                ]
                # original component should also be included, even if it is not in the variant translations table
                aa_clingen_id_first_allele.append(allele_id_components[1])
                related_aa_clingen_id_combinations = list(
                    itertools.product(aa_clingen_id_first_allele, aa_clingen_id_second_allele)
                )
                # turn each inner list into a single string and sort each pair lexicographically, since this is how they are stored in the db
                joined_related_aa_clingen_id_combinations = [
                    ",".join(sorted(list(combination)))  # type: ignore
                    for combination in related_aa_clingen_id_combinations
                ]
                related_nt_clingen_id_first_allele = [
                    translation.nt_clingen_id for translation in related_clingen_id_components[0]
                ]
                related_nt_clingen_id_second_allele = [
                    translation.nt_clingen_id for translation in related_clingen_id_components[1]
                ]
                related_nt_clingen_id_combinations = list(
                    itertools.product(related_nt_clingen_id_first_allele, related_nt_clingen_id_second_allele)
                )
                joined_related_nt_clingen_id_combinations = [
                    ",".join(sorted(list(combination)))  # type: ignore
                    for combination in related_nt_clingen_id_combinations
                ]

                # query the db for variants with these combinations
                related_aa_variants = db.execute(
                    select(Variant, MappedVariant.clingen_allele_id)
                    .join(MappedVariant)
                    .options(joinedload(Variant.score_set).joinedload(ScoreSet.experiment))
                    .where(MappedVariant.clingen_allele_id.in_(joined_related_aa_clingen_id_combinations))
                    .where(MappedVariant.clingen_allele_id != allele_id)
                    .where(MappedVariant.current == True)  # noqa: E712
                ).all()
                related_nt_variants = db.execute(
                    select(Variant, MappedVariant.clingen_allele_id)
                    .join(MappedVariant)
                    .options(joinedload(Variant.score_set).joinedload(ScoreSet.experiment))
                    .where(MappedVariant.clingen_allele_id.in_(joined_related_nt_clingen_id_combinations))
                    .where(MappedVariant.current == True)  # noqa: E712
                ).all()

            elif all(component.startswith("CA") for component in allele_id_components):
                related_clingen_id_components = []  # list of lists, one list for each component
                for component in allele_id_components:
                    subquery = (
                        db.execute(
                            select(VariantTranslation.aa_clingen_id).where(
                                VariantTranslation.nt_clingen_id == allele_id
                            )
                        )
                        .scalars()
                        .all()
                    )
                    related_clingen_id_components.append(
                        db.execute(
                            select(VariantTranslation).where(
                                or_(
                                    VariantTranslation.nt_clingen_id == allele_id,
                                    VariantTranslation.aa_clingen_id.in_(subquery),
                                )
                            )
                        )
                        .scalars()
                        .all()
                    )
                # create every possible combination of those two, including the original components, except for the version with both original components
                # assuming that aa variants should always be paired with aa variants, and nt variants with nt variants,
                # create lists of just the aa variants, and lists of just the nt variants.
                # assuming only 2 components for any multivariant in db.
                aa_clingen_id_first_allele = [
                    translation.aa_clingen_id for translation in related_clingen_id_components[0]
                ]
                aa_clingen_id_second_allele = [
                    translation.aa_clingen_id for translation in related_clingen_id_components[1]
                ]
                related_aa_clingen_id_combinations = list(
                    itertools.product(aa_clingen_id_first_allele, aa_clingen_id_second_allele)
                )
                # turn each inner list into a single string and sort each pair lexicographically, since this is how they are stored in the db
                joined_related_aa_clingen_id_combinations = [
                    ",".join(sorted(list(combination)))  # type: ignore
                    for combination in related_aa_clingen_id_combinations
                ]

                related_nt_clingen_id_first_allele = [
                    translation.nt_clingen_id for translation in related_clingen_id_components[0]
                ]
                # original component should also be included, even if it is not in the variant translations table
                related_nt_clingen_id_first_allele.append(allele_id_components[0])
                related_nt_clingen_id_second_allele = [
                    translation.nt_clingen_id for translation in related_clingen_id_components[1]
                ]
                # original component should also be included, even if it is not in the variant translations table
                related_nt_clingen_id_first_allele.append(allele_id_components[1])
                related_nt_clingen_id_combinations = list(
                    itertools.product(related_nt_clingen_id_first_allele, related_nt_clingen_id_second_allele)
                )
                joined_related_nt_clingen_id_combinations = [
                    ",".join(sorted(list(combination)))  # type: ignore
                    for combination in related_nt_clingen_id_combinations
                ]
                # query the db for variants with these combinations
                related_aa_variants = db.execute(
                    select(Variant, MappedVariant.clingen_allele_id)
                    .join(MappedVariant)
                    .options(joinedload(Variant.score_set).joinedload(ScoreSet.experiment))
                    .where(MappedVariant.clingen_allele_id.in_(joined_related_aa_clingen_id_combinations))
                    .where(MappedVariant.current == True)  # noqa: E712
                ).all()
                related_nt_variants = db.execute(
                    select(Variant, MappedVariant.clingen_allele_id)
                    .join(MappedVariant)
                    .options(joinedload(Variant.score_set).joinedload(ScoreSet.experiment))
                    .where(MappedVariant.clingen_allele_id.in_(joined_related_nt_clingen_id_combinations))
                    .where(MappedVariant.clingen_allele_id != allele_id)
                    .where(MappedVariant.current == True)  # noqa: E712
                ).all()

            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid Clingen Allele ID '{allele_id}': all components must be either PA or CA",
                )

            equivalent_aa_variants = {}
            for variant, related_allele_id in related_aa_variants:
                if has_permission(user_data, variant.score_set, Action.READ).permitted:
                    if related_allele_id not in equivalent_aa_variants:
                        equivalent_aa_variants[related_allele_id] = {
                            "clingen_allele_id": related_allele_id,
                            "variant_effect_measurements": [],
                        }
                    equivalent_aa_variants[related_allele_id]["variant_effect_measurements"].append(variant)

            variants_by_allele_id[allele_id]["equivalent_aa"] = [
                equivalent_aa_variants[related_allele_id] for related_allele_id in equivalent_aa_variants
            ]

            equivalent_nt_variants = {}
            for variant, related_allele_id in related_nt_variants:
                if has_permission(user_data, variant.score_set, Action.READ).permitted:
                    if related_allele_id not in equivalent_nt_variants:
                        equivalent_nt_variants[related_allele_id] = {
                            "clingen_allele_id": related_allele_id,
                            "variant_effect_measurements": [],
                        }
                    equivalent_nt_variants[related_allele_id]["variant_effect_measurements"].append(variant)

            variants_by_allele_id[allele_id]["equivalent_nt"] = [
                equivalent_nt_variants[related_allele_id] for related_allele_id in equivalent_nt_variants
            ]

    return [variants_by_allele_id[allele_id] for allele_id in request.clingen_allele_ids]


@router.get(
    "/variants/{urn}",
    status_code=200,
    response_model=VariantEffectMeasurementWithScoreSet,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    response_model_exclude_none=True,
    summary="Fetch variant by URN",
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
        logger.info(msg="Could not fetch the requested variant; Multiple such variants exist.", extra=logging_context())
        raise HTTPException(status_code=500, detail=f"multiple variants with URN '{urn}' were found")

    if not variant:
        logger.info(msg="Could not fetch the requested variant; No such variant exists.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"variant with URN '{urn}' not found")

    assert_permission(user_data, variant.score_set, Action.READ)
    return variant


@router.get(
    "/variants/{urn}/csv-namespaces",
    status_code=200,
    response_model=list[AvailableCsvNamespace],
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="List the CSV column namespaces this variant has data for",
)
def get_variant_csv_namespaces(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
    principal: Principal = Depends(get_principal),
) -> Any:
    """
    List the CSV column namespaces this variant has data for, labeled and grouped for a picker.

    Widens over the variant's equivalent measurements the same way the CSV does, so a calibration
    belonging to another score set that also measured this allele is offered here too.

    Parameters
    __________
    urn : str
        The URN of the variant to inspect.
    db : Session
        The database session to use.
    user_data : Optional[UserData]
        The user data of the current user. If None, no user-specific permissions are checked.

    Returns
    _______
    list[AvailableCsvNamespace]
        The namespaces with data, each with a human-readable label and group.
    """
    save_to_logging_context({"requested_resource": urn, "resource_property": "csv-namespaces"})

    variant = db.query(Variant).filter(Variant.urn == urn).one_or_none()
    if not variant:
        logger.info(msg="Could not fetch CSV namespaces; No such variant exists.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"variant with URN '{urn}' not found")

    assert_permission(user_data, variant.score_set, Action.READ)

    return available_variant_csv_namespaces(
        db,
        urn,
        may_read_score_set=lambda score_set: has_permission(user_data, score_set, Action.READ).permitted,
        viewer=principal.viewer_for(ScoreCalibrationViewer),
    )


@router.get(
    "/variants/{urn}/csv",
    status_code=200,
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": (
                "Variant data in CSV format, one row per measurement of the variant's allele. Columns"
                " cover identity, mapped coordinates, the measured score, external annotations, and each"
                " requested calibration's functional and ACMG interpretation."
            ),
        },
        **BASE_400_RESPONSE,
        **ACCESS_CONTROL_ERROR_RESPONSES,
    },
    summary="Get variant data in CSV format",
)
def get_variant_csv_data(
    *,
    urn: str,
    namespaces: Optional[List[CsvNamespaceStr]] = Query(default=None, description=CSV_NAMESPACES_PARAM_DESCRIPTION),
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
    principal: Principal = Depends(get_principal),
) -> Any:
    """
    Return tabular data for a single variant, identified by URN, in CSV format.

    Where the variant-level annotation endpoints return nested VA-Spec objects, this flattens the same
    interpretation into columns a clinical information system can consume: ACMG criteria, evidence
    strengths, and evidence outcome codes alongside the measurement they were derived from.

    A row is emitted for every current measurement of the variant's ClinGen allele, so a variant assayed
    in several score sets yields several rows. The requested variant is always first.

    Parameters
    __________
    urn : str
        The URN of the variant to fetch.
    namespaces : Optional[List[str]]
        The groups of columns to include. When omitted, the response includes the fixed groups plus one
        namespace per calibration eligible to annotate these measurements and the most recent ClinVar
        release covering them.
    db : Session
        The database session to use.
    user_data : Optional[UserData]
        The user data of the current user. If None, no user-specific permissions are checked.

    Returns
    _______
    Any
        StreamingResponse containing the CSV data.
    """
    save_to_logging_context({"requested_resource": urn, "resource_property": "csv", "namespaces": namespaces})

    try:
        variant = db.query(Variant).filter(Variant.urn == urn).one_or_none()
    except MultipleResultsFound:
        logger.info(msg="Could not fetch the requested variant; Multiple such variants exist.", extra=logging_context())
        raise HTTPException(status_code=500, detail=f"multiple variants with URN '{urn}' were found")

    if not variant:
        logger.info(msg="Could not fetch the requested variant; No such variant exists.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"variant with URN '{urn}' not found")

    assert_permission(user_data, variant.score_set, Action.READ)

    # Only measurements the requester may read are emitted. The predicate runs against the score sets
    # reached by the widening, keeping the permission check proportional to the result.
    # A calibration's READ permission is stricter than its score set's, so it is asked separately: being
    # able to read the measurement does not entitle a caller to a private calibration's interpretation.
    csv_str = get_variant_csv(
        db,
        urn,
        namespaces=namespaces,
        may_read_score_set=lambda score_set: has_permission(user_data, score_set, Action.READ).permitted,
        viewer=principal.viewer_for(ScoreCalibrationViewer),
    )
    return StreamingResponse(
        iter([csv_str]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{urn}.csv"'},
    )

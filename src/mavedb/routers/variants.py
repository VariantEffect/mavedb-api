import logging
from datetime import datetime
from typing import Annotated, Any, List, Optional

from fastapi import APIRouter, Depends, Path, Query, Response
from fastapi.exceptions import HTTPException
from fastapi.responses import StreamingResponse
from ga4gh.core.identifiers import GA4GH_IR_REGEXP
from ga4gh.va_spec.acmg_2015 import VariantPathogenicityStatement
from ga4gh.va_spec.base.core import ExperimentalVariantFunctionalImpactStudyResult, Statement
from sqlalchemy import select
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.alleles import find_variants_by_vrs_identifier
from mavedb.lib.annotation.annotate import (
    variant_functional_impact_statement,
    variant_pathogenicity_statement,
    variant_study_result,
)
from mavedb.lib.annotation.context import variant_annotation_context
from mavedb.lib.annotation.exceptions import MappingDataDoesntExistException
from mavedb.lib.authentication import get_current_user
from mavedb.lib.authorization import get_principal
from mavedb.lib.csv.namespaces import CSV_NAMESPACES_PARAM_DESCRIPTION, CsvNamespaceStr
from mavedb.lib.csv.variant import available_variant_csv_namespaces, get_variant_csv
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.permissions import Action, assert_permission, has_permission
from mavedb.lib.permissions.principal import Principal
from mavedb.lib.permissions.score_calibration import ScoreCalibrationViewer
from mavedb.lib.types.authentication import UserData
from mavedb.lib.variant_detail import get_variant_detail
from mavedb.models.variant import Variant
from mavedb.routers.shared import (
    ACCESS_CONTROL_ERROR_RESPONSES,
    BASE_400_RESPONSE,
    PUBLIC_ERROR_RESPONSES,
    ROUTER_BASE_PREFIX,
)
from mavedb.view_models.csv_namespace import AvailableCsvNamespace
from mavedb.view_models.variant import VariantVrsMatch
from mavedb.view_models.variant_detail import VariantDetail

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


def _fetch_readable_variant(db: Session, user_data: Optional[UserData], urn: str) -> Variant:
    """Fetch a single variant by URN and assert the caller may read its score set.

    Raises 404 when no such variant exists, 500 when the URN is not unique (an invariant break), and the
    standard 403 (via ``assert_permission``) when the score set is not readable.
    """
    try:
        variant = db.scalars(select(Variant).where(Variant.urn == urn)).one_or_none()
    except MultipleResultsFound:
        logger.info(msg="Could not fetch the requested variant; Multiple such variants exist.", extra=logging_context())
        raise HTTPException(status_code=500, detail=f"multiple variants with URN '{urn}' were found")

    if not variant:
        logger.info(msg="Could not fetch the requested variant; No such variant exists.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"variant with URN '{urn}' not found")

    assert_permission(user_data, variant.score_set, Action.READ)
    return variant


@router.get(
    "/variants/vrs/{identifier}",
    status_code=200,
    response_model=list[VariantVrsMatch],
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Look up variants by VRS identifier",
)
def lookup_variants_by_vrs_identifier(
    *,
    response: Response,
    identifier: Annotated[
        str,
        Path(
            description="A valid GA4GH digest-based identifier for the mapped allele.",
            json_schema_extra={"example": "ga4gh:VA.0123abcd"},
            regex=GA4GH_IR_REGEXP,
        ),
    ],
    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Reconstruct the molecular layer (Cat-VRS membership, VEP/gnomAD/ClinVar annotations) as it "
            "stood at this instant, over the variant's fixed score. ISO 8601, ideally timezone-aware. "
            "Content valid-time only — it never re-selects a score-set version, and scores/classifications "
            "are as-of-invariant. Defaults to current."
        ),
    ),
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> list[VariantVrsMatch]:
    """Resolve a GA4GH VRS identifier to the readable variants whose mapping links that allele.

    A deduplicated allele may be shared across score sets, so one identifier can resolve to several
    variants. This is a lookup returning a collection: results are filtered to the score sets the caller
    may read, and an empty list is returned when nothing readable matches. An absent identifier and a
    match visible only in a private score set are deliberately indistinguishable (both yield ``[]``), so
    the response never reveals a private allele's existence.
    """
    save_to_logging_context({"requested_resource": identifier, "as_of": as_of})
    response.headers["X-As-Of"] = as_of.isoformat() if as_of is not None else "current"

    matches = find_variants_by_vrs_identifier(db, identifier, as_of=as_of)
    permitted = [
        (variant, allele)
        for variant, allele in matches
        if has_permission(user_data, variant.score_set, Action.READ).permitted
    ]

    return [
        VariantVrsMatch(
            variant_urn=variant.urn or "",
            clingen_allele_id=allele.clingen_allele_id,
            vrs_id=(allele.post_mapped or {}).get("id"),
            level=allele.level,
        )
        for variant, allele in permitted
    ]


@router.get(
    "/variants/{urn}",
    status_code=200,
    response_model=VariantDetail,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    # Emit nulls rather than omitting absent fields: this shape is a stable, self-describing envelope
    # (its bulk pair, GET /score-sets/{urn}/variant-details, streams the same object one-per-line for
    # dataframe/columnar consumption), so every response carries the same key set.
    response_model_exclude_none=False,
    summary="Fetch assayed variant detail by URN",
)
def get_variant(
    *,
    urn: str,
    response: Response,
    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Reconstruct the molecular layer (Cat-VRS membership, VEP/gnomAD/ClinVar annotations) as it "
            "stood at this instant, over the variant's fixed score. ISO 8601, ideally timezone-aware. "
            "Content valid-time only — it never re-selects a score-set version, and scores/classifications "
            "are as-of-invariant. Defaults to current."
        ),
    ),
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
):
    """Fetch the two-tier detail envelope for a single assayed variant by URN.

    Flat assay-level fields for the common UI case plus the spec-pure GA4GH CategoricalVariant and a
    digest-keyed annotation map for machine/standard consumers. A superseded variant is served (it is
    the citable unit) but self-describes via isCurrent/supersededByScoreSet rather than reading as current.
    """
    save_to_logging_context({"requested_resource": urn, "as_of": as_of})
    response.headers["X-As-Of"] = as_of.isoformat() if as_of is not None else "current"
    variant = _fetch_readable_variant(db, user_data, urn)

    # Version standing: resolve the superseding version's visibility exactly as fetch_score_set_by_urn
    # does — a newer version the user cannot read is not leaked (the variant then reads as current).
    superseding = variant.score_set.superseding_score_set
    if superseding is not None and not has_permission(user_data, superseding, Action.READ).permitted:
        superseding = None

    visible_calibration_ids = {
        sc.id
        for sc in variant.score_set.score_calibrations
        if sc.id is not None and has_permission(user_data, sc, Action.READ).permitted
    }

    return get_variant_detail(
        db,
        variant,
        superseding_score_set=superseding,
        visible_calibration_ids=visible_calibration_ids,
        as_of=as_of,
    )


@router.get(
    "/variants/{urn}/va/study-result",
    status_code=200,
    response_model=ExperimentalVariantFunctionalImpactStudyResult,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Construct a VA-Spec StudyResult for a variant",
)
def get_variant_study_result(
    *,
    response: Response,
    urn: str,
    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Reconstruct the molecular layer (Cat-VRS membership, VEP/gnomAD/ClinVar annotations) as it "
            "stood at this instant, over the variant's fixed score. ISO 8601, ideally timezone-aware. "
            "Content valid-time only — it never re-selects a score-set version, and scores/classifications "
            "are as-of-invariant. Defaults to current."
        ),
    ),
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> ExperimentalVariantFunctionalImpactStudyResult:
    """Construct a single VA-Spec StudyResult for a variant by URN, from its mapping substrate."""
    save_to_logging_context({"requested_resource": urn, "as_of": as_of})
    response.headers["X-As-Of"] = as_of.isoformat() if as_of is not None else "current"

    variant = _fetch_readable_variant(db, user_data, urn)

    context = variant_annotation_context(db, variant, as_of=as_of)
    if context is None:
        raise HTTPException(
            status_code=404, detail=f"No study result exists for variant {urn}: no mapping data exists."
        )

    try:
        return variant_study_result(context)
    except MappingDataDoesntExistException as e:
        logger.info(msg=f"Could not construct a study result for variant {urn}: {e}", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"No study result exists for variant {urn}: {e}")


@router.get(
    "/variants/{urn}/va/functional-statement",
    status_code=200,
    response_model=Statement,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Construct a VA-Spec functional-impact Statement for a variant",
)
def get_variant_functional_impact_statement(
    *,
    response: Response,
    urn: str,
    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Reconstruct the molecular layer (Cat-VRS membership, VEP/gnomAD/ClinVar annotations) as it "
            "stood at this instant, over the variant's fixed score. ISO 8601, ideally timezone-aware. "
            "Content valid-time only — it never re-selects a score-set version, and scores/classifications "
            "are as-of-invariant. Defaults to current."
        ),
    ),
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
    principal: Principal = Depends(get_principal),
) -> Statement:
    """Construct a single VA-Spec functional-impact Statement for a variant by URN."""
    save_to_logging_context({"requested_resource": urn, "as_of": as_of})
    response.headers["X-As-Of"] = as_of.isoformat() if as_of is not None else "current"

    variant = _fetch_readable_variant(db, user_data, urn)

    context = variant_annotation_context(db, variant, as_of=as_of)
    if context is None:
        raise HTTPException(
            status_code=404,
            detail=f"No functional impact statement exists for variant {urn}: no mapping data exists.",
        )

    try:
        functional_impact = variant_functional_impact_statement(context, principal=principal)
    except MappingDataDoesntExistException as e:
        logger.info(
            msg=f"Could not construct a functional impact statement for variant {urn}: {e}", extra=logging_context()
        )
        raise HTTPException(status_code=404, detail=f"No functional impact statement exists for variant {urn}: {e}")

    if not functional_impact:
        logger.info(
            msg=f"Variant {urn} does not have sufficient evidence to evaluate its functional impact.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=404,
            detail=(
                f"No functional impact statement exists for variant {urn}. Variant does not have sufficient "
                "evidence to evaluate its functional impact."
            ),
        )

    return functional_impact


@router.get(
    "/variants/{urn}/va/pathogenicity-statement",
    status_code=200,
    response_model=VariantPathogenicityStatement,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Construct a VA-Spec pathogenicity Statement for a variant",
)
def get_variant_pathogenicity_statement(
    *,
    response: Response,
    urn: str,
    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Reconstruct the molecular layer (Cat-VRS membership, VEP/gnomAD/ClinVar annotations) as it "
            "stood at this instant, over the variant's fixed score. ISO 8601, ideally timezone-aware. "
            "Content valid-time only — it never re-selects a score-set version, and scores/classifications "
            "are as-of-invariant. Defaults to current."
        ),
    ),
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
    principal: Principal = Depends(get_principal),
) -> VariantPathogenicityStatement:
    """Construct a single VA-Spec pathogenicity Statement for a variant by URN."""
    save_to_logging_context({"requested_resource": urn, "as_of": as_of})
    response.headers["X-As-Of"] = as_of.isoformat() if as_of is not None else "current"

    variant = _fetch_readable_variant(db, user_data, urn)

    context = variant_annotation_context(db, variant, as_of=as_of)
    if context is None:
        raise HTTPException(
            status_code=404,
            detail=f"No pathogenicity statement exists for variant {urn}: no mapping data exists.",
        )

    try:
        pathogenicity_statement = variant_pathogenicity_statement(context, principal=principal)
    except MappingDataDoesntExistException as e:
        logger.info(
            msg=f"Could not construct a pathogenicity statement for variant {urn}: {e}", extra=logging_context()
        )
        raise HTTPException(status_code=404, detail=f"No pathogenicity statement exists for variant {urn}: {e}")

    if not pathogenicity_statement:
        logger.info(
            msg=f"Variant {urn} does not have sufficient evidence to evaluate its pathogenicity.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=404,
            detail=(
                f"No pathogenicity statement exists for variant {urn}; Variant does not have sufficient "
                "evidence to evaluate its pathogenicity."
            ),
        )

    return pathogenicity_statement


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
    response: Response,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
    principal: Principal = Depends(get_principal),
    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Reconstruct the offered namespaces as they stood at this instant, so discovery matches an "
            "`as_of` download. ISO 8601, ideally timezone-aware. Defaults to current."
        ),
    ),
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

    # Echoed like the CSV endpoints: the instant discovery answered for is part of the answer.
    response.headers["X-As-Of"] = as_of.isoformat() if as_of is not None else "current"
    return available_variant_csv_namespaces(
        db,
        urn,
        may_read_score_set=lambda score_set: has_permission(user_data, score_set, Action.READ).permitted,
        viewer=principal.viewer_for(ScoreCalibrationViewer),
        as_of=as_of,
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
    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Reconstruct the mapping-derived columns (reference HGVS, VEP, gnomAD, ClinVar) as they "
            "stood at this instant, over the variant's fixed score. ISO 8601, ideally timezone-aware. "
            "Defaults to current."
        ),
    ),
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
        as_of=as_of,
    )
    return StreamingResponse(
        iter([csv_str]),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{urn}.csv"',
            "X-As-Of": as_of.isoformat() if as_of is not None else "current",
            "Access-Control-Expose-Headers": "X-As-Of",
        },
    )

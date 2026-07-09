import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query, Response
from fastapi.exceptions import HTTPException
from sqlalchemy import select
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.authentication import get_current_user
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.permissions import Action, assert_permission, has_permission
from mavedb.lib.types.authentication import UserData
from mavedb.lib.variant_detail import get_variant_detail
from mavedb.models.variant import Variant
from mavedb.routers.shared import (
    ACCESS_CONTROL_ERROR_RESPONSES,
    PUBLIC_ERROR_RESPONSES,
    ROUTER_BASE_PREFIX,
)
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


@router.get(
    "/variants/{urn}",
    status_code=200,
    response_model=VariantDetail,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    response_model_exclude_none=True,
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
    try:
        variant = db.scalars(select(Variant).where(Variant.urn == urn)).one_or_none()
    except MultipleResultsFound:
        logger.info(msg="Could not fetch the requested variant; Multiple such variants exist.", extra=logging_context())
        raise HTTPException(status_code=500, detail=f"multiple variants with URN '{urn}' were found")

    if not variant:
        logger.info(msg="Could not fetch the requested variant; No such variant exists.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"variant with URN '{urn}' not found")

    assert_permission(user_data, variant.score_set, Action.READ)

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

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.allele_measurements import get_allele_measurements
from mavedb.lib.authentication import get_current_user
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import save_to_logging_context
from mavedb.lib.types.authentication import UserData
from mavedb.routers.shared import ACCESS_CONTROL_ERROR_RESPONSES, PUBLIC_ERROR_RESPONSES, ROUTER_BASE_PREFIX
from mavedb.view_models.allele_measurement import AlleleMeasurement

TAG_NAME = "ClinGen alleles"

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix=f"{ROUTER_BASE_PREFIX}/clingen-alleles",
    tags=[TAG_NAME],
    responses={**PUBLIC_ERROR_RESPONSES},
    route_class=LoggedRoute,
)

metadata = {
    "name": TAG_NAME,
    "description": "The ClinGen-allele-centric variant page: measurements across a variant's equivalence class.",
}


@router.get(
    "/{clingen_allele_id}/measurements",
    status_code=200,
    response_model=list[AlleleMeasurement],
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    response_model_exclude_none=True,
    summary="List measurements for a ClinGen allele's equivalence class",
)
def get_clingen_allele_measurements(
    *,
    clingen_allele_id: str,
    response: Response,
    include_superseded: bool = Query(
        default=False,
        description=(
            "Include measurements from superseded score-set versions. Default false — superseded "
            "measurements are a deliberate power-user / citation path, never surfaced by discovery."
        ),
    ),
    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Reconstruct the equivalence class (which mapping records / allele links are live) as it "
            "stood at this instant. ISO 8601, ideally timezone-aware. Scores and classifications are "
            "as-of-invariant. Defaults to current."
        ),
    ),
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
):
    """List every measurement whose cross-layer equivalence class touches this ClinGen allele (a ``CA`` or
    ``PA``) — the direct measurements assayed at this change plus the reverse-translation-related ones,
    each labeled by its assayed level and relationship. This is the ClinGen-allele-centric variant page's
    entrypoint. A private score set's measurement is never included; its inline classification is withheld
    where the calibration is unreadable while the measurement still shows.
    """
    save_to_logging_context(
        {
            "requested_resource": clingen_allele_id,
            "as_of": as_of,
            "include_superseded": include_superseded,
        }
    )
    response.headers["X-As-Of"] = as_of.isoformat() if as_of is not None else "current"

    return get_allele_measurements(
        db,
        clingen_allele_id,
        user_data=user_data,
        include_superseded=include_superseded,
        as_of=as_of,
    )

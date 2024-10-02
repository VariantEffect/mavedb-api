import logging
from typing import Any
from operator import attrgetter

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import or_

from mavedb import deps
from mavedb.lib.authentication import get_current_user, UserData
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.models.contributor import Contributor
from mavedb.models.experiment_set import ExperimentSet
from mavedb.view_models import experiment_set

router = APIRouter(
    prefix="/api/v1/experiment-sets",
    tags=["experiment-sets"],
    responses={404: {"description": "Not found"}},
    route_class=LoggedRoute,
)

logger = logging.getLogger(__name__)


@router.get(
    "/check-authorizations/{urn}",
    status_code=200,
    response_model=bool
)
async def check_experiment_set_authorization(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(get_current_user),
) -> bool:
    """
    Check whether users have authorizations in this experiment set.
    """
    query = db.query(ExperimentSet).filter(ExperimentSet.urn == urn)

    if user_data is not None:
        query = query.filter(
            or_(
                ExperimentSet.created_by_id == user_data.user.id,
                ExperimentSet.contributors.any(Contributor.orcid_id == user_data.user.username),
            )
        )
    else:
        return False

    save_to_logging_context({"Experiment set requested resource": urn})
    item = query.first()
    if item:
        return True
    else:
        return False


@router.get(
    "/{urn}",
    status_code=200,
    response_model=experiment_set.ExperimentSet,
    responses={404: {}},
)
def fetch_experiment_set(*, urn: str, db: Session = Depends(deps.get_db)) -> Any:
    """
    Fetch a single experiment set by URN.
    """
    # item = db.query(ExperimentSet).filter(ExperimentSet.urn == urn).filter(ExperimentSet.private.is_(False)).first()
    item = db.query(ExperimentSet).filter(ExperimentSet.urn == urn).first()
    save_to_logging_context({"requested_resource": urn})

    if not item:
        # the exception is raised, not returned - you will get a validation
        # error otherwise.
        logger.debug(msg="The requested resources does not exist.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"Experiment set with URN {urn} not found")
    else:
        item.experiments.sort(key=attrgetter("urn"))

    return item

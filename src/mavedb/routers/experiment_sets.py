import logging
from operator import attrgetter
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.authentication import UserData, get_current_user
from mavedb.lib.experiments import enrich_experiment_with_num_score_sets
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.permissions import Action, assert_permission, has_permission
from mavedb.models.experiment_set import ExperimentSet
from mavedb.view_models import experiment_set

router = APIRouter(
    prefix="/api/v1/experiment-sets",
    tags=["Experiment Sets"],
    responses={
        404: {"description": "Not found"},
        500: {"description": "Internal server error"},
    },
    route_class=LoggedRoute,
)

logger = logging.getLogger(__name__)


@router.get(
    "/{urn}",
    status_code=200,
    response_model=experiment_set.ExperimentSet,
    responses={
        401: {"description": "Not authenticated"},
        403: {"description": "Not authorized"},
    },
    summary="Fetch experiment set by URN",
)
def fetch_experiment_set(
    *, urn: str, db: Session = Depends(deps.get_db), user_data: UserData = Depends(get_current_user)
) -> Any:
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

    assert_permission(user_data, item, Action.READ)

    # Filter experiment sub-resources to only those experiments readable by the requesting user.
    item.experiments[:] = [exp for exp in item.experiments if has_permission(user_data, exp, Action.READ).permitted]
    enriched_experiments = [enrich_experiment_with_num_score_sets(exp, user_data) for exp in item.experiments]
    enriched_item = experiment_set.ExperimentSet.model_validate(item).copy(
        update={"experiments": enriched_experiments, "num_experiments": len(enriched_experiments)}
    )

    return enriched_item

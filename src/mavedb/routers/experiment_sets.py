import logging
from operator import attrgetter
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.authentication import get_current_user
from mavedb.lib.experiments import enrich_experiment_with_num_score_sets
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.permissions import Action, assert_permission, has_permission
from mavedb.lib.types.authentication import UserData
from mavedb.models.experiment_set import ExperimentSet
from mavedb.routers.shared import ACCESS_CONTROL_ERROR_RESPONSES, PUBLIC_ERROR_RESPONSES, ROUTER_BASE_PREFIX
from mavedb.view_models import experiment_set

TAG_NAME = "Experiment Sets"

router = APIRouter(
    prefix=f"{ROUTER_BASE_PREFIX}/experiment-sets",
    tags=[TAG_NAME],
    responses={**PUBLIC_ERROR_RESPONSES},
    route_class=LoggedRoute,
)

metadata = {
    "name": TAG_NAME,
    "description": "Retrieve experiment sets and their associated experiments.",
    "externalDocs": {
        "description": "Experiment Sets Documentation",
        "url": "https://mavedb.org/docs/mavedb/record_types.html#experiment-sets",
    },
}

logger = logging.getLogger(__name__)


@router.get(
    "/{urn}",
    status_code=200,
    response_model=experiment_set.ExperimentSet,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
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
        raise HTTPException(status_code=404, detail=f"experiment set with URN {urn} not found")

    assert_permission(user_data, item, Action.READ)

    # Narrow to the experiments this caller may read, without touching item.experiments.
    # ExperimentSet.experiments is mapped with cascade="all, delete-orphan": removing members from the ORM
    # collection marks them as orphans, and the next flush deletes those experiments along with their score
    # sets and variants. Only autoflush=False and the absence of a commit on this path made that survivable.
    readable_experiments = sorted(
        (experiment for experiment in item.experiments if has_permission(user_data, experiment, Action.READ).permitted),
        key=attrgetter("urn"),
    )
    enriched_experiments = [
        enrich_experiment_with_num_score_sets(experiment, user_data) for experiment in readable_experiments
    ]

    return experiment_set.ExperimentSet.model_validate(item).copy(
        update={"experiments": enriched_experiments, "num_experiments": len(enriched_experiments)}
    )

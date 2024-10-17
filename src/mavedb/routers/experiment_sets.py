import logging
from operator import attrgetter
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.authentication import UserData, get_current_user
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.permissions import Action, has_permission
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
    "/{urn}",
    status_code=200,
    response_model=experiment_set.ExperimentSet,
    responses={404: {}},
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

    has_permission(user_data, item, Action.READ)

    # Filter experiment sub-resources to only those experiments readable by the requesting user.
    item.experiments[:] = [exp for exp in item.experiments if has_permission(user_data, exp, Action.READ).permitted]

    return item

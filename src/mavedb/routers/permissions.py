import logging
from enum import Enum
from typing import Optional, Union

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.authentication import UserData, get_current_user
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.permissions import Action, has_permission
from mavedb.models.collection import Collection
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.score_set import ScoreSet

router = APIRouter(
    prefix="/api/v1/permissions",
    tags=["permissions"],
    responses={404: {"description": "Not found"}},
    route_class=LoggedRoute,
)

logger = logging.getLogger(__name__)


class ModelName(str, Enum):
    collection = "collection"
    experiment = "experiment"
    experiment_set = "experiment-set"
    score_set = "score-set"


@router.get(
    "/user-is-permitted/{model_name}/{urn}/{action}",
    status_code=200,
    response_model=bool,
)
async def check_permission(
    *,
    model_name: ModelName,
    urn: str,
    action: Action,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(get_current_user),
) -> bool:
    """
    Check whether users have authorizations in adding/editing/deleting/publishing experiment or score set.
    """
    save_to_logging_context({"requested_resource": urn})

    item: Optional[Union[Collection, ExperimentSet, Experiment, ScoreSet]] = None

    if model_name == ModelName.experiment_set:
        item = db.query(ExperimentSet).filter(ExperimentSet.urn == urn).one_or_none()
    elif model_name == ModelName.experiment:
        item = db.query(Experiment).filter(Experiment.urn == urn).one_or_none()
    elif model_name == ModelName.score_set:
        item = db.query(ScoreSet).filter(ScoreSet.urn == urn).one_or_none()
    elif model_name == ModelName.collection:
        item = db.query(Collection).filter(Collection.urn == urn).one_or_none()

    if item:
        permission = has_permission(user_data, item, action).permitted
        return permission
    else:
        logger.debug(msg="The requested resources does not exist.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"{model_name.value} with URN '{urn}' not found")

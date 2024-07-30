import logging
from typing import Any
from operator import attrgetter

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import dump_context, save_to_context
from mavedb.models.experiment_set import ExperimentSet
from mavedb.view_models import experiment_set

router = APIRouter(
    prefix="/api/v1/experiment-sets",
    tags=["experiment-sets"],
    responses={404: {"description": "Not found"}},
    route_class=LoggedRoute,
)

logger = logging.getLogger(__name__)


@router.get("/{urn}", status_code=200, response_model=experiment_set.ExperimentSet, responses={404: {}})
def fetch_experiment_set(*, urn: str, db: Session = Depends(deps.get_db)) -> Any:
    """
    Fetch a single experiment set by URN.
    """
    # item = db.query(ExperimentSet).filter(ExperimentSet.urn == urn).filter(ExperimentSet.private.is_(False)).first()
    item = db.query(ExperimentSet).filter(ExperimentSet.urn == urn).first()
    save_to_context({"requested_resource": urn})

    if not item:
        # the exception is raised, not returned - you will get a validation
        # error otherwise.
        logger.debug(f"The requested resources does not exist. {dump_context()}")
        raise HTTPException(status_code=404, detail=f"Experiment set with URN {urn} not found")
    else:
        item.experiments.sort(key=attrgetter("urn"))

    return item

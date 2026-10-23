from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.authorization import get_principal
from mavedb.lib.identifiers import search_identifiers_on_readable_records
from mavedb.lib.permissions.experiment import ExperimentViewer
from mavedb.lib.permissions.experiment_set import ExperimentSetViewer
from mavedb.lib.permissions.principal import Principal
from mavedb.lib.permissions.score_set import ScoreSetViewer
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.raw_read_identifier import (
    ExperimentRawReadIdentifier,
    ExperimentSetRawReadIdentifier,
    ScoreSetRawReadIdentifier,
)
from mavedb.models.score_set import ScoreSet
from mavedb.routers.shared import BASE_400_RESPONSE, PUBLIC_ERROR_RESPONSES, ROUTER_BASE_PREFIX
from mavedb.view_models import raw_read_identifier
from mavedb.view_models.search import TextSearch

TAG_NAME = "Raw Read Identifiers"

router = APIRouter(
    prefix=f"{ROUTER_BASE_PREFIX}/raw-read-identifiers",
    tags=[TAG_NAME],
    responses={**PUBLIC_ERROR_RESPONSES},
)

metadata = {
    "name": TAG_NAME,
    "description": "Search and retrieve Raw Read identifiers associated with MaveDB records.",
}


@router.post(
    "/search",
    status_code=200,
    response_model=List[raw_read_identifier.RawReadIdentifier],
    responses={**BASE_400_RESPONSE},
    summary="Search Raw Read identifiers",
)
def search_raw_read_identifiers(
    search: TextSearch,
    db: Session = Depends(deps.get_db),
    principal: Principal = Depends(get_principal),
) -> Any:
    """
    Search the Raw Read identifiers cited by records you can read.
    """
    if not search.text or not search.text.strip():
        raise HTTPException(status_code=400, detail="Search text is required")

    return search_identifiers_on_readable_records(
        db,
        principal,
        search.text.strip(),
        [
            (
                ScoreSetRawReadIdentifier,
                ScoreSetViewer,
                select(ScoreSetRawReadIdentifier, ScoreSet).join(
                    ScoreSet, ScoreSet.id == ScoreSetRawReadIdentifier.score_set_id
                ),
            ),
            (
                ExperimentRawReadIdentifier,
                ExperimentViewer,
                select(ExperimentRawReadIdentifier, Experiment).join(
                    Experiment, Experiment.id == ExperimentRawReadIdentifier.experiment_id
                ),
            ),
            (
                ExperimentSetRawReadIdentifier,
                ExperimentSetViewer,
                select(ExperimentSetRawReadIdentifier, ExperimentSet).join(
                    ExperimentSet, ExperimentSet.id == ExperimentSetRawReadIdentifier.experiment_set_id
                ),
            ),
        ],
    )

import logging
from operator import attrgetter
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.authentication import get_current_user
from mavedb.lib.authorization import require_current_user
from mavedb.lib.experiments import search_experiments as _search_experiments
from mavedb.lib.identifiers import (
    find_or_create_doi_identifier,
    find_or_create_publication_identifier,
    find_or_create_raw_read_identifier,
)
from mavedb.lib.permissions import has_permission, Action
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.view_models import experiment, score_set
from mavedb.view_models.search import ExperimentsSearch

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["experiments"], responses={404: {"description": "Not found"}})


@router.get(
    "/experiments/", status_code=200, response_model=list[experiment.Experiment], response_model_exclude_none=True
)
def list_experiments(
    *,
    editable: Optional[bool] = None,
    q: Optional[str] = None,
    db: Session = Depends(deps.get_db),
    user: User = Depends(get_current_user),
) -> list[Experiment]:
    """
    List experiments.
    """
    query = db.query(Experiment)
    if q is not None:
        if user is None:
            return []
        if len(q) > 0:
            query = query.filter(Experiment.created_by_id == user.id)  # .filter(Experiment.published_date is None)
        # else:
        #     query = query.filter(Experiment.created_by_id == user.id).filter(Experiment.published_date is None)
    items = query.order_by(Experiment.urn).all()
    return items


@router.post("/experiments/search", status_code=200, response_model=list[experiment.ShortExperiment])
def search_experiments(search: ExperimentsSearch, db: Session = Depends(deps.get_db)) -> Any:
    """
    Search experiments.
    """
    return _search_experiments(db, None, search)


@router.post("/me/experiments/search", status_code=200, response_model=list[experiment.ShortExperiment])
def search_my_experiments(
    search: ExperimentsSearch, db: Session = Depends(deps.get_db), user: User = Depends(require_current_user)
) -> Any:
    """
    Search experiments created by the current user..
    """
    return _search_experiments(db, user, search)


@router.get(
    "/experiments/{urn}",
    status_code=200,
    response_model=experiment.Experiment,
    responses={404: {}},
    response_model_exclude_none=True,
)
def fetch_experiment(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user: User = Depends(get_current_user),
) -> Experiment:
    """
    Fetch a single experiment by URN.
    """
    # item = db.query(Experiment).filter(Experiment.urn == urn).filter(Experiment.private.is_(False)).first()
    item = db.query(Experiment).filter(Experiment.urn == urn).first()
    if not item:
        raise HTTPException(status_code=404, detail=f"Experiment with URN {urn} not found")
    permission = has_permission(user, item, Action.READ)
    if not permission.permitted:
        raise HTTPException(status_code=permission.http_code, detail=permission.message)
    return item


@router.get(
    "/experiments/{urn}/score-sets",
    status_code=200,
    response_model=list[score_set.ScoreSet],
    responses={404: {}},
    response_model_exclude_none=True,
)
def get_experiment_score_sets(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user: User = Depends(get_current_user),
) -> Any:
    """
    Get all score sets belonging to an experiment.
    """
    experiment = db.query(Experiment).filter(Experiment.urn == urn).first()
    if not experiment:
        raise HTTPException(status_code=404, detail=f"experiment with URN '{urn}' not found")
    permission = has_permission(user, experiment, Action.READ)
    if not permission.permitted:
        raise HTTPException(status_code=permission.http_code, detail=permission.message)
    # Only get published score sets. Unpublished score sets won't be shown on experiment page.
    # score_sets = db.query(ScoreSet).filter(ScoreSet.experiment_id == experiment.id).filter(not ScoreSet.private).all()
    score_sets = db.query(ScoreSet).filter(ScoreSet.experiment_id == experiment.id).filter(ScoreSet.private.is_(False)).all()
    if not score_sets:
        raise HTTPException(status_code=404, detail="no associated score sets")
    else:
        score_sets.sort(key=attrgetter("urn"))
    return score_sets


@router.post(
    "/experiments/", response_model=experiment.Experiment, responses={422: {}}, response_model_exclude_none=True
)
async def create_experiment(
    *,
    item_create: experiment.ExperimentCreate,
    db: Session = Depends(deps.get_db),
    user: User = Depends(require_current_user),
) -> Any:
    """
    Create an experiment.
    """
    if item_create is None:
        return None
    experiment_set = None
    if item_create.experiment_set_urn is not None:
        experiment_set = (
            db.query(ExperimentSet).filter(ExperimentSet.urn == item_create.experiment_set_urn).one_or_none()
        )
        if not experiment_set:
            raise HTTPException(
                status_code=404, detail=f"experiment set with URN '{item_create.experiment_set_urn}' not found."
            )
        permission = has_permission(user, experiment_set, Action.ADD_EXPERIMENT)
        if not permission.permitted:
            raise HTTPException(status_code=permission.http_code, detail=permission.message)
    doi_identifiers = [
        await find_or_create_doi_identifier(db, identifier.identifier)
        for identifier in item_create.doi_identifiers or []
    ]
    raw_read_identifiers = [
        await find_or_create_raw_read_identifier(db, identifier.identifier)
        for identifier in item_create.raw_read_identifiers or []
    ]

    primary_publication_identifiers = [
        await find_or_create_publication_identifier(db, identifier.identifier, identifier.db_name)
        for identifier in item_create.primary_publication_identifiers or []
    ]
    publication_identifiers = [
        await find_or_create_publication_identifier(db, identifier.identifier, identifier.db_name)
        for identifier in item_create.secondary_publication_identifiers or []
    ] + primary_publication_identifiers
    # create a temporary `primary` attribute on each of our publications that indicates
    # to our association proxy whether it is a primary publication or not
    primary_identifiers = [pub.identifier for pub in primary_publication_identifiers]
    for publication in publication_identifiers:
        setattr(publication, "primary", publication.identifier in primary_identifiers)

    item = Experiment(
        **jsonable_encoder(
            item_create,
            by_alias=False,
            exclude={
                "doi_identifiers",
                "experiment_set_urn",
                "keywords",
                "primary_publication_identifiers",
                "secondary_publication_identifiers",
                "raw_read_identifiers",
            },
        ),
        experiment_set=experiment_set,
        doi_identifiers=doi_identifiers,
        publication_identifiers=publication_identifiers,  # an internal association proxy representation
        raw_read_identifiers=raw_read_identifiers,
        created_by=user,
        modified_by=user,
    )
    await item.set_keywords(db, item_create.keywords)
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@router.put(
    "/experiments/{urn}", response_model=experiment.Experiment, responses={422: {}}, response_model_exclude_none=True
)
async def update_experiment(
    *,
    item_update: experiment.ExperimentUpdate,
    urn: str,
    db: Session = Depends(deps.get_db),
    user: User = Depends(require_current_user),
) -> Any:
    """
    Update an experiment.
    """
    if item_update is None:
        return None
    # item = db.query(Experiment).filter(Experiment.urn == urn).filter(Experiment.private.is_(False)).one_or_none()
    item = db.query(Experiment).filter(Experiment.urn == urn).one_or_none()
    if item is None:
        raise HTTPException(status_code=404, detail=f"experiment with URN {urn} not found")
    permission = has_permission(user, item, Action.UPDATE)
    if not permission.permitted:
        raise HTTPException(status_code=permission.http_code, detail=permission.message)

    pairs = {
        k: v
        for k, v in vars(item_update).items()
        if k
        not in [
            "doi_identifiers",
            "keywords",
            "secondary_publication_identifiers",
            "primary_publication_identifiers",
            "raw_read_identifiers",
        ]
    }
    for var, value in pairs.items():  # vars(item_update).items():
        setattr(item, var, value) if value else None

    doi_identifiers = [
        await find_or_create_doi_identifier(db, identifier.identifier)
        for identifier in item_update.doi_identifiers or []
    ]
    raw_read_identifiers = [
        await find_or_create_raw_read_identifier(db, identifier.identifier)
        for identifier in item_update.raw_read_identifiers or []
    ]

    primary_publication_identifiers = [
        await find_or_create_publication_identifier(db, identifier.identifier, identifier.db_name)
        for identifier in item_update.primary_publication_identifiers or []
    ]
    publication_identifiers = [
        await find_or_create_publication_identifier(db, identifier.identifier, identifier.db_name)
        for identifier in item_update.secondary_publication_identifiers or []
    ] + primary_publication_identifiers
    # create a temporary `primary` attribute on each of our publications that indicates
    # to our association proxy whether it is a primary publication or not
    primary_identifiers = [pub.identifier for pub in primary_publication_identifiers]
    for publication in publication_identifiers:
        setattr(publication, "primary", publication.identifier in primary_identifiers)

    item.doi_identifiers = doi_identifiers
    item.publication_identifiers = publication_identifiers
    item.raw_read_identifiers = raw_read_identifiers

    await item.set_keywords(db, item_update.keywords)
    item.modified_by = user

    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@router.delete(
    "/experiments/{urn}", response_model=experiment.Experiment, responses={422: {}}, response_model_exclude_none=True
)
async def delete_experiment(
    *, urn: str, db: Session = Depends(deps.get_db), user: User = Depends(require_current_user)
) -> Any:
    """
    Delete a experiment .

    Raises

    Returns
    _______
    Does not return anything
    string : HTTP code 200 successful but returning content
    or
    communitcate to client whether the operation succeeded
    204 if successful but not returning content - likely going with this
    """
    item = db.query(Experiment).filter(Experiment.urn == urn).one_or_none()
    if not item:
        raise HTTPException(status_code=404, detail=f"experiment with URN '{urn}' not found.")
    permission = has_permission(user, item, Action.DELETE)
    if not permission.permitted:
        raise HTTPException(status_code=permission.http_code, detail=permission.message)

    db.delete(item)
    db.commit()

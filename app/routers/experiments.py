import logging

from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session
from typing import Any, Optional
from operator import attrgetter

from app import deps
from app.lib.auth import get_current_user, require_current_user
from app.lib.experiments import search_experiments as _search_experiments
from app.lib.identifiers import find_or_create_doi_identifier, find_or_create_pubmed_identifier
from app.models.experiment import Experiment
from app.models.user import User
from app.models.scoreset import Scoreset
from app.view_models import experiment,scoreset
from app.view_models.search import ExperimentsSearch

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix='/api/v1',
    tags=['experiments'],
    responses={404: {'description': 'Not found'}}
)


@router.get('/experiments/', status_code=200, response_model=list[experiment.Experiment])
def list_experiments(
    *,
    editable: Optional[bool] = None,
    q: Optional[str] = None,
    db: Session = Depends(deps.get_db),
    user: User = Depends(get_current_user)
) -> list[Experiment]:
    """
    List experiments.
    """
    query = db.query(Experiment)
    if q is not None:
        if user is None:
            logger.error('USER IS NONE')
            return []
        if len(q) > 0:
            logger.error('Here')
            logger.error(user.id)
            query = query.filter(Experiment.created_by_id == user.id) # .filter(Experiment.published_date is None)
        # else:
        #     query = query.filter(Experiment.created_by_id == user.id).filter(Experiment.published_date is None)
    items = query.order_by(Experiment.urn).all()
    logger.error(len(items))
    return items


@router.post(
    '/experiments/search',
    status_code=200,
    response_model=list[experiment.ShortExperiment]
)
#Is this typo? search_experiments may be better. routers/scoresets has a same function.
def search_experiments(
    search: ExperimentsSearch,
    db: Session = Depends(deps.get_db)
) -> Any:
    """
    Search experiments.
    """
    return _search_experiments(db, None, search)


@router.post(
    '/me/experiments/search',
    status_code=200,
    response_model=list[experiment.ShortExperiment]
)
def search_my_experiments(
    search: ExperimentsSearch,
    db: Session = Depends(deps.get_db),
    user: User = Depends(require_current_user)
) -> Any:
    """
    Search experiments created by the current user..
    """
    return _search_experiments(db, user, search)


@router.get('/experiments/{urn}', status_code=200, response_model=experiment.Experiment, responses={404: {}})
def fetch_experiment(
    *,
    urn: str,
    db: Session = Depends(deps.get_db)
) -> Experiment:
    '''
    Fetch a single experiment by URN.
    '''
    #item = db.query(Experiment).filter(Experiment.urn == urn).filter(Experiment.private.is_(False)).first()
    item = db.query(Experiment).filter(Experiment.urn == urn).first()
    if not item:
        raise HTTPException(
            status_code=404, detail=f'Experiment with URN {urn} not found'
        )
    return item

@router.get('/experiments/{urn}/associated_scoresets', status_code=200, response_model=list[scoreset.Scoreset], responses={404:{}})
def get_experiment_all_scoresets(
    *,
    urn: str,
    db: Session = Depends(deps.get_db)
) -> Any:
    '''
    Get all of scoresets for an experiment
    '''
    experiment = db.query(Experiment).filter(Experiment.urn == urn).first()
    if not experiment:
        raise HTTPException(
            status_code=404, detail=f'Experiment with URN {urn} not found'
        )
    #Only get published scoreset. Unpublished scoreset won't be shown on experiment page.
    scoreset_list = db.query(Scoreset).filter(Scoreset.experiment_id == experiment.id).filter(Scoreset.urn.contains('urn')).all()
    if not scoreset_list:
        raise HTTPException(
            status_code=404, detail=f'No associated score set'
        )
    else:
        scoreset_list.sort(key=attrgetter('urn'))
    return scoreset_list

@router.post("/experiments/", response_model=experiment.Experiment, responses={422: {}})
async def create_experiment(
    *,
    item_create: experiment.ExperimentCreate,
    db: Session = Depends(deps.get_db),
    user: User = Depends(require_current_user)
) -> Any:
    """
    Create an experiment.
    """
    if item_create is None:
        return None
    doi_identifiers = [await find_or_create_doi_identifier(db, identifier.identifier) for identifier in item_create.doi_identifiers or []]
    pubmed_identifiers = [await find_or_create_pubmed_identifier(db, identifier.identifier) for identifier in item_create.pubmed_identifiers or []]
    item = Experiment(
        **jsonable_encoder(item_create, by_alias=False, exclude=['doi_identifiers', 'keywords', 'pubmed_identifiers']),
        doi_identifiers=doi_identifiers,
        pubmed_identifiers=pubmed_identifiers,
        created_by=user,
        modified_by=user
    )
    await item.set_keywords(db, item_create.keywords)
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@router.put("/experiments/{urn}", response_model=experiment.Experiment, responses={422: {}})
async def update_experiment(
    *,
    item_update: experiment.ExperimentUpdate,
    urn: str,
    db: Session = Depends(deps.get_db),
    user: User = Depends(require_current_user)
) -> Any:
    '''
    Update an experiment.
    '''
    if item_update is None:
        return None
    #item = db.query(Experiment).filter(Experiment.urn == urn).filter(Experiment.private.is_(False)).one_or_none()
    item = db.query(Experiment).filter(Experiment.urn == urn).one_or_none()
    if item is None:
        return None

    pairs = {k: v for k, v in vars(item_update).items() if k not in ['doi_identifiers', 'keywords', 'pubmed_identifiers']}
    for var, value in pairs.items():  # vars(item_update).items():
        setattr(item, var, value) if value else None

    doi_identifiers = [await find_or_create_doi_identifier(db, identifier.identifier) for identifier in item_update.doi_identifiers or []]
    pubmed_identifiers = [await find_or_create_pubmed_identifier(db, identifier.identifier) for identifier in item_update.pubmed_identifiers or []]
    item.doi_identifiers = doi_identifiers
    item.pubmed_identifiers = pubmed_identifiers

    await item.set_keywords(db, item_update.keywords)
    item.modified_by = user

    db.add(item)
    db.commit()
    db.refresh(item)
    return item

@router.delete("/experiments/{urn}", response_model=experiment.Experiment, responses={422: {}})
async def delete_experiment(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user: User = Depends(require_current_user)
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
        raise HTTPException(
            status_code=404, detail=f'Experiment with URN {urn} not found.'
        )

    db.delete(item)
    db.commit()
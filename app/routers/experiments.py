from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session
from typing import Any

from app import deps
from app.lib.auth import require_current_user
from app.models.experiment import Experiment
from app.models.user import User
from app.view_models import experiment

router = APIRouter(
    prefix='/api/v1/experiments',
    tags=['experiments'],
    responses={404: {'description': 'Not found'}}
)


@router.get('/{urn}', status_code=200, response_model=experiment.Experiment, responses={404: {}})
def fetch_experiment(
    *,
    urn: str,
    db: Session = Depends(deps.get_db)
) -> Any:
    '''
    Fetch a single experiment by URN.
    '''
    item = db.query(Experiment).filter(Experiment.urn == urn).filter(Experiment.private.is_(False)).first()
    if not item:
        raise HTTPException(
            status_code=404, detail=f'Experiment with URN {urn} not found'
        )
    return item


@router.post("/", response_model=experiment.Experiment, responses={422: {}})
async def create_experiment(
    *,
    item_create: experiment.ExperimentCreate,
    db: Session = Depends(deps.get_db),
    user: User = Depends(require_current_user)
) -> Any:
    '''
    Create an experiment.
    '''
    if item_create is None:
        return None
    item = Experiment(
        **jsonable_encoder(item_create, by_alias=False),
        created_by=user
    )  # type: ignore
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@router.put("/{urn}", response_model=experiment.Experiment, responses={422: {}})
async def update_experiment(
    *,
    urn: str,
    item_update: experiment.ExperimentUpdate,
    db: Session = Depends(deps.get_db)
) -> Any:
    '''
    Update an experiment.
    '''
    if item_update is None:
        return None
    item = db.query(Experiment).filter(Experiment.urn == urn).filter(Experiment.private.is_(False)).one_or_none()
    if item is None:
        return None
    for var, value in vars(item_update).items():
        setattr(item, var, value) if value else None
    db.add(item)
    db.commit()
    db.refresh(item)
    return item

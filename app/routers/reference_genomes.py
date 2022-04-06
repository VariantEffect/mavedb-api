from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Any, List


from app import deps
from app.models.reference_genome import ReferenceGenome
from app.view_models import reference_genome


router = APIRouter(
    prefix='/api/v1/referenceGenomes',
    tags=['reference-genomes'],
    responses={404: {'description': 'Not found'}}
)


@router.get('/', status_code=200, response_model=List[reference_genome.ReferenceGenome], responses={404: {}})
def list_reference_genomes(
    *,
    db: Session = Depends(deps.get_db),
) -> Any:
    '''
    List reference genomes.
    '''
    items = db.query(ReferenceGenome).order_by(ReferenceGenome.short_name).all()
    return items


@router.get('/{item_id}', status_code=200, response_model=reference_genome.ReferenceGenome, responses={404: {}})
def fetch_reference_genome(
    *,
    item_id: int,
    db: Session = Depends(deps.get_db),
) -> Any:
    '''
    Fetch a single reference genome by ID.
    '''
    item = db.query(ReferenceGenome).filter(ReferenceGenome.id == item_id).first()
    if not item:
        raise HTTPException(
            status_code=404, detail=f'ReferenceGenome with ID {item_id} not found'
        )
    return item

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Any, List


from app import deps
from app.models.target_gene import TargetGene
from app.view_models import target_gene


router = APIRouter(
    prefix='/api/v1/targetGenes',
    tags=['target-genes'],
    responses={404: {'description': 'Not found'}}
)


@router.get('/', status_code=200, response_model=List[target_gene.TargetGene], responses={404: {}})
def list_target_genes(
    *,
    db: Session = Depends(deps.get_db),
) -> Any:
    '''
    List target genes.
    '''
    items = db.query(TargetGene).order_by(TargetGene.name).all()
    return items


@router.get('/{item_id}', status_code=200, response_model=target_gene.TargetGene, responses={404: {}})
def fetch_target_gene(
    *,
    item_id: int,
    db: Session = Depends(deps.get_db),
) -> Any:
    '''
    Fetch a single target gene by ID.
    '''
    item = db.query(TargetGene).filter(TargetGene.id == item_id).first()
    if not item:
        raise HTTPException(
            status_code=404, detail=f'TargetGene with ID {item_id} not found'
        )
    return item

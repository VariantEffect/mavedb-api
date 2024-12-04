from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.authentication import UserData
from mavedb.lib.authorization import require_current_user
from mavedb.lib.target_genes import (
    search_target_genes as _search_target_genes,
)
from mavedb.models.target_gene import TargetGene
from mavedb.view_models import target_gene
from mavedb.view_models.search import TextSearch

router = APIRouter(prefix="/api/v1", tags=["target-genes"], responses={404: {"description": "Not found"}})


@router.post("/me/target-genes/search", status_code=200, response_model=List[target_gene.TargetGene])
def search_my_target_genes(
    search: TextSearch,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user)
) -> Any:
    """
    Search my target genes.
    """

    return _search_target_genes(db, user_data.user, search, 50)


@router.get("/target-genes", status_code=200, response_model=List[target_gene.TargetGene], responses={404: {}})
def list_target_genes(
    *,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    List target genes.
    """
    items = db.query(TargetGene).order_by(TargetGene.name).all()
    return items


@router.get("/target-genes/names", status_code=200, response_model=List[str], responses={404: {}})
def list_target_gene_names(
    *,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    List distinct target gene names, in alphabetical order.
    """

    items = db.query(TargetGene).all()
    names = map(lambda item: item.name, items)
    return sorted(list(set(names)))


@router.get("/target-genes/categories", status_code=200, response_model=List[str], responses={404: {}})
def list_target_gene_categories(
    *,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    List distinct target genes categories, in alphabetical order.
    """

    items = db.query(TargetGene).all()
    categories = map(lambda item: item.category, items)
    return sorted(list(set(categories)))


@router.get("/target-genes/{item_id}", status_code=200, response_model=target_gene.TargetGene, responses={404: {}})
def fetch_target_gene(
    *,
    item_id: int,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    Fetch a single target gene by ID.
    """
    item = db.query(TargetGene).filter(TargetGene.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail=f"TargetGene with ID {item_id} not found")
    return item


@router.post("/target-genes/search", status_code=200, response_model=List[target_gene.TargetGene])
def search_target_genes(
    search: TextSearch,
    db: Session = Depends(deps.get_db)
) -> Any:
    """
    Search target genes.
    """

    return _search_target_genes(db, None, search, 50)

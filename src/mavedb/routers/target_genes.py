from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session, selectinload

from mavedb import deps
from mavedb.lib.authentication import UserData, get_current_user
from mavedb.lib.authorization import require_current_user
from mavedb.lib.permissions import Action, has_permission
from mavedb.lib.score_sets import find_superseded_score_set_tail
from mavedb.lib.target_genes import (
    search_target_genes as _search_target_genes,
)
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_gene import TargetGene
from mavedb.view_models import target_gene
from mavedb.view_models.search import TextSearch

router = APIRouter(
    prefix="/api/v1",
    tags=["Target Genes"],
    responses={
        404: {"description": "Not found"},
        500: {"description": "Internal server error"},
    },
)


@router.post(
    "/me/target-genes/search",
    status_code=200,
    response_model=List[target_gene.TargetGeneWithScoreSetUrn],
    responses={
        401: {"description": "Not authenticated"},
        403: {"description": "User lacks necessary permissions"},
    },
    summary="Search my target genes",
)
def search_my_target_genes(
    search: TextSearch, db: Session = Depends(deps.get_db), user_data: UserData = Depends(require_current_user)
) -> Any:
    """
    Search my target genes.
    """
    items = _search_target_genes(db, user_data.user, search, 50)

    return [i for i in items if i.score_set.superseding_score_set is None]


@router.get(
    "/target-genes",
    status_code=200,
    response_model=List[target_gene.TargetGeneWithScoreSetUrn],
    responses={
        401: {"description": "Not authenticated"},
        403: {"description": "User lacks necessary permissions"},
    },
    summary="List target genes",
)
def list_target_genes(
    *,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> Any:
    """
    List target genes.
    Selectinload is more efficient if we need more queries search.
    """
    items = (
        db.query(TargetGene)
        .options(selectinload(TargetGene.score_set).selectinload(ScoreSet.superseding_score_set))
        .all()
    )
    validated_items = []
    for gene in items:
        latest_score_set = find_superseded_score_set_tail(gene.score_set, Action.READ, user_data)
        if latest_score_set and gene.score_set.urn == latest_score_set.urn:
            validated_items.append(gene)
    return sorted(validated_items, key=lambda i: i.name)


@router.get("/target-genes/names", status_code=200, response_model=List[str], summary="List target gene names")
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


@router.get(
    "/target-genes/categories", status_code=200, response_model=List[str], summary="List target gene categories"
)
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


@router.get(
    "/target-genes/{item_id}",
    status_code=200,
    response_model=target_gene.TargetGeneWithScoreSetUrn,
    summary="Fetch target gene by ID",
)
def fetch_target_gene(
    *,
    item_id: int,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> Any:
    """
    Fetch a single target gene by ID. Only check the permission
    """
    item = db.query(TargetGene).filter(TargetGene.id == item_id).first()
    if not item or not has_permission(user_data, item.score_set, Action.READ).permitted:
        raise HTTPException(status_code=404, detail=f"TargetGene with ID {item_id} not found")
    return item


@router.post(
    "/target-genes/search",
    status_code=200,
    response_model=List[target_gene.TargetGeneWithScoreSetUrn],
    responses={
        401: {"description": "Not authenticated"},
        403: {"description": "User lacks necessary permissions"},
    },
    summary="Search target genes",
)
def search_target_genes(
    search: TextSearch, db: Session = Depends(deps.get_db), user_data: Optional[UserData] = Depends(get_current_user)
) -> Any:
    """
    Search target genes.
    """
    items = _search_target_genes(db, None, search, 50)
    validated_items = []
    for gene in items:
        latest_score_set = find_superseded_score_set_tail(gene.score_set, Action.READ, user_data)
        if latest_score_set and gene.score_set.urn == latest_score_set.urn:
            validated_items.append(gene)
    return sorted(validated_items, key=lambda i: i.name)

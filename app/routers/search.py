from fastapi import APIRouter, Depends
from sqlalchemy import or_, func
from sqlalchemy.orm import Session
from typing import Any, List

from app import deps
from app.lib.auth import JWTBearer, get_current_user
from app.models.experiment import Experiment
from app.models.keyword import Keyword
from app.models.reference_genome import ReferenceGenome
from app.models.reference_map import ReferenceMap
from app.models.scoreset import Scoreset
from app.models.target_gene import TargetGene
import app.view_models.scoreset
from app.view_models.search import Search


router = APIRouter(
    prefix="/api/v1",
    tags=["scoresets"],
    responses={404: {"description": "Not found"}}
)


@router.post(
    '/scoresets/search',
    status_code=200,
    # dependencies=[Depends(JWTBearer())],
    response_model=List[app.view_models.scoreset.Scoreset]
)
def search_scoresets(
    search: Search,  # = Body(..., embed=True),
    db: Session = Depends(deps.get_db),
    user: str = Depends(get_current_user)
) -> Any:
    """
    Search scoresets.
    """

    scoresets_query = db.query(Scoreset)\
        .filter(Scoreset.private.is_(False))

    if search.text:
        lower_search_text = search.text.lower()
        scoresets_query = scoresets_query.filter(or_(
            Scoreset.target_gene.has(func.lower(TargetGene.name).contains(lower_search_text)),
            Scoreset.target_gene.has(func.lower(TargetGene.category).contains(lower_search_text)),
            Scoreset.keyword_objs.any(func.lower(Keyword.text).contains(lower_search_text))
            # Add: ORGANISM_NAME UNIPROT, ENSEMBL, REFSEQ, LICENSE, plus TAX_ID if numeric
        ))

    if search.targets:
        scoresets_query = scoresets_query.filter(
            Scoreset.target_gene.has(TargetGene.name.in_(search.targets))
        )

    if search.target_organism_names:
        scoresets_query = scoresets_query.filter(
            Scoreset.target_gene.has(TargetGene.reference_maps.any(ReferenceMap.genome.has(ReferenceGenome.organism_name.in_(search.target_organism_names))))
        )

    if search.target_types:
        scoresets_query = scoresets_query.filter(
            Scoreset.target_gene.has(TargetGene.category.in_(search.target_types))
        )

    scoresets = scoresets_query\
        .join(Scoreset.experiment)\
        .join(Scoreset.target_gene)\
        .order_by(Experiment.title)\
        .all()
    if not scoresets:
        scoresets = []
    return scoresets  # filter_visible_scoresets(scoresets)

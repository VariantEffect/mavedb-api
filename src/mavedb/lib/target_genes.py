import logging
from typing import Optional

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.models.contributor import Contributor
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_gene import TargetGene
from mavedb.models.user import User
from mavedb.view_models.search import TextSearch

logger = logging.getLogger(__name__)


def search_target_genes(
    db: Session,
    owner_or_contributor: Optional[User],
    search: TextSearch,
    limit: Optional[int],
) -> list[TargetGene]:
    save_to_logging_context({"target_gene_search_criteria": search.dict()})

    query = db.query(TargetGene)

    if search.text and len(search.text.strip()) > 0:
        lower_search_text = search.text.strip().lower()
        query = query.filter(func.lower(TargetGene.name).contains(lower_search_text))
    if owner_or_contributor is not None:
        query = query.filter(
            TargetGene.score_set.has(
                or_(
                    ScoreSet.created_by_id == owner_or_contributor.id,
                    ScoreSet.contributors.any(
                        Contributor.orcid_id == owner_or_contributor.username
                    ),
                )
            )
        )

    query = query.order_by(TargetGene.name)
    if limit is not None:
        query = query.limit(limit)

    target_genes = query.all()
    if not target_genes:
        target_genes = []

    save_to_logging_context({"matching_resources": len(target_genes)})
    logger.debug(
        msg=f"Target gene search yielded {len(target_genes)} matching resources.",
        extra=logging_context(),
    )

    return target_genes

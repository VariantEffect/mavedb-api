import logging
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload, selectinload
from starlette.convertors import Convertor, register_url_convertor

from mavedb import deps
from mavedb.lib.experiments import enrich_experiment_with_num_score_sets
from mavedb.lib.hgnc.client import fetch_gene_info
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.models.ensembl_offset import EnsemblOffset
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_controlled_keyword import ExperimentControlledKeywordAssociation
from mavedb.models.experiment_publication_identifier import ExperimentPublicationIdentifierAssociation
from mavedb.models.refseq_offset import RefseqOffset
from mavedb.models.score_set import ScoreSet
from mavedb.models.score_set_publication_identifier import ScoreSetPublicationIdentifierAssociation
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_sequence import TargetSequence
from mavedb.models.uniprot_offset import UniprotOffset
from mavedb.routers.shared import GATEWAY_ERROR_RESPONSES, PUBLIC_ERROR_RESPONSES, ROUTER_BASE_PREFIX
from mavedb.view_models.gene import GeneResponse
from mavedb.view_models.score_set import ShortScoreSet

TAG_NAME = "Genes"
logger = logging.getLogger(__name__)

GENE_SCORE_SETS_MAX_LIMIT = 100


# See the equivalent pattern in publication_identifiers.py for context on this approach.
# HGNC-approved symbols contain uppercase Latin letters and Arabic numerals, with hyphens
# allowed for specific gene groups (e.g. HLA-A, BRCA1).
class GeneSymbolConverter(Convertor):
    regex = r"[A-Za-z][A-Za-z0-9-]*"

    def convert(self, value: str) -> str:
        return value

    def to_string(self, value: str) -> str:
        return str(value)


register_url_convertor("gene_symbol", GeneSymbolConverter())

router = APIRouter(
    prefix=f"{ROUTER_BASE_PREFIX}/genes",
    tags=[TAG_NAME],
    responses={**PUBLIC_ERROR_RESPONSES, **GATEWAY_ERROR_RESPONSES},
    route_class=LoggedRoute,
)

metadata = {
    "name": TAG_NAME,
    "description": "Retrieve gene identity and associated public MaveDB score sets.",
}


def _gene_score_set_base_query(db: Session, symbol: str):
    return db.query(ScoreSet).filter(
        ScoreSet.target_genes.any(TargetGene.mapped_hgnc_name == symbol),
        ScoreSet.private.is_(False),
        ScoreSet.published_date.isnot(None),
        ~ScoreSet.superseding_score_set.has(ScoreSet.published_date.isnot(None)),
    )


def _score_set_load_options():
    return (
        selectinload(ScoreSet.experiment).options(
            selectinload(Experiment.experiment_set),
            selectinload(Experiment.keyword_objs).joinedload(ExperimentControlledKeywordAssociation.controlled_keyword),
            selectinload(Experiment.created_by),
            selectinload(Experiment.modified_by),
            selectinload(Experiment.doi_identifiers),
            selectinload(Experiment.publication_identifier_associations).joinedload(
                ExperimentPublicationIdentifierAssociation.publication
            ),
            selectinload(Experiment.raw_read_identifiers),
            selectinload(Experiment.score_sets),
            selectinload(Experiment.official_collections),
        ),
        selectinload(ScoreSet.license),
        selectinload(ScoreSet.doi_identifiers),
        selectinload(ScoreSet.publication_identifier_associations).joinedload(
            ScoreSetPublicationIdentifierAssociation.publication
        ),
        selectinload(ScoreSet.target_genes).options(
            joinedload(TargetGene.ensembl_offset).joinedload(EnsemblOffset.identifier),
            joinedload(TargetGene.refseq_offset).joinedload(RefseqOffset.identifier),
            joinedload(TargetGene.uniprot_offset).joinedload(UniprotOffset.identifier),
            joinedload(TargetGene.target_sequence).joinedload(TargetSequence.taxonomy),
            joinedload(TargetGene.target_accession),
        ),
    )


@router.get(
    "/{symbol:gene_symbol}",
    status_code=200,
    response_model=GeneResponse,
    response_model_exclude_none=True,
    summary="Fetch a gene and associated published score sets",
)
def get_gene(
    symbol: str,
    limit: int = Query(
        default=20,
        ge=1,
        le=GENE_SCORE_SETS_MAX_LIMIT,
        description=f"Number of score sets to return (maximum {GENE_SCORE_SETS_MAX_LIMIT}).",
    ),
    offset: int = Query(default=0, ge=0, description="Number of score sets to skip."),
    db: Session = Depends(deps.get_db),
) -> Any:
    save_to_logging_context({"requested_resource": "gene", "hgnc_symbol": symbol, "limit": limit, "offset": offset})
    gene_info = fetch_gene_info(symbol)

    base_query = _gene_score_set_base_query(db, gene_info.symbol)
    total = base_query.order_by(None).limit(None).offset(None).count()
    total_scored_variants = (
        base_query.order_by(None)
        .limit(None)
        .offset(None)
        .with_entities(func.coalesce(func.sum(ScoreSet.num_variants), 0))
        .scalar()
    )
    score_sets = (
        base_query.options(*_score_set_load_options())
        .order_by(ScoreSet.published_date.desc(), ScoreSet.urn.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )

    logger.debug(msg=f"Gene endpoint yielded {len(score_sets)} score sets.", extra=logging_context())
    response_score_sets = []
    for score_set in score_sets:
        enriched_experiment = enrich_experiment_with_num_score_sets(score_set.experiment, None)
        response_score_sets.append(
            ShortScoreSet.model_validate(score_set).model_copy(update={"experiment": enriched_experiment})
        )

    return GeneResponse(
        symbol=gene_info.symbol,
        name=gene_info.name,
        hgnc_id=gene_info.hgnc_id,
        locus_group=gene_info.locus_group,
        location=gene_info.location,
        omim_id=gene_info.omim_id,
        score_sets=response_score_sets,
        limit=limit,
        offset=offset,
        total=total,
        total_scored_variants=total_scored_variants,
    )

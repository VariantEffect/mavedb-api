import logging
from typing import Sequence

import click
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.gnomad import gnomad_variant_data_for_caids, link_gnomad_variants_to_mapped_variants
from mavedb.models.score_set import ScoreSet
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.variant import Variant
from mavedb.scripts.environment import with_database_session


logger = logging.getLogger(__name__)


@click.command()
@with_database_session
@click.option(
    "--score-set-urn", multiple=True, type=str, help="Score set URN(s) to process. Can be used multiple times."
)
@click.option("--all", "all_score_sets", is_flag=True, help="Process all score sets in the database.", default=False)
@click.option("--only-current", is_flag=True, help="Only process current mapped variants.", default=True)
def link_gnomad_variants(db: Session, score_set_urn: list[str], all_score_sets: bool, only_current: bool) -> None:
    """
    Query AWS Athena for gnomAD variants matching mapped variant CAIDs for one or more score sets.
    """
    # 1. Collect all CAIDs for mapped variants in the selected score sets
    if all_score_sets:
        score_sets = db.query(ScoreSet.id).all()
        score_set_ids = [s.id for s in score_sets]
    else:
        if not score_set_urn:
            logger.error("No score set URNs specified.")
            return

        score_sets = db.query(ScoreSet.id).filter(ScoreSet.urn.in_(score_set_urn)).all()
        score_set_ids = [s.id for s in score_sets]
        if len(score_set_ids) != len(score_set_urn):
            logger.warning("Some provided URNs were not found in the database.")

    if not score_set_ids:
        logger.error("No score sets found.")
        return

    caid_query = (
        select(MappedVariant.clingen_allele_id)
        .join(Variant)
        .where(Variant.score_set_id.in_(score_set_ids), MappedVariant.clingen_allele_id.is_not(None))
    )

    if only_current:
        caid_query = caid_query.where(MappedVariant.current.is_(True))

    # We filter out Nonetype CAIDs to avoid issues with Athena queries, so we can type this as Sequence[str] and ignore MyPy warnings
    caids: Sequence[str] = db.scalars(caid_query.distinct()).all()  # type: ignore
    if not caids:
        logger.error("No CAIDs found for the selected score sets.")
        return

    logger.info(f"Found {len(caids)} CAIDs for the selected score sets to link to gnomAD variants.")

    # 2. Query Athena for gnomAD variants matching the CAIDs
    gnomad_variant_data = gnomad_variant_data_for_caids(caids)

    if not gnomad_variant_data:
        logger.error("No gnomAD records found for the provided CAIDs.")
        return

    logger.info(f"Fetched {len(gnomad_variant_data)} gnomAD records from Athena.")

    # 3. Link gnomAD variants to mapped variants in the database
    link_gnomad_variants_to_mapped_variants(db, gnomad_variant_data, only_current=only_current)

    logger.info("Done linking gnomAD variants.")


if __name__ == "__main__":
    link_gnomad_variants()

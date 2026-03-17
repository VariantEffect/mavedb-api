import logging
from typing import Sequence

import click
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.gnomad import (
    gnomad_queryable_caids_for_clingen_allele_ids,
    gnomad_variant_data_for_caids,
    link_gnomad_variants_to_mapped_variants,
)
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
    Query AWS Athena for gnomAD variants matching mapped variant ClinGen allele IDs for one or more score sets.
    """
    # 1. Collect all ClinGen allele IDs for mapped variants in the selected score sets
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

    clingen_allele_id_query = (
        select(MappedVariant.clingen_allele_id)
        .join(Variant)
        .where(Variant.score_set_id.in_(score_set_ids), MappedVariant.clingen_allele_id.is_not(None))
    )

    if only_current:
        clingen_allele_id_query = clingen_allele_id_query.where(MappedVariant.current.is_(True))

    # We filter out None values in the query above, so this can be safely typed as Sequence[str].
    clingen_allele_ids: Sequence[str] = db.scalars(clingen_allele_id_query.distinct()).all()  # type: ignore
    if not clingen_allele_ids:
        logger.error("No ClinGen allele IDs found for the selected score sets.")
        return

    caids = gnomad_queryable_caids_for_clingen_allele_ids(db, clingen_allele_ids)
    if not caids:
        logger.error("No queryable CAIDs found for the selected score sets.")
        return

    logger.info(f"Found {len(caids)} queryable CAIDs for the selected score sets to link to gnomAD variants.")

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

import click
import logging
from typing import Sequence

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from mavedb.lib.clingen.linked_data_hub import get_clingen_variation, clingen_allele_id_from_ldh_variation
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.mapped_variant import MappedVariant
from mavedb.scripts.environment import with_database_session

logger = logging.getLogger(__name__)


@click.command()
@with_database_session
@click.argument("urns", nargs=-1)
@click.option("--score-sets/--variants", default=False)
@click.option("--unlinked", default=False, is_flag=True)
def link_clingen_variants(db: Session, urns: Sequence[str], score_sets: bool, unlinked: bool) -> None:
    """
    Submit data to ClinGen for mapped variant allele ID generation for the given URNs.
    """
    if not urns:
        logger.error("No URNs provided. Please provide at least one URN.")
        return

    # Convert score set URNs to variant URNs.
    if score_sets:
        query = (
            select(Variant.urn)
            .join(MappedVariant)
            .join(ScoreSet)
            .where(MappedVariant.current.is_(True), MappedVariant.post_mapped.is_not(None))
        )

        if unlinked:
            query = query.where(MappedVariant.clingen_allele_id.is_(None))

        variants = [db.scalars(query.where(ScoreSet.urn == urn)).all() for urn in urns]
        urns = [variant for sublist in variants for variant in sublist if variant is not None]

    failed_urns = []
    for urn in urns:
        ldh_variation = get_clingen_variation(urn)
        allele_id = clingen_allele_id_from_ldh_variation(ldh_variation)

        if not allele_id:
            failed_urns.append(urn)
            continue

        mapped_variant = db.scalar(select(MappedVariant).join(Variant).where(and_(Variant.urn == urn, MappedVariant.current.is_(True))))

        if not mapped_variant:
            logger.warning(f"No mapped variant found for URN {urn}.")
            failed_urns.append(urn)
            continue

        mapped_variant.clingen_allele_id = allele_id
        db.add(mapped_variant)

        logger.info(f"Successfully linked URN {urn} to ClinGen variation {allele_id}.")

    if failed_urns:
        logger.warning(f"Failed to link the following {len(failed_urns)} URNs: {', '.join(failed_urns)}")

    logger.info(f"Linking process completed. Linked {len(urns) - len(failed_urns)}/{len(urns)} URNs successfully.")


if __name__ == "__main__":
    link_clingen_variants()

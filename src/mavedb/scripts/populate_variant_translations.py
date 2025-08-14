import logging
from typing import Sequence, Optional

import click
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.clingen.allele_registry import get_canonical_pa_ids, get_matching_registered_ca_ids
from mavedb.lib.logging.context import format_raised_exception_info_as_dict

from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.variant_translation import VariantTranslation

from mavedb.scripts.environment import script_environment, with_database_session

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@script_environment.command()
@with_database_session
@click.argument("urns", nargs=-1)
@click.option("--all", help="Populate mapped variants for every score set in MaveDB.", is_flag=True)
def populate_variant_translations(db: Session, urns: Sequence[Optional[str]], all: bool):
    # TODO keep track of what has been processed.
    # I think this makes sense to track on the mapped variant level in order to allow
    # for individual variant translation failure, and also so that we don't have to reset the
    # score set log to unprocessed if we redo a mapping. Since we create new mapped variant entries
    # if a scoreset is remapped, we can just update the processed column once per mapped variant.
    # However, this will also require keeping track of exactly what mapped variants fail here.
    # Skipping this for now.

    score_set_ids: Sequence[Optional[int]]
    if all:
        score_set_ids = db.scalars(select(ScoreSet.id)).all()
        logger.info(
            f"Command invoked with --all. Routine will populate mapped variant data for {len(urns)} score sets."
        )
    else:
        score_set_ids = db.scalars(select(ScoreSet.id).where(ScoreSet.urn.in_(urns))).all()
        logger.info(f"Populating mapped variant data for the provided score sets ({len(urns)}).")

    for idx, ss_id in enumerate(score_set_ids):
        if not ss_id:
            continue

        score_set = db.scalar(select(ScoreSet).where(ScoreSet.id == ss_id))
        if not score_set:
            logger.warning(f"Could not fetch score set with id={ss_id}.")
            continue

        clingen_allele_ids = db.scalars(
            select(MappedVariant.clingen_allele_id)
            .join(Variant)
            .join(ScoreSet)
            .where(ScoreSet.id == ss_id)
            .where(MappedVariant.current)
        ).all()
        logger.info(
            f"Found {len(clingen_allele_ids)} clingen allele IDs in the database associated with this score set."
        )

        # treat multi-variants separately
        expanded_allele_ids = []
        for allele_id in clingen_allele_ids:
            if not allele_id:
                continue
            if "," in allele_id:
                expanded_allele_ids.extend([single_allele_id for single_allele_id in allele_id.split(",")])
            else:
                expanded_allele_ids.append(allele_id)

        for allele_id in set(expanded_allele_ids):
            try:
                if allele_id.startswith("CA"):
                    # Get the canonical PA ID(s) from the ClinGen API
                    canonical_pa_ids = get_canonical_pa_ids(allele_id)
                    if not canonical_pa_ids:
                        logger.warning(
                            f"No canonical PA IDs found for {allele_id}. This may be expected if the query is noncoding."
                        )
                        continue
                    for pa_id in canonical_pa_ids:
                        existing_variant_translation = db.scalars(
                            select(VariantTranslation).where(
                                VariantTranslation.aa_clingen_id == pa_id, VariantTranslation.nt_clingen_id == allele_id
                            )
                        ).one_or_none()
                        if not existing_variant_translation:
                            db.add(
                                VariantTranslation(
                                    aa_clingen_id=pa_id,
                                    nt_clingen_id=allele_id,
                                )
                            )
                            # commit after each addition in order to query the database for existing variant translations
                            db.commit()

                        # For each canonical PA ID, get the matching registered transcript CA IDs
                        ca_ids = get_matching_registered_ca_ids(pa_id)
                        if not ca_ids:
                            logger.warning(f"No matching registered transcript CA IDs found for {pa_id}.")
                            continue
                        for ca_id in ca_ids:
                            existing_variant_translation = db.scalars(
                                select(VariantTranslation).where(
                                    VariantTranslation.aa_clingen_id == pa_id, VariantTranslation.nt_clingen_id == ca_id
                                )
                            ).one_or_none()
                            if not existing_variant_translation:
                                db.add(
                                    VariantTranslation(
                                        aa_clingen_id=pa_id,
                                        nt_clingen_id=ca_id,
                                    )
                                )
                                db.commit()

                elif allele_id.startswith("PA"):
                    # Get the matching registered transcript CA IDs from the ClinGen API
                    ca_ids = get_matching_registered_ca_ids(allele_id)
                    if not ca_ids:
                        logger.warning(
                            f"No matching registered transcript CA IDs found for {allele_id}. This is unexpected."
                        )
                        continue
                    for ca_id in ca_ids:
                        existing_variant_translation = db.scalars(
                            select(VariantTranslation).where(
                                VariantTranslation.aa_clingen_id == allele_id, VariantTranslation.nt_clingen_id == ca_id
                            )
                        ).one_or_none()
                        if not existing_variant_translation:
                            db.add(
                                VariantTranslation(
                                    aa_clingen_id=allele_id,
                                    nt_clingen_id=ca_id,
                                )
                            )
                            db.commit()

                else:
                    logger.warning(f"Invalid clingen allele ID format: {allele_id}")

            except Exception as e:
                logging_context = {
                    "processed_score_sets": urns[:idx],
                    "unprocessed_score_sets": urns[idx:],
                }
                logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
                logger.error(f"Unexpected error processing clingen allele ID {allele_id}: {e}")
                db.rollback()

        logger.info(f"Done with score set {score_set.urn}. ({idx+1}/{len(urns)}).")

    logger.info("Done populating variant translations.")


if __name__ == "__main__":
    populate_variant_translations()

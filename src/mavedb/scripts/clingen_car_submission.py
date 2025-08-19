import click
import logging
from typing import Sequence
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.mapped_variant import MappedVariant
from mavedb.scripts.environment import with_database_session
from mavedb.lib.clingen.services import ClinGenAlleleRegistryService, get_allele_registry_associations
from mavedb.lib.clingen.constants import CAR_SUBMISSION_ENDPOINT
from mavedb.lib.variants import get_single_variation_hgvs_from_post_mapped

logger = logging.getLogger(__name__)


def submit_urns_to_car(db: Session, urns: Sequence[str], debug: bool) -> list[str]:
    if not CAR_SUBMISSION_ENDPOINT:
        logger.error("`CAR_SUBMISSION_ENDPOINT` is not set. Please check your configuration.")
        return []

    car_service = ClinGenAlleleRegistryService(url=CAR_SUBMISSION_ENDPOINT)
    submitted_entities = []

    if debug:
        logger.debug("Debug mode enabled. Submitting only one request to ClinGen CAR.")
        urns = urns[:1]

    for idx, urn in enumerate(urns):
        logger.info(f"Processing URN: {urn}. (Scoreset {idx + 1}/{len(urns)})")
        try:
            score_set = db.scalars(select(ScoreSet).where(ScoreSet.urn == urn)).one_or_none()
            if not score_set:
                logger.warning(f"No score set found for URN: {urn}")
                continue

            logger.info(f"Submitting mapped variants to CAR service for score set with URN: {urn}")
            variant_objects = db.execute(
                select(Variant, MappedVariant)
                .join(MappedVariant, MappedVariant.variant_id == Variant.id)
                .join(ScoreSet)
                .where(ScoreSet.urn == urn)
                .where(MappedVariant.post_mapped.is_not(None))
                .where(MappedVariant.current.is_(True))
            ).all()

            if not variant_objects:
                logger.warning(f"No mapped variants found for score set with URN: {urn}")
                continue

            if debug:
                logger.debug(f"Debug mode enabled. Submitting only one variant to ClinGen CAR for URN: {urn}")
                variant_objects = variant_objects[:1]

            logger.debug(f"Preparing {len(variant_objects)} mapped variants for CAR submission")
            hgvs_to_mapped_variant: dict[str, list[int]] = {}
            for variant, mapped_variant in variant_objects:
                hgvs_variations = get_single_variation_hgvs_from_post_mapped(mapped_variant.post_mapped)
                if len(hgvs_variations) > 0:
                    print(f"Found HGVS\t{variant.urn}\t{','.join(hgvs_variations)}")
                for hgvs in hgvs_variations:
                    if hgvs and hgvs not in hgvs_to_mapped_variant:
                        hgvs_to_mapped_variant[hgvs] = [mapped_variant.id]
                    elif hgvs and hgvs in hgvs_to_mapped_variant:
                        hgvs_to_mapped_variant[hgvs].append(mapped_variant.id)
                    else:
                        logger.warning(f"No HGVS string found for mapped variant {variant.urn}")

            if not hgvs_to_mapped_variant:
                logger.warning(f"No HGVS strings to submit for URN: {urn}")
                continue

            logger.info(f"Submitting {len(hgvs_to_mapped_variant)} HGVS strings to CAR service for URN: {urn}")
            response = car_service.dispatch_submissions(list(hgvs_to_mapped_variant.keys()))

            if not response:
                logger.error(f"CAR submission failed for URN: {urn}")
            else:
                logger.info(f"Successfully submitted to CAR for URN: {urn}")
                # Associate CAIDs with mapped variants
                associations = get_allele_registry_associations(list(hgvs_to_mapped_variant.keys()), response)

                mapped_variant_clingen_allele_ids: dict[int, list[str]] = {}
                for hgvs, caid in associations.items():
                    mapped_variant_ids = hgvs_to_mapped_variant.get(hgvs, [])
                    for mv_id in mapped_variant_ids:
                        if not mapped_variant:
                            logger.warning(f"Mapped variant with ID {mv_id} not found for HGVS {hgvs}.")
                            continue
                        elif mv_id in mapped_variant_clingen_allele_ids:
                            # This is a multi-variant. Since ClinGen doesn't support haplotypes right now, build a list
                            # of allele IDs, which we will sort and store as a comma-separated string.
                            mapped_variant_clingen_allele_ids[mv_id].append(caid)
                        else:
                            mapped_variant_clingen_allele_ids[mv_id] = [caid]
                for mv_id, allele_ids in mapped_variant_clingen_allele_ids.items():
                    mapped_variant = db.scalar(select(MappedVariant).where(MappedVariant.id == mv_id))
                    # For multi-variants, store a comma-separate list of ClinGen allele IDs, sorted lexically. (So
                    # CA10 comes before CA2.)
                    mapped_variant.clingen_allele_id = ",".join(sorted(allele_ids))
                    db.add(mapped_variant)

                submitted_entities.extend([variant.urn for variant, _ in variant_objects])

        except Exception as e:
            logger.error(f"Error processing URN {urn}", exc_info=e)

    return submitted_entities


@click.command()
@with_database_session
@click.argument("urns", nargs=-1)
@click.option("--all", help="Submit variants for every score set in MaveDB.", is_flag=True)
@click.option("--suppress-output", help="Suppress final print output to the console.", is_flag=True)
@click.option("--debug", help="Enable debug mode. This will send only one request at most to ClinGen CAR", is_flag=True)
def submit_car_urns_command(
    db: Session,
    urns: Sequence[str],
    all: bool,
    suppress_output: bool,
    debug: bool,
) -> None:
    """
    Submit data to ClinGen Allele Registry for mapped variant CAID generation for the given URNs.
    """
    if urns and all:
        logger.error("Cannot provide both URNs and --all option.")
        return

    if all:
        urns = db.scalars(select(ScoreSet.urn)).all()  # type: ignore

    if not urns:
        logger.error("No URNs provided. Please provide at least one URN.")
        return

    submitted_variant_urns = submit_urns_to_car(db, urns, debug)

    if not suppress_output:
        print(", ".join(submitted_variant_urns))


if __name__ == "__main__":
    submit_car_urns_command()

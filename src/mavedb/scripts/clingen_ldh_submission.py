import click
import logging
from typing import Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.mapped_variant import MappedVariant
from mavedb.scripts.environment import with_database_session
from mavedb.lib.clingen.linked_data_hub import ClinGenLdhService
from mavedb.lib.clingen.constants import DEFAULT_LDH_SUBMISSION_BATCH_SIZE, LDH_SUBMISSION_ENDPOINT
from mavedb.lib.clingen.content_constructors import construct_ldh_submission
from mavedb.lib.variants import hgvs_from_mapped_variant

logger = logging.getLogger(__name__)


def submit_urns_to_clingen(db: Session, urns: Sequence[str], debug: bool) -> list[str]:
    ldh_service = ClinGenLdhService(url=LDH_SUBMISSION_ENDPOINT)
    ldh_service.authenticate()

    submitted_entities = []

    if debug:
        logger.debug("Debug mode enabled. Submitting only one request to ClinGen.")
        urns = urns[:1]

    for idx, urn in enumerate(urns):
        logger.info(f"Processing URN: {urn}. (Scoreset {idx + 1}/{len(urns)})")

        try:
            score_set = db.scalars(select(ScoreSet).where(ScoreSet.urn == urn)).one_or_none()
            if not score_set:
                logger.warning(f"No score set found for URN: {urn}")
                continue

            logger.info(f"Submitting mapped variants to LDH service for score set with URN: {urn}")
            variant_objects = db.execute(
                select(Variant, MappedVariant)
                .join(MappedVariant)
                .join(ScoreSet)
                .where(ScoreSet.urn == urn)
                .where(MappedVariant.post_mapped.is_not(None))
                .where(MappedVariant.current.is_(True))
            ).all()

            if not variant_objects:
                logger.warning(f"No mapped variants found for score set with URN: {urn}")
                continue

            logger.debug(f"Preparing {len(variant_objects)} mapped variants for submission")

            variant_content: list[tuple[str, Variant, MappedVariant]] = []
            for variant, mapped_variant in variant_objects:
                variation = hgvs_from_mapped_variant(mapped_variant)

                if not variation:
                    logger.warning(f"No variation found for variant {variant.urn}.")
                    continue

                for allele in variation:
                    variant_content.append((allele, variant, mapped_variant))

            if debug:
                logger.debug("Debug mode enabled. Submitting only one request to ClinGen.")
                variant_content = variant_content[:1]

            logger.debug(f"Constructing LDH submission for {len(variant_content)} variants")
            submission_content = construct_ldh_submission(variant_content)
            submission_successes, submission_failures = ldh_service.dispatch_submissions(
                submission_content, DEFAULT_LDH_SUBMISSION_BATCH_SIZE
            )

            if submission_failures:
                logger.error(f"Failed to submit some variants for URN: {urn}")
            else:
                logger.info(f"Successfully submitted all variants for URN: {urn}")

            submitted_entities.extend([variant.urn for _, variant, _ in variant_content])

        except Exception as e:
            logger.error(f"Error processing URN {urn}", exc_info=e)

    # TODO#372: non-nullable urns.
    return submitted_entities  # type: ignore


@click.command()
@with_database_session
@click.argument("urns", nargs=-1)
@click.option("--all", help="Submit mapped variants for every score set in MaveDB.", is_flag=True)
@click.option("--suppress-output", help="Suppress final print output to the console.", is_flag=True)
@click.option("--debug", help="Enable debug mode. This will send only one request at most to ClinGen", is_flag=True)
def submit_clingen_urns_command(
    db: Session, urns: Sequence[str], all: bool, suppress_output: bool, debug: bool
) -> None:
    """
    Submit data to ClinGen for mapped variant allele ID generation for the given URNs.
    """
    if urns and all:
        logger.error("Cannot provide both URNs and --all option.")
        return

    if all:
        # TODO#372: non-nullable urns.
        urns = db.scalars(select(ScoreSet.urn)).all()  # type: ignore

    if not urns:
        logger.error("No URNs provided. Please provide at least one URN.")
        return

    submitted_variant_urns = submit_urns_to_clingen(db, urns, debug)

    if not suppress_output:
        print(", ".join(submitted_variant_urns))


if __name__ == "__main__":
    submit_clingen_urns_command()

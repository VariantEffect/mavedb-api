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
from mavedb.lib.clingen.constants import DEFAULT_LDH_SUBMISSION_BATCH_SIZE, LDH_SUBMISSION_URL
from mavedb.lib.clingen.content_constructors import construct_ldh_submission

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def _variation_from_post_mapped(mapped_variant: MappedVariant) -> str:
    """
    Extract the variation from the post_mapped field of the MappedVariant object.
    """
    try:
        # Assuming post_mapped is a dictionary with a specific structure
        return mapped_variant.post_mapped["expressions"][0]["value"]  # type: ignore
    except KeyError:
        return mapped_variant.post_mapped["variation"]["expressions"][0]["value"]  # type: ignore


def submit_urns_to_clingen(db: Session, urns: Sequence[str]) -> list[str]:
    ldh_service = ClinGenLdhService(url=LDH_SUBMISSION_URL)
    ldh_service.authenticate()

    submitted_entities = []

    for urn in urns:
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
                .where(MappedVariant.current.is_(True))
            ).all()

            if not variant_objects:
                logger.warning(f"No mapped variants found for score set with URN: {urn}")
                continue

            logger.debug(f"Preparing {len(variant_objects)} mapped variants for submission")
            variant_content = [
                (
                    _variation_from_post_mapped(mapped_variant),
                    variant,
                    mapped_variant,
                )
                for variant, mapped_variant in variant_objects
            ]

            submission_content = construct_ldh_submission(variant_content)
            submission_successes, submission_failures = ldh_service.dispatch_submissions(
                submission_content, DEFAULT_LDH_SUBMISSION_BATCH_SIZE
            )

            if submission_failures:
                logger.error(f"Failed to submit some variants for URN: {urn}")
            else:
                logger.info(f"Successfully submitted all variants for URN: {urn}")

            submitted_entities.extend([variant[1].urn for variant in variant_content])

        except Exception as e:
            logger.error(f"Error processing URN {urn}", exc_info=e)

    return submitted_entities


@click.command()
@with_database_session
@click.argument("urns", nargs=-1)
@click.option("--all", help="Submit mapped variants for every score set in MaveDB.", is_flag=True)
@click.option("--suppress-output", help="Suppress final print output to the console.", is_flag=True)
def submit_clingen_urns_command(db: Session, urns: Sequence[str], all: bool, suppress_output: bool) -> None:
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

    submitted_variant_urns = submit_urns_to_clingen(db, urns)

    if not suppress_output:
        print(submitted_variant_urns)


if __name__ == "__main__":
    submit_clingen_urns_command()

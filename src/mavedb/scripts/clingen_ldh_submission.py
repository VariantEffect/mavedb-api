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


def submit_urns_to_clingen(db: Session, urns: list[str]) -> None:
    ldh_service = ClinGenLdhService(url=LDH_SUBMISSION_URL)
    ldh_service.authenticate()

    for urn in urns:
        try:
            score_set = db.scalars(select(ScoreSet).where(ScoreSet.urn == urn)).one_or_none()
            if not score_set:
                logger.warning(f"No score set found for URN: {urn}")
                continue

            logger.info(f"Submitting mapped variants to LDH service for score set with URN: {urn}")
            variant_objects = db.scalars(
                select(Variant, MappedVariant)
                .join(MappedVariant)
                .join(ScoreSet)
                .where(ScoreSet.urn == urn)
                .where(MappedVariant.current.is_(True))
            ).all()

            if not variant_objects:
                logger.warning(f"No mapped variants found for score set with URN: {urn}")
                continue

            variant_content = [
                (
                    mapped_variant.post_mapped["variation"]["expressions"][0]["value"],
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

        except Exception as e:
            logger.error(f"Error processing URN {urn}: {e}")


@click.command()
@with_database_session
@click.argument("urns", nargs=-1)
@click.option("--all", help="Submit mapped variants for every score set in MaveDB.", is_flag=True)
def submit_clingen_urns_command(db: Session, urns: Sequence[str], all: bool) -> None:
    """
    Submit data to ClinGen for mapped variant allele ID generation for the given URNs.
    """
    if urns and all:
        logger.error("Cannot provide both URNs and --all option.")
        return

    if all:
        urns = db.scalars(select(ScoreSet.urn)).all()

    if not urns:
        logger.error("No URNs provided. Please provide at least one URN.")
        return

    submit_urns_to_clingen(db, urns)


if __name__ == "__main__":
    submit_clingen_urns_command()

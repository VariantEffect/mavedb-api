import click
import requests
import logging
from typing import Optional, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.models.variant import Variant
from mavedb.models.mapped_variant import MappedVariant
from mavedb.scripts.environment import with_database_session
from mavedb.lib.clingen.constants import LDH_LINKED_DATA_URL

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_clingen_variation(urn: str) -> Optional[dict]:
    response = requests.get(
        f"{LDH_LINKED_DATA_URL}/{urn}",
        headers={"Accept": "application/json"},
    )

    if response.status_code == 200:
        return response.json()["data"]
    else:
        logger.error(f"Failed to fetch data for URN {urn}: {response.status_code} - {response.text}")
        return None


@click.command()
@with_database_session
@click.argument("urns", nargs=-1)
def link_clingen_variants(db: Session, urns: Sequence[str]) -> None:
    """
    Submit data to ClinGen for mapped variant allele ID generation for the given URNs.
    """
    if not urns:
        logger.error("No URNs provided. Please provide at least one URN.")
        return

    failed_urns = []
    for urn in urns:
        ldh_variation = get_clingen_variation(urn)

        if not ldh_variation:
            failed_urns.append(urn)
            continue

        mapped_variant = db.scalar(select(MappedVariant).join(Variant).where(Variant.urn == urn))

        if not mapped_variant:
            logger.warning(f"No mapped variant found for URN {urn}.")
            failed_urns.append(urn)
            continue

        mapped_variant.clingen_allele_id = ldh_variation["id"]
        db.add(mapped_variant)
        db.commit()

        logger.info(f"Successfully linked URN {urn} to ClinGen variation {ldh_variation['id']}.")

    if failed_urns:
        logger.warning(f"Failed to link the following URNs: {', '.join(failed_urns)}")

    logger.info(f"Linking process completed. Linked {len(urns) - len(failed_urns)} URNs successfully.")


if __name__ == "__main__":
    link_clingen_variants()

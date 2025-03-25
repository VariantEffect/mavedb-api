import logging
import requests

from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.variant_translation import VariantTranslation

from mavedb.scripts.environment import script_environment, with_database_session

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

CLINGEN_API_URL = "https://reg.test.genome.network/allele"


def get_canonical_pa_ids(clingen_allele_id: str) -> list[str]:
    """ "Retrieve any canonical PA IDs from the ClinGen API for a given clingen allele ID."""
    response = requests.get(f"{CLINGEN_API_URL}/{clingen_allele_id}")
    if response.status_code != 200:
        logger.error(f"Failed to query ClinGen API for {clingen_allele_id}: {response.status_code}")
        return []

    data = response.json()

    pa_ids = []
    if data.get("transcriptAlleles"):
        for allele in data["transcriptAlleles"]:
            if allele.get("MANE") and allele.get("@id"):
                # @id field returns url; the last component is the PA ID
                pa_ids.append(allele["@id"].split("/")[-1])

    return pa_ids


def get_matching_registered_ca_ids(clingen_pa_id: str) -> list[str]:
    """Retrieve all matching registered transcript CA IDs for a given PA ID from the ClinGen API."""
    response = requests.get(f"{CLINGEN_API_URL}/{clingen_pa_id}")
    if response.status_code != 200:
        logger.error(f"Failed to query ClinGen API for {clingen_pa_id}: {response.status_code}")
        return []

    data = response.json()

    ca_ids = []
    if data.get("aminoAcidAlleles"):
        for allele in data["aminoAcidAlleles"]:
            if allele.get("matchingRegisteredTranscripts"):
                # @id field returns url; the last component is the PA ID
                ca_ids.extend([allele["@id"].split("/")[-1] for allele in allele["matchingRegisteredTranscripts"]])

    return ca_ids


@script_environment.command()
@with_database_session
def populate_variant_translations(db: Session):
    clingen_allele_ids = db.scalars(select(MappedVariant.clingen_allele_id)).all()
    logger.info(f"Found {len(clingen_allele_ids)} clingen allele IDs in the database.")

    for allele_id in clingen_allele_ids:
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
                        logger.warning(
                            f"No matching registered transcript CA IDs found for {pa_id}. This is unexpected."
                        )
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
            logger.error(f"Error processing clingen allele ID {allele_id}: {e}")
            db.rollback()

    logger.info("Done populating variant translations.")


if __name__ == "__main__":
    populate_variant_translations()

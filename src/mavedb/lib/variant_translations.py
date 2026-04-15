"""Variant translation library functions for ClinGen allele registry mapping."""

import logging

import requests
from sqlalchemy.orm import Session

from mavedb.lib.clingen.allele_registry import get_canonical_pa_ids, get_matching_registered_ca_ids
from mavedb.lib.exceptions import VariantTranslationProcessingError
from mavedb.models.variant_translation import VariantTranslation

logger = logging.getLogger(__name__)


async def populate_variant_translations_for_variant(db: Session, allele_id: str) -> int:
    """Populate variant translations for a single ClinGen allele ID.

    Queries the ClinGen Allele Registry API to resolve canonical PA IDs and matching
    registered transcript CA IDs, creating VariantTranslation records for each mapping.

    Args:
        db (Session): Database session for persisting changes.
        allele_id (str): The ClinGen allele ID to process (CA or PA format).

    Returns:
        int: Number of new VariantTranslation records created.

    Raises:
        VariantTranslationProcessingError: If critical errors occur during processing.
    """
    translations_created = 0

    try:
        if allele_id.startswith("CA"):
            # Get the canonical PA ID(s) from the ClinGen API (with automatic caching)
            try:
                canonical_pa_ids = await get_canonical_pa_ids(allele_id)
            except requests.exceptions.RequestException as exc:
                logger.error(
                    f"Error fetching canonical PA IDs for {allele_id} from ClinGen API: {exc}",
                    exc_info=True,
                )
                raise VariantTranslationProcessingError(f"Failed to fetch canonical PA IDs for {allele_id}: {str(exc)}")

            if not canonical_pa_ids:
                logger.warning(
                    f"No canonical PA IDs found for {allele_id}. This may be expected if the query is noncoding."
                )
                return 0

            for pa_id in canonical_pa_ids:
                # Check if translation already exists
                existing = (
                    db.query(VariantTranslation)
                    .filter(
                        VariantTranslation.aa_clingen_id == pa_id,
                        VariantTranslation.nt_clingen_id == allele_id,
                    )
                    .one_or_none()
                )

                if not existing:
                    db.add(VariantTranslation(aa_clingen_id=pa_id, nt_clingen_id=allele_id))
                    db.commit()
                    translations_created += 1

                # For each canonical PA ID, get the matching registered transcript CA IDs
                try:
                    ca_ids = await get_matching_registered_ca_ids(pa_id)
                except requests.exceptions.RequestException as exc:
                    logger.error(
                        f"Error fetching matching registered CA IDs for {pa_id} from ClinGen API: {exc}",
                        exc_info=True,
                    )
                    continue

                if not ca_ids:
                    logger.warning(f"No matching registered transcript CA IDs found for {pa_id}.")
                    continue

                for ca_id in ca_ids:
                    existing = (
                        db.query(VariantTranslation)
                        .filter(
                            VariantTranslation.aa_clingen_id == pa_id,
                            VariantTranslation.nt_clingen_id == ca_id,
                        )
                        .one_or_none()
                    )

                    if not existing:
                        db.add(VariantTranslation(aa_clingen_id=pa_id, nt_clingen_id=ca_id))
                        db.commit()
                        translations_created += 1

        elif allele_id.startswith("PA"):
            # Get the matching registered transcript CA IDs from the ClinGen API
            try:
                ca_ids = await get_matching_registered_ca_ids(allele_id)
            except requests.exceptions.RequestException as exc:
                logger.error(
                    f"Error fetching matching registered CA IDs for {allele_id} from ClinGen API: {exc}",
                    exc_info=True,
                )
                raise VariantTranslationProcessingError(f"Failed to fetch matching CA IDs for {allele_id}: {str(exc)}")

            if not ca_ids:
                logger.warning(f"No matching registered transcript CA IDs found for {allele_id}. This is unexpected.")
                return 0

            for ca_id in ca_ids:
                existing = (
                    db.query(VariantTranslation)
                    .filter(
                        VariantTranslation.aa_clingen_id == allele_id,
                        VariantTranslation.nt_clingen_id == ca_id,
                    )
                    .one_or_none()
                )

                if not existing:
                    db.add(VariantTranslation(aa_clingen_id=allele_id, nt_clingen_id=ca_id))
                    db.commit()
                    translations_created += 1

        return translations_created

    except VariantTranslationProcessingError:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in populate_variant_translations_for_score_set: {str(e)}", exc_info=True)
        raise VariantTranslationProcessingError(f"Unexpected error processing allele ID {allele_id}: {str(e)}")

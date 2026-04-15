"""HGVS nomenclature library functions for variant mapping and nomenclature conversion."""

import logging
from typing import Sequence

from sqlalchemy.orm import Session

from mavedb.lib.exceptions import HGVSProcessingError
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet

logger = logging.getLogger(__name__)


def populate_mapped_hgvs_for_variants(
    db: Session,
    score_set: ScoreSet,
    mapped_variants: Sequence[MappedVariant],
) -> bool:
    """Populate HGVS nomenclature for mapped variants.

    This function takes mapped variants and populates their HGVS expressions
    (genomic, transcript, and protein nomenclature) based on the variant coordinates
    and the score set's target gene information.

    Args:
        db (Session): Database session for persisting changes.
        score_set (ScoreSet): The score set containing the variants.
        mapped_variants (Sequence[MappedVariant]): Variants to populate HGVS for.

    Returns:
        bool: True if HGVS was successfully populated, False otherwise.

    Raises:
        HGVSProcessingError: If critical errors occur during HGVS mapping.
    """
    try:
        # Import here to avoid circular imports
        from mavedb.scripts.populate_mapped_hgvs import get_target_info
        from mavedb.lib.vrs_mapping import get_hgvs_from_variant

        # Get target information from the score set
        target_is_coding, transcript_accession = get_target_info(score_set)

        # Process each mapped variant
        for mapped_variant in mapped_variants:
            try:
                # Get HGVS nomenclature for this variant
                hgvs_data = get_hgvs_from_variant(
                    mapped_variant=mapped_variant,
                    transcript_accession=transcript_accession,
                    target_is_coding=target_is_coding,
                )

                if hgvs_data:
                    mapped_variant.post_mapped = hgvs_data
                    db.add(mapped_variant)
                else:
                    logger.warning(f"Could not generate HGVS for mapped variant {mapped_variant.id}")
                    return False

            except Exception as e:
                logger.error(f"Error processing HGVS for variant {mapped_variant.id}: {str(e)}")
                return False

        db.flush()
        return True

    except Exception as e:
        logger.error(f"Error in populate_mapped_hgvs_for_variants: {str(e)}")
        raise HGVSProcessingError(f"Failed to populate HGVS nomenclature: {str(e)}")

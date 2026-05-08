"""Variant translation utilities for managing PA<->CA allele relationships.

This module provides database operations for the variant_translations table,
which stores relationships between protein allele (PA) and nucleotide allele (CA)
ClinGen IDs.
"""

from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.models.variant_translation import VariantTranslation


def upsert_variant_translations(db: Session, translations: list[tuple[str, str]]) -> tuple[int, int]:
    """Insert VariantTranslation rows for (aa, nt) pairs that don't already exist.

    Returns (created, existing) counts.
    """
    created = 0
    existing = 0
    for aa_clingen_id, nt_clingen_id in translations:
        found = db.scalars(
            select(VariantTranslation).where(
                VariantTranslation.aa_clingen_id == aa_clingen_id,
                VariantTranslation.nt_clingen_id == nt_clingen_id,
            )
        ).one_or_none()

        if found:
            existing += 1
        else:
            db.add(VariantTranslation(aa_clingen_id=aa_clingen_id, nt_clingen_id=nt_clingen_id))
            created += 1

    return created, existing

"""Variant translation utilities for managing PA<->CA allele relationships.

This module provides database operations for the variant_translations table,
which stores relationships between protein allele (PA) and nucleotide allele (CA)
ClinGen IDs.
"""

from typing import cast

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session

from mavedb.models.variant_translation import VariantTranslation


def upsert_variant_translations(db: Session, translations: list[tuple[str, str]]) -> tuple[int, int]:
    """Insert VariantTranslation rows for (aa, nt) pairs that don't already exist.

    Uses INSERT ... ON CONFLICT DO NOTHING to avoid race conditions between
    concurrent jobs and duplicate pairs accumulating within a single session
    before a commit.

    Returns (created, existing) counts.
    """
    if not translations:
        return 0, 0

    unique = list({(aa, nt) for aa, nt in translations})
    rows = [{"aa_clingen_id": aa, "nt_clingen_id": nt} for aa, nt in unique]

    stmt = (
        insert(VariantTranslation)
        .values(rows)
        .on_conflict_do_nothing(index_elements=["aa_clingen_id", "nt_clingen_id"])
    )
    result = cast(CursorResult, db.execute(stmt))

    created = result.rowcount
    existing = len(unique) - created
    return created, existing

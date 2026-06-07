"""Variant translation utilities for managing PA<->CA allele relationships.

This module provides database operations for the variant_translations table,
which stores relationships between protein allele (PA) and nucleotide allele (CA)
ClinGen IDs.
"""

from typing import cast

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session

from mavedb.models.allele import Allele
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


def get_or_create_allele(db: Session, allele_draft: Allele) -> Allele:
    """Return the existing Allele matching ``allele_draft``'s vrs_digest, else add the draft.

    This is a get-or-create, not an upsert: a matching row is returned untouched, and on
    a miss the draft is added to the session (not flushed). The draft is never used to
    update an existing row.

    NOTE: This function does not persist the returned Allele to the database; the caller
          is responsible for committing the session.
    """
    existing = db.scalars(select(Allele).where(Allele.vrs_digest == allele_draft.vrs_digest)).one_or_none()
    if existing is not None:
        return existing

    db.add(allele_draft)
    return allele_draft

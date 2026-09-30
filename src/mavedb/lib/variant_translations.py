"""Variant translation utilities for managing PA<->CA allele relationships.

This module provides database operations for the variant_translations table,
which stores relationships between protein allele (PA) and nucleotide allele (CA)
ClinGen IDs.

FROZEN (serving-only). The populate_variant_translations_for_score_set job that wrote this table was
retired in the #742 migration: the reverse-translation allele equivalence space (genomic/coding/protein
VRS alleles per variant, linked via MappingRecordAllele with HGVS on Allele) now covers PA<->CA
relationships without querying ClinGen. These helpers and the variant_translations table remain only to
serve existing old-model data; they are never written for new score sets and are dropped at read-cutover.
"""

from typing import cast

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from mavedb.lib.vrs_utils import location_refgets
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


class AlleleIdentityConflictError(ValueError):
    """Raised when an allele's HGVS expression already exists under a different vrs_digest.

    An allele digest covers the refget of the sequence the allele sits on, and alleles deduplicate on
    it. Two digests for one HGVS expression therefore mean two writers built the allele on different
    sequences (most often by resolving its reference accession to different sequences), so the
    copies would never merge. Nothing is wrong with the variant; the writers disagree about the
    reference.

    To debug: compare the two refgets in the message with the sequence SeqRepo holds for the accession
    (``SELECT * FROM seqalias WHERE alias = '<accession>'`` in the SeqRepo's aliases.sqlite3); check that
    the mapper and the worker read the same SeqRepo (``SEQREPO_ROOT_DIR`` and ``HGVS_SEQREPO_DIR``) and
    that dcd_mapping is current. Duplicates that already exist have to be removed, with the rows
    that reference them, and the affected score sets remapped. Remove them first: this check refuses to
    create an allele while another allele holds its HGVS.
    """


def _identity_conflict(existing: Allele, incoming: Allele) -> AlleleIdentityConflictError:
    return AlleleIdentityConflictError(
        f"Allele identity conflict for {incoming.hgvs}: an allele with this HGVS already "
        f"exists under a different digest. Existing: id={existing.id} digest={existing.vrs_digest} "
        f"refgets={location_refgets(existing.post_mapped or {})}. Incoming: digest={incoming.vrs_digest} "
        f"refgets={location_refgets(incoming.post_mapped or {})}. Two writers disagree on the sequence "
        "this allele sits on; see AlleleIdentityConflictError for how to debug this error."
    )


def get_or_create_allele(db: Session, allele_draft: Allele) -> Allele:
    """Return the existing Allele matching ``allele_draft``'s vrs_digest, else add the draft.

    This is a get-or-create, not an upsert: a matching row is returned untouched, and on
    a miss the draft is added to the session and flushed. The draft is never used to
    update an existing row.

    :raise AlleleIdentityConflictError: if the draft's HGVS already exists under a different digest

    NOTE: This function does not commit; the caller is responsible for committing the session.
    """
    existing = db.scalars(select(Allele).where(Allele.vrs_digest == allele_draft.vrs_digest)).one_or_none()
    if existing is not None:
        return existing

    hgvs = allele_draft.hgvs
    if hgvs:
        conflicting = db.scalars(select(Allele).where(Allele.hgvs == hgvs)).first()
        if conflicting is not None:
            raise _identity_conflict(conflicting, allele_draft)

    # Flushed inside a savepoint so a concurrent writer that slipped past the check above surfaces as
    # the same error, through the unique index, without poisoning the caller's transaction.
    try:
        with db.begin_nested():
            db.add(allele_draft)
            db.flush()

    except IntegrityError as e:
        if "uq_alleles_hgvs" not in str(e.orig) or not hgvs:
            raise

        conflicting = db.scalars(select(Allele).where(Allele.hgvs == hgvs)).first()
        if conflicting is None:
            raise

        raise _identity_conflict(conflicting, allele_draft) from e

    return allele_draft

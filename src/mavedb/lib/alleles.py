"""Allele-graph queries over the deduplicated allele model.

These traverse the ``MappingRecordAllele`` link graph rather than any external identifier, because an
allele's cross-layer equivalence (its genomic / coding / protein representations) is established by the
mapping + reverse-translation process and materialized as co-membership in a ``MappingRecord``'s allele
set. No single identifier spans the layers: ClinGen's canonical allele id (CAID) covers only the
nucleotide layers, the protein allele carries a distinct PA, so the link graph is the only thing that
ties all three together.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from mavedb.models.allele import Allele
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele


def get_live_record_allele_links(
    db: Session, variant_id: int, *, as_of: Optional[datetime] = None
) -> list[MappingRecordAllele]:
    """Return the live ``MappingRecordAllele`` links of a variant's single live ``MappingRecord``,
    each with its ``allele`` eagerly loaded and its ``is_authoritative`` flag.

    This is **record-scoped**, deliberately unlike :func:`get_allele_translations`: it stays within
    one variant's own mapping record rather than taking the cross-record union an anchor allele can
    belong to. That scope is what the per-variant Cat-VRS transit needs — the variant's measured
    (authoritative) allele as the defining representation and exactly its co-linked members, not the
    equivalence class assembled from every record that happens to share a deduplicated allele.

    Temporal: defaults to the currently-live record and links (``valid_to IS NULL``). ``as_of``
    applies the same half-open predicate to both the record and the links, so the set is evaluated at
    one instant. Returns ``[]`` when the variant has no live record.
    """
    record_id = db.scalar(
        select(MappingRecord.id).where(MappingRecord.variant_id == variant_id).where(MappingRecord.live_at(as_of))
    )
    if record_id is None:
        return []

    return list(
        db.scalars(
            select(MappingRecordAllele)
            .where(MappingRecordAllele.mapping_record_id == record_id)
            .where(MappingRecordAllele.live_at(as_of))
            .options(joinedload(MappingRecordAllele.allele))
        ).all()
    )


def get_allele_translations(db: Session, allele_id: int, *, as_of: Optional[datetime] = None) -> list[Allele]:
    """Return the cross-layer equivalence set of ``allele_id``: every allele co-linked to a
    ``MappingRecord`` that links it (the anchor allele itself included), spanning the genomic, coding,
    and protein layers.

    The relation is co-membership in a record's allele set, not a shared identifier — see the module
    docstring. Because alleles are deduplicated by ``vrs_digest`` and shared across variants/score sets,
    the anchor may belong to several ``MappingRecord``s; the result is the union of their allele sets
    (normally the same biological equivalence class). Scope by record or score set upstream if a single
    context is required.

    Temporal: by default returns the currently-live set (``valid_to IS NULL``). Pass ``as_of`` to
    reconstruct the set as it stood at a past instant — both the anchor hop and the fan-out hop apply
    the same half-open ``[valid_from, valid_to)`` predicate, so the whole set is evaluated at one
    instant. The retire-cascade invariant (a live link implies a live record) holds under ``as_of`` too,
    so filtering the links alone is sufficient.
    """
    record_ids = db.scalars(
        select(MappingRecordAllele.mapping_record_id)
        .where(MappingRecordAllele.allele_id == allele_id)
        .where(MappingRecordAllele.live_at(as_of))
    ).all()
    if not record_ids:
        return []

    return list(
        db.scalars(
            select(Allele)
            .join(MappingRecordAllele, MappingRecordAllele.allele_id == Allele.id)
            .where(MappingRecordAllele.mapping_record_id.in_(record_ids))
            .where(MappingRecordAllele.live_at(as_of))
            .distinct()
        ).all()
    )

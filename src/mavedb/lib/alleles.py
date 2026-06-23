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
from sqlalchemy.orm import Session

from mavedb.models.allele import Allele
from mavedb.models.mapping_record_allele import MappingRecordAllele


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
    link_live = MappingRecordAllele.as_of(as_of) if as_of is not None else MappingRecordAllele.current

    record_ids = db.scalars(
        select(MappingRecordAllele.mapping_record_id).where(MappingRecordAllele.allele_id == allele_id).where(link_live)
    ).all()
    if not record_ids:
        return []

    return list(
        db.scalars(
            select(Allele)
            .join(MappingRecordAllele, MappingRecordAllele.allele_id == Allele.id)
            .where(MappingRecordAllele.mapping_record_id.in_(record_ids))
            .where(link_live)
            .distinct()
        ).all()
    )

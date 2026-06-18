"""Query helpers for fetching score-set alleles for ClinGen registration.

Both submit_score_set_mappings_to_car and warm_clingen_cache use the same allele
scope: all current MappingRecordAllele links (authoritative and RT-derived) for a
score set.  A single definition here prevents the two jobs from drifting apart.
"""

from typing import NamedTuple

from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.models.allele import Allele
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.variant import Variant


class ScoreSetAlleleRow(NamedTuple):
    """One (allele, variant) link for a score set. An allele shared by multiple variants
    appears once per variant so callers can fan annotation statuses out correctly.

    ``is_authoritative`` is a property of the link, not the allele: the same VRS allele can be
    the authoritative measurement for one variant and an RT-derived equivalence for another.
    """

    allele_id: int
    post_mapped: dict | None
    clingen_allele_id: str | None
    variant_id: int
    is_authoritative: bool


def get_alleles_for_score_set(db: Session, score_set_id: int) -> list[ScoreSetAlleleRow]:
    """Return all current alleles for a score set with their linked variant IDs.

    Covers both authoritative mapper alleles and RT-derived equivalence alleles —
    the full set that requires ClinGen registration before the annotation fan-out
    can run.

    Only alleles with a non-null ``post_mapped`` are returned — variants that failed
    or were benignly absent have no allele link and cannot receive a CAID.
    """
    rows = db.execute(
        select(
            Allele.id,
            Allele.post_mapped,
            Allele.clingen_allele_id,
            Variant.id.label("variant_id"),
            MappingRecordAllele.is_authoritative,
        )
        .join(MappingRecordAllele, MappingRecordAllele.allele_id == Allele.id)
        .join(MappingRecord, MappingRecord.id == MappingRecordAllele.mapping_record_id)
        .join(Variant, Variant.id == MappingRecord.variant_id)
        .where(Variant.score_set_id == score_set_id)
        .where(MappingRecord.current)
        .where(MappingRecordAllele.current)
        .where(Allele.post_mapped.is_not(None))
    ).all()

    return [ScoreSetAlleleRow(r.id, r.post_mapped, r.clingen_allele_id, r.variant_id, r.is_authoritative) for r in rows]

"""Query helpers for fetching score-set alleles for ClinGen registration.

Both submit_score_set_mappings_to_car and warm_clingen_cache use the same allele
scope: all current MappingRecordAllele links (authoritative and RT-derived) for a
score set.  A single definition here prevents the two jobs from drifting apart.
"""

from dataclasses import dataclass, field
from typing import Callable, Generic, Iterable, NamedTuple, Optional, TypeVar

from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.models.allele import Allele
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.variant import Variant

P = TypeVar("P")


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


@dataclass
class AlleleAnnotationGroup(Generic[P]):
    """One allele's annotation work-unit: a job-specific ``payload`` plus the variants that receive a
    per-variant annotation-status (VAS) row for it.

    ``authoritative_variant_ids`` is the interim **bandaid seam** (see
    docs/design/allele-annotation-status.md). An annotation is an allele-level fact, but VAS is
    per-variant; fanning it to every variant the allele backs would write multiple ``current`` rows
    for one variant. So status is fanned only to the variants for which this allele is the
    *authoritative* measurement. At the AnnotationEvent migration the per-variant fan-out goes away
    and this list is no longer needed. The allele is still grouped (and linked/annotated at the
    allele level) regardless — a purely RT-derived allele simply has an empty list here.
    """

    payload: P
    authoritative_variant_ids: list[int] = field(default_factory=list)


def group_alleles_for_annotation(
    rows: Iterable[ScoreSetAlleleRow],
    payload: Callable[[ScoreSetAlleleRow], Optional[P]],
) -> dict[int, AlleleAnnotationGroup[P]]:
    """Collapse the per-(allele, variant) rows from :func:`get_alleles_for_score_set` into one
    work-unit per allele, keyed by ``allele_id``.

    The same allele recurs once per variant that links it (and can be authoritative for one variant
    while RT-derived for another). This groups those rows so every annotation job shares one shape:
    one entry per allele, carrying the authoritative-variant fan-out set for the bandaid.

    ``payload`` builds the job-specific payload from the first row seen for an allele — the CAID for
    gnomAD/ClinVar, the HGVS for VEP, etc. Returning ``None`` skips the allele entirely (e.g. it
    carries no CAID), replacing each job's ad-hoc ``if row.x is None: continue``. ``payload`` must be
    a pure function of allele-level fields so its result is stable across an allele's rows.

    Grouping on ``allele_id`` rather than ``vrs_digest`` is intentional: the two are 1:1 (content
    addressing makes ``vrs_digest`` unique), so the groups are identical either way, and ``allele_id``
    is the permanent, never-reused surrogate. If annotation storage later keys on the digest, carry
    it on the row and store against it — the grouping contract here does not change.
    """
    groups: dict[int, AlleleAnnotationGroup[P]] = {}
    for row in rows:
        if row.allele_id not in groups:
            built = payload(row)
            if built is None:
                continue
            groups[row.allele_id] = AlleleAnnotationGroup(payload=built)
        if row.is_authoritative:
            groups[row.allele_id].authoritative_variant_ids.append(row.variant_id)
    return groups

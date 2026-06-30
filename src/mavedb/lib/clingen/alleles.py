"""Query helpers for fetching score-set alleles for ClinGen registration.

Both submit_score_set_mappings_to_car and warm_clingen_cache use the same allele
scope: all current MappingRecordAllele links (authoritative and RT-derived) for a
score set.  A single definition here prevents the two jobs from drifting apart.
"""

from typing import Callable, Iterable, NamedTuple, Optional, TypeVar

from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.models.allele import Allele
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.variant import Variant

P = TypeVar("P")


class ScoreSetAlleleRow(NamedTuple):
    """One (allele, variant) link for a score set. An allele shared by multiple variants appears once
    per variant; :func:`group_alleles_for_annotation` collapses those duplicates into one work-unit
    per allele.

    ``hgvs_g``/``hgvs_c``/``hgvs_p`` are allele-level (stable by construction), carried here so the
    VEP job can build its HGVS payload without a second query. They are optional with a ``None``
    default so payloads keying only on the CAID (gnomAD/ClinVar) need not name them.
    """

    allele_id: int
    post_mapped: dict | None
    clingen_allele_id: str | None
    variant_id: int
    hgvs_g: str | None = None
    hgvs_c: str | None = None
    hgvs_p: str | None = None


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
            Allele.hgvs_g,
            Allele.hgvs_c,
            Allele.hgvs_p,
        )
        .join(MappingRecordAllele, MappingRecordAllele.allele_id == Allele.id)
        .join(MappingRecord, MappingRecord.id == MappingRecordAllele.mapping_record_id)
        .join(Variant, Variant.id == MappingRecord.variant_id)
        .where(Variant.score_set_id == score_set_id)
        .where(MappingRecord.current)
        .where(MappingRecordAllele.current)
        .where(Allele.post_mapped.is_not(None))
    ).all()

    return [
        ScoreSetAlleleRow(r.id, r.post_mapped, r.clingen_allele_id, r.variant_id, r.hgvs_g, r.hgvs_c, r.hgvs_p)
        for r in rows
    ]


def group_alleles_for_annotation(
    rows: Iterable[ScoreSetAlleleRow],
    payload: Callable[[ScoreSetAlleleRow], Optional[P]],
) -> dict[int, P]:
    """Collapse the per-(allele, variant) rows from :func:`get_alleles_for_score_set` into one
    job-specific payload per allele, keyed by ``allele_id``.

    The same allele recurs once per variant that links it, so this dedups those rows down to one
    entry per allele — the shape every allele-keyed annotation job wants now that annotation events
    are allele-keyed (one event per allele, never fanned per-variant).

    ``payload`` builds the job-specific payload from the first row seen for an allele — the CAID for
    gnomAD/ClinVar, the HGVS for VEP, etc. Returning ``None`` skips the allele entirely (e.g. it
    carries no CAID), replacing each job's ad-hoc ``if row.x is None: continue``. ``payload`` must be
    a pure function of allele-level fields so its result is stable across an allele's rows.

    Grouping on ``allele_id`` rather than ``vrs_digest`` is intentional: the two are 1:1 (content
    addressing makes ``vrs_digest`` unique), so the groups are identical either way, and ``allele_id``
    is the permanent, never-reused surrogate. If annotation storage later keys on the digest, carry
    it on the row and store against it — the grouping contract here does not change.
    """
    groups: dict[int, P] = {}
    for row in rows:
        if row.allele_id in groups:
            continue

        built = payload(row)
        if built is None:
            continue

        groups[row.allele_id] = built

    return groups

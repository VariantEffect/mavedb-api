"""Lean whole-set view backing the score-set page (table, heatmap, score/effect histograms).

Assembles one record per variant in a single query: the score set's variants, with each per-variant
annotation fetched by a ``LATERAL ... LIMIT 1`` lookup (see :func:`get_lean_score_set_variants` for why).

Each record carries the **submitted** HGVS (``hgvs_nt``/``hgvs_pro``/``hgvs_splice``, the depositor's
target frame) and the **mapped** (reference-frame) HGVS as a ``MappedTriple`` — one canonical HGVS per
level (``genomic``/``cdna``/``protein``), with ``assay_level`` naming the measured slot. A nucleotide
assay fills all three slots; a protein assay fills only ``protein`` (the c/g fan-out is ambiguous, so
no pick is fabricated). This is the canonical projection only, never the reverse-translation fan-out.

The mapped slots come from the record's measured projection group (the group its authoritative link points
at), which describes the measured variant at every level it has, including levels with no VRS allele
(an intronic coding projection). Where the group is not yet written, only the measured slot is filled.
Does not assemble Cat-VRS.

``as_of`` reconstructs the annotation layer at a past instant; submitted HGVS and scores are immutable
and unaffected. Defaults to currently-live rows.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy import and_, select, true
from sqlalchemy.orm import Session, aliased

from mavedb.lib.hgvs import parse_simple_substitution
from mavedb.lib.variants import score_from_variant_data
from mavedb.models.allele import Allele
from mavedb.models.enums.sequence_level import SequenceLevel
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.projection_group import ProjectionGroup
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.vep_allele_consequence import VepAlleleConsequence


@dataclass(frozen=True)
class HgvsField:
    """An HGVS string plus its parsed position/ref/alt when it's a placeable simple substitution.

    ``position``/``ref``/``alt`` are ``None`` for splice/intronic, indels, frameshifts, and
    multivariants — those carry ``hgvs`` alone.
    """

    hgvs: str
    position: Optional[int] = None
    ref: Optional[str] = None
    alt: Optional[str] = None


@dataclass(frozen=True)
class MappedTriple:
    """The canonical mapped (reference-frame) HGVS at each level, or ``None`` where absent.

    A nucleotide assay populates the measured slot and whichever projections its measured projection
    group holds (no protein for an intronic or UTR change); a protein assay populates only ``protein`` —
    the c/g fan-out is ambiguous, so no pick is fabricated for ``cdna``/``genomic``.

    ``cdna`` is the level-invariant field: populated whether the assay was measured at ``cdna`` or
    ``genomic``, so it's the field to key cross-assay searches on.
    """

    genomic: Optional[HgvsField] = None
    cdna: Optional[HgvsField] = None
    protein: Optional[HgvsField] = None


@dataclass(frozen=True)
class LeanVariantRecord:
    """One pre-chewed per-variant record, keyed by ``variant_urn``.

    ``consequence`` is the allele's VEP molecular-consequence call. ``clingen_allele_id``/``assay_level_digest``
    bridge into the annotation dimensions. ``mapped`` is the canonical :class:`MappedTriple`;
    ``assay_level`` names which of its slots is the measured one. Any field is ``None`` when its
    source is absent (an unmapped variant → null ``assay_level`` and an empty triple).
    """

    variant_urn: str
    score: Optional[float]
    consequence: Optional[str]
    clingen_allele_id: Optional[str]
    assay_level_digest: Optional[str]
    hgvs_nt: Optional[HgvsField]
    hgvs_pro: Optional[HgvsField]
    hgvs_splice: Optional[HgvsField]
    assay_level: Optional[SequenceLevel]
    mapped: MappedTriple


def _level(value: Optional[str]) -> Optional[SequenceLevel]:
    """Coerce a stored assay-level string into :class:`SequenceLevel`, or ``None`` if absent."""
    return SequenceLevel(value) if value else None


def _hgvs_field_for_str(hgvs: Optional[str]) -> Optional[HgvsField]:
    """Wrap an HGVS string as an :class:`HgvsField`, attaching a parsed block when it is a placeable
    simple substitution. ``None`` for an absent string."""
    if not hgvs:
        return None

    block = parse_simple_substitution(hgvs)
    if block is None:
        return HgvsField(hgvs=hgvs)

    return HgvsField(hgvs=hgvs, position=block.position, ref=block.ref, alt=block.alt)


def mapped_triple(*, hgvs_c: Optional[str], hgvs_g: Optional[str], hgvs_p: Optional[str]) -> MappedTriple:
    """Wrap a measured projection group's HGVS strings as a :class:`MappedTriple`.

    The group describes the measured variant at every level it has, the measured level included: a
    protein assay's group holds only ``hgvs_p``, and a nucleotide assay's holds its normalized measured
    string and the projections. A record whose group is not yet written (between mapping and the
    projection step) gets an empty triple.
    """
    return MappedTriple(
        genomic=_hgvs_field_for_str(hgvs_g),
        cdna=_hgvs_field_for_str(hgvs_c),
        protein=_hgvs_field_for_str(hgvs_p),
    )


def get_lean_score_set_variants(
    db: Session, score_set: ScoreSet, *, as_of: Optional[datetime] = None
) -> list[LeanVariantRecord]:
    """Assemble the lean whole-set view for ``score_set``, one record per variant, ordered by variant
    number. Unmapped variants are retained (null mapped fields) so the table and score histogram still
    see them. ``as_of`` defaults to currently-live rows.

    Every per-variant lookup is a ``LATERAL ... LIMIT 1`` subquery. As plain joins, Postgres may choose a
    hash join that scans the whole of ``mapping_record_alleles`` once its row estimate for the score set
    crosses a threshold, which is orders of magnitude slower when that table is not cached. A lateral subquery with a
    ``LIMIT`` cannot be flattened into the join tree, so each lookup stays a per-variant index probe
    whatever the planner's statistics say. Each lookup yields at most one row by invariant (one live
    record per variant, one live authoritative link per record, which points at one group); the ``ORDER BY``
    only makes a violated invariant deterministic.
    """
    # The variant's live mapping record.
    record = (
        select(MappingRecord.id, MappingRecord.assay_level)
        .where(MappingRecord.variant_id == Variant.id, MappingRecord.live_at(as_of))
        .order_by(MappingRecord.id)
        .limit(1)
        .lateral("record")
    )

    # The authoritative (measured) allele, with its VEP consequence and its measured projection group's
    # HGVS. ``is_authoritative`` is tested as a bare boolean: ``IS TRUE`` would not match the partial unique
    # index's ``WHERE is_authoritative``.
    authoritative_link = aliased(MappingRecordAllele)
    authoritative = (
        select(
            Allele.vrs_digest,
            Allele.clingen_allele_id,
            VepAlleleConsequence.functional_consequence,
            ProjectionGroup.hgvs_c,
            ProjectionGroup.hgvs_g,
            ProjectionGroup.hgvs_p,
        )
        .select_from(authoritative_link)
        .join(Allele, Allele.id == authoritative_link.allele_id)
        .outerjoin(
            VepAlleleConsequence, and_(VepAlleleConsequence.allele_id == Allele.id, VepAlleleConsequence.live_at(as_of))
        )
        .outerjoin(
            ProjectionGroup,
            and_(ProjectionGroup.id == authoritative_link.projection_group, ProjectionGroup.live_at(as_of)),
        )
        .where(
            authoritative_link.mapping_record_id == record.c.id,
            authoritative_link.is_authoritative,
            authoritative_link.live_at(as_of),
        )
        .order_by(authoritative_link.id)
        .limit(1)
        .lateral("authoritative")
    )

    statement = (
        select(
            Variant.urn,
            Variant.data,
            Variant.hgvs_nt,
            Variant.hgvs_pro,
            Variant.hgvs_splice,
            record.c.assay_level,
            authoritative.c.vrs_digest,
            authoritative.c.clingen_allele_id,
            authoritative.c.functional_consequence,
            authoritative.c.hgvs_c,
            authoritative.c.hgvs_g,
            authoritative.c.hgvs_p,
        )
        .select_from(Variant)
        # LEFT joins so unmapped variants stay (null mapped fields) for the table + score histogram.
        .outerjoin(record, true())
        .outerjoin(authoritative, true())
        .where(Variant.score_set_id == score_set.id)
        # Natural table order: variant number (the integer after '#'), id breaks ties stably.
        .order_by(Variant.variant_number, Variant.id)
    )

    return [
        LeanVariantRecord(
            variant_urn=row.urn,
            score=score_from_variant_data(row.data),
            consequence=row.functional_consequence,
            clingen_allele_id=row.clingen_allele_id,
            assay_level_digest=row.vrs_digest,
            hgvs_nt=_hgvs_field_for_str(row.hgvs_nt),
            hgvs_pro=_hgvs_field_for_str(row.hgvs_pro),
            hgvs_splice=_hgvs_field_for_str(row.hgvs_splice),
            assay_level=_level(row.assay_level),
            mapped=mapped_triple(hgvs_c=row.hgvs_c, hgvs_g=row.hgvs_g, hgvs_p=row.hgvs_p),
        )
        for row in db.execute(statement)
    ]

"""Lean whole-set view backing the score-set page (table, heatmap, score/effect histograms).

Assembles one record per variant in a single query: the score set's variants, with each per-variant
annotation fetched by a ``LATERAL ... LIMIT 1`` lookup (see :func:`get_lean_score_set_variants` for why).

Each record carries the **submitted** HGVS (``hgvs_nt``/``hgvs_pro``/``hgvs_splice``, the depositor's
target frame) and the **mapped** (reference-frame) HGVS as a ``MappedTriple`` — one canonical HGVS per
level (``genomic``/``cdna``/``protein``), with ``assay_level`` naming the measured slot. A nucleotide
assay fills all three slots; a protein assay fills only ``protein`` (the c/g fan-out is ambiguous, so
no pick is fabricated). This is the canonical projection only, never the reverse-translation fan-out.

The nucleotide pair comes from the authoritative (measured) allele plus its ``projection_group``
partner at the other nucleotide level (a group has ≤2 members, so this stays one row per variant).
Where ``projection_group`` is unpopulated (pre-reverse-translation data), that slot is ``None`` until
reprocessed. Does not assemble Cat-VRS.

``as_of`` reconstructs the annotation layer at a past instant; submitted HGVS and scores are immutable
and unaffected. Defaults to currently-live rows.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Sequence

from sqlalchemy import and_, func, select, true
from sqlalchemy.orm import Session, aliased

from mavedb.lib.hgvs import parse_simple_substitution
from mavedb.lib.variants import score_from_variant_data
from mavedb.models.allele import Allele
from mavedb.models.enums.sequence_level import SequenceLevel
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
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

    A nucleotide assay populates all three slots (measured + the authoritative allele's
    ``projection_group`` partner + protein apex); a protein assay populates only ``protein`` — the
    c/g fan-out is ambiguous, so no pick is fabricated for ``cdna``/``genomic``.

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


def mapped_hgvs_by_level(
    *,
    assay_level: Optional[SequenceLevel],
    assay_level_hgvs: Optional[str],
    projection_level: Optional[str],
    projection_hgvs: Optional[str],
    protein_hgvs: Optional[str],
) -> dict[str, Optional[str]]:
    """The canonical mapped HGVS string at each populated level, keyed by ``SequenceLevel`` value.

    - Measured slot ← ``assay_level_hgvs``.
    - Other nucleotide slot ← the authoritative link's ``projection_group`` partner (nucleotide assays
      only, and only where the pairing is recorded).
    - ``protein`` slot ← the separate protein-apex subquery.

    A protein assay fills only ``protein``. An unmapped variant (``assay_level is None``) yields an
    empty mapping.
    """
    slots: dict[str, Optional[str]] = {}
    if assay_level == SequenceLevel.protein:
        slots[SequenceLevel.protein.value] = assay_level_hgvs
    elif assay_level in (SequenceLevel.cdna, SequenceLevel.genomic):
        slots[assay_level.value] = assay_level_hgvs
        slots[SequenceLevel.protein.value] = protein_hgvs
        # The other nucleotide level, from the projection_group partner (null where unpopulated).
        if projection_level is not None:
            slots[projection_level] = projection_hgvs

    return slots


def _mapped_triple(
    *,
    assay_level: Optional[SequenceLevel],
    assay_level_hgvs: Optional[str],
    projection_level: Optional[str],
    projection_hgvs: Optional[str],
    protein_hgvs: Optional[str],
) -> MappedTriple:
    """Assemble the canonical :class:`MappedTriple` for one variant, wrapping each slot from
    :func:`mapped_hgvs_by_level` as an :class:`HgvsField`."""
    slots = mapped_hgvs_by_level(
        assay_level=assay_level,
        assay_level_hgvs=assay_level_hgvs,
        projection_level=projection_level,
        projection_hgvs=projection_hgvs,
        protein_hgvs=protein_hgvs,
    )
    return MappedTriple(
        genomic=_hgvs_field_for_str(slots.get(SequenceLevel.genomic.value)),
        cdna=_hgvs_field_for_str(slots.get(SequenceLevel.cdna.value)),
        protein=_hgvs_field_for_str(slots.get(SequenceLevel.protein.value)),
    )


def get_protein_hgvs_by_record(
    db: Session,
    score_set_id: Optional[int] = None,
    *,
    variant_ids: Optional[Sequence[int]] = None,
    as_of: Optional[datetime] = None,
) -> dict[int, str]:
    """The canonical protein-level mapped HGVS for a set of live mapping records, keyed by
    ``mapping_record.id``. Records with no protein projection (UTR/intronic) are absent from the map.
    Used by the CSV export.

    Exactly one of *score_set_id* (every variant in one score set) or *variant_ids* (an explicit set,
    which may span several score sets — the variant-level CSV's cross-set equivalent measurements) is
    required.

    Runs as its own query rather than a join: the ``DISTINCT ON`` cardinality is opaque to the planner,
    so a join lands it on the inner side of a nested loop and rescans it once per variant — O(N^2), and
    historically ~97% of the lean endpoint's runtime. Stitching by ``mapping_record_id`` in Python keeps
    both queries O(N).
    """
    if (score_set_id is None) == (variant_ids is None):
        raise ValueError("exactly one of score_set_id or variant_ids must be provided")

    prot_link = aliased(MappingRecordAllele)
    prot_allele = aliased(Allele)
    prot_record = aliased(MappingRecord)
    prot_variant = aliased(Variant)
    protein_statement = (
        select(
            prot_link.mapping_record_id.label("mapping_record_id"),
            prot_allele.hgvs_p.label("protein_hgvs"),
        )
        # DISTINCT ON -> at most one protein level row per record.
        .distinct(prot_link.mapping_record_id)
        # link -> allele: level and hgvs_p live on the *allele*.
        .join(prot_allele, prot_allele.id == prot_link.allele_id)
        # link -> record -> variant: the bridge to scoreset_id/variant_id, to scope the query.
        .join(prot_record, prot_record.id == prot_link.mapping_record_id)
        .join(prot_variant, prot_variant.id == prot_record.variant_id)
        # Scope to this score set (or this explicit variant set) so the cost scales with the response.
        .where(
            prot_variant.score_set_id == score_set_id
            if score_set_id is not None
            else prot_variant.id.in_(variant_ids or [])
        )
        # Live links only; a live link implies a live parent record (ValidTime invariant), so no
        # separate prot_record.live_at check is needed.
        .where(prot_link.live_at(as_of))
        # Only the protein-level allele.
        .where(prot_allele.level == SequenceLevel.protein.value)
        # DISTINCT ON needs ORDER BY to lead with its column; allele.id breaks ties (inert today —
        # only one protein allele exists per record).
        .order_by(prot_link.mapping_record_id, prot_allele.id)
    )
    # Keyed by record id (a globally-unique PK -> the right protein row, no cross-set risk).
    return {row.mapping_record_id: row.protein_hgvs for row in db.execute(protein_statement)}


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
    record per variant, one live authoritative link per record, at most one projection partner, one
    protein allele); the ``ORDER BY`` only makes a violated invariant deterministic.
    """
    # The variant's live mapping record.
    record = (
        select(MappingRecord.id, MappingRecord.assay_level, MappingRecord.hgvs_assay_level)
        .where(MappingRecord.variant_id == Variant.id, MappingRecord.live_at(as_of))
        .order_by(MappingRecord.id)
        .limit(1)
        .lateral("record")
    )

    # The authoritative (measured) allele, with its VEP consequence. ``is_authoritative`` is tested as a
    # bare boolean: ``IS TRUE`` would not match the partial unique index's ``WHERE is_authoritative``.
    authoritative_link = aliased(MappingRecordAllele)
    authoritative = (
        select(
            authoritative_link.projection_group,
            Allele.vrs_digest,
            Allele.clingen_allele_id,
            VepAlleleConsequence.functional_consequence,
        )
        .select_from(authoritative_link)
        .join(Allele, Allele.id == authoritative_link.allele_id)
        .outerjoin(
            VepAlleleConsequence, and_(VepAlleleConsequence.allele_id == Allele.id, VepAlleleConsequence.live_at(as_of))
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

    # The authoritative link's projection_group partner: the canonical HGVS at the unmeasured nucleotide
    # level. A NULL group (protein assay, or pre-reverse-translation data) matches nothing, so the slot
    # degrades to None.
    projection_link = aliased(MappingRecordAllele)
    projection_allele = aliased(Allele)
    projection = (
        select(
            projection_allele.level.label("projection_level"),
            # Exactly one of hgvs_g/hgvs_c is populated (it's a nucleotide allele); coalesce picks it.
            func.coalesce(projection_allele.hgvs_g, projection_allele.hgvs_c).label("projection_hgvs"),
        )
        .select_from(projection_link)
        .join(projection_allele, projection_allele.id == projection_link.allele_id)
        .where(
            projection_link.mapping_record_id == record.c.id,
            projection_link.projection_group == authoritative.c.projection_group,
            ~projection_link.is_authoritative,
            projection_link.live_at(as_of),
        )
        .order_by(projection_link.id)
        .limit(1)
        .lateral("projection")
    )

    # The record's protein-level allele (the protein apex for a nucleotide assay). Absent for UTR/intronic.
    protein_link = aliased(MappingRecordAllele)
    protein_allele = aliased(Allele)
    protein = (
        select(protein_allele.hgvs_p.label("protein_hgvs"))
        .select_from(protein_link)
        .join(protein_allele, protein_allele.id == protein_link.allele_id)
        .where(
            protein_link.mapping_record_id == record.c.id,
            protein_link.live_at(as_of),
            protein_allele.level == SequenceLevel.protein.value,
        )
        .order_by(protein_allele.id)
        .limit(1)
        .lateral("protein")
    )

    statement = (
        select(
            Variant.urn,
            Variant.data,
            Variant.hgvs_nt,
            Variant.hgvs_pro,
            Variant.hgvs_splice,
            record.c.assay_level,
            record.c.hgvs_assay_level,
            authoritative.c.vrs_digest,
            authoritative.c.clingen_allele_id,
            authoritative.c.functional_consequence,
            projection.c.projection_level,
            projection.c.projection_hgvs,
            protein.c.protein_hgvs,
        )
        .select_from(Variant)
        # LEFT joins so unmapped variants stay (null mapped fields) for the table + score histogram.
        .outerjoin(record, true())
        .outerjoin(authoritative, true())
        .outerjoin(projection, true())
        .outerjoin(protein, true())
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
            mapped=_mapped_triple(
                assay_level=_level(row.assay_level),
                assay_level_hgvs=row.hgvs_assay_level,
                projection_level=row.projection_level,
                projection_hgvs=row.projection_hgvs,
                protein_hgvs=row.protein_hgvs,
            ),
        )
        for row in db.execute(statement)
    ]

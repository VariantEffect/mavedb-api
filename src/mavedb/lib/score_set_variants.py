"""The lean whole-set view backing the score-set page.

The score-set table, heatmap, and score/effect histograms all compose from one pre-chewed per-variant
dataset. This module assembles that dataset for an entire score set in two O(N) bulk queries — a base
per-variant projection plus a standalone protein-HGVS projection stitched back in Python — and returns
one record per variant. (The protein projection is deliberately *not* a join: folded in, its opaque
cardinality drives the SQL planner into an O(N^2) per-variant rescan; see ``get_lean_score_set_variants``.)

Each record carries the **submitted** HGVS the heatmap's frame toggle needs (``hgvs_nt``/``hgvs_pro``/
``hgvs_splice``, the depositor's target frame) plus the **mapped** (reference frame) representation as a
``MappedTriple`` — one canonical HGVS per level (``genomic`` / ``cdna`` / ``protein``) and an
``assay_level`` pointer naming which slot is the measured one. ``mapped[assay_level]`` is the measured
representation. Only the *canonical* projection is served here — never the reverse-translation
fan-out (that lives in the detail endpoint), so a nucleotide assay fills all three slots while a protein
assay fills only ``protein`` (``cdna``/``genomic`` stay ``None`` — the c/g fan-out is ambiguous, so no
pick is fabricated). Each HGVS rides as an ``HgvsField``: the string (canonical, lossless) plus a parsed
``position``/``ref``/``alt`` block when it is a placeable simple substitution (the heatmap grid; ``None``
for splice/indels/multivariants).

The canonical nucleotide pair comes from the **authoritative** (measured) allele — its digest, ClinGen
id, and VEP consequence — plus one indexed join to its ``projection_group`` sibling at the other
nucleotide level (still **one row per variant**: a group has ≤2 members, so the authoritative has ≤1
sibling). The protein slot is the separate ``DISTINCT ON`` subquery. Where ``projection_group`` is
unpopulated (pre-reverse-translation data) the sibling join returns null → the other nucleotide slot
stays ``None`` and improves as data is re-processed. This view does not assemble Cat-VRS. ``as_of``
reconstructs the annotation layer at a past instant over the variant's immutable scores/submitted HGVS;
it defaults to the currently-live rows.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy import Integer, and_, cast, func, select
from sqlalchemy.orm import Session, aliased

from mavedb.lib.hgvs import parse_simple_substitution
from mavedb.lib.variants import score_from_variant_data
from mavedb.models.allele import Allele
from mavedb.models.enums.annotation_layer import AnnotationLayer
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.vep_allele_consequence import VepAlleleConsequence


@dataclass(frozen=True)
class HgvsField:
    """An HGVS expression plus its parsed simple-substitution block when representable.

    ``hgvs`` is always present (canonical, lossless). ``position``/``ref``/``alt`` are populated only
    when the expression is a single-locus substitution the heatmap can place; they stay ``None`` for
    splice/intronic positions, indels, frameshifts, and multivariants, which carry the string alone.
    """

    hgvs: str
    position: Optional[int] = None
    ref: Optional[str] = None
    alt: Optional[str] = None


@dataclass(frozen=True)
class MappedTriple:
    """The mapped (reference-frame) HGVS at each level — the canonical projection of the measured change.

    One slot per level, each an :class:`HgvsField` or ``None``. A **nucleotide** assay populates all
    three (``mapped[assay_level]`` is the measured slot, the other nucleotide slot is the authoritative
    allele's ``projection_group`` sibling, ``protein`` is the apex). A **protein** assay populates only
    ``protein`` — the c/g fan-out is ambiguous, so ``cdna``/``genomic`` stay ``None`` rather than
    fabricate a canonical pick. A slot is also ``None`` when its source is simply absent (an unmapped
    variant; a projection that failed; pre-reverse-translation data with no sibling recorded).

    ``cdna`` is the level-invariant search key — populated even when ``assay_level`` is ``genomic``.
    """

    genomic: Optional[HgvsField] = None
    cdna: Optional[HgvsField] = None
    protein: Optional[HgvsField] = None


@dataclass(frozen=True)
class LeanVariantRecord:
    """One pre-chewed per-variant record: the selection key (``variant_urn``), the baseline ``score``,
    a representative (lossy) VEP ``consequence``, the bridge identifiers into the annotation dimensions
    (``clingen_allele_id``, ``assay_level_digest``), the submitted HGVS at each level, and the mapped
    (reference-frame) representation as an ``assay_level`` pointer (an ``AnnotationLayer`` value naming
    the measured/canonical slot) plus a ``mapped`` :class:`MappedTriple`. ``mapped[assay_level]`` is the
    measured representation; ``mapped.cdna`` is the level-invariant search key. Any field is ``None``
    when its source is absent (unmapped variant → null ``assay_level`` + empty triple)."""

    variant_urn: str
    score: Optional[float]
    consequence: Optional[str]
    clingen_allele_id: Optional[str]
    assay_level_digest: Optional[str]
    hgvs_nt: Optional[HgvsField]
    hgvs_pro: Optional[HgvsField]
    hgvs_splice: Optional[HgvsField]
    assay_level: Optional[str]
    mapped: MappedTriple


def _hgvs_field_for_str(hgvs: Optional[str]) -> Optional[HgvsField]:
    """Wrap an HGVS string as an :class:`HgvsField`, attaching a parsed block when it is a placeable
    simple substitution. ``None`` for an absent string."""
    if not hgvs:
        return None
    block = parse_simple_substitution(hgvs)
    if block is None:
        return HgvsField(hgvs=hgvs)
    return HgvsField(hgvs=hgvs, position=block.position, ref=block.ref, alt=block.alt)


def _mapped_triple(
    *,
    assay_level: Optional[str],
    assay_level_hgvs: Optional[str],
    sibling_level: Optional[str],
    sibling_hgvs: Optional[str],
    protein_hgvs: Optional[str],
) -> MappedTriple:
    """Assemble the canonical :class:`MappedTriple` for one variant from its per-row query fields.

    - Measured slot (``mapped[assay_level]``) ← ``assay_level_hgvs`` (the record's mapped assay HGVS).
    - Other nucleotide slot ← the authoritative link's ``projection_group`` sibling (``sibling_level`` /
      ``sibling_hgvs``), populated only on a nucleotide assay and only where the pairing is recorded.
    - ``protein`` slot ← the separate protein-apex subquery.

    A **protein** assay fills only ``protein`` (the measured slot is protein itself); its nucleotide
    fan-out is ambiguous, so ``cdna``/``genomic`` are deliberately left ``None``. An unmapped variant
    (``assay_level is None``) yields an empty triple.
    """
    slots: dict[str, Optional[HgvsField]] = {}
    # Protein assay: the measured slot *is* protein. No canonical c/g — do not fabricate one.
    if assay_level == AnnotationLayer.protein.value:
        slots[AnnotationLayer.protein.value] = _hgvs_field_for_str(assay_level_hgvs)
    elif assay_level in (AnnotationLayer.cdna.value, AnnotationLayer.genomic.value):
        slots[assay_level] = _hgvs_field_for_str(assay_level_hgvs)
        # The other nucleotide level, from the projection_group sibling (null where unpopulated).
        if sibling_level is not None:
            slots[sibling_level] = _hgvs_field_for_str(sibling_hgvs)
        slots[AnnotationLayer.protein.value] = _hgvs_field_for_str(protein_hgvs)

    return MappedTriple(
        genomic=slots.get(AnnotationLayer.genomic.value),
        cdna=slots.get(AnnotationLayer.cdna.value),
        protein=slots.get(AnnotationLayer.protein.value),
    )


def get_lean_score_set_variants(
    db: Session, score_set: ScoreSet, *, as_of: Optional[datetime] = None
) -> list[LeanVariantRecord]:
    """Assemble the lean whole-set view for ``score_set`` — one record per variant, ordered by variant
    number. Unmapped variants are retained (left joins) with null mapped fields so the table and score
    histogram still see them. ``as_of`` reconstructs the annotation layer at a past instant (submitted
    HGVS and scores are immutable and unaffected); it defaults to the currently-live rows.
    """
    # The other-nucleotide sibling of the authoritative link: same mapping record + same
    # projection_group, but NOT authoritative (so it is the *other* member of the ≤2-member group). Its
    # allele carries the canonical HGVS at the level the assay was not measured at. See the join below.
    sibling_link = aliased(MappingRecordAllele)
    sibling_allele = aliased(Allele)

    # The base per-variant projection: 1:1 annotation joins, so this whole query is a stream of O(N)
    # index-scan lookups. MappingRecord.id rides along as the key the protein projection (a *separate*
    # query, below) is stitched back on.
    base_statement = (
        select(
            Variant.urn,
            Variant.data,
            Variant.hgvs_nt,
            Variant.hgvs_pro,
            Variant.hgvs_splice,
            MappingRecord.id.label("mapping_record_id"),
            MappingRecord.assay_level,
            MappingRecord.hgvs_assay_level,
            Allele.vrs_digest,
            Allele.clingen_allele_id,
            VepAlleleConsequence.functional_consequence,
            sibling_allele.level.label("sibling_level"),
            # Exactly one of the sibling's hgvs_g/hgvs_c is populated (it is a nucleotide allele), so
            # coalesce yields its canonical string regardless of which nucleotide level it sits at.
            func.coalesce(sibling_allele.hgvs_g, sibling_allele.hgvs_c).label("sibling_hgvs"),
        )
        # Variant -> its live mapping record. LEFT join so unmapped variants stay (with null mapped fields)
        # for the table + score histogram. live_at rides in the ON clause, never a WHERE: a WHERE would
        # reject the null-record row an outer join makes for an unmapped variant, silently collapsing the
        # LEFT join to an inner one. Same pattern on every join below.
        .outerjoin(MappingRecord, and_(MappingRecord.variant_id == Variant.id, MappingRecord.live_at(as_of)))
        # The one authoritative (measured) allele. is_authoritative keeps this 1:1 with the variant
        # (one authoritative link per live record — the invariant the mapping job upholds).
        .outerjoin(
            MappingRecordAllele,
            and_(
                MappingRecordAllele.mapping_record_id == MappingRecord.id,
                MappingRecordAllele.is_authoritative.is_(True),
                MappingRecordAllele.live_at(as_of),
            ),
        )
        # The authoritative allele row itself (digest, ClinGen id). No live_at: alleles are immutable and
        # deduplicated, not ValidTime — the live link already establishes what applies.
        .outerjoin(Allele, Allele.id == MappingRecordAllele.allele_id)
        # Its VEP consequence.
        .outerjoin(
            VepAlleleConsequence, and_(VepAlleleConsequence.allele_id == Allele.id, VepAlleleConsequence.live_at(as_of))
        )
        # The authoritative link's projection_group sibling — the canonical projection at the *other*
        # nucleotide level. UNLIKE the protein subquery, this stays a direct 1:1 outer join: a group has
        # ≤2 members and the authoritative link is one of them, so at most one non-authoritative sibling
        # shares its (mapping_record_id, projection_group). The mapping_record_id predicate rides the
        # existing ix_mapping_record_alleles_mapping_record_id index; within a record only a handful of
        # links are scanned, so no O(N^2) rescan (contrast the opaque-cardinality protein subquery below).
        # A NULL authoritative group (protein assay, or pre-reverse-translation data) matches nothing —
        # SQL equality on NULL is never true — so the sibling stays null and the slot degrades gracefully.
        .outerjoin(
            sibling_link,
            and_(
                sibling_link.mapping_record_id == MappingRecord.id,
                sibling_link.projection_group == MappingRecordAllele.projection_group,
                sibling_link.is_authoritative.is_(False),
                sibling_link.live_at(as_of),
            ),
        )
        .outerjoin(sibling_allele, sibling_allele.id == sibling_link.allele_id)
        # The anchor: just this score set's variants.
        .where(Variant.score_set_id == score_set.id)
        # Natural table order: variant number (the integer after '#'), id breaks ties stably.
        .order_by(cast(func.split_part(Variant.urn, "#", 2), Integer), Variant.id)
    )

    # The protein projection runs as its own query, NOT a join into the base statement. Joined in, it forces
    # an O(N^2) plan: the DISTINCT ON cardinality is opaque to the planner (est. ~18 vs ~11.5k actual), so it
    # buried the projection on the inner side of a nested loop and rescanned it in full once per variant —
    # ~97% of the endpoint's total time. MATERIALIZED in the query above fixes the *construction* but not the
    # per-variant *rescan*. Pulling out and stitching these queries by mapping_record_id in Python enables both
    # queries to run in O(N) time, making this function (and the endpoint it serves) considerably faster.
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
        # link -> record -> variant: the bridge to scoreset_id, to scope the query.
        .join(prot_record, prot_record.id == prot_link.mapping_record_id)
        .join(prot_variant, prot_variant.id == prot_record.variant_id)
        # Scope to this score set so the cost scales with the response.
        .where(prot_variant.score_set_id == score_set.id)
        # Live links only. A live link implies a live parent record (ValidTime invariant), so this also
        # covers record-liveness — no prot_record.live_at needed; the MR join is just the scoping bridge.
        .where(prot_link.live_at(as_of))
        # Only the protein-level allele.
        .where(prot_allele.level == AnnotationLayer.protein.value)
        # DISTINCT ON requires ORDER BY to lead with its column; allele.id then picks which row survives
        # per record — inert today (only one protein allele exists).
        .order_by(prot_link.mapping_record_id, prot_allele.id)
    )
    # Keyed by record id (a globally-unique PK -> the right protein row, no cross-set risk). Some records
    # have no protein projection (UTR/intronic) -> absent from the map -> null protein field below.
    protein_hgvs_by_record: dict[int, str] = {
        row.mapping_record_id: row.protein_hgvs for row in db.execute(protein_statement)
    }

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
            assay_level=row.assay_level,
            mapped=_mapped_triple(
                assay_level=row.assay_level,
                assay_level_hgvs=row.hgvs_assay_level,
                sibling_level=row.sibling_level,
                sibling_hgvs=row.sibling_hgvs,
                protein_hgvs=protein_hgvs_by_record.get(row.mapping_record_id),
            ),
        )
        for row in db.execute(base_statement)
    ]

"""The lean whole-set view backing the score-set page.

The score-set table, heatmap, and score/effect histograms all compose from one pre-chewed per-variant
dataset. This module assembles that dataset for an entire score set in a single bulk query and returns
one record per variant.

Each record carries the HGVS in both frames the heatmap toggles between — the depositor-**submitted**
strings (``hgvs_nt``/``hgvs_pro``/``hgvs_splice``, the target frame) and the **mapped** assay-level
string (reference frame) — plus, riding alongside each string, the parsed ``position``/``ref``/``alt``
block when the expression is a placeable simple substitution (the heatmap grid; ``None`` for
splice/indels/multivariants, which keep the string alone). The string is the canonical, lossless field;
the block is a convenience the client would otherwise derive by parsing.

Because every mapped string now comes from the ``MappingRecord`` and the only allele we need is the
**authoritative** (measured) one — for its digest, ClinGen id, and VEP consequence — the query joins
the authoritative link only and so returns **one row per variant** (no allele fan-out to regroup). It
does not assemble Cat-VRS. ``as_of`` reconstructs the annotation layer at a past instant over
the variant's immutable scores/submitted HGVS; it defaults to the currently-live rows.
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


# TODO#784: Add a canonical nucleotide level projection in parallel with the canonical protein level projection.
@dataclass(frozen=True)
class LeanVariantRecord:
    """One pre-chewed per-variant record: the selection key (``variant_urn``), the baseline ``score``,
    a representative (lossy) VEP ``consequence``, the bridge identifiers into the annotation dimensions
    (``clingen_allele_id``, ``assay_level_digest``), the submitted HGVS at each level, and the two mapped
    representations: ``assay_level_hgvs`` (the measured level) and ``protein_level_hgvs`` (the protein level).
    For a protein assay the two mapped fields coincide. Any field is ``None`` when its source is absent
    (unmapped variant → null mapped fields; a level the variant does not carry → null slot)."""

    variant_urn: str
    score: Optional[float]
    consequence: Optional[str]
    clingen_allele_id: Optional[str]
    assay_level_digest: Optional[str]
    hgvs_nt: Optional[HgvsField]
    hgvs_pro: Optional[HgvsField]
    hgvs_splice: Optional[HgvsField]
    assay_level_hgvs: Optional[HgvsField]
    protein_level_hgvs: Optional[HgvsField]


def _hgvs_field_for_str(hgvs: Optional[str]) -> Optional[HgvsField]:
    """Wrap an HGVS string as an :class:`HgvsField`, attaching a parsed block when it is a placeable
    simple substitution. ``None`` for an absent string."""
    if not hgvs:
        return None
    block = parse_simple_substitution(hgvs)
    if block is None:
        return HgvsField(hgvs=hgvs)
    return HgvsField(hgvs=hgvs, position=block.position, ref=block.ref, alt=block.alt)


def get_lean_score_set_variants(
    db: Session, score_set: ScoreSet, *, as_of: Optional[datetime] = None
) -> list[LeanVariantRecord]:
    """Assemble the lean whole-set view for ``score_set`` — one record per variant, ordered by variant
    number. Unmapped variants are retained (left joins) with null mapped fields so the table and score
    histogram still see them. ``as_of`` reconstructs the annotation layer at a past instant (submitted
    HGVS and scores are immutable and unaffected); it defaults to the currently-live rows.
    """
    # Per record, the forward-translated protein HGVS (hgvs_p) of its protein-level allele.
    prot_link = aliased(MappingRecordAllele)
    prot_allele = aliased(Allele)
    prot_record = aliased(MappingRecord)
    prot_variant = aliased(Variant)
    protein_allele = (
        select(
            prot_link.mapping_record_id.label("mapping_record_id"),
            prot_allele.hgvs_p.label("protein_hgvs"),
        )
        # DISTINCT ON -> at most one protein level row per record.
        .distinct(prot_link.mapping_record_id)
        # link -> allele: level and hgvs_p live on the *allele*.
        .join(prot_allele, prot_allele.id == prot_link.allele_id)
        # link -> record -> variant: the bridge to scoreset_id, to scope the subquery (next).
        .join(prot_record, prot_record.id == prot_link.mapping_record_id)
        .join(prot_variant, prot_variant.id == prot_record.variant_id)
        # Scope to this score set so the subquery's cost scales with the response.
        .where(prot_variant.score_set_id == score_set.id)
        # Live links only. A live link implies a live parent record (ValidTime invariant), so this also
        # covers record-liveness — no prot_record.live_at needed; the MR join is just the scoping bridge.
        .where(prot_link.live_at(as_of))
        # Only the protein-level allele.
        .where(prot_allele.level == AnnotationLayer.protein.value)
        # DISTINCT ON requires ORDER BY to lead with its column; allele.id then picks which row survives
        # per record — inert today (only one protein allele exists).
        .order_by(prot_link.mapping_record_id, prot_allele.id)
        .subquery()
    )

    statement = (
        select(
            Variant.urn,
            Variant.data,
            Variant.hgvs_nt,
            Variant.hgvs_pro,
            Variant.hgvs_splice,
            MappingRecord.hgvs_assay_level,
            protein_allele.c.protein_hgvs.label("protein_hgvs"),
            Allele.vrs_digest,
            Allele.clingen_allele_id,
            VepAlleleConsequence.functional_consequence,
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
        # The protein subquery, matched back by record id (a globally-unique PK -> the right protein row,
        # no cross-set risk). LEFT: some rows have no protein projection (UTR/intronic) -> null.
        .outerjoin(protein_allele, protein_allele.c.mapping_record_id == MappingRecord.id)
        # The anchor: just this score set's variants.
        .where(Variant.score_set_id == score_set.id)
        # Natural table order: variant number (the integer after '#'), id breaks ties stably.
        .order_by(cast(func.split_part(Variant.urn, "#", 2), Integer), Variant.id)
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
            assay_level_hgvs=_hgvs_field_for_str(row.hgvs_assay_level),
            protein_level_hgvs=_hgvs_field_for_str(row.protein_hgvs),
        )
        for row in db.execute(statement)
    ]

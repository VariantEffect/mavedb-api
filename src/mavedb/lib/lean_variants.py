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

from sqlalchemy import Integer, and_, cast, func, or_, select
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
    # Content valid-time: each ValidTime layer is evaluated at the same instant, or at the current tail.
    record_live = MappingRecord.as_of(as_of) if as_of is not None else MappingRecord.current
    link_live = MappingRecordAllele.as_of(as_of) if as_of is not None else MappingRecordAllele.current
    vep_live = VepAlleleConsequence.as_of(as_of) if as_of is not None else VepAlleleConsequence.current

    # The post-mapped protein HGVS is the forward-translated protein consequence, stored in the hgvs_p
    # column of the record's protein-level allele. Pull it as a correlated scalar subquery so the main
    # query stays one row per variant. It is  null when no forward-translated protein is available. For
    # a protein assay this is the measured allele, so it coincides with assay_level_hgvs.
    prot_link = aliased(MappingRecordAllele)
    prot_allele = aliased(Allele)
    prot_link_live = (
        and_(prot_link.valid_from <= as_of, or_(prot_link.valid_to.is_(None), prot_link.valid_to > as_of))
        if as_of is not None
        else prot_link.valid_to.is_(None)
    )
    protein_hgvs_subquery = (
        select(prot_allele.hgvs_p)
        .join(prot_link, prot_link.allele_id == prot_allele.id)
        .where(prot_link.mapping_record_id == MappingRecord.id)
        .where(prot_link_live)
        .where(prot_allele.level == AnnotationLayer.protein.value)
        .order_by(prot_allele.id)
        .limit(1)
        .correlate(MappingRecord)
        .scalar_subquery()
    )

    statement = (
        select(
            Variant.urn,
            Variant.data,
            Variant.hgvs_nt,
            Variant.hgvs_pro,
            Variant.hgvs_splice,
            MappingRecord.hgvs_assay_level,
            protein_hgvs_subquery.label("protein_hgvs"),
            Allele.vrs_digest,
            Allele.clingen_allele_id,
            VepAlleleConsequence.functional_consequence,
        )
        .outerjoin(MappingRecord, and_(MappingRecord.variant_id == Variant.id, record_live))
        # Only the authoritative (measured) allele is needed, so the link join stays 1:1 with the
        # variant — one authoritative link per live record (the invariant the mapping job upholds).
        .outerjoin(
            MappingRecordAllele,
            and_(
                MappingRecordAllele.mapping_record_id == MappingRecord.id,
                MappingRecordAllele.is_authoritative.is_(True),
                link_live,
            ),
        )
        .outerjoin(Allele, Allele.id == MappingRecordAllele.allele_id)
        .outerjoin(VepAlleleConsequence, and_(VepAlleleConsequence.allele_id == Allele.id, vep_live))
        .where(Variant.score_set_id == score_set.id)
        # Variant number (the integer after '#') is the natural table order; id breaks ties stably.
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

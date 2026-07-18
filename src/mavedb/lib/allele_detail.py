"""The allele-detail view backing ``GET /alleles/{digest:CAID}``.

The allele-grain sibling of :mod:`lib.variant_detail`. Where the variant view anchors on a
*measurement*, this anchors on an *allele* (a VRS digest, or a CAID naming the nt-canonical change)
and serves that allele's identity, its full cross-layer **equivalence class**, and the digest-keyed
annotations that ride alongside.

It is measurement-agnostic, and that shapes what it does *not* carry:

- No score, counts, classification, or version standing — those are properties of a measured variant,
  not of an allele (an allele is deduplicated by digest and shared across every score set mapping to it).
- No spec-pure Cat-VRS. The ``CategoricalVariant`` / ``DefiningAlleleConstraint`` is emitted only by the
  measurement view, rooted at the measured allele. Here the members are labelled on a flat map
  *relative to the focus allele*.

**The equivalence class is the full cross-record union** (:func:`lib.alleles.get_allele_translations`):
every allele co-linked to any live record touching the focus. That is the discoverable "everything
related to this change" set. Each member is then labelled relative to the focus — which the graph
determines even without a measurement, because candidate-ness is *directional* (protein→nt is
many-to-one ⇒ candidate; nt→protein is deterministic ⇒ consequence; nt↔nt is a faithful coordinate
transform). "Less power than a measurement" only means we cannot say which nt was *measured* — but we
can still say faithful vs. candidate vs. convergent from the graph and the focus.

``as_of`` reconstructs the molecular layer — class membership + annotations — at the past instant. The
focus allele's own identity is immutable and content-addressed. ``as_of`` defaults to currently-live rows.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.allele_annotations import AlleleAnnotations, get_allele_annotations
from mavedb.lib.allele_identity import AlleleDerivation, AlleleIdentity
from mavedb.lib.alleles import get_allele_translations
from mavedb.lib.cat_vrs import CatVrsRelation
from mavedb.lib.vrs import vrs_object_from_mapped_variant
from mavedb.models.allele import Allele
from mavedb.models.enums.sequence_level import NUCLEOTIDE_LEVELS, SequenceLevel
from mavedb.models.mapping_record_allele import MappingRecordAllele


@dataclass(frozen=True)
class AlleleDetail:
    """The assembled allele-detail envelope (transit; serialized by ``view_models.allele_detail``)."""

    # Flat anchor identity — the queried allele's own molecular facts (for a CAID fetch, its
    # representative allele; ``clingenAlleleId`` carries the CAID either way).
    digest: str
    level: Optional[str]
    hgvs: Optional[str]  # reference-frame HGVS (exactly one of hgvs_g/c/p, depending on level)
    clingen_allele_id: Optional[str]
    vrs: Optional[dict[str, Any]]  # spec-pure GA4GH VRS variation from post_mapped

    # The full cross-layer equivalence class, keyed by VRS digest and sharing keys with `annotations`.
    # Each member is an `AlleleIdentity` labelled relative to the focus. The focus allele(s) carry
    # `is_focus=True`.
    alleles: dict[str, AlleleIdentity]

    # External annotations, keyed by VRS digest, for every member of the class.
    annotations: dict[str, AlleleAnnotations]


def _focus_projection_pairs(db: Session, focus_alleles: list[Allele], *, as_of: Optional[datetime]) -> dict[str, str]:
    """A symmetric ``digest -> sibling digest`` map for each focus allele's c↔g coordinate partner.

    Read from any one live record of the focus: the partner is the ≤1 *other* allele sharing the focus
    allele's ``projection_group`` (a projection group is a c↔g pair). This is the one place the
    focus/partner-vs-convergent-cousin distinction needs the link-level ``projection_group`` — every
    other member is classified by level alone.
    """
    pairs: dict[str, str] = {}
    for focus in focus_alleles:
        if focus.vrs_digest is None:
            continue
        link = db.scalar(
            select(MappingRecordAllele)
            .where(MappingRecordAllele.allele_id == focus.id)
            .where(MappingRecordAllele.projection_group.isnot(None))
            .where(MappingRecordAllele.live_at(as_of))
            .limit(1)
        )
        if link is None:
            continue
        sibling_digests = db.scalars(
            select(Allele.vrs_digest)
            .select_from(MappingRecordAllele)
            .join(Allele, Allele.id == MappingRecordAllele.allele_id)
            .where(MappingRecordAllele.mapping_record_id == link.mapping_record_id)
            .where(MappingRecordAllele.projection_group == link.projection_group)
            .where(MappingRecordAllele.allele_id != focus.id)
            .where(MappingRecordAllele.live_at(as_of))
        ).all()
        for sibling in sibling_digests:
            if sibling is not None:
                pairs[focus.vrs_digest] = sibling
                pairs.setdefault(sibling, focus.vrs_digest)
    return pairs


def _member_label(
    *, focus_is_protein: bool, member_level: Optional[str], is_coordinate_partner: bool
) -> tuple[Optional[CatVrsRelation], Optional[AlleleDerivation]]:
    """The (relation, derivation) for a non-focus member, read *relative to the focus*.

    - **protein focus** → every nt member is a reverse-translation ``candidate`` that ``encodes`` it.
    - **nt focus** → the protein consequence is ``translation_of`` (a deterministic ``projection``); the
      focus's coordinate partner is ``coordinate_representation_of`` (also ``projection``); any other nt
      is a synonymous cousin — ``co_encodes`` / ``convergent`` (a distinct change sharing the consequence).
    """
    if focus_is_protein:
        if member_level in NUCLEOTIDE_LEVELS:
            return CatVrsRelation.ENCODES, AlleleDerivation.CANDIDATE
        return None, None

    if member_level == SequenceLevel.protein.value:
        return CatVrsRelation.TRANSLATION_OF, AlleleDerivation.PROJECTION
    if member_level in NUCLEOTIDE_LEVELS:
        if is_coordinate_partner:
            return CatVrsRelation.COORDINATE_REPRESENTATION_OF, AlleleDerivation.PROJECTION
        return CatVrsRelation.CO_ENCODES, AlleleDerivation.CONVERGENT
    return None, None


def get_allele_detail(
    db: Session, anchor: Allele, *, focus_digests: set[str], as_of: Optional[datetime] = None
) -> AlleleDetail:
    """Assemble the allele-detail envelope anchored on ``anchor``.

    ``focus_digests`` is the set of VRS digests that are the *focus* of this view — a single digest for
    a by-digest fetch, or the (genomic + coding) representations of a CAID for a by-CAID fetch. The
    equivalence class is the record-agnostic union co-linked to any live record touching ``anchor``
    (:func:`get_allele_translations`, which includes ``anchor``); an orphan allele with no live links
    falls back to itself. Every member is labelled relative to the focus (see :func:`_member_label`).
    ``as_of`` reconstructs the molecular layer; see the module docstring.
    """
    equivalence_class = get_allele_translations(db, anchor.id, as_of=as_of) or [anchor]

    annotations = get_allele_annotations(db, equivalence_class, as_of=as_of)

    focus_alleles = [a for a in equivalence_class if a.vrs_digest in focus_digests] or [anchor]
    focus_is_protein = any(a.level == SequenceLevel.protein.value for a in focus_alleles)

    pairs = _focus_projection_pairs(db, focus_alleles, as_of=as_of)
    # The focus's coordinate partner(s) that are not themselves focus (for a CAID fetch the c↔g pair is
    # jointly the focus, so this is empty and the partners are simply flagged is_focus instead).
    coordinate_partner_digests = {pairs[d] for d in focus_digests if d in pairs} - focus_digests

    alleles: dict[str, AlleleIdentity] = {}
    for member in equivalence_class:
        digest = member.vrs_digest
        if digest is None:
            continue

        if digest in focus_digests:
            relation, derivation = None, None
        else:
            rel, der = _member_label(
                focus_is_protein=focus_is_protein,
                member_level=member.level,
                is_coordinate_partner=digest in coordinate_partner_digests,
            )
            relation = rel.value if rel is not None else None
            derivation = der.value if der is not None else None

        alleles[digest] = AlleleIdentity(
            level=member.level,
            hgvs=member.hgvs_g or member.hgvs_c or member.hgvs_p,
            clingen_allele_id=member.clingen_allele_id,
            is_focus=digest in focus_digests,
            relation=relation,
            derivation=derivation,
            projection_of=pairs.get(digest),
        )

    return AlleleDetail(
        digest=anchor.vrs_digest or "",
        level=anchor.level,
        hgvs=anchor.hgvs_g or anchor.hgvs_c or anchor.hgvs_p,
        clingen_allele_id=anchor.clingen_allele_id,
        vrs=vrs_object_from_mapped_variant(anchor.post_mapped).model_dump(mode="json", exclude_none=True)
        if anchor.post_mapped is not None
        else None,
        alleles=alleles,
        annotations=annotations,
    )

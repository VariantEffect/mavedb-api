"""The allele-detail view backing ``GET /alleles/{digest|CAID}``.

The allele-grain counterpart of :mod:`lib.variant_detail`. Where the variant view anchors on a
*measurement*, this anchors on an *allele* (a VRS digest, or a CAID naming the nt-canonical change)
and serves that allele's identity, its full cross-layer **equivalence class**, and the digest-keyed
annotations that ride alongside.

It is measurement-agnostic, and that shapes what it does *not* carry:

- No score, counts, classification, or version standing. Those are properties of a measured variant,
  not of an allele (an allele is deduplicated by digest and shared across every score set mapping to it).
- No spec-pure Cat-VRS. The ``CategoricalVariant`` / ``DefiningAlleleConstraint`` is emitted only by the
  measurement view, rooted at the measured allele. Here the members are labelled on a flat map
  *relative to the focus allele*.

**The equivalence class is the full cross-record union** (:func:`lib.alleles.get_allele_translations`):
every allele co-linked to any live record touching the focus. That is the discoverable "everything
related to this change" set. Each member is then labelled relative to the focus allele. Even though this
level is unable to determine which allele was actually measured, we can still say how a given allele relates
to the focus allele (e.g., faithful, candidate, convergent) based on the graph structure.

``as_of`` reconstructs the molecular layer, class membership + annotations, at the past instant. The
focus allele's own identity is immutable and content-addressed. ``as_of`` defaults to this instant.
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

    # Own identity (the focus allele itself).
    digest: str
    level: Optional[str]
    """The sequence level of the allele (e.g., nucleotide or amino acid)."""
    hgvs: Optional[str]
    """The reference-frame HGVS string (exactly one of hgvs_g/c/p, depending on level)."""
    clingen_allele_id: Optional[str]
    vrs: Optional[dict[str, Any]]

    # Cross-layer equivalence class (all alleles equivalent to the focus).
    alleles: dict[str, AlleleIdentity]
    """The full cross-layer equivalence class, keyed by VRS digest and sharing keys with `annotations`.
    Each member is an `AlleleIdentity` labelled relative to the focus. The focus allele(s) carry
    `is_focus=True`."""

    # External annotations.
    annotations: dict[str, AlleleAnnotations]
    """External annotations for each member of the cross-layer equivalence class, keyed by VRS digest."""


def _focus_projection_pairs(db: Session, focus_alleles: list[Allele], *, as_of: Optional[datetime]) -> dict[str, str]:
    """A symmetric ``digest -> digest`` map pairing each focus allele with its c↔g projection.

    A ``projection_group`` is one c↔g pair, local to a single mapping record. The projection is the ≤1
    *other* allele sharing the same ``projection_group`` as the focus allele. Reading any one live link
    is safe: the focus holds one live link per record it appears in, and each record re-encodes the same
    change onto the same content-addressed digests, so the pairing is record-independent (assuming those
    records agree on genome assembly).

    This is the one place the projection-vs-convergent-encoding distinction needs the link-level
    ``projection_group`` — every other member is classified by level alone.
    """
    pairs: dict[str, str] = {}
    for focus in focus_alleles:
        if focus.vrs_digest is None:
            continue

        # limit(1) rather than one_or_none(), which would raise for any allele measured more than once.
        link = db.scalar(
            select(MappingRecordAllele)
            .where(MappingRecordAllele.allele_id == focus.id)
            .where(MappingRecordAllele.projection_group.isnot(None))
            .where(MappingRecordAllele.live_at(as_of))
            .limit(1)
        )
        if link is None:
            continue

        projection_digests = db.scalars(
            select(Allele.vrs_digest)
            .select_from(MappingRecordAllele)
            .join(Allele, Allele.id == MappingRecordAllele.allele_id)
            .where(MappingRecordAllele.mapping_record_id == link.mapping_record_id)
            .where(MappingRecordAllele.projection_group == link.projection_group)
            .where(MappingRecordAllele.allele_id != focus.id)
            .where(MappingRecordAllele.live_at(as_of))
        ).all()
        for projection in projection_digests:
            if projection is not None:
                pairs[focus.vrs_digest] = projection
                pairs.setdefault(projection, focus.vrs_digest)

    return pairs


def _member_label(
    *, focus_is_protein: bool, member_level: Optional[str], is_projection: bool
) -> tuple[Optional[CatVrsRelation], Optional[AlleleDerivation]]:
    """The (relation, derivation) for a non-focus member, read *relative to the focus*.

    - **protein focus** → every nt member is a reverse-translation ``candidate`` that ``encodes`` it.
    - **nt focus** → The protein consequence is ``translation_of`` (a deterministic ``projection`` of the focus).
      The focus's projection partner is ``coordinate_representation_of`` (also a deterministic ``projection``).
      Any other nt is a convergent encoding: ``co_encodes`` and ``convergent`` (a distinct change which shares the
      ultimate protein consequence of the focus).
    """
    if focus_is_protein:
        if member_level in NUCLEOTIDE_LEVELS:
            return CatVrsRelation.ENCODES, AlleleDerivation.CANDIDATE

        return None, None

    if member_level == SequenceLevel.protein.value:
        return CatVrsRelation.TRANSLATION_OF, AlleleDerivation.PROJECTION
    if member_level in NUCLEOTIDE_LEVELS:
        if is_projection:
            return CatVrsRelation.COORDINATE_REPRESENTATION_OF, AlleleDerivation.PROJECTION

        return CatVrsRelation.CO_ENCODES, AlleleDerivation.CONVERGENT

    return None, None


def get_allele_detail(
    db: Session, focus: Allele, *, focus_digests: set[str], as_of: Optional[datetime] = None
) -> AlleleDetail:
    """Assemble the allele-detail envelope anchored on ``focus``.

    ``focus_digests`` is the set of VRS digests that are the *focus* of this view. A single digest for
    a by-digest fetch, or the (genomic + coding) representations of a CAID for a by-CAID fetch. The
    equivalence class is every allele co-linked to a live record touching the ``focus``
    (:func:`get_allele_translations`). An orphan allele falls back to ``focus`` alone. Non-focus members
    are labelled by :func:`_member_label`.
    """
    equivalence_class = get_allele_translations(db, focus.id, as_of=as_of) or [focus]
    annotations = get_allele_annotations(db, equivalence_class, as_of=as_of)

    focus_alleles = [a for a in equivalence_class if a.vrs_digest in focus_digests] or [focus]
    focus_is_protein = any(a.level == SequenceLevel.protein.value for a in focus_alleles)

    pairs = _focus_projection_pairs(db, focus_alleles, as_of=as_of)
    # The focus's projection(s) that are not themselves focus (for a CAID fetch the projection pair is
    # jointly the focus, so this is empty and they are simply flagged is_focus instead).
    projection_digests = {pairs[d] for d in focus_digests if d in pairs} - focus_digests

    alleles: dict[str, AlleleIdentity] = {}
    for member in equivalence_class:
        digest = member.vrs_digest
        if digest is None:
            continue

        # Determine the relation and derivation for non-focus members. Focus
        # members have no relation or derivation relative to themselves.
        relation, derivation = None, None
        if digest not in focus_digests:
            rel, der = _member_label(
                focus_is_protein=focus_is_protein,
                member_level=member.level,
                is_projection=digest in projection_digests,
            )
            relation = rel.value if rel is not None else None
            derivation = der

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
        digest=focus.vrs_digest or "",
        level=focus.level,
        hgvs=focus.hgvs_g or focus.hgvs_c or focus.hgvs_p,
        clingen_allele_id=focus.clingen_allele_id,
        vrs=vrs_object_from_mapped_variant(focus.post_mapped).model_dump(mode="json", exclude_none=True)
        if focus.post_mapped is not None
        else None,
        alleles=alleles,
        annotations=annotations,
    )

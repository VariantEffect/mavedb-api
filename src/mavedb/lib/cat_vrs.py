"""Cat-VRS transit, built on the fly from a variant's live allele links.

The Categorical Variant served on ``/variants/{urn}`` is **not** materialized — it is assembled
per request from the variant's live ``MappingRecordAllele`` links (see
``lib/alleles.py::get_live_record_allele_links``). A stored skeleton would be a denormalized cache
of that link graph with a real sync cost and no benefit, since the only consumer is this bounded
single-variant build (full-scan paths never assemble Cat-VRS). The output is response-only and
HTTP-cacheable.

This targets Cat-VRS spec **1.0.0** (``ga4gh.cat_vrs`` package 0.7.2). In that schema a
``CategoricalVariant``'s ``members`` are bare VRS variations with no per-member relation field;
relations ride on the ``DefiningAlleleConstraint`` as a list of ``MappableConcept``. The MaveDB
relation codes (read member -> defining) therefore cannot live on individual members, so the
per-member mapping is returned alongside, keyed by VRS digest, for the MaveDB layer that rides
beside the spec-pure object.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

from ga4gh.cat_vrs.models import CategoricalVariant, DefiningAlleleConstraint, MappableConcept
from ga4gh.core.models import Coding, iriReference
from ga4gh.vrs.models import Allele as VrsAllele
from ga4gh.vrs.models import CisPhasedBlock
from sqlalchemy.orm import Session

from mavedb.lib.alleles import get_live_record_allele_links
from mavedb.lib.logging.context import logging_context
from mavedb.lib.vrs import vrs_object_from_mapped_variant
from mavedb.models.allele import Allele
from mavedb.models.enums.sequence_level import SequenceLevel
from mavedb.models.mapping_record_allele import MappingRecordAllele

logger = logging.getLogger(__name__)

# MaveDB-namespaced relation codes, read as `member -> defining`. Namespaced for MaveDB now,
# TODO: Harmonize with GA4GH Cat-VRS group for standardization.
_RELATION_SYSTEM = "https://mavedb.org/cat-vrs/relations"


# TODO: Verify Cat-VRS relation names with GA4GH Cat-VRS group; these are the MaveDB-namespaced codes we raised for standardization.
class CatVrsRelation(str, Enum):
    """Relation of a member allele to the single defining (measured) allele."""

    # defining is protein; member is an nt allele (coding or genomic) that encodes it. Implied.
    ENCODES = "encodes"
    # defining is nt; member is the same variant in other nt coordinates (genomic<->coding). Faithful.
    COORDINATE_REPRESENTATION_OF = "coordinate_representation_of"
    # defining is nt; member is the protein consequence. Consequence, no independent score.
    TRANSLATION_OF = "translation_of"
    # defining is nt; member is a *synonymous cousin* — an nt allele in a different projection group that
    # encodes the same protein consequence as the measured change.
    CO_ENCODES = "co_encodes"


class CatVrsMode(str, Enum):
    """The score-collapse semantics of the categorical variant."""

    PROJECTION = "projection"  # Mode 1 — nt measured; score rides faithfully.
    REVERSE_TRANSLATION = "reverse_translation"  # Mode 2 — protein measured; score is implied.


_NUCLEOTIDE_LEVELS = {SequenceLevel.genomic.value, SequenceLevel.cdna.value}


@dataclass
class CategoricalVariantTransit:
    """The spec-pure Cat-VRS object plus the MaveDB layer that rides beside it."""

    categorical_variant: CategoricalVariant
    mode: CatVrsMode
    # member vrs_digest -> relation (member -> defining). Excludes the defining allele itself.
    member_relations: dict[str, CatVrsRelation]


def is_convergent_cousin(
    member_level: Optional[str], member_group: Optional[int], *, defining_group: Optional[int]
) -> bool:
    """Whether a projection-mode member is a *synonymous cousin* of the measured change.

    Concretely:
    - The defining (measured) allele is nt (genomic or cdna).
    - The member is nt (genomic or cdna).
    - The member is in a different projection group than the defining (measured) allele.
    """
    return (
        member_level in _NUCLEOTIDE_LEVELS
        and member_group is not None
        and defining_group is not None
        and member_group != defining_group
    )


def _relation_for(
    defining_level: Optional[str],
    member_level: Optional[str],
    *,
    defining_group: Optional[int] = None,
    member_group: Optional[int] = None,
) -> Optional[CatVrsRelation]:
    """Derive the member -> defining relation from the two levels, or None for the defining itself.

    Star model: every member relates to the single defining allele, so the relation depends only on the
    (defining, member) level pair — plus, in projection mode, on whether the nt member shares the measured
    change's ``projection_group`` (its coordinate partner) or lives in another group (a synonymous cousin).
    """
    # Protein measured. Every nt member encodes it, the protein member is the defining. Grouping is irrelevant.
    if defining_level == SequenceLevel.protein.value:
        return CatVrsRelation.ENCODES if member_level in _NUCLEOTIDE_LEVELS else None

    # Nt measured (genomic or cdna).
    if member_level == SequenceLevel.protein.value:
        return CatVrsRelation.TRANSLATION_OF
    if member_level in _NUCLEOTIDE_LEVELS:
        if is_convergent_cousin(member_level, member_group, defining_group=defining_group):
            return CatVrsRelation.CO_ENCODES
        return CatVrsRelation.COORDINATE_REPRESENTATION_OF

    return None  # pragma: no cover -- the levels are constrained by the DB and the mapping job


def _is_precise_projection_member(
    member_level: Optional[str], member_group: Optional[int], *, defining_group: Optional[int]
) -> bool:
    """In projection mode, whether a member is a *precise* representation of the measured change.

    True for the protein consequence (the apex, shared by the whole equivalence class) and the measured
    change's own coordinate partner — the other member of its ``projection_group`` (a c↔g pair). False for
    the synonymous cousins in other projection groups.
    """
    if member_level == SequenceLevel.protein.value:
        return True

    return not is_convergent_cousin(member_level, member_group, defining_group=defining_group)


def _relation_concept(relation: CatVrsRelation) -> MappableConcept:
    """Wrap a MaveDB relation code as a Cat-VRS ``MappableConcept`` for the constraint."""
    return MappableConcept(
        name=relation.value,
        primaryCoding=Coding(id=f"mavedb:{relation.value}", code=relation.value, system=_RELATION_SYSTEM),
    )


def _hydrate_vrs(allele: Allele) -> Optional[VrsAllele | CisPhasedBlock]:
    """Bare VRS variation (Allele or CisPhasedBlock) from the stored post_mapped JSONB.

    ``None`` when the allele has no post_mapped representation — it cannot be a Cat-VRS member.
    """
    if allele.post_mapped is None:
        return None

    variation = vrs_object_from_mapped_variant(allele.post_mapped).root
    # post_mapped only ever holds an Allele or CisPhasedBlock (the two shapes
    # vrs_object_from_mapped_variant produces); MolecularVariation.root is a wider union, so narrow.
    assert isinstance(variation, (VrsAllele, CisPhasedBlock))
    return variation


def build_categorical_variant(
    links: list[MappingRecordAllele], *, name: str, include_convergent: bool = True
) -> Optional[CategoricalVariantTransit]:
    """Assemble a Cat-VRS ``CategoricalVariant`` from a variant's live allele links.

    ``links`` is the record-scoped live link set from ``get_live_record_allele_links`` — exactly one
    is authoritative (the measured/defining allele), the rest are derived members. Returns ``None``
    when there is no authoritative, hydratable link to anchor on (e.g. an unmapped variant).

    **The categorical variant always anchors on the measured allele**. Member selection therefore differs by mode:

    - **Reverse translation** (protein measured): the defining allele is the protein change and every
      nt allele in the record ``encodes`` it, so the **full equivalence class** is unfurled — that class
      is exactly what the protein measurement's claim ranges over. ``include_convergent`` is inert here.
    - **Projection** (nt measured): the record also carries the reverse-translation fan — the synonymous
      *cousins* (other nt encoders of the same protein consequence, in different projection groups), which
      are distinct, unmeasured variants rather than representations of the measured change.
      ``include_convergent`` selects how they are handled:

      - ``True`` (default; the detail envelope): the cousins are kept as members wearing the
        :attr:`CatVrsRelation.CO_ENCODES` relation, so the object is the full closure. The measured change's
        coordinate partner and protein consequence stay ``coordinate_representation_of`` / ``translation_of``.
      - ``False`` (the VA-Spec subject): the cousins are dropped (see :func:`_is_precise_projection_member`)
        and only the measured change's precise coordinate partner and protein consequence remain.
    """
    defining_link = next((link for link in links if link.is_authoritative), None)
    if defining_link is None:
        return None

    defining_allele = defining_link.allele
    defining_vrs = _hydrate_vrs(defining_allele)
    # The authoritative (measured) allele having no post_mapped breaks an invariant the mapping
    # job upholds. Returning None here is analogous to "unmapped".
    if defining_vrs is None:
        logger.warning(
            msg=(
                f"Cat-VRS for {name!r}: authoritative allele {defining_allele.vrs_digest!r} has no "
                "post_mapped representation; treating the variant as unmapped."
            ),
            extra=logging_context(),
        )
        return None

    defining_level = defining_allele.level
    mode = CatVrsMode.REVERSE_TRANSLATION if defining_level == SequenceLevel.protein.value else CatVrsMode.PROJECTION

    members: list[VrsAllele | CisPhasedBlock | iriReference] = [defining_vrs]
    member_relations: dict[str, CatVrsRelation] = {}
    relations_present: dict[CatVrsRelation, None] = {}  # insertion-ordered set of relation kinds

    for link in links:
        if link is defining_link:
            continue

        allele = link.allele

        # Projection mode, narrow object (include_convergent=False): drop any synonymous cousins the
        # reverse-translation fan left on the record, keeping only the measured change's precise coordinate
        # partner and its protein consequence.
        if (
            mode is CatVrsMode.PROJECTION
            and not include_convergent
            and not _is_precise_projection_member(
                allele.level, link.projection_group, defining_group=defining_link.projection_group
            )
        ):
            continue

        member_vrs = _hydrate_vrs(allele)
        # A live link to an allele with no post_mapped is an unexpected data state.
        if member_vrs is None:
            logger.warning(
                msg=(
                    f"Cat-VRS for {name!r}: skipping member allele {allele.vrs_digest!r} with no "
                    "post_mapped representation."
                ),
                extra=logging_context(),
            )
            continue

        members.append(member_vrs)
        relation = _relation_for(
            defining_level,
            allele.level,
            defining_group=defining_link.projection_group,
            member_group=link.projection_group,
        )
        if relation is not None and allele.vrs_digest is not None:
            member_relations[allele.vrs_digest] = relation
            relations_present[relation] = None

    # The defining allele anchors the DefiningAlleleConstraint. Cat-VRS 1.0.0 requires a bare
    # vrs:Allele (or an iri ref) there, so a multi-variant defining (CisPhasedBlock) is referenced by
    # its digest instead.
    defining_ref: VrsAllele | iriReference = (
        defining_vrs if isinstance(defining_vrs, VrsAllele) else iriReference(root=defining_allele.vrs_digest or "")
    )

    categorical_variant = CategoricalVariant(
        name=name,
        members=members,
        constraints=[
            DefiningAlleleConstraint(
                allele=defining_ref,
                relations=[_relation_concept(relation) for relation in relations_present] or None,
            )
        ],
    )

    return CategoricalVariantTransit(
        categorical_variant=categorical_variant,
        mode=mode,
        member_relations=member_relations,
    )


def categorical_variant_for_variant(
    db: Session, variant_id: int, *, name: str, as_of: Optional[datetime] = None
) -> Optional[CategoricalVariantTransit]:
    """Fetch a variant's live allele links and assemble its Cat-VRS transit object.

    The DB-backed entry point routers use. ``as_of`` is a fetch concern, threaded into the link query
    only: membership is what varies over time, while each allele's VRS is immutable and content-addressed,
    so :func:`build_categorical_variant` stays pure and time-agnostic over the links it is handed. Returns
    ``None`` for an unmapped variant (no authoritative link at ``as_of``).
    """
    links = get_live_record_allele_links(db, variant_id, as_of=as_of)
    return build_categorical_variant(links, name=name)

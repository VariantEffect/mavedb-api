"""Cat-VRS transit, built on the fly from a variant's live allele links.

The Categorical Variant served on ``/variants/{urn}`` is assembled per request from the variant's
live ``MappingRecordAllele`` links (see ``lib/alleles.py::get_live_record_allele_links``).
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

from ga4gh.cat_vrs.models import CategoricalVariant, DefiningAlleleConstraint, MappableConcept, Relation
from ga4gh.core.models import ConceptMapping, iriReference
from ga4gh.core.models import Relation as MappingRelation
from ga4gh.vrs.models import Allele as VrsAllele
from ga4gh.vrs.models import CisPhasedBlock
from sqlalchemy.orm import Session

from mavedb.lib.alleles import get_live_record_allele_links
from mavedb.lib.logging.context import logging_context
from mavedb.lib.term_systems import GKS_ALLELE_RELATION, MAVEDB_CAT_VRS_RELATION, SEQUENCE_ONTOLOGY, TermSystem
from mavedb.lib.term_systems import coding as term_coding
from mavedb.lib.vrs import vrs_object_from_mapped_variant
from mavedb.models.allele import Allele
from mavedb.models.enums.sequence_level import NUCLEOTIDE_LEVELS, SequenceLevel
from mavedb.models.mapping_record_allele import MappingRecordAllele

logger = logging.getLogger(__name__)

# Cat-VRS does not attribute its own Relation values to one uniform system: translation_of and
# transcribed_to are Sequence Ontology terms, while liftover_to has no ontology equivalent and is
# carried on Cat-VRS's own internal vocabulary instead. Per-term, not a single constant, so that
# extending `_SPEC_EQUIVALENT` can't silently mislabel a future mapping with the wrong system.
#
# Sourced from the spec's published examples/schema (ga4gh/cat-vrs recipes-source.yaml and
# examples/json/proteinSequenceConsequence-ex2.json).
_SPEC_TERM_SYSTEM: dict[Relation, TermSystem] = {
    Relation.TRANSLATION_OF: SEQUENCE_ONTOLOGY,
    Relation.TRANSCRIBED_TO: SEQUENCE_ONTOLOGY,
    Relation.LIFTOVER_TO: GKS_ALLELE_RELATION,
}


class CatVrsRelation(str, Enum):
    """Relation of a member allele to the single defining (measured) allele.

    Deliberately **not** a subclass of Cat-VRS's ``Relation``: Python forbids extending an enum that
    already has members, and inheriting would buy nothing anyway. Interop here is carried by
    ``Coding.system`` on the emitted concept, not by Python enum identity — see
    :func:`_relation_concept`, which maps the codes that have a spec equivalent onto it. Keeping this a
    plain enum keeps the members statically visible to mypy and greppable.
    """

    # defining is protein; member is an nt allele (coding or genomic) that encodes it. Implied.
    ENCODES = "encodes"
    # defining is nt; member is the same variant in other nt coordinates (genomic<->coding). Faithful.
    COORDINATE_REPRESENTATION_OF = "coordinate_representation_of"
    # defining is nt; member is the protein consequence. Consequence, no independent score.
    TRANSLATION_OF = "translation_of"
    # defining is nt; member is a *convergent encoding* — an nt allele in a different projection group that
    # encodes the same protein consequence as the measured change.
    CO_ENCODES = "co_encodes"


# MaveDB code -> the Cat-VRS term it is an exact match for. A code with no entry has no spec equivalent.
_SPEC_EQUIVALENT: dict[CatVrsRelation, Relation] = {
    CatVrsRelation.TRANSLATION_OF: Relation.TRANSLATION_OF,
}


class CatVrsMode(str, Enum):
    """The score-collapse semantics of the categorical variant."""

    PROJECTION = "projection"  # Mode 1 — nt measured; score rides faithfully.
    REVERSE_TRANSLATION = "reverse_translation"  # Mode 2 — protein measured; score is implied.


@dataclass
class CategoricalVariantTransit:
    """The spec-pure Cat-VRS object plus the MaveDB layer that rides beside it."""

    categorical_variant: CategoricalVariant
    mode: CatVrsMode
    member_relations: dict[str, CatVrsRelation]
    """vrs_digest keyed dict encoding digest: relation for the member -> defining relationship.
    Excludes the defining allele itself.
    """


def is_convergent_encoding(
    member_level: Optional[str], member_group: Optional[int], *, defining_group: Optional[int]
) -> bool:
    """Whether a projection-mode member is a *convergent encoding* of the measured change.

    Concretely:
    - The defining (measured) allele is nt (genomic or cdna).
    - The member is nt (genomic or cdna).
    - The member is in a different projection group than the defining (measured) allele.
    """
    return (
        member_level in NUCLEOTIDE_LEVELS
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
    (defining, member) level pair. Note that in projection mode relation also depends on whether the nt
    member shares the measured change's ``projection_group`` (its projection) or lives in another group
    (a convergent encoding).
    """
    # Protein measured. Every nt member encodes it, the protein member is the defining. Grouping is irrelevant.
    if defining_level == SequenceLevel.protein.value:
        return CatVrsRelation.ENCODES if member_level in NUCLEOTIDE_LEVELS else None

    # Nt measured (genomic or cdna).
    if member_level == SequenceLevel.protein.value:
        return CatVrsRelation.TRANSLATION_OF
    if member_level in NUCLEOTIDE_LEVELS:
        if is_convergent_encoding(member_level, member_group, defining_group=defining_group):
            return CatVrsRelation.CO_ENCODES
        return CatVrsRelation.COORDINATE_REPRESENTATION_OF

    # Unreachable in practice. Levels are constrained by the DB and the mapping job.
    return None  # pragma: no cover


def _is_precise_projection_member(
    member_level: Optional[str], member_group: Optional[int], *, defining_group: Optional[int]
) -> bool:
    """In projection mode, whether a member is a *precise* representation of the measured change.

    True for the protein consequence (the apex, shared by the whole equivalence class) and the measured
    change's own projection. False for convergent encodings in other projection groups.
    """
    if member_level == SequenceLevel.protein.value:
        return True

    return not is_convergent_encoding(member_level, member_group, defining_group=defining_group)


def _relation_concept(relation: CatVrsRelation) -> MappableConcept:
    """Wrap a MaveDB relation code as a Cat-VRS ``MappableConcept``.

    The MaveDB code is always the ``primaryCoding``. Codes are read *member -> defining*, which is
    MaveDB's own framing, so claiming the spec's system for them outright would misrepresent their
    provenance. Where a code *is* the same concept as a published Cat-VRS relation, an ``exactMatch``
    :class:`ConceptMapping` to that term is attached via ``MappableConcept.mappings``.

    A code with no spec equivalent carries no mapping. See :data:`_SPEC_EQUIVALENT` for which spec
    gaps exist and why.
    """
    spec_term = _SPEC_EQUIVALENT.get(relation)
    return MappableConcept(
        name=relation.value,
        primaryCoding=term_coding(MAVEDB_CAT_VRS_RELATION, relation.value),
        mappings=(
            [
                ConceptMapping(
                    coding=term_coding(_SPEC_TERM_SYSTEM[spec_term], spec_term.value),
                    relation=MappingRelation.EXACT_MATCH,
                )
            ]
            if spec_term is not None
            else None
        ),
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

    ``links`` is the record-scoped live link set from ``get_live_record_allele_links``. Exactly one
    is authoritative (the measured/defining allele), the rest are derived members. Returns ``None``
    when there is no authoritative, hydratable link to anchor on (e.g. an unmapped variant).

    **The categorical variant always anchors on the measured allele**. Member selection therefore differs by mode:

    - **Reverse translation** (protein measured): the defining allele is the protein change and every
      nt allele in the record ``encodes`` it, so the **full equivalence class** is unfurled. That class
      is exactly what the protein measurement's claim ranges over. ``include_convergent`` is inert here.
    - **Projection** (nt measured): the record also carries the reverse-translation fan out. This includes
      all synonymous *convergent encodings* (other nt alleles encoding the same protein consequence, in
      different projection groups), which are distinct, unmeasured variants rather than representations
      of the measured change. ``include_convergent`` selects how they are handled:

      - ``True`` (default; the detail envelope): the encodings are kept as members wearing the
        :attr:`CatVrsRelation.CO_ENCODES` relation, so the object is the full closure. The measured change's
        projection and protein consequence stay ``coordinate_representation_of`` / ``translation_of``.
      - ``False`` (the VA-Spec subject): the encodings are dropped (see :func:`_is_precise_projection_member`)
        and only the measured change's precise projection and protein consequence remain.
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

        # Projection mode, narrow object (include_convergent=False): drop any convergent encodings the
        # reverse-translation fan left on the record, keeping only the measured change's precise coordinate
        # projection and its protein consequence.
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

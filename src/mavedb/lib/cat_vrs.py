"""Cat-VRS transit, built on the fly from a variant's live allele links.

The Categorical Variant served on ``/variants/{urn}`` is assembled per request from the variant's
live ``MappingRecordAllele`` links (see ``lib/alleles.py::get_live_record_allele_links``).
"""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Mapping, Optional

from ga4gh.cat_vrs.models import CategoricalVariant, DefiningAlleleConstraint, MappableConcept
from ga4gh.cat_vrs.relations import (
    LIFTOVER_TO_RELATION,
    TRANSCRIBED_TO_RELATION,
    TRANSLATION_OF_RELATION,
    Relation,
)
from ga4gh.core.models import Coding, ConceptMapping, iriReference
from ga4gh.core.models import Relation as MappingRelation
from ga4gh.vrs.models import Allele as VrsAllele
from ga4gh.vrs.models import CisPhasedBlock

from mavedb.lib.allele_annotations import AlleleCrossReferences
from mavedb.lib.allele_identity import AlleleDerivation
from mavedb.lib.clingen.allele_registry import clingen_allele_url
from mavedb.lib.clinvar.utils import clinvar_variation_url
from mavedb.lib.gnomad import gnomad_variant_url
from mavedb.lib.logging.context import logging_context
from mavedb.lib.term_systems import (
    CLINGEN_ALLELE_REGISTRY,
    CLINVAR_VARIATION,
    GNOMAD,
    MAVEDB_CAT_VRS_RELATION,
)
from mavedb.lib.term_systems import coding as term_coding
from mavedb.lib.vrs import vrs_object_from_mapped_variant
from mavedb.models.allele import Allele
from mavedb.models.enums.sequence_level import NUCLEOTIDE_LEVELS, SequenceLevel
from mavedb.models.mapping_record_allele import MappingRecordAllele

logger = logging.getLogger(__name__)

# The spec's own concept for each Cat-VRS relation term. Each carries its term's system: Sequence Ontology
# for translation_of and transcribed_to, Cat-VRS's internal ga4gh-gkm-term vocabulary for liftover_to.
_SPEC_RELATION_CONCEPTS: dict[Relation, MappableConcept] = {
    Relation.TRANSLATION_OF: TRANSLATION_OF_RELATION,
    Relation.TRANSCRIBED_TO: TRANSCRIBED_TO_RELATION,
    Relation.LIFTOVER_TO: LIFTOVER_TO_RELATION,
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
    # defining is nt; member is a *convergent encoding* — an nt allele not paired with the measured change that
    # encodes the same protein consequence.
    CO_ENCODES = "co_encodes"


# MaveDB code -> the Cat-VRS term it is an exact match for. A code with no entry has no spec equivalent.
_SPEC_EQUIVALENT: dict[CatVrsRelation, Relation] = {
    CatVrsRelation.TRANSLATION_OF: Relation.TRANSLATION_OF,
}


class CatVrsMode(str, Enum):
    """The score-collapse semantics of the categorical variant."""

    PROJECTION = "projection"  # Mode 1 — nt measured; score rides faithfully.
    REVERSE_TRANSLATION = "reverse_translation"  # Mode 2 — protein measured; score is implied.

    @classmethod
    def for_defining_level(cls, level: Optional[str]) -> "CatVrsMode":
        """The mode a categorical variant takes from its defining (measured) allele's level."""
        return cls.REVERSE_TRANSLATION if level == SequenceLevel.protein.value else cls.PROJECTION


@dataclass
class CategoricalVariantTransit:
    """The spec-pure Cat-VRS object plus the MaveDB layer that rides beside it."""

    categorical_variant: CategoricalVariant
    mode: CatVrsMode


def is_projection_partner(member_group: Optional[int], *, focus_group: Optional[int]) -> bool:
    """Whether a member is the focus allele's own c↔g projection: the two share a projection group.

    A member or focus with no group can't be shown to pair, so it counts as no projection. That includes a
    measured allele RT's fold-in missed (``fold_in_missed`` on its ``cross_level_translation`` event), whose
    own twin then reads as a convergent encoding: under-claiming one exact match rather than presenting
    distinct encodings, and their ClinVar/gnomAD records, as the measured change.
    """
    return member_group is not None and member_group == focus_group


def member_label(
    focus_level: Optional[str], member_level: Optional[str], *, is_projection: bool
) -> tuple[Optional[CatVrsRelation], Optional[AlleleDerivation]]:
    """A member allele's ``(relation, derivation)`` relative to the focus allele, ``(None, None)`` for none.

    The single source of both axes for every view (Cat-VRS, variant detail, allele detail), so they cannot
    label the same pair differently. ``is_projection`` is whether the member is the focus's own c↔g pair
    (:func:`is_projection_partner` within a record).

    - Protein focus: every nucleotide member ``encodes`` it, a reverse-translation ``candidate``.
    - Nucleotide focus: the protein consequence is its ``translation_of`` and its pair its
      ``coordinate_representation_of``, both deterministic ``projection``s; any other nucleotide member
      ``co_encodes`` the consequence, a distinct ``convergent`` change.
    """
    if focus_level == SequenceLevel.protein.value:
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
                    coding=_SPEC_RELATION_CONCEPTS[spec_term].primaryCoding,
                    relation=MappingRelation.EXACT_MATCH,
                )
            ]
            if spec_term is not None
            else None
        ),
    )


def _external_codings(allele: Allele, references: Optional[AlleleCrossReferences]) -> list[Coding]:
    """The allele's ClinGen, gnomAD and ClinVar identifiers as resolvable codings. Identifiers only."""
    codings: list[Coding] = []
    if allele.clingen_allele_id:
        caid = allele.clingen_allele_id
        codings.append(term_coding(CLINGEN_ALLELE_REGISTRY, caid, iri=clingen_allele_url(caid)))

    if references is None:
        return codings

    if references.gnomad is not None:
        gnomad = references.gnomad
        codings.append(
            term_coding(
                GNOMAD,
                gnomad.db_identifier,
                iri=gnomad_variant_url(gnomad.db_identifier, gnomad.db_version),
                system_version=gnomad.db_version,
            )
        )

    for variation_id in references.clinvar_variation_ids:
        codings.append(term_coding(CLINVAR_VARIATION, variation_id, iri=clinvar_variation_url(variation_id)))

    return codings


def _external_mappings(
    members: list[tuple[Allele, Optional[CatVrsRelation]]], references: Mapping[str, AlleleCrossReferences]
) -> list[ConceptMapping]:
    """Cross-reference the members' external records as ``CategoricalVariant.mappings``.

    ``members`` pairs each included allele with its relation to the defining allele, ``None`` marking the
    defining allele itself. The defining allele and its coordinate representations are the categorical
    variant, so their records are an ``exactMatch``. Every other member is a distinct variant tied to it by
    encoding or translation, so its records are a ``relatedMatch``, following the ga4gh/cat-vrs v1
    protein-consequence examples (proteinSequenceConsequence-ex1.json).

    The c↔g twins of one change share a CAID, gnomAD ID and ClinVar variation, and a ClinVar variation
    recurs once per live release, so mappings are deduplicated by (system, code) with ``exactMatch``
    taking precedence. Mappings are sorted by (system, code) so the output does not depend on the order
    the database returned the links in.
    """
    mappings: dict[tuple[str, str], ConceptMapping] = {}
    for allele, relation in members:
        is_exact = relation is None or relation is CatVrsRelation.COORDINATE_REPRESENTATION_OF
        match = MappingRelation.EXACT_MATCH if is_exact else MappingRelation.RELATED_MATCH

        for coding in _external_codings(allele, references.get(allele.vrs_digest) if allele.vrs_digest else None):
            key = (coding.system, coding.code.root)
            existing = mappings.get(key)
            if existing is None or (is_exact and existing.relation is not MappingRelation.EXACT_MATCH):
                mappings[key] = ConceptMapping(coding=coding, relation=match)

    return [mappings[key] for key in sorted(mappings)]


def categorical_member_links(
    links: list[MappingRecordAllele], *, include_convergent: bool = True
) -> list[MappingRecordAllele]:
    """The links :func:`build_categorical_variant` draws members from, the defining (authoritative) link first.

    Empty when no link is authoritative. Selection is by level and projection group only; a selected
    allele with no ``post_mapped`` is skipped later, at hydration. Callers use this to size or prefetch
    for the categorical variant before building it.
    """
    defining_link = next((link for link in links if link.is_authoritative), None)
    if defining_link is None:
        return []

    # Projection mode, narrow object (include_convergent=False): drop any convergent encodings the
    # reverse-translation fan left on the record, keeping only the measured change's precise coordinate
    # projection and its protein consequence.
    mode = CatVrsMode.for_defining_level(defining_link.allele.level)
    members = [defining_link]
    for link in links:
        if link is defining_link:
            continue

        relation, _ = member_label(
            defining_link.allele.level,
            link.allele.level,
            is_projection=is_projection_partner(link.projection_group, focus_group=defining_link.projection_group),
        )
        if mode is CatVrsMode.PROJECTION and not include_convergent and relation is CatVrsRelation.CO_ENCODES:
            continue

        members.append(link)

    return members


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
    links: list[MappingRecordAllele],
    *,
    name: str,
    include_convergent: bool = True,
    cross_references: Optional[Mapping[str, AlleleCrossReferences]] = None,
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
      - ``False`` (the VA-Spec subject): the encodings are dropped (see :func:`member_label`) and only the
        measured change's precise projection and protein consequence remain.

    ``mappings`` cross-references the included members' external records (see :func:`_external_mappings`).
    CAIDs come from the alleles themselves; gnomAD and ClinVar identifiers come from ``cross_references``,
    the digest-keyed map from :func:`~mavedb.lib.allele_annotations.get_allele_cross_references` fetched at
    the same ``as_of`` as ``links``. Without it, only CAIDs are mapped.
    """
    member_links = categorical_member_links(links, include_convergent=include_convergent)
    if not member_links:
        return None

    defining_link, *other_links = member_links
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
    mode = CatVrsMode.for_defining_level(defining_level)

    members: list[VrsAllele | CisPhasedBlock | iriReference] = [defining_vrs]
    mapped_members: list[tuple[Allele, Optional[CatVrsRelation]]] = [(defining_allele, None)]
    relations_present: dict[CatVrsRelation, None] = {}  # insertion-ordered set of relation kinds

    for link in other_links:
        allele = link.allele
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
        relation, _ = member_label(
            defining_level,
            allele.level,
            is_projection=is_projection_partner(link.projection_group, focus_group=defining_link.projection_group),
        )
        if relation is not None:
            mapped_members.append((allele, relation))
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
        mappings=_external_mappings(mapped_members, cross_references or {}) or None,
    )

    return CategoricalVariantTransit(
        categorical_variant=categorical_variant,
        mode=mode,
    )

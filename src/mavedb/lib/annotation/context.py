"""Allele-graph variant context for annotation builders.

Serves ``MappingRecord`` / ``Allele`` / ``MappingRecordAllele`` substrate to VA-Spec builders,
and provides a single point of truth for the live (or as-of) mapping record, authoritative allele,
and pre-built VA proposition subject.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Sequence

from ga4gh.cat_vrs.models import CategoricalVariant
from ga4gh.vrs.models import MolecularVariation
from sqlalchemy.orm import Session

from mavedb.lib.allele_annotations import AlleleCrossReferences, get_allele_cross_references
from mavedb.lib.alleles import LiveRecordLinks, get_live_record_allele_links
from mavedb.lib.cat_vrs import build_categorical_variant, categorical_member_links
from mavedb.lib.vrs import vrs_object_from_mapped_variant
from mavedb.models.allele import Allele
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.variant import Variant


@dataclass
class VariantAnnotationContext:
    """A variant's annotation inputs, sourced from it's allele-graph.

    ``record`` is the live (or as-of) ``MappingRecord``. Its ``mapping_api_version`` / ``mapped_date``
    supply VA provenance, and its ``ValidTime`` is what ``as_of`` and supersession are evaluated against.
    ``measured_allele`` is the authoritative allele (the assayed representation) and its ``post_mapped`` VRS is
    the concrete study-result focus. ``subject_variant`` is the VA *proposition* subject. When projections
    exist, this is a Cat-VRS ``CategoricalVariant`` object. Otherwise, it is the measured ``MolecularVariation``.
    """

    variant: Variant
    record: MappingRecord
    measured_allele: Allele
    subject_variant: MolecularVariation | CategoricalVariant
    as_of: Optional[datetime]


@dataclass(frozen=True)
class AnnotationContextInputs:
    """The database inputs for a chunk of variants' annotation contexts, loaded up front.

    ``record_links`` holds each variant's live (or as-of) record and links; ``cross_references`` holds the
    gnomAD and ClinVar identifiers of every allele a proposition subject in the chunk draws on, keyed by
    digest. :meth:`context_for` builds one variant's context from these without touching the database, so a
    caller can isolate a failure to that variant.
    """

    record_links: dict[int, LiveRecordLinks]
    cross_references: dict[str, AlleleCrossReferences]
    as_of: Optional[datetime]

    def context_for(self, variant: Variant) -> Optional[VariantAnnotationContext]:
        """Build ``variant``'s context, or ``None`` when it is unmapped at ``as_of`` or its authoritative
        allele carries no ``post_mapped`` VRS (nothing to annotate)."""
        record_links = self.record_links.get(variant.id)
        if record_links is None:
            return None

        links = record_links.links
        measured_allele = next((link.allele for link in links if link.is_authoritative), None)
        if measured_allele is None or measured_allele.post_mapped is None:
            return None

        # The proposition subject follows the measured as anchor rule: the categorical variant when it carries a
        # projection member, else the concrete measured variation. The VA subject is deliberately *narrow*
        # (include_convergent=False): the convergent encodings are dropped, because VA-Spec carries no per-member
        # provenance to mark them as unmeasured, and StudyResult.focus already pins the concrete measured
        # allele. A lone measured allele is served bare.
        transit = None
        if len(categorical_member_links(links, include_convergent=False)) > 1:
            transit = build_categorical_variant(
                links, name=variant.urn or "", include_convergent=False, cross_references=self.cross_references
            )
        if transit is not None and len(transit.categorical_variant.members or []) > 1:
            subject_variant: MolecularVariation | CategoricalVariant = transit.categorical_variant
        else:
            subject_variant = vrs_object_from_mapped_variant(measured_allele.post_mapped)

        return VariantAnnotationContext(
            variant=variant,
            record=record_links.record,
            measured_allele=measured_allele,
            subject_variant=subject_variant,
            as_of=self.as_of,
        )


def load_annotation_context_inputs(
    db: Session, variants: Sequence[Variant], *, as_of: Optional[datetime] = None
) -> AnnotationContextInputs:
    """Load the annotation-context inputs for ``variants`` in at most four queries in total, however many
    variants there are (where building each context alone would cost three or four per variant).

    Two for the records and links (:func:`get_live_record_allele_links`), and two for the cross-references
    of the alleles in subjects with a projection member, skipped when no subject has one. Whole-set callers pass a chunk of variants
    (:data:`~mavedb.lib.alleles.VARIANT_CHUNK_SIZE`).
    """
    record_links = get_live_record_allele_links(db, [variant.id for variant in variants], as_of=as_of)

    subject_alleles: list[Allele] = []
    for entry in record_links.values():
        member_links = categorical_member_links(entry.links, include_convergent=False)
        if len(member_links) > 1:
            subject_alleles.extend(link.allele for link in member_links)

    return AnnotationContextInputs(
        record_links=record_links,
        cross_references=get_allele_cross_references(db, subject_alleles, as_of=as_of) if subject_alleles else {},
        as_of=as_of,
    )


def variant_annotation_context(
    db: Session, variant: Variant, *, as_of: Optional[datetime] = None
) -> Optional[VariantAnnotationContext]:
    """Assemble the annotation context for one variant from the live (or as-of) mapping substrate.

    The single-variant form of :func:`load_annotation_context_inputs`; see
    :meth:`AnnotationContextInputs.context_for` for when it returns ``None``.
    """
    return load_annotation_context_inputs(db, [variant], as_of=as_of).context_for(variant)

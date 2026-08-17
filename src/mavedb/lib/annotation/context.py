"""Allele-graph variant context for annotation builders.

Serves ``MappingRecord`` / ``Allele`` / ``MappingRecordAllele`` substrate to VA-Spec builders,
and provides a single point of truth for the live (or as-of) mapping record, authoritative allele,
and pre-built VA proposition subject.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from ga4gh.cat_vrs.models import CategoricalVariant
from ga4gh.vrs.models import MolecularVariation
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.alleles import get_live_record_allele_links
from mavedb.lib.cat_vrs import build_categorical_variant
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


def variant_annotation_context(
    db: Session, variant: Variant, *, as_of: Optional[datetime] = None
) -> Optional[VariantAnnotationContext]:
    """Assemble the annotation context for ``variant`` from the live (or as-of) mapping substrate.

    Returns ``None`` when the variant is unmapped at ``as_of`` or an authoritative allele that carries
    no ``post_mapped`` VRS (nothing to annotate). One record fetch + one allele-link fetch; the Cat-VRS
    transit is built from the same links, so the subject costs no redundant query.
    """
    record = db.scalar(
        select(MappingRecord).where(MappingRecord.variant_id == variant.id).where(MappingRecord.live_at(as_of))
    )
    if record is None:
        return None

    links = get_live_record_allele_links(db, variant.id, as_of=as_of)
    measured_allele = next((link.allele for link in links if link.is_authoritative), None)
    if measured_allele is None or measured_allele.post_mapped is None:
        return None

    # The proposition subject follows the measured as anchor rule: the categorical variant when it carries a
    # projection member, else the concrete measured variation. The VA subject is deliberately *narrow*
    # (include_convergent=False): the convergent encodings are dropped, because VA-Spec carries no per-member
    # provenance to mark them as unmeasured, and StudyResult.focusVariant already pins the concrete measured
    # allele.
    transit = build_categorical_variant(links, name=variant.urn or "", include_convergent=False)
    if transit is not None and len(transit.categorical_variant.members) > 1:
        subject_variant: MolecularVariation | CategoricalVariant = transit.categorical_variant
    else:
        subject_variant = vrs_object_from_mapped_variant(measured_allele.post_mapped)

    return VariantAnnotationContext(
        variant=variant,
        record=record,
        measured_allele=measured_allele,
        subject_variant=subject_variant,
        as_of=as_of,
    )

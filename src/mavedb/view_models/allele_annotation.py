"""Response view models for an allele's external annotations (VEP / gnomAD / ClinVar).

The pydantic serialization boundary over the ``lib.allele_annotations`` transit dataclasses
(``from_attributes`` coerces them directly). These are the sparse, digest-keyed annotation blocks
that ride *alongside* the spec-pure Cat-VRS on ``GET /variants/{urn}`` and alongside the cross-layer
equivalence class on ``GET /alleles/{digest}`` — so they live here, shared by both serving views rather
than in either one's file.
"""

from typing import Optional

from mavedb.models.enums.vep import VepConsequenceSource
from mavedb.view_models.base.base import BaseModel


class VepAnnotation(BaseModel):
    """VEP molecular consequence for an allele and the Ensembl release it resolved under.

    ``consequence_source`` is ``transcript`` when the consequence was read for the allele's own
    transcript (named by ``matched_transcript``), and ``most_severe`` when no transcript could be
    matched and it is VEP's worst case across every overlapping transcript, which may not apply to the
    transcript of interest. ``reference_identical`` marks an allele with no sequence change, for which
    VEP was never called. ``consequence_terms`` is the severity-ordered term list ``consequence``
    heads. All three are null on rows resolved before provenance was recorded.
    """

    consequence: Optional[str] = None
    source_version: str
    consequence_source: Optional[VepConsequenceSource] = None
    consequence_terms: Optional[list[str]] = None
    matched_transcript: Optional[str] = None

    class Config:
        from_attributes = True


class GnomadAnnotation(BaseModel):
    """gnomAD population frequency for an allele."""

    allele_frequency: float
    allele_count: int
    allele_number: int
    faf95_max: Optional[float] = None
    db_version: str
    db_identifier: str

    class Config:
        from_attributes = True


class ClinvarAnnotation(BaseModel):
    """One ClinVar assertion for an allele (an allele may carry one per release)."""

    clinical_significance: str
    clinical_review_status: str
    clinvar_variation_id: Optional[str] = None
    clinvar_allele_id: str
    db_version: str

    class Config:
        from_attributes = True


class AlleleAnnotations(BaseModel):
    """The external annotations for one allele, sparse — each source absent unless it has data."""

    vep: Optional[VepAnnotation] = None
    gnomad: Optional[GnomadAnnotation] = None
    clinvar: list[ClinvarAnnotation] = []

    class Config:
        from_attributes = True

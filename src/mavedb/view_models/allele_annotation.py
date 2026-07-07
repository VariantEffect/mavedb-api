"""Response view models for an allele's external annotations (VEP / gnomAD / ClinVar).

The pydantic serialization boundary over the ``lib.allele_annotations`` transit dataclasses
(``from_attributes`` coerces them directly). These are the sparse, digest-keyed annotation blocks
that ride *alongside* the spec-pure Cat-VRS on ``GET /variants/{urn}`` and are the allele's own block
on ``GET /alleles/{digest}`` — so they live here, shared by both serving views rather than in either
one's file.
"""

from typing import Optional

from mavedb.view_models.base.base import BaseModel


class VepAnnotation(BaseModel):
    """VEP most-severe functional consequence and the Ensembl release it resolved under."""

    consequence: Optional[str] = None
    source_version: str

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

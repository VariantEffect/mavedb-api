"""Response view models for the lean whole-set view (``GET /score-sets/{urn}/variants``).

The pydantic serialization boundary over the ``lib.score_set_variants`` transit dataclasses. ``from_attributes``
lets the route return the ``LeanVariantRecord`` dataclasses directly and have FastAPI's ``response_model``
coerce them (field names line up); aliases camelize per the shared base config, so clients see
``variantUrn`` / ``assayLevel`` / ``mapped`` etc. ``response_model_exclude_none`` drops absent fields, so a
slot with no parsed block serializes as just ``{"hgvs": "..."}`` and a null mapped level drops out of the
``mapped`` object entirely.
"""

from typing import Optional

from mavedb.view_models.base.base import BaseModel


class HgvsField(BaseModel):
    """An HGVS expression with its parsed substitution block riding alongside when representable.

    ``hgvs`` is always present; ``position``/``ref``/``alt`` appear only for a placeable simple
    substitution (the heatmap grid) and are omitted for splice/indels/multivariants.
    """

    hgvs: str
    position: Optional[int] = None
    ref: Optional[str] = None
    alt: Optional[str] = None

    class Config:
        from_attributes = True


class MappedTriple(BaseModel):
    """The mapped (reference-frame) HGVS keyed by level — the canonical projection of the measured change.

    One slot per level. A nucleotide assay populates all three (``mapped[assayLevel]`` is the measured
    slot; ``cdna`` is the level-invariant search key, present even when ``assayLevel`` is ``genomic``); a
    protein assay populates only ``protein`` (the ambiguous c/g fan-out is not fabricated). Null slots are
    omitted under ``response_model_exclude_none``.
    """

    genomic: Optional[HgvsField] = None
    cdna: Optional[HgvsField] = None
    protein: Optional[HgvsField] = None

    class Config:
        from_attributes = True


class LeanVariant(BaseModel):
    """One pre-chewed per-variant record feeding the score-set table, heatmap, and histograms.

    ``variantUrn`` is the universal selection key; ``assayLevelDigest`` bridges into the digest-keyed
    annotation dimensions. The submitted HGVS (``hgvsNt``/``hgvsPro``/``hgvsSplice``, target frame) carry
    the depositor's frame for the heatmap's raw↔mapped toggle. The mapped (reference) frame is the
    ``mapped`` :class:`MappedTriple` plus the ``assayLevel`` pointer (an ``AnnotationLayer`` value) naming
    the measured/canonical slot: ``mapped[assayLevel]`` is the measured representation and ``mapped.cdna``
    the level-invariant search key. Fields are omitted when null.
    """

    variant_urn: str
    score: Optional[float] = None
    consequence: Optional[str] = None
    clingen_allele_id: Optional[str] = None
    assay_level_digest: Optional[str] = None
    hgvs_nt: Optional[HgvsField] = None
    hgvs_pro: Optional[HgvsField] = None
    hgvs_splice: Optional[HgvsField] = None
    assay_level: Optional[str] = None
    mapped: MappedTriple = MappedTriple()

    class Config:
        from_attributes = True

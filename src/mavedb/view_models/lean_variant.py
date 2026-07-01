"""Response view models for the lean whole-set view (``GET /score-sets/{urn}/variants``).

The pydantic serialization boundary over the ``lib.lean_variants`` transit dataclasses. ``from_attributes``
lets the route return the ``LeanVariantRecord`` dataclasses directly and have FastAPI's ``response_model``
coerce them (field names line up); aliases camelize per the shared base config, so clients see
``variantUrn`` / ``assayLevelHgvs`` etc. ``response_model_exclude_none`` drops absent fields, so a slot
with no parsed block serializes as just ``{"hgvs": "..."}``.
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


class LeanVariant(BaseModel):
    """One pre-chewed per-variant record feeding the score-set table, heatmap, and histograms.

    ``variantUrn`` is the universal selection key; ``assayLevelDigest`` bridges into the digest-keyed
    annotation dimensions. The submitted HGVS (``hgvsNt``/``hgvsPro``/``hgvsSplice``, target frame) and
    the mapped ``assayLevelHgvs`` (reference frame) carry both sides of the heatmap's frame toggle, each
    with an optional parsed block for the level toggle. ``proteinLevelHgvs`` is the mapped protein
    representation (distinct from the *submitted* ``hgvsPro``); for a protein assay it coincides with
    ``assayLevelHgvs``. Fields are omitted when null.
    """

    variant_urn: str
    score: Optional[float] = None
    consequence: Optional[str] = None
    clingen_allele_id: Optional[str] = None
    assay_level_digest: Optional[str] = None
    hgvs_nt: Optional[HgvsField] = None
    hgvs_pro: Optional[HgvsField] = None
    hgvs_splice: Optional[HgvsField] = None
    assay_level_hgvs: Optional[HgvsField] = None
    protein_level_hgvs: Optional[HgvsField] = None

    class Config:
        from_attributes = True

"""Response view models for the assayed variant-detail view (``GET /variants/{urn}``, design §7.1).

The pydantic serialization boundary over the ``lib.variant_detail`` transit dataclasses
(``from_attributes`` coerces them directly); aliases camelize per the shared base config. Kept in its
own module — mirroring ``lib/variant_detail.py`` and the ``lean_variant`` precedent — so
``view_models/variant.py`` stays the core measurement + lookup file.
"""

from typing import Any, Optional

from mavedb.view_models.allele_annotation import AlleleAnnotations
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.score_calibration import SavedFunctionalClassification


class VariantClassification(BaseModel):
    """A functional classification the variant falls into, tagged with its calibration context.

    A score set may carry several calibrations, so a variant has one classification per calibration;
    ``primary`` flags the UI default. The classifications are calibration-derived but as-of-invariant
    (calibrations carry no valid-time), so they are always the current calibration state.
    """

    calibration_id: int
    primary: bool
    classification: SavedFunctionalClassification

    class Config:
        from_attributes = True


class VariantDetail(BaseModel):
    """The assayed variant-detail envelope (``GET /variants/{urn}``, design §7.1).

    Two tiers: flat, UI-ergonomic assay fields (the ``targetHgvs``/``referenceHgvs`` coordinate pair
    is a client-side toggle, no refetch) plus the spec-pure GA4GH ``molecularRepresentation``
    (``CategoricalVariant``, no MaveDB fields inside). The MaveDB layer rides alongside keyed by VRS
    digest: ``memberRelations`` (member→defining relation) and the ``annotations`` map. ``isCurrent``
    /``supersededBy`` let a superseded variant self-describe. Absent fields are omitted.
    """

    urn: str
    scores: Optional[dict[str, Any]] = None
    counts: Optional[dict[str, Any]] = None
    classifications: list[VariantClassification] = []

    assay_level: Optional[str] = None
    target_hgvs: Optional[str] = None
    reference_hgvs: Optional[str] = None
    assay_level_digest: Optional[str] = None
    clingen_allele_id: Optional[str] = None

    molecular_representation: Optional[dict[str, Any]] = None
    mode: Optional[str] = None
    member_relations: dict[str, str] = {}

    annotations: dict[str, AlleleAnnotations] = {}

    is_current: bool
    superseded_by: Optional[str] = None

    class Config:
        from_attributes = True

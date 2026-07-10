"""Response view models for the assayed variant-detail view (``GET /variants/{urn}``).

The pydantic serialization boundary over the ``lib.variant_detail`` transit dataclasses
(``from_attributes`` coerces them directly); aliases camelize per the shared base config. Kept in its
own module — mirroring ``lib/variant_detail.py`` and the ``lean_variant`` precedent — so
``view_models/variant.py`` stays the core measurement + lookup file.
"""

from typing import Any, Optional

from mavedb.models.enums.sequence_level import SequenceLevel
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


class AlleleIdentity(BaseModel):
    """The MaveDB molecular-identity facts for one of the variant's linked alleles.

    An entry of the ``alleles`` sidecar, keyed by VRS digest. ``level`` + ``hgvs`` (the reference-frame
    HGVS) are what the UI labels the per-level annotation panel by — never the digest.

    Three independent axes:
    - ``relation`` (Cat-VRS, structural): member→defining relation; ``null`` when it *is* the measured
      allele, or when the allele is not a Cat-VRS member.
    - ``derivation`` (provenance): ``authoritative`` (measured) / ``projection`` (deterministic,
      precise) / ``candidate`` (reverse-translation, ambiguous). Orthogonal to ``relation`` — never
      conflate them.
    - ``projectionOf`` (provenance): the VRS digest of this allele's projection sibling (the paired c↔g member of
      its projection pair group); ``null`` for the protein apex and pre-reverse-translation data.
    """

    level: Optional[str] = None
    hgvs: Optional[str] = None
    clingen_allele_id: Optional[str] = None
    relation: Optional[str] = None
    derivation: Optional[str] = None
    projection_of: Optional[str] = None

    class Config:
        from_attributes = True


class VariantDetail(BaseModel):
    """The assayed variant-detail envelope (``GET /variants/{urn}``).

    Two tiers: flat, UI-ergonomic assay fields (the ``targetHgvs``/``referenceHgvs`` coordinate pair
    is a client-side toggle, no refetch) plus the spec-pure GA4GH ``molecularRepresentation``
    (``CategoricalVariant``, no MaveDB fields inside). The MaveDB layer rides alongside, keyed by VRS
    digest: the ``alleles`` identity sidecar (per-allele ``level`` / ``hgvs`` / ``clingenAlleleId`` /
    ``relation`` — one entry per linked allele, sharing keys with ``annotations``) and the
    ``annotations`` map. ``isCurrent``/``supersededByScoreSet`` let a superseded variant self-describe:
    ``supersededByScoreSet`` is the superseding *score set*'s URN, not a variant URN. Supersession is
    versioned at the score-set level, and a newer version may add, drop, or renumber variants — so there
    is no stable superseding-*variant* pointer to hand back; a consumer resolves the current measurement
    by looking this variant up within that score set. Absent fields are omitted.
    """

    urn: str
    scores: Optional[dict[str, Any]] = None
    counts: Optional[dict[str, Any]] = None
    classifications: list[VariantClassification] = []

    assay_level: Optional[SequenceLevel] = None
    target_hgvs: Optional[str] = None
    reference_hgvs: Optional[str] = None
    assay_level_digest: Optional[str] = None
    clingen_allele_id: Optional[str] = None

    molecular_representation: Optional[dict[str, Any]] = None
    mode: Optional[str] = None
    alleles: dict[str, AlleleIdentity] = {}

    annotations: dict[str, AlleleAnnotations] = {}

    is_current: bool
    superseded_by_score_set: Optional[str] = None

    class Config:
        from_attributes = True

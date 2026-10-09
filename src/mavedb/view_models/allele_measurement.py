"""Response view model for the variant page's measurements list
(``GET /clingen-alleles/{caid}/measurements``).

The pydantic serialization boundary over the ``lib.allele_measurements`` transit dataclass
(``from_attributes`` coerces it directly); aliases camelize per the shared base config.
"""

from typing import Optional

from pydantic import model_validator

from mavedb.lib.allele_measurements import MeasurementRelationship
from mavedb.models.enums.sequence_level import SequenceLevel
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.score_calibration import SavedFunctionalClassification


class AlleleMeasurement(BaseModel):
    """One measurement in the queried ClinGen allele's cross-layer equivalence class.

    ``assayLevel`` is the level at which this measurement was actually assayed (``protein`` / ``cdna`` /
    ``genomic``) — always shown, since the measured level is the clinically load-bearing fact.
    ``relationship`` says how the measurement relates to the queried ClinGen id: ``direct`` (assayed at
    this allele), ``protein_consequence`` (a protein measurement of a nt query's consequence), or
    ``nucleotide_encoding`` (a nt measurement encoding a protein query). ``preferredClassification`` is the
    readable functional classification the UI defaults to (primary-first cascade, RUO excluded), omitted
    when absent or gated; ranking uses it alone. ``researchUseOnlyClassification`` is a display-only
    fallback from research-use-only calibrations, present only when the score set has no readable non-RUO
    calibration; it must not be treated as clinical evidence. ``isCurrent`` /
    ``supersededByScoreSet`` let a superseded measurement (surfaced only under ``include_superseded``)
    self-describe; ``supersededByScoreSet`` is the superseding *score set*'s URN.
    """

    variant_urn: str
    score: Optional[float] = None
    assay_level: Optional[SequenceLevel] = None
    relationship: MeasurementRelationship
    assay_level_hgvs: Optional[str] = None
    submitted_hgvs: Optional[str] = None
    score_set_urn: str
    score_set_title: str
    preferred_classification: Optional[SavedFunctionalClassification] = None
    research_use_only_classification: Optional[SavedFunctionalClassification] = None
    is_current: bool
    superseded_by_score_set: Optional[str] = None

    @model_validator(mode="after")
    def classifications_are_mutually_exclusive(self) -> "AlleleMeasurement":
        """The RUO fallback exists only where no preferred call does; both set means the builder broke that."""
        if self.preferred_classification is not None and self.research_use_only_classification is not None:
            raise ValueError("researchUseOnlyClassification may only be set when preferredClassification is absent.")
        return self

    class Config:
        from_attributes = True

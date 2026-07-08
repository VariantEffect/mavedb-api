"""Response view model for the variant page's measurements list
(``GET /clingen-alleles/{caid}/measurements``, design §7.5).

The pydantic serialization boundary over the ``lib.allele_measurements`` transit dataclass
(``from_attributes`` coerces it directly); aliases camelize per the shared base config.
"""

from typing import Optional

from mavedb.lib.allele_measurements import MeasurementRelationship
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.score_calibration import SavedFunctionalClassification


class AlleleMeasurement(BaseModel):
    """One measurement in the queried ClinGen allele's cross-layer equivalence class.

    ``assayLevel`` is the level at which this measurement was actually assayed (``protein`` / ``cdna`` /
    ``genomic``) — always shown, since the measured level is the clinically load-bearing fact.
    ``relationship`` says how the measurement relates to the queried ClinGen id: ``direct`` (assayed at
    this allele), ``protein_consequence`` (a protein measurement of a nt query's consequence), or
    ``nucleotide_encoding`` (a nt measurement encoding a protein query). ``primaryClassification`` is the
    primary readable functional classification, omitted when absent or gated. ``isCurrent`` /
    ``supersededByScoreSet`` let a superseded measurement (surfaced only under ``include_superseded``)
    self-describe; ``supersededByScoreSet`` is the superseding *score set*'s URN.
    """

    variant_urn: str
    score: Optional[float] = None
    assay_level: Optional[str] = None
    relationship: MeasurementRelationship
    assay_level_hgvs: Optional[str] = None
    submitted_hgvs: Optional[str] = None
    score_set_urn: str
    score_set_title: str
    primary_classification: Optional[SavedFunctionalClassification] = None
    is_current: bool
    superseded_by_score_set: Optional[str] = None

    class Config:
        from_attributes = True

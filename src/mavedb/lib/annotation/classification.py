import logging
from enum import StrEnum
from typing import Optional

from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

from mavedb.models.mapped_variant import MappedVariant
from mavedb.lib.annotation.constants import PILLAR_PROJECT_CALIBRATION_STRENGTH_OF_EVIDENCE_MAP
from mavedb.lib.validation.utilities import inf_or_float
from mavedb.view_models.score_range import ScoreSetRanges

logger = logging.getLogger(__name__)


class ExperimentalVariantFunctionalImpactClassification(StrEnum):
    """Enum for the classification of a variant's functional impact."""

    NORMAL = "normal"
    ABNORMAL = "abnormal"
    INDETERMINATE = "indeterminate"


def functional_classification_of_variant(
    mapped_variant: MappedVariant,
) -> ExperimentalVariantFunctionalImpactClassification:
    if mapped_variant.variant.score_set.score_ranges is None:
        raise ValueError(
            f"Variant {mapped_variant.variant.urn} does not have a score set with score ranges."
            " Unable to classify functional impact."
        )

    # This view model object is much simpler to work with.
    score_ranges = ScoreSetRanges(**mapped_variant.variant.score_set.score_ranges).investigator_provided

    if not score_ranges:
        raise ValueError(
            f"Variant {mapped_variant.variant.urn} does not have investigator-provided score ranges."
            " Unable to classify functional impact."
        )

    # This property of this column is guaranteed to be defined.
    functional_score: Optional[float] = mapped_variant.variant.data["score_data"]["score"]  # type: ignore
    if functional_score is None:
        raise ValueError(
            f"Variant {mapped_variant.variant.urn} does not have a functional score."
            " Unable to classify functional impact."
        )

    for range in score_ranges.ranges:
        lower_bound, upper_bound = inf_or_float(range.range[0], lower=True), inf_or_float(range.range[1], lower=False)
        if functional_score > lower_bound and functional_score <= upper_bound:
            if range.classification == "normal":
                return ExperimentalVariantFunctionalImpactClassification.NORMAL
            elif range.classification == "abnormal":
                return ExperimentalVariantFunctionalImpactClassification.ABNORMAL
            else:
                return ExperimentalVariantFunctionalImpactClassification.INDETERMINATE

    return ExperimentalVariantFunctionalImpactClassification.INDETERMINATE


def pillar_project_clinical_classification_of_variant(
    mapped_variant: MappedVariant,
) -> tuple[VariantPathogenicityEvidenceLine.Criterion, Optional[StrengthOfEvidenceProvided]]:
    if mapped_variant.variant.score_set.score_ranges is None:
        raise ValueError(
            f"Variant {mapped_variant.variant.urn} does not have a score set with score thresholds."
            " Unable to classify clinical impact."
        )

    score_ranges = ScoreSetRanges(**mapped_variant.variant.score_set.score_ranges).pillar_project

    if not score_ranges:
        raise ValueError(
            f"Variant {mapped_variant.variant.urn} does not have pillar project score ranges."
            " Unable to classify clinical impact."
        )

    # This property of this column is guaranteed to be defined.
    functional_score: Optional[float] = mapped_variant.variant.data["score_data"]["score"]  # type: ignore
    if functional_score is None:
        raise ValueError(
            f"Variant {mapped_variant.variant.urn} does not have a functional score."
            " Unable to classify clinical impact."
        )

    for range in score_ranges.ranges:
        lower_bound, upper_bound = inf_or_float(range.range[0], lower=True), inf_or_float(range.range[1], lower=False)
        if functional_score > lower_bound and functional_score <= upper_bound:
            return PILLAR_PROJECT_CALIBRATION_STRENGTH_OF_EVIDENCE_MAP[range.evidence_strength]

    return PILLAR_PROJECT_CALIBRATION_STRENGTH_OF_EVIDENCE_MAP[0]

import logging
from enum import StrEnum
from typing import Optional

from ga4gh.va_spec.acmg_2015 import EvidenceOutcome
from ga4gh.va_spec.base import StrengthOfEvidenceProvided

from mavedb.models.mapped_variant import MappedVariant
from mavedb.lib.validation.utilities import inf_or_float
from mavedb.view_models.score_set import ScoreRanges
from mavedb.view_models.calibration import PillarProjectCalibration

logger = logging.getLogger(__name__)


PILLAR_PROJECT_CALIBRATION_STRENGTH_OF_EVIDENCE_MAP = {
    # No evidence
    0: (None, None),
    # Supporting evidence
    -1: (EvidenceOutcome.BS3_SUPPORTING, StrengthOfEvidenceProvided.SUPPORTING),
    1: (EvidenceOutcome.PS3_SUPPORTING, StrengthOfEvidenceProvided.SUPPORTING),
    # Moderate evidence
    -2: (EvidenceOutcome.BS3_MODERATE, StrengthOfEvidenceProvided.MODERATE),
    2: (EvidenceOutcome.PS3_MODERATE, StrengthOfEvidenceProvided.MODERATE),
    -3: (EvidenceOutcome.BS3_MODERATE, StrengthOfEvidenceProvided.MODERATE),
    3: (EvidenceOutcome.PS3_MODERATE, StrengthOfEvidenceProvided.MODERATE),
    # Strong evidence
    -4: (EvidenceOutcome.BS3, StrengthOfEvidenceProvided.STRONG),
    4: (EvidenceOutcome.PS3, StrengthOfEvidenceProvided.STRONG),
    -5: (EvidenceOutcome.BS3, StrengthOfEvidenceProvided.STRONG),
    5: (EvidenceOutcome.PS3, StrengthOfEvidenceProvided.STRONG),
    -6: (EvidenceOutcome.BS3, StrengthOfEvidenceProvided.STRONG),
    6: (EvidenceOutcome.PS3, StrengthOfEvidenceProvided.STRONG),
    -7: (EvidenceOutcome.BS3, StrengthOfEvidenceProvided.STRONG),
    7: (EvidenceOutcome.PS3, StrengthOfEvidenceProvided.STRONG),
    # Very Strong evidence
    -8: (EvidenceOutcome.BS3, StrengthOfEvidenceProvided.VERY_STRONG),
    8: (EvidenceOutcome.PS3, StrengthOfEvidenceProvided.VERY_STRONG),
}


class ExperimentalVariantFunctionalImpactClassification(StrEnum):
    """Enum for the classification of a variant's functional impact."""

    NORMAL = "normal"
    ABNORMAL = "abnormal"
    INDETERMINATE = "indeterminate"


def functional_classification_of_variant(
    mapped_variant: MappedVariant,
) -> Optional[ExperimentalVariantFunctionalImpactClassification]:
    if mapped_variant.variant.score_set.score_ranges is None:
        raise ValueError(
            f"Variant {mapped_variant.variant.urn} does not have a score set with score ranges."
            " Unable to classify functional impact."
        )

    # This view model object is much simpler to work with.
    score_ranges = ScoreRanges(**mapped_variant.variant.score_set.score_ranges)

    # This property of this column is guaranteed to be defined.
    functional_score: float = mapped_variant.variant.data["score_data"]["score"]  # type: ignore
    for range in score_ranges.ranges:
        lower_bound, upper_bound = inf_or_float(range.range[0], lower=True), inf_or_float(range.range[1], lower=False)
        if functional_score > lower_bound and functional_score <= upper_bound:
            return (
                ExperimentalVariantFunctionalImpactClassification.NORMAL
                if range.classification == "normal"
                else ExperimentalVariantFunctionalImpactClassification.ABNORMAL
            )

    return ExperimentalVariantFunctionalImpactClassification.INDETERMINATE


def pillar_project_clinical_classification_of_variant(
    mapped_variant: MappedVariant,
) -> tuple[Optional[EvidenceOutcome], Optional[StrengthOfEvidenceProvided]]:
    if mapped_variant.variant.score_set.score_calibrations is None:
        raise ValueError(
            f"Variant {mapped_variant.variant.urn} does not have a score set with score thresholds."
            " Unable to classify clinical impact."
        )

    score_calibration = PillarProjectCalibration(
        **mapped_variant.variant.score_set.score_calibrations["pillar_project"]
    )

    # NOTE: It is presumed these thresholds are ordered.

    # This property of this column is guaranteed to be defined.
    functional_score: float = mapped_variant.variant.data["score_data"]["score"]  # type: ignore

    most_extreme_evidence_strength = 0
    for idx, threshold in enumerate(score_calibration.thresholds):
        if idx == 0:
            # If the first threshold is larger than the final threshold, the initial cardinality is positive.
            current_cardinality = 1 if score_calibration.thresholds[0] > score_calibration.thresholds[-1] else -1
        else:
            # After setting the initial cardinality, the cardinality will change only once the signs of the evidence strengths flip.
            current_cardinality = (
                current_cardinality * -1
                if score_calibration.evidence_strengths[idx - 1] * score_calibration.evidence_strengths[idx] < 0
                else current_cardinality
            )

        # If the cardinality is positive, we ensure the score is greater than the threshold. If the cardinality is negative,
        # check the threshold is greater than the score.
        score_threshold_pair = (
            (functional_score, threshold) if current_cardinality > 0 else (threshold, functional_score)
        )
        if score_threshold_pair[0] >= score_threshold_pair[1]:
            if abs(score_calibration.evidence_strengths[idx]) > abs(most_extreme_evidence_strength):
                most_extreme_evidence_strength = score_calibration.evidence_strengths[idx]

    return PILLAR_PROJECT_CALIBRATION_STRENGTH_OF_EVIDENCE_MAP[most_extreme_evidence_strength]

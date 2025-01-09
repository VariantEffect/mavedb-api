import logging
from typing import Optional

from fastapi.encoders import jsonable_encoder
from ga4gh.va_spec.profiles.assay_var_effect import AveFunctionalClassification, AveClinicalClassification

from mavedb.models.mapped_variant import MappedVariant
from mavedb.lib.validation.utilities import inf_or_float
from mavedb.view_models.score_set import ScoreRanges
from mavedb.view_models.calibration import PillarProjectCalibration

logger = logging.getLogger(__name__)


PILLAR_PROJECT_CALIBRATION_EVIDENCE_STRENGTH_MAP = {
    # No evidence
    0: None,
    # Supporting evidence
    -1: AveClinicalClassification.BS3_SUPPORTING,
    1: AveClinicalClassification.PS3_SUPPORTING,
    # Moderate evidence
    -2: AveClinicalClassification.BS3_MODERATE,
    2: AveClinicalClassification.PS3_MODERATE,
    -3: AveClinicalClassification.BS3_MODERATE,
    3: AveClinicalClassification.PS3_MODERATE,
    # Strong evidence
    -4: AveClinicalClassification.BS3_STRONG,
    4: AveClinicalClassification.PS3_STRONG,
    -5: AveClinicalClassification.BS3_STRONG,
    5: AveClinicalClassification.PS3_STRONG,
    -6: AveClinicalClassification.BS3_STRONG,
    6: AveClinicalClassification.PS3_STRONG,
    -7: AveClinicalClassification.BS3_STRONG,
    7: AveClinicalClassification.PS3_STRONG,
    # TODO: Very Strong evidence
    -8: AveClinicalClassification.BS3_STRONG,
    8: AveClinicalClassification.PS3_STRONG,
}


def functional_classification_of_variant(mapped_variant: MappedVariant) -> Optional[AveFunctionalClassification]:
    if mapped_variant.variant.score_set.score_ranges is None:
        return None

    # This view model object is much simpler to work with.
    score_ranges = ScoreRanges(**jsonable_encoder(mapped_variant.variant.score_set.score_ranges))

    # This property of this column is guaranteed to be defined.
    functional_score: float = mapped_variant.variant.data["score_data"]["score"]  # type: ignore
    for range in score_ranges.ranges:
        lower_bound, upper_bound = inf_or_float(range.range[0], lower=True), inf_or_float(range.range[1], lower=False)
        if functional_score > lower_bound and functional_score <= upper_bound:
            return (
                AveFunctionalClassification.NORMAL
                if range.classification == "normal"
                else AveFunctionalClassification.ABNORMAL
            )

    return AveFunctionalClassification.INDETERMINATE


def pillar_project_clinical_classification_of_variant(
    mapped_variant: MappedVariant,
) -> Optional[AveClinicalClassification]:
    if mapped_variant.variant.score_set.score_calibrations is None:
        return None

    score_calibration = PillarProjectCalibration(
        **jsonable_encoder(mapped_variant.variant.score_set.score_calibrations["pillar_project"])
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

    return PILLAR_PROJECT_CALIBRATION_EVIDENCE_STRENGTH_MAP[most_extreme_evidence_strength]

import logging
from enum import StrEnum
from typing import Optional

from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

from mavedb.models.enums.functional_classification import FunctionalClassification as FunctionalClassificationOptions
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification

logger = logging.getLogger(__name__)


class ExperimentalVariantFunctionalImpactClassification(StrEnum):
    """Enum for the classification of a variant's functional impact."""

    NORMAL = "normal"
    ABNORMAL = "abnormal"
    INDETERMINATE = "indeterminate"


def functional_classification_of_variant(
    mapped_variant: MappedVariant, score_calibration: ScoreCalibration
) -> tuple[Optional[ScoreCalibrationFunctionalClassification], ExperimentalVariantFunctionalImpactClassification]:
    """Classify a variant's functional impact as normal, abnormal, or indeterminate.

    Uses the primary score calibration and its functional ranges.
    Raises ValueError if required calibration or score is missing.
    """
    if not mapped_variant.variant.score_set.score_calibrations:
        raise ValueError(
            f"Variant {mapped_variant.variant.urn} does not have a score set with score calibrations."
            " Unable to classify functional impact."
        )

    if not score_calibration.functional_classifications:
        raise ValueError(
            f"Variant {mapped_variant.variant.urn} does not have ranges defined in its primary score calibration."
            " Unable to classify functional impact."
        )

    # TODO#XXX: Performance: avoid ORM relationship membership checks (`variant in functional_range.variants`) in this
    #           DB-agnostic function. Resolve class-based matches in an upstream DB-aware layer using the association table,
    #           pass matched functional classification IDs into this function, and use O(1) ID membership checks here.
    for functional_range in score_calibration.functional_classifications:
        if mapped_variant.variant in functional_range.variants:
            if functional_range.functional_classification is FunctionalClassificationOptions.normal:
                return functional_range, ExperimentalVariantFunctionalImpactClassification.NORMAL
            elif functional_range.functional_classification is FunctionalClassificationOptions.abnormal:
                return functional_range, ExperimentalVariantFunctionalImpactClassification.ABNORMAL
            else:
                return functional_range, ExperimentalVariantFunctionalImpactClassification.INDETERMINATE
    return None, ExperimentalVariantFunctionalImpactClassification.INDETERMINATE


def pathogenicity_classification_of_variant(
    mapped_variant: MappedVariant,
    score_calibration: ScoreCalibration,
) -> tuple[
    Optional[ScoreCalibrationFunctionalClassification],
    VariantPathogenicityEvidenceLine.Criterion,
    Optional[StrengthOfEvidenceProvided],
]:
    """Classify a variant's pathogenicity and evidence strength using clinical calibration.

    Uses the first clinical score calibration and its functional ranges.

    NOTE: Even when a variant is not contained in any score ranges, this function returns the PS3 criterion.
          Consumers of this method's return information should take care to note that when a criterion is
          returned with no evidence strength, it should not be interpreted as evidence for the criterion,
          but rather as an indication that the variant was evaluated for this criterion but did not meet its
          criteria.

    Raises ValueError if required calibration, score, or evidence strength is missing.
    """
    if not mapped_variant.variant.score_set.score_calibrations:
        raise ValueError(
            f"Variant {mapped_variant.variant.urn} does not have a score set with score calibrations."
            " Unable to classify clinical impact."
        )

    if not score_calibration.functional_classifications:
        raise ValueError(
            f"Variant {mapped_variant.variant.urn} does not have ranges defined in its primary score calibration."
            " Unable to classify clinical impact."
        )

    # TODO#XXX: Performance: avoid ORM relationship membership checks (`variant in pathogenicity_range.variants`) in this
    #           DB-agnostic function. Resolve class-based matches in an upstream DB-aware layer using the association table,
    #           pass matched functional classification IDs into this function, and use O(1) ID membership checks here.
    for pathogenicity_range in score_calibration.functional_classifications:
        if mapped_variant.variant in pathogenicity_range.variants:
            if pathogenicity_range.acmg_classification is None:
                return (pathogenicity_range, VariantPathogenicityEvidenceLine.Criterion.PS3, None)

            if (
                pathogenicity_range.acmg_classification.evidence_strength is None
                or pathogenicity_range.acmg_classification.criterion is None
            ):  # pragma: no cover
                return (pathogenicity_range, VariantPathogenicityEvidenceLine.Criterion.PS3, None)

            # MODERATE_PLUS to MODERATE mapping for VA-Spec compatibility
            #
            # MaveDB's internal ACMG evidence strength scale includes MODERATE_PLUS (M+), which represents
            # evidence strength between MODERATE and STRONG. This granularity is useful for distinguishing
            # functional assays that provide slightly stronger evidence than typical moderate-strength evidence.
            #
            # However, the GA4GH VA-Spec standard (ga4gh.va_spec.base.enums.StrengthOfEvidenceProvided)
            # only recognizes five levels: SUPPORTING, MODERATE, STRONG, VERY_STRONG, and STANDALONE.
            # It does not include MODERATE_PLUS.
            #
            # To maintain VA-Spec compliance while preserving our internal granularity, we map MODERATE_PLUS
            # down to MODERATE when constructing VA-Spec compliant variant annotations. This means:
            # - MaveDB can internally distinguish M+ evidence for more precise functional scoring
            # - External consumers receive VA-Spec compliant annotations with standard evidence levels
            # - Information is slightly lossy but maintains semantic correctness (M+ is still moderate-strength)
            #
            # Future consideration: If VA-Spec adds support for intermediate evidence strengths, we should
            # revisit this mapping to preserve the full granularity of our evidence strength assessments.
            evidence_strength_name = pathogenicity_range.acmg_classification.evidence_strength.name
            if evidence_strength_name == "MODERATE_PLUS":
                mapped_evidence_strength = StrengthOfEvidenceProvided.MODERATE.name
            else:
                mapped_evidence_strength = StrengthOfEvidenceProvided[evidence_strength_name].name

            if (
                pathogenicity_range.acmg_classification.criterion.name
                not in VariantPathogenicityEvidenceLine.Criterion._member_names_
            ):  # pragma: no cover - enforced by model validators in FunctionalClassification view model
                raise ValueError(
                    f"Variant {mapped_variant.variant.urn} is contained in a clinical calibration range with an invalid criterion."
                    " Unable to classify clinical impact."
                )

            return (
                pathogenicity_range,
                VariantPathogenicityEvidenceLine.Criterion[pathogenicity_range.acmg_classification.criterion.name],
                StrengthOfEvidenceProvided[mapped_evidence_strength],
            )

    return (None, VariantPathogenicityEvidenceLine.Criterion.PS3, None)

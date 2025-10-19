import logging
from enum import StrEnum
from typing import Optional

from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

from mavedb.models.mapped_variant import MappedVariant
from mavedb.view_models.score_calibration import FunctionalRange

logger = logging.getLogger(__name__)


class ExperimentalVariantFunctionalImpactClassification(StrEnum):
    """Enum for the classification of a variant's functional impact."""

    NORMAL = "normal"
    ABNORMAL = "abnormal"
    INDETERMINATE = "indeterminate"


def functional_classification_of_variant(
    mapped_variant: MappedVariant,
) -> ExperimentalVariantFunctionalImpactClassification:
    """Classify a variant's functional impact as normal, abnormal, or indeterminate.

    Uses the primary score calibration and its functional ranges.
    Raises ValueError if required calibration or score is missing.
    """
    if not mapped_variant.variant.score_set.score_calibrations:
        raise ValueError(
            f"Variant {mapped_variant.variant.urn} does not have a score set with score calibrations."
            " Unable to classify functional impact."
        )

    # TODO#494: Support for multiple calibrations (all non-research use only).
    score_calibrations = mapped_variant.variant.score_set.score_calibrations or []
    primary_calibration = next((c for c in score_calibrations if c.primary), None)

    if not primary_calibration:
        raise ValueError(
            f"Variant {mapped_variant.variant.urn} does not have a primary score calibration."
            " Unable to classify functional impact."
        )

    if not primary_calibration.functional_ranges:
        raise ValueError(
            f"Variant {mapped_variant.variant.urn} does not have ranges defined in its primary score calibration."
            " Unable to classify functional impact."
        )

    # This property of this column is guaranteed to be defined.
    functional_score: Optional[float] = mapped_variant.variant.data["score_data"]["score"]  # type: ignore
    if functional_score is None:
        raise ValueError(
            f"Variant {mapped_variant.variant.urn} does not have a functional score."
            " Unable to classify functional impact."
        )

    for functional_range in primary_calibration.functional_ranges:
        # It's easier to reason with the view model objects for functional ranges than the JSONB fields in the raw database object.
        functional_range_view = FunctionalRange.model_validate(functional_range)

        if functional_range_view.is_contained_by_range(functional_score):
            if functional_range_view.classification == "normal":
                return ExperimentalVariantFunctionalImpactClassification.NORMAL
            elif functional_range_view.classification == "abnormal":
                return ExperimentalVariantFunctionalImpactClassification.ABNORMAL
            else:
                return ExperimentalVariantFunctionalImpactClassification.INDETERMINATE

    return ExperimentalVariantFunctionalImpactClassification.INDETERMINATE


def pathogenicity_classification_of_variant(
    mapped_variant: MappedVariant,
) -> tuple[VariantPathogenicityEvidenceLine.Criterion, Optional[StrengthOfEvidenceProvided]]:
    """Classify a variant's pathogenicity and evidence strength using clinical calibration.

    Uses the first clinical score calibration and its functional ranges.
    Raises ValueError if required calibration, score, or evidence strength is missing.
    """
    if not mapped_variant.variant.score_set.score_calibrations:
        raise ValueError(
            f"Variant {mapped_variant.variant.urn} does not have a score set with score calibrations."
            " Unable to classify clinical impact."
        )

    # TODO#494: Support multiple clinical calibrations.
    score_calibrations = mapped_variant.variant.score_set.score_calibrations or []
    primary_calibration = next((c for c in score_calibrations if c.primary), None)

    if not primary_calibration:
        raise ValueError(
            f"Variant {mapped_variant.variant.urn} does not have a primary score calibration."
            " Unable to classify clinical impact."
        )

    if not primary_calibration.functional_ranges:
        raise ValueError(
            f"Variant {mapped_variant.variant.urn} does not have ranges defined in its primary score calibration."
            " Unable to classify clinical impact."
        )

    # This property of this column is guaranteed to be defined.
    functional_score: Optional[float] = mapped_variant.variant.data["score_data"]["score"]  # type: ignore
    if functional_score is None:
        raise ValueError(
            f"Variant {mapped_variant.variant.urn} does not have a functional score."
            " Unable to classify clinical impact."
        )

    for pathogenicity_range in primary_calibration.functional_ranges:
        # It's easier to reason with the view model objects for functional ranges than the JSONB fields in the raw database object.
        pathogenicity_range_view = FunctionalRange.model_validate(pathogenicity_range)

        if pathogenicity_range_view.is_contained_by_range(functional_score):
            if pathogenicity_range_view.acmg_classification is None:
                return (VariantPathogenicityEvidenceLine.Criterion.PS3, None)

            # More of a type guard, as the ACMGClassification model we construct above enforces that
            # criterion and evidence strength are mutually defined.
            if (
                pathogenicity_range_view.acmg_classification.evidence_strength is None
                or pathogenicity_range_view.acmg_classification.criterion is None
            ):  # pragma: no cover - enforced by model validators in FunctionalRange view model
                return (VariantPathogenicityEvidenceLine.Criterion.PS3, None)

            # TODO#540: Handle moderate+
            if (
                pathogenicity_range_view.acmg_classification.evidence_strength.name
                not in StrengthOfEvidenceProvided._member_names_
            ):
                raise ValueError(
                    f"Variant {mapped_variant.variant.urn} is contained in a clinical calibration range with an invalid evidence strength."
                    " Unable to classify clinical impact."
                )

            if (
                pathogenicity_range_view.acmg_classification.criterion.name
                not in VariantPathogenicityEvidenceLine.Criterion._member_names_
            ):  # pragma: no cover - enforced by model validators in FunctionalRange view model
                raise ValueError(
                    f"Variant {mapped_variant.variant.urn} is contained in a clinical calibration range with an invalid criterion."
                    " Unable to classify clinical impact."
                )

            return (
                VariantPathogenicityEvidenceLine.Criterion[pathogenicity_range_view.acmg_classification.criterion.name],
                StrengthOfEvidenceProvided[pathogenicity_range_view.acmg_classification.evidence_strength.name],
            )

    return (VariantPathogenicityEvidenceLine.Criterion.PS3, None)

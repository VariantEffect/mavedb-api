"""Score-calibration eligibility and selection for VA-Spec annotation.

Which of a score set's calibrations may back an annotation (``score_calibration_may_be_used_for_annotation``)
and, among the eligible ones, which carries the strongest evidence for a given variant
(``select_strongest_*``). The selection resolves ties/conflicts to the conservative call (normal / uncertain
significance). Consumed by the annotation builders in ``annotate.py``.
"""

from typing import Iterable, Literal, Optional

from ga4gh.core.models import Extension
from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided as VaSpecStrengthOfEvidenceProvided

from mavedb.lib.annotation.classification import (
    ExperimentalVariantFunctionalImpactClassification,
    functional_classification_of_variant,
    pathogenicity_classification_of_variant,
)
from mavedb.lib.annotation.context import VariantAnnotationContext
from mavedb.lib.permissions.principal import Principal
from mavedb.lib.permissions.score_calibration import ScoreCalibrationViewer
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification

CALIBRATION_SCOPE_EXTENSION_NAME = "mavedb_calibration_scope"
"""Extension naming the principal an annotation was built for. See ``calibration_scope_extension``."""


def calibrations_available_for_annotation(
    context: VariantAnnotationContext,
    annotation_type: Literal["pathogenicity", "functional"],
    allow_research_use_only_calibrations: bool = False,
    principal: Optional[Principal] = None,
) -> list[ScoreCalibration]:
    """
    Select the calibrations on a mapped variant's score set that may build the requested annotation.

    Two independent questions decide this, and they are asked by different collaborators.
      - Eligibility: Does this calibration carry the classifications the annotation type needs, and is
        its research-use-only standing permitted here. This is ``score_calibration_may_be_used_for_annotation``.
      - Visibility: May this principal read it at all. This is the viewer's, because a calibration's READ rule
        is stricter than its score set's.

    Args:
        context (VariantAnnotationContext): The context containing the variant whose score set's calibrations are considered.
        annotation_type (Literal["pathogenicity", "functional"]): The type of annotation to be built.
        allow_research_use_only_calibrations (bool, optional): Whether calibrations marked research use
            only are eligible. Defaults to False.
        principal (Optional[Principal], optional): The caller being served. Defaults to None, an anonymous
            caller, so a function that omits it gets public calibrations only.

    Returns:
        list[ScoreCalibration]: The eligible, visible calibrations, in score set order.
    """
    viewer = (principal if principal is not None else Principal()).viewer_for(ScoreCalibrationViewer)

    return [
        score_calibration
        for score_calibration in viewer.visible(context.variant.score_set.score_calibrations)
        if score_calibration_may_be_used_for_annotation(
            score_calibration,
            annotation_type,
            allow_research_use_only_calibrations=allow_research_use_only_calibrations,
        )
    ]


def calibration_scope_extension(calibrations: Iterable[ScoreCalibration]) -> Extension:
    """
    Describe the principal an annotation was built for, given the calibrations behind it.

    VA-Spec statements carry no stable identifier, so two callers can receive materially different
    statements from the same URL. Naming the scope on the object itself is what keeps that honest: a
    consumer holding a record can tell whether it is the one anyone would get, or one widened by the
    requester's own access.

    Args:
        calibrations (Iterable[ScoreCalibration]): The calibrations contributing evidence to the annotation.

    Returns:
        Extension: A ``mavedb_calibration_scope`` extension, ``restricted`` when any contributing
            calibration is private and ``public`` otherwise.
    """
    if any(calibration.private for calibration in calibrations):
        return Extension(
            name=CALIBRATION_SCOPE_EXTENSION_NAME,
            value="restricted",
            description=(
                "Built from at least one private score calibration, visible to the requesting viewer. "
                "Another viewer requesting this variant may receive fewer evidence lines, or none."
            ),
        )

    return Extension(
        name=CALIBRATION_SCOPE_EXTENSION_NAME,
        value="public",
        description=(
            "Built only from public score calibrations. Any viewer requesting this variant receives the same evidence."
        ),
    )


def score_calibration_may_be_used_for_annotation(
    score_calibration: ScoreCalibration,
    annotation_type: Literal["pathogenicity", "functional"],
    allow_research_use_only_calibrations: bool = False,
) -> bool:
    """
    Check if a score calibration may be used for annotation based on its properties.

    This function evaluates whether a given score calibration is suitable for use in
    annotation by checking its research use only status and the presence of required
    classifications based on the annotation type.

    Args:
        score_calibration (ScoreCalibration): The score calibration to evaluate.
        annotation_type (Literal["pathogenicity", "functional"]): The type of annotation
            being considered, which determines the required classifications for validity.
        allow_research_use_only_calibrations (bool, optional): Whether to allow calibrations
            marked as research use only for annotation. Defaults to False.

    Returns:
        bool: True if the score calibration can be used for annotation, False otherwise.
    """
    if score_calibration.research_use_only and not allow_research_use_only_calibrations:
        return False

    if score_calibration.functional_classifications is None or len(score_calibration.functional_classifications) == 0:
        return False

    if annotation_type == "pathogenicity" and all(
        fr.acmg_classification is None for fr in score_calibration.functional_classifications
    ):
        return False

    return True


def select_strongest_functional_calibration(
    context: VariantAnnotationContext,
    calibrations: list[ScoreCalibration],
) -> tuple[Optional[ScoreCalibration], Optional[ScoreCalibrationFunctionalClassification]]:
    """
    Select the calibration with the strongest evidence for functional classification.

    In case of ties or conflicting classifications, defaults to normal classification.
    Returns the calibration and its functional classification range that contains the variant.

    If the variant is not contained in any range, returns (first_calibration, None) to indicate
    the variant should be classified as INDETERMINATE but still receive annotations.
    """
    if not calibrations:
        return None, None

    # Collect all calibrations and their classifications
    candidates: list[
        tuple[
            ScoreCalibration,
            ScoreCalibrationFunctionalClassification,
            ExperimentalVariantFunctionalImpactClassification,
        ]
    ] = []

    for calibration in calibrations:
        functional_range, classification = functional_classification_of_variant(context.variant, calibration)
        if functional_range is not None:
            candidates.append((calibration, functional_range, classification))

    # If variant is not in any range, return first calibration with None to indicate INDETERMINATE
    if not candidates:
        return calibrations[0], None

    # If only one candidate, return it
    if len(candidates) == 1:
        return candidates[0][0], candidates[0][1]

    # Check if all classifications agree
    classifications = [c[2] for c in candidates]
    if all(cls == classifications[0] for cls in classifications):
        # All agree, return the first one
        return candidates[0][0], candidates[0][1]

    # Conflict exists: default to normal classification
    normal_candidates = [c for c in candidates if c[2] == ExperimentalVariantFunctionalImpactClassification.NORMAL]
    if normal_candidates:
        return normal_candidates[0][0], normal_candidates[0][1]

    # If no normal classification, return the first candidate
    return candidates[0][0], candidates[0][1]


def select_strongest_pathogenicity_calibration(
    context: VariantAnnotationContext,
    calibrations: list[ScoreCalibration],
) -> tuple[Optional[ScoreCalibration], Optional[ScoreCalibrationFunctionalClassification]]:
    """
    Select the calibration with the strongest evidence for pathogenicity classification.

    Uses ACMG evidence strength to determine the strongest evidence.
    In case of ties with conflicting evidence (both benign and pathogenic), defaults to uncertain
    significance by returning None for the functional range.
    Returns the calibration and its functional classification range that contains the variant.

    If the variant is not contained in any range, returns (first_calibration, None) to indicate
    the variant should receive annotations even though it's not classified in any range.
    """
    if not calibrations:
        return None, None

    # Define evidence strength ordering (higher index = stronger evidence)
    # Note: VA-Spec StrengthOfEvidenceProvided doesn't have MODERATE_PLUS, only our MaveDB enum does.
    # The classification.py module maps MODERATE_PLUS to MODERATE when returning VA-Spec enum values.
    strength_order = {
        None: 0,
        VaSpecStrengthOfEvidenceProvided.SUPPORTING: 1,
        VaSpecStrengthOfEvidenceProvided.MODERATE: 2,
        VaSpecStrengthOfEvidenceProvided.STRONG: 3,
        VaSpecStrengthOfEvidenceProvided.VERY_STRONG: 4,
    }

    # Collect all calibrations with their evidence strength and criterion
    candidates: list[tuple[ScoreCalibration, ScoreCalibrationFunctionalClassification, int, bool]] = []

    for calibration in calibrations:
        functional_range, criterion, evidence_strength = pathogenicity_classification_of_variant(
            context.variant, calibration
        )
        if functional_range is not None:
            strength_value = strength_order.get(evidence_strength, 0)
            is_benign = criterion.name.startswith("B") if criterion else False
            candidates.append((calibration, functional_range, strength_value, is_benign))

    # If variant is not in any range, return first calibration with None
    if not candidates:
        return calibrations[0], None

    # If only one candidate, return it
    if len(candidates) == 1:
        return candidates[0][0], candidates[0][1]

    # Find the maximum strength
    max_strength = max(c[2] for c in candidates)
    strongest_candidates = [c for c in candidates if c[2] == max_strength]

    # If only one with max strength, return it
    if len(strongest_candidates) == 1:
        return strongest_candidates[0][0], strongest_candidates[0][1]

    # Tie: check for conflicting evidence (both benign and pathogenic)
    has_benign = any(c[3] for c in strongest_candidates)
    has_pathogenic = any(not c[3] for c in strongest_candidates)

    # If there's a conflict between benign and pathogenic evidence of equal strength,
    # return None for the functional range to indicate uncertain significance
    if has_benign and has_pathogenic:
        return strongest_candidates[0][0], None

    # Otherwise return the first of the strongest
    return strongest_candidates[0][0], strongest_candidates[0][1]

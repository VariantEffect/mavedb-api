"""Whether a variant is eligible for VA-Spec annotation.

Gate predicates the annotation builders consult before constructing a statement: a variant needs a real
score (the base assumption) and its score set needs at least one calibration usable for the target
annotation type (:func:`calibration.score_calibration_may_be_used_for_annotation`). Split by annotation
type into ``can_annotate_variant_for_pathogenicity_evidence`` / ``can_annotate_variant_for_functional_statement``.
"""

from typing import Literal

from mavedb.lib.annotation.calibration import score_calibration_may_be_used_for_annotation
from mavedb.lib.variants import variant_score
from mavedb.models.variant import Variant


def _can_annotate_variant_base_assumptions(variant: Variant) -> bool:
    """
    Check if a variant meets the basic requirements for annotation.

    This function validates that a variant has the necessary data to proceed with
    annotation by checking for a valid score value.

    Args:
        variant (Variant): The variant to check for annotation eligibility.

    Returns:
        bool: True if the variant can be annotated (has a non-None numeric score), False otherwise.
    """
    # A variant is annotatable only if it carries a real (non-null, numeric) score.
    if variant_score(variant) is None:
        return False

    return True


def _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
    variant: Variant,
    annotation_type: Literal["pathogenicity", "functional"],
    allow_research_use_only_calibrations: bool = False,
) -> bool:
    """
    Check if a variant's score set contains any of the required calibrations for annotation.

    Args:
        variant (Variant): The variant whose score set is checked.
        annotation_type (Literal["pathogenicity", "functional"]): The type of annotation to check for.
            Must be either "pathogenicity" or "functional".
        allow_research_use_only_calibrations (bool, optional): Whether to consider calibrations marked as
            research use only as valid for annotation. Defaults to False.

    Returns:
        bool: True if the variant's score set contains at least one valid calibration with the required
            classifications for the specified annotation type. False otherwise.
    """
    if variant.score_set.score_calibrations is None:
        return False

    return any(
        score_calibration_may_be_used_for_annotation(
            score_calibration,
            annotation_type,
            allow_research_use_only_calibrations=allow_research_use_only_calibrations,
        )
        for score_calibration in variant.score_set.score_calibrations
    )


def can_annotate_variant_for_pathogenicity_evidence(
    variant: Variant, allow_research_use_only_calibrations=False
) -> bool:
    """
    Determine if a variant can be annotated for pathogenicity evidence.

    This function checks if a variant meets all the necessary conditions to receive
    pathogenicity evidence annotations by validating base assumptions and ensuring the variant's
    score calibrations contain the required kinds for pathogenicity evidence annotation.

    Args:
        variant (Variant): The variant to evaluate for pathogenicity evidence annotation eligibility.

    Returns:
        bool: True if the variant can be annotated for pathogenicity evidence, False otherwise.

    Notes:
        The function performs two main validation checks:
        1. Basic annotation assumptions via _can_annotate_variant_base_assumptions
        2. Verifies score calibrations have an appropriate calibration for pathogenicity evidence annotation.

        Both checks must pass for the variant to be considered eligible for
        pathogenicity evidence annotation.
    """
    if not _can_annotate_variant_base_assumptions(variant):
        return False
    if not _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
        variant, "pathogenicity", allow_research_use_only_calibrations=allow_research_use_only_calibrations
    ):
        return False

    return True


def can_annotate_variant_for_functional_statement(variant: Variant, allow_research_use_only_calibrations=False) -> bool:
    """
    Determine if a variant can be annotated for functional statements.

    This function checks if a variant meets all the necessary conditions to receive
    functional annotations by validating base assumptions and ensuring the variant's
    score calibrations contain the required kinds for functional annotation.

    Args:
        variant (Variant): The variant to check for annotation eligibility.

    Returns:
        bool: True if the variant can be annotated for functional statements, False otherwise.

    Notes:
        The function performs two main checks:
        1. Validates base assumptions using _can_annotate_variant_base_assumptions
        2. Verifies score calibrations have an appropriate calibration for functional annotation.
    """
    if not _can_annotate_variant_base_assumptions(variant):
        return False
    if not _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
        variant, "functional", allow_research_use_only_calibrations=allow_research_use_only_calibrations
    ):
        return False

    return True

"""Whether a variant is eligible for VA-Spec annotation.

Gate predicates the annotation builders consult before constructing a statement: a variant needs a real
score (the base assumption) and its score set needs at least one calibration usable for the target
annotation type (:func:`calibration.calibrations_available_for_annotation`). This calibration must be readable
by the requesting principal. Split by annotation type into ``can_annotate_variant_for_pathogenicity_evidence`` /
``can_annotate_variant_for_functional_statement``.
"""

from typing import Optional

from mavedb.lib.annotation.calibration import calibrations_available_for_annotation
from mavedb.lib.annotation.context import VariantAnnotationContext
from mavedb.lib.permissions.principal import Principal
from mavedb.lib.variants import variant_score


def _can_annotate_variant_base_assumptions(context: VariantAnnotationContext) -> bool:
    """
    Check if a variant meets the basic requirements for annotation.

    This function validates that a variant has the necessary data to proceed with
    annotation by checking for a valid score value.

    Args:
        context (VariantAnnotationContext): The context containing the variant to check for annotation eligibility.

    Returns:
        bool: True if the variant can be annotated (has a non-None numeric score), False otherwise.
    """
    # A variant is annotatable only if it carries a real (non-null, numeric) score.
    if variant_score(context.variant) is None:
        return False

    return True


def can_annotate_variant_for_pathogenicity_evidence(
    context: VariantAnnotationContext,
    allow_research_use_only_calibrations=False,
    principal: Optional[Principal] = None,
) -> bool:
    """
    Determine if a variant can be annotated for pathogenicity evidence.

    This function checks if a variant meets all the necessary conditions to receive
    pathogenicity evidence annotations by validating base assumptions and ensuring the variant's
    score calibrations contain the required kinds for pathogenicity evidence annotation.

    Args:
        context (VariantAnnotationContext): The context containing the variant to evaluate for pathogenicity evidence annotation eligibility.

    Returns:
        bool: True if the variant can be annotated for pathogenicity evidence, False otherwise.

    Notes:
        The function performs two main validation checks:
        1. Basic annotation assumptions via _can_annotate_variant_base_assumptions
        2. Verifies score calibrations have an appropriate calibration for pathogenicity evidence annotation.

        Both checks must pass for the variant to be considered eligible for
        pathogenicity evidence annotation.
    """
    if not _can_annotate_variant_base_assumptions(context):
        return False

    return bool(
        calibrations_available_for_annotation(
            context,
            "pathogenicity",
            allow_research_use_only_calibrations=allow_research_use_only_calibrations,
            principal=principal,
        )
    )


def can_annotate_variant_for_functional_statement(
    context: VariantAnnotationContext, allow_research_use_only_calibrations=False, principal: Optional[Principal] = None
) -> bool:
    """
    Determine if a variant can be annotated for functional statements.

    This function checks if a variant meets all the necessary conditions to receive
    functional annotations by validating base assumptions and ensuring the variant's
    score calibrations contain the required kinds for functional annotation.

    Args:
        context (VariantAnnotationContext): The context containing the variant to check for annotation eligibility.

    Returns:
        bool: True if the variant can be annotated for functional statements, False otherwise.

    Notes:
        The function performs two main checks:
        1. Validates base assumptions using _can_annotate_variant_base_assumptions
        2. Verifies score calibrations have an appropriate calibration for functional annotation.
    """
    if not _can_annotate_variant_base_assumptions(context):
        return False

    return bool(
        calibrations_available_for_annotation(
            context,
            "functional",
            allow_research_use_only_calibrations=allow_research_use_only_calibrations,
            principal=principal,
        )
    )

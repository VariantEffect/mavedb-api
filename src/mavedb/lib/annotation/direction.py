"""
Direction of support determination for variant annotations.

This module provides functions to determine the direction of support for variant
classifications in both functional and pathogenicity contexts. The implementations
in this module follow MaveDB's default directionality convention, where variants
are presumed to have some effect (whether functional or pathogenic) rather than
being neutral or having no effect.

The direction mapping follows these principles:
- SUPPORTS: Evidence that the variant has an effect (abnormal function or pathogenic)
- DISPUTES: Evidence that the variant does not have an effect (normal function or benign)
- NEUTRAL: Insufficient or indeterminate evidence

This convention ensures consistency across MaveDB's annotation system.
"""

from typing import Union

from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
from ga4gh.va_spec.base.core import Direction, EvidenceLine

from mavedb.lib.annotation.classification import ExperimentalVariantFunctionalImpactClassification


def aggregate_direction_of_evidence(
    evidence: Union[list[EvidenceLine], list[VariantPathogenicityEvidenceLine]],
) -> Direction:
    """
    Aggregate a list of evidence directions into a single overall direction.

    This function takes a list of Direction values (SUPPORTS, DISPUTES, NEUTRAL) and determines the overall direction of support for a variant classification based on the following rules:
    - If all evidence items SUPPORT the classification, the overall direction is SUPPORTS.
    - If all evidence items DISPUTE the classification, the overall direction is DISPUTES.
    - If there is a mix of SUPPORTS and DISPUTES, or if any evidence item is NEUTRAL, the overall direction is NEUTRAL.

    Args:
        evidence (list[EvidenceLine]): A list of EvidenceLine objects, each containing a directionOfEvidenceProvided attribute.

    Returns:
        Direction: The overall direction of support for the variant classification, determined by aggregating the directions of the individual evidence items.

    Example:
        >>> evidence = [
        ...     EvidenceLine(directionOfEvidenceProvided=Direction.SUPPORTS),
        ...     EvidenceLine(directionOfEvidenceProvided=Direction.SUPPORTS),
        ... ]
        >>> aggregate_direction_of_evidence(evidence)
        Direction.SUPPORTS
    """
    if not evidence:
        return Direction.NEUTRAL  # No evidence provided, so default to NEUTRAL

    evidence_directions = [evidence_item.directionOfEvidenceProvided for evidence_item in evidence]
    if all(direction == Direction.SUPPORTS for direction in evidence_directions):
        return Direction.SUPPORTS
    elif all(direction == Direction.DISPUTES for direction in evidence_directions):
        return Direction.DISPUTES
    else:
        return Direction.NEUTRAL


def direction_of_support_for_functional_classification(
    classification: ExperimentalVariantFunctionalImpactClassification,
) -> Direction:
    """
    Determine the direction of support for a given functional classification.

    This function maps experimental variant functional impact classifications to
    their corresponding direction of support values.

    Args:
        classification (ExperimentalVariantFunctionalImpactClassification):
            The functional impact classification of an experimental variant.

    Returns:
        Direction: The direction of support for the given classification:
            - Direction.DISPUTES for NORMAL classification
            - Direction.SUPPORTS for ABNORMAL classification
            - Direction.NEUTRAL for all other classifications

    Example:
        >>> direction_of_support_for_functional_classification(
        ...     ExperimentalVariantFunctionalImpactClassification.NORMAL
        ... )
        Direction.DISPUTES
    """
    if classification == ExperimentalVariantFunctionalImpactClassification.NORMAL:
        return Direction.DISPUTES
    elif classification == ExperimentalVariantFunctionalImpactClassification.ABNORMAL:
        return Direction.SUPPORTS
    else:
        return Direction.NEUTRAL


def direction_of_support_for_pathogenicity_classification(
    criterion: VariantPathogenicityEvidenceLine.Criterion,
) -> Direction:
    """
    Determine the direction of support for a given pathogenicity classification.

    This function maps ACMG pathogenicity evidence line criteria to their corresponding direction of support values.

    NOTE: Only PS3 and BS3 criterion are supported.

    Args:
        criterion (VariantPathogenicityEvidenceLine.Criterion):
            The ACMG criterion used for pathogenicity classification.
    Returns:
        Direction: The direction of support for the given criterion:
            - Direction.NEUTRAL if criterion is None (e.g., for functional evidence within clinical statements or if no criterion is provided)
            - Direction.SUPPORTS for PS3 criterion
            - Direction.DISPUTES for BS3 criterion
            - raises ValueError for unsupported criteria
    Raises:
        ValueError: The function raises a ValueError if an unsupported ACMG criterion is provided.
    Example:
        >>> direction_of_support_for_pathogenicity_classification(
        ...     VariantPathogenicityEvidenceLine.Criterion.PS3
        ... )
        Direction.SUPPORTS
    """
    if criterion is None:
        return Direction.NEUTRAL
    elif criterion == VariantPathogenicityEvidenceLine.Criterion.PS3:
        return Direction.SUPPORTS
    elif criterion == VariantPathogenicityEvidenceLine.Criterion.BS3:
        return Direction.DISPUTES
    else:
        raise ValueError(f"Unsupported ACMG criterion: {criterion}")

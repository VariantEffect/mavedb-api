from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.models.acmg_classification import ACMGClassification
from mavedb.models.enums.acmg_criterion import ACMGCriterion
from mavedb.models.enums.strength_of_evidence import StrengthOfEvidenceProvided


def points_evidence_strength_equivalent(
    points: int,
) -> tuple[Optional[ACMGCriterion], Optional[StrengthOfEvidenceProvided]]:
    """Infer the evidence strength and criterion from a given point value.

    Parameters
    ----------
    points : int
        The point value to classify. Positive values indicate pathogenic evidence,
        negative values indicate benign evidence, and zero indicates no evidence.

    Returns
    -------
    tuple[Optional[ACMGCriterion], Optional[StrengthOfEvidenceProvided]]
        The enumerated evidence strength and criterion corresponding to the point value.

    Raises
    ------
    TypeError
        If points is not an integer (depending on external validation; this function assumes an int input and does not explicitly check type).
    ValueError
        If the points value is outside the range of -8 to 8.

    Examples
    --------
    >>> inferred_evidence_strength_from_points(8)
    (ACMGCriterion.PS3, StrengthOfEvidenceProvided.VERY_STRONG)
    >>> inferred_evidence_strength_from_points(2)
    (ACMGCriterion.PS3, StrengthOfEvidenceProvided.MODERATE)
    >>> inferred_evidence_strength_from_points(0)
    (None, None)
    >>> inferred_evidence_strength_from_points(-1)
    (ACMGCriterion.BS3, StrengthOfEvidenceProvided.SUPPORTING)
    >>> inferred_evidence_strength_from_points(-5)
    (ACMGCriterion.BS3, StrengthOfEvidenceProvided.STRONG)

    Notes
    -----
    These thresholds reflect predefined cut points aligning with qualitative evidence strength categories.
    Adjust carefully if underlying classification criteria change, ensuring ordering and exclusivity are preserved.
    """
    if points > 8 or points < -8:
        raise ValueError("Points value must be between -8 and 8 inclusive")

    if points >= 8:
        return (ACMGCriterion.PS3, StrengthOfEvidenceProvided.VERY_STRONG)
    elif points >= 4:
        return (ACMGCriterion.PS3, StrengthOfEvidenceProvided.STRONG)
    elif points >= 3:
        return (ACMGCriterion.PS3, StrengthOfEvidenceProvided.MODERATE_PLUS)
    elif points >= 2:
        return (ACMGCriterion.PS3, StrengthOfEvidenceProvided.MODERATE)
    elif points > 0:
        return (ACMGCriterion.PS3, StrengthOfEvidenceProvided.SUPPORTING)
    elif points == 0:
        return (None, None)
    elif points > -2:
        return (ACMGCriterion.BS3, StrengthOfEvidenceProvided.SUPPORTING)
    elif points > -3:
        return (ACMGCriterion.BS3, StrengthOfEvidenceProvided.MODERATE)
    elif points > -4:
        return (ACMGCriterion.BS3, StrengthOfEvidenceProvided.MODERATE_PLUS)
    elif points > -8:
        return (ACMGCriterion.BS3, StrengthOfEvidenceProvided.STRONG)
    else:  # points <= -8
        return (ACMGCriterion.BS3, StrengthOfEvidenceProvided.VERY_STRONG)


def find_or_create_acmg_classification(
    db: Session,
    criterion: Optional[ACMGCriterion],
    evidence_strength: Optional[StrengthOfEvidenceProvided],
    points: Optional[int],
):
    """Create or find an ACMG classification based on criterion, evidence strength, and points.

    Parameters
    ----------
    db : Session
        The database session to use for querying and creating the ACMG classification.
    criterion : Optional[ACMGCriterion]
        The ACMG criterion for the classification.
    evidence_strength : Optional[StrengthOfEvidenceProvided]
        The strength of evidence provided for the classification.
    points : Optional[int]
        The point value associated with the classification.

    Returns
    -------
    ACMGClassification
        The existing or newly created ACMG classification instance.

    Raises
    ------
    ValueError
        If the combination of criterion, evidence strength, and points does not correspond to a valid ACMG classification.

    Notes
    -----
    - This function does not commit the new entry to the database; the caller is responsible for committing the session.
    """
    if (criterion is None) != (evidence_strength is None):
        raise ValueError("Both criterion and evidence_strength must be provided together or both be None, with points.")
    elif criterion is None and evidence_strength is None and points is not None:
        criterion, evidence_strength = points_evidence_strength_equivalent(points)

    # If we cannot infer a classification, return None
    if criterion is None and evidence_strength is None:
        return None

    acmg_classification = db.execute(
        select(ACMGClassification)
        .where(ACMGClassification.criterion == criterion)
        .where(ACMGClassification.evidence_strength == evidence_strength)
        .where(ACMGClassification.points == points)
    ).scalar_one_or_none()

    if not acmg_classification:
        acmg_classification = ACMGClassification(
            criterion=criterion, evidence_strength=evidence_strength, points=points
        )
        db.add(acmg_classification)

    return acmg_classification

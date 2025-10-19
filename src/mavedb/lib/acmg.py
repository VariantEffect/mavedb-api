from enum import Enum
from typing import Optional


class ACMGCriterion(str, Enum):
    """Enum for ACMG criteria codes."""

    PVS1 = "PVS1"
    PS1 = "PS1"
    PS2 = "PS2"
    PS3 = "PS3"
    PS4 = "PS4"
    PM1 = "PM1"
    PM2 = "PM2"
    PM3 = "PM3"
    PM4 = "PM4"
    PM5 = "PM5"
    PM6 = "PM6"
    PP1 = "PP1"
    PP2 = "PP2"
    PP3 = "PP3"
    PP4 = "PP4"
    PP5 = "PP5"
    BA1 = "BA1"
    BS1 = "BS1"
    BS2 = "BS2"
    BS3 = "BS3"
    BS4 = "BS4"
    BP1 = "BP1"
    BP2 = "BP2"
    BP3 = "BP3"
    BP4 = "BP4"
    BP5 = "BP5"
    BP6 = "BP6"
    BP7 = "BP7"

    @property
    def is_pathogenic(self) -> bool:
        """Return True if the criterion is pathogenic, False if benign."""
        return self.name.startswith("P")  # PVS, PS, PM, PP are pathogenic criteria

    @property
    def is_benign(self) -> bool:
        """Return True if the criterion is benign, False if pathogenic."""
        return self.name.startswith("B")  # BA, BS, BP are benign criteria


class StrengthOfEvidenceProvided(str, Enum):
    """Enum for strength of evidence provided."""

    VERY_STRONG = "very_strong"
    STRONG = "strong"
    MODERATE_PLUS = "moderate+"
    MODERATE = "moderate"
    SUPPORTING = "supporting"


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

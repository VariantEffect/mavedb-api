from typing import Optional

from mavedb.lib.acmg import StrengthOfEvidenceProvided, ACMGCriterion


def oddspaths_evidence_strength_equivalent(
    ratio: float,
) -> tuple[Optional[ACMGCriterion], Optional[StrengthOfEvidenceProvided]]:
    """
    Based on the guidelines laid out in Table 3 of:
    Brnich, S.E., Abou Tayoun, A.N., Couch, F.J. et al. Recommendations for application
    of the functional evidence PS3/BS3 criterion using the ACMG/AMP sequence variant
    interpretation framework. Genome Med 12, 3 (2020).
    https://doi.org/10.1186/s13073-019-0690-2

    Classify an odds (likelihood) ratio into a ACMGCriterion and StrengthOfEvidenceProvided.

    This function infers the ACMG/AMP-style evidence strength category from a
    precomputed odds (likelihood) ratio by applying a series of descending
    threshold comparisons. The mapping is asymmetric: higher ratios favor
    pathogenic (PS3*) evidence levels; lower ratios favor benign (BS3*) evidence
    levels; an intermediate band is considered indeterminate.

    Threshold logic (first condition matched is returned):
        ratio > 350       -> (PS3, VERY_STRONG)
        ratio > 18.6      -> (PS3, STRONG)
        ratio > 4.3       -> (PS3, MODERATE)
        ratio > 2.1       -> (PS3, SUPPORTING)
        ratio >= 0.48     -> Indeterminate (None, None)
        ratio >= 0.23     -> (BS3, SUPPORTING)
        ratio >= 0.053    -> (BS3, MODERATE)
        ratio < 0.053     -> (BS3, STRONG)

    Interval semantics:
        - Upper (pathogenic) tiers use strictly greater-than (>) comparisons.
        - Lower (benign) tiers and the indeterminate band use inclusive lower
            bounds (>=) to form closed intervals extending downward until a prior
            condition matches.
        - Because of the ordering, each numeric ratio falls into exactly one tier.

    Parameters
    ----------
    ratio : float
            The odds or likelihood ratio to classify. Must be a positive value in
            typical use. Values <= 0 are not biologically meaningful in this context
            and will be treated as < 0.053, yielding a benign-leaning classification.

    Returns
    -------
    tuple[Optional[ACMGCriterion], Optional[StrengthOfEvidenceProvided]]
            The enumerated evidence strength and criterion corresponding to the ratio.

    Raises
    ------
    TypeError
            If ratio is not a real (float/int) number (depending on external validation;
            this function assumes a float input and does not explicitly check type).
    ValueError
            If the ratio is negative (less than 0).

    Examples
    --------
    >>> inferred_evidence_strength_from_ratio(500.0)
    (ACMGCriterion.PS3, StrengthOfEvidenceProvided.VERY_STRONG)
    >>> inferred_evidence_strength_from_ratio(10.0)
    (ACMGCriterion.PS3, StrengthOfEvidenceProvided.MODERATE)
    >>> inferred_evidence_strength_from_ratio(0.30)
    (ACMGCriterion.BS3, StrengthOfEvidenceProvided.SUPPORTING)
    >>> inferred_evidence_strength_from_ratio(0.06)
    (ACMGCriterion.BS3, StrengthOfEvidenceProvided.MODERATE)
    >>> inferred_evidence_strength_from_ratio(0.5)
    (None, None)

    Notes
    -----
    These thresholds reflect predefined likelihood ratio cut points aligning with
    qualitative evidence strength categories. Adjust carefully if underlying
    classification criteria change, ensuring ordering and exclusivity are preserved.
    """
    if ratio < 0:
        raise ValueError("OddsPaths ratio must be a non-negative value")

    if ratio > 350:
        return (ACMGCriterion.PS3, StrengthOfEvidenceProvided.VERY_STRONG)
    elif ratio > 18.6:
        return (ACMGCriterion.PS3, StrengthOfEvidenceProvided.STRONG)
    elif ratio > 4.3:
        return (ACMGCriterion.PS3, StrengthOfEvidenceProvided.MODERATE)
    elif ratio > 2.1:
        return (ACMGCriterion.PS3, StrengthOfEvidenceProvided.SUPPORTING)
    elif ratio >= 0.48:
        return (None, None)
    elif ratio >= 0.23:
        return (ACMGCriterion.BS3, StrengthOfEvidenceProvided.SUPPORTING)
    elif ratio >= 0.053:
        return (ACMGCriterion.BS3, StrengthOfEvidenceProvided.MODERATE)
    else:  # ratio < 0.053
        return (ACMGCriterion.BS3, StrengthOfEvidenceProvided.STRONG)

import pytest

from mavedb.lib.acmg import ACMGCriterion, StrengthOfEvidenceProvided
from mavedb.lib.oddspaths import oddspaths_evidence_strength_equivalent


@pytest.mark.parametrize(
    "ratio,expected_criterion,expected_strength",
    [
        # Upper pathogenic tiers (strict >)
        (351, ACMGCriterion.PS3, StrengthOfEvidenceProvided.VERY_STRONG),
        (350.0001, ACMGCriterion.PS3, StrengthOfEvidenceProvided.VERY_STRONG),
        (350, ACMGCriterion.PS3, StrengthOfEvidenceProvided.STRONG),  # boundary
        (19, ACMGCriterion.PS3, StrengthOfEvidenceProvided.STRONG),
        (18.60001, ACMGCriterion.PS3, StrengthOfEvidenceProvided.STRONG),
        (18.6, ACMGCriterion.PS3, StrengthOfEvidenceProvided.MODERATE),  # boundary
        (5, ACMGCriterion.PS3, StrengthOfEvidenceProvided.MODERATE),
        (4.30001, ACMGCriterion.PS3, StrengthOfEvidenceProvided.MODERATE),
        (4.3, ACMGCriterion.PS3, StrengthOfEvidenceProvided.SUPPORTING),  # boundary
        (2.10001, ACMGCriterion.PS3, StrengthOfEvidenceProvided.SUPPORTING),
        # Indeterminate band
        (2.1, None, None),  # boundary just below >2.1
        (0.48, None, None),
        (0.50001, None, None),
        # Benign supporting
        (0.479999, ACMGCriterion.BS3, StrengthOfEvidenceProvided.SUPPORTING),
        (0.23, ACMGCriterion.BS3, StrengthOfEvidenceProvided.SUPPORTING),
        # Benign moderate
        (0.229999, ACMGCriterion.BS3, StrengthOfEvidenceProvided.MODERATE),
        (0.053, ACMGCriterion.BS3, StrengthOfEvidenceProvided.MODERATE),
        # Benign strong
        (0.052999, ACMGCriterion.BS3, StrengthOfEvidenceProvided.STRONG),
        (0.01, ACMGCriterion.BS3, StrengthOfEvidenceProvided.STRONG),
        (0.0, ACMGCriterion.BS3, StrengthOfEvidenceProvided.STRONG),
        # Very high ratio
        (1000, ACMGCriterion.PS3, StrengthOfEvidenceProvided.VERY_STRONG),
    ],
)
def test_oddspaths_classification(ratio, expected_criterion, expected_strength):
    criterion, strength = oddspaths_evidence_strength_equivalent(ratio)
    assert criterion == expected_criterion
    assert strength == expected_strength


@pytest.mark.parametrize("neg_ratio", [-1e-9, -0.01, -5])
def test_negative_ratio_raises_value_error(neg_ratio):
    with pytest.raises(ValueError):
        oddspaths_evidence_strength_equivalent(neg_ratio)


def test_each_interval_is_exclusive():
    # Sorted representative ratios spanning all tiers
    samples = [
        (0.0, 0.0529999),  # BS3 STRONG
        (0.053, 0.229999),  # BS3 MODERATE
        (0.23, 0.479999),  # BS3 SUPPORTING
        (0.48, 2.1),  # Indeterminate
        (2.10001, 4.3),  # PS3 SUPPORTING
        (4.30001, 18.6),  # PS3 MODERATE
        (18.60001, 350),  # PS3 STRONG
        (350.0001, float("inf")),  # PS3 VERY_STRONG (no upper bound)
    ]
    seen = set()
    for r in samples:
        lower_result = oddspaths_evidence_strength_equivalent(r[0])
        upper_result = oddspaths_evidence_strength_equivalent(r[1])
        assert lower_result == upper_result, f"Mismatch at interval {r}"

        assert all(
            result not in seen for result in [lower_result, upper_result]
        ), f"Duplicate classification for ratio {r}"
        seen.add(lower_result)


@pytest.mark.parametrize(
    "lower,upper",
    [
        (0.053, 0.23),  # BS3 MODERATE -> BS3 SUPPORTING transition
        (0.23, 0.48),  # BS3 SUPPORTING -> Indeterminate
        (0.48, 2.1),  # Indeterminate band
        (2.1, 4.3),  # Indeterminate -> PS3 SUPPORTING
        (4.3, 18.6),  # PS3 SUPPORTING -> PS3 MODERATE
        (18.6, 350),  # PS3 MODERATE -> PS3 STRONG
        (350, 351),  # PS3 STRONG -> PS3 VERY_STRONG
    ],
)
def test_monotonic_direction(lower, upper):
    crit_low, strength_low = oddspaths_evidence_strength_equivalent(lower)
    crit_high, strength_high = oddspaths_evidence_strength_equivalent(upper)
    # If categories differ, ensure ordering progression (not regression to benign when moving upward)
    benign_set = {ACMGCriterion.BS3}
    pathogenic_set = {ACMGCriterion.PS3}
    if crit_low != crit_high:
        # Moving upward should not go from pathogenic to benign
        assert not (crit_low in pathogenic_set and crit_high in benign_set)


def test_return_types():
    c, s = oddspaths_evidence_strength_equivalent(0.7)
    assert (c is None and s is None) or (isinstance(c, ACMGCriterion) and isinstance(s, StrengthOfEvidenceProvided))

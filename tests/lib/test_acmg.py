import pytest

from mavedb.lib.acmg import (
    ACMGCriterion,
    StrengthOfEvidenceProvided,
    points_evidence_strength_equivalent,
)


@pytest.mark.parametrize(
    "points,expected_criterion,expected_strength",
    [
        (8, ACMGCriterion.PS3, StrengthOfEvidenceProvided.VERY_STRONG),
        (7, ACMGCriterion.PS3, StrengthOfEvidenceProvided.STRONG),
        (4, ACMGCriterion.PS3, StrengthOfEvidenceProvided.STRONG),
        (3, ACMGCriterion.PS3, StrengthOfEvidenceProvided.MODERATE_PLUS),
        (2, ACMGCriterion.PS3, StrengthOfEvidenceProvided.MODERATE),
        (1, ACMGCriterion.PS3, StrengthOfEvidenceProvided.SUPPORTING),
        (0, None, None),
        (-1, ACMGCriterion.BS3, StrengthOfEvidenceProvided.SUPPORTING),
        (-2, ACMGCriterion.BS3, StrengthOfEvidenceProvided.MODERATE),
        (-3, ACMGCriterion.BS3, StrengthOfEvidenceProvided.MODERATE_PLUS),
        (-4, ACMGCriterion.BS3, StrengthOfEvidenceProvided.STRONG),
        (-5, ACMGCriterion.BS3, StrengthOfEvidenceProvided.STRONG),
        (-7, ACMGCriterion.BS3, StrengthOfEvidenceProvided.STRONG),
        (-8, ACMGCriterion.BS3, StrengthOfEvidenceProvided.VERY_STRONG),
    ],
)
def test_points_mapping(points, expected_criterion, expected_strength):
    criterion, strength = points_evidence_strength_equivalent(points)
    assert criterion == expected_criterion
    assert strength == expected_strength


@pytest.mark.parametrize("invalid_points", [-9, 9, 100, -100])
def test_out_of_points_range_raises(invalid_points):
    with pytest.raises(
        ValueError,
        match="Points value must be between -8 and 8 inclusive",
    ):
        points_evidence_strength_equivalent(invalid_points)


def test_pathogenic_vs_benign_flags():
    for p in range(-8, 9):
        criterion, strength = points_evidence_strength_equivalent(p)
        if p > 0:
            assert criterion is not None
            assert criterion.is_pathogenic
            assert not criterion.is_benign
        elif p < 0:
            assert criterion is not None
            assert criterion.is_benign
            assert not criterion.is_pathogenic
        else:
            assert criterion is None
            assert strength is None


def test_positive_always_ps3_negative_always_bs3():
    positives = [p for p in range(1, 9)]
    negatives = [p for p in range(-8, 0)]
    for p in positives:
        c, _ = points_evidence_strength_equivalent(p)
        assert c == ACMGCriterion.PS3
    for p in negatives:
        c, _ = points_evidence_strength_equivalent(p)
        assert c == ACMGCriterion.BS3


def test_all_strength_categories_covered():
    seen = set()
    for p in range(-8, 9):
        _, strength = points_evidence_strength_equivalent(p)
        if strength:
            seen.add(strength)
    assert StrengthOfEvidenceProvided.VERY_STRONG in seen
    assert StrengthOfEvidenceProvided.STRONG in seen
    assert StrengthOfEvidenceProvided.MODERATE_PLUS in seen
    assert StrengthOfEvidenceProvided.MODERATE in seen
    assert StrengthOfEvidenceProvided.SUPPORTING in seen

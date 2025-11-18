# ruff: noqa: E402

import pytest
from sqlalchemy import select

pytest.importorskip("psycopg2")

from mavedb.lib.acmg import (
    ACMGCriterion,
    StrengthOfEvidenceProvided,
    find_or_create_acmg_classification,
    points_evidence_strength_equivalent,
)
from mavedb.models.acmg_classification import ACMGClassification

###############################################################################
# Tests for points_evidence_strength_equivalent
###############################################################################


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


###############################################################################
# Tests for find_or_create_acmg_classification
###############################################################################


@pytest.mark.parametrize(
    "criterion,evidence_strength,points",
    [
        # Valid combinations
        (ACMGCriterion.PS3, StrengthOfEvidenceProvided.STRONG, 4),
        (ACMGCriterion.BS3, StrengthOfEvidenceProvided.MODERATE, -2),
        (None, None, None),  # Should return None
        (None, None, 5),  # Should derive from points
    ],
)
def test_find_or_create_acmg_classification_validation_does_not_raise_on_valid_combinations(
    session, criterion, evidence_strength, points
):
    """Test input validation for find_or_create_acmg_classification valid values."""
    result = find_or_create_acmg_classification(session, criterion, evidence_strength, points)

    if criterion is None and evidence_strength is None and points is None:
        assert result is None
    else:
        assert result is not None


@pytest.mark.parametrize(
    "criterion,evidence_strength,points",
    [
        # Invalid combinations - only one is None
        (ACMGCriterion.PS3, None, 4),
        (None, StrengthOfEvidenceProvided.STRONG, 4),
    ],
)
def test_find_or_create_acmg_classification_validation_raises_on_invalid_combinations(
    session, criterion, evidence_strength, points
):
    """Test input validation for find_or_create_acmg_classification invalid values."""
    with pytest.raises(
        ValueError,
        match="Both criterion and evidence_strength must be provided together or both be None, with points.",
    ):
        find_or_create_acmg_classification(session, criterion, evidence_strength, points)


def test_find_or_create_acmg_classification_returns_none_for_all_none(session):
    """Test that function returns None when all parameters are None."""

    result = find_or_create_acmg_classification(session, None, None, None)
    assert result is None


def test_find_or_create_acmg_classification_derives_from_points(session):
    """Test that function derives criterion and evidence_strength from points when they are None."""

    result = find_or_create_acmg_classification(session, None, None, 4)

    assert result is not None
    assert result.criterion == ACMGCriterion.PS3
    assert result.evidence_strength == StrengthOfEvidenceProvided.STRONG
    assert result.points == 4


def test_find_or_create_acmg_classification_creates_new_entry(session):
    """Test that function creates a new ACMGClassification when one doesn't exist."""

    # Verify no existing entry
    existing = session.execute(
        select(ACMGClassification)
        .where(ACMGClassification.criterion == ACMGCriterion.PS3)
        .where(ACMGClassification.evidence_strength == StrengthOfEvidenceProvided.MODERATE)
        .where(ACMGClassification.points == 2)
    ).scalar_one_or_none()
    assert existing is None

    result = find_or_create_acmg_classification(session, ACMGCriterion.PS3, StrengthOfEvidenceProvided.MODERATE, 2)

    assert result is not None
    assert result.criterion == ACMGCriterion.PS3
    assert result.evidence_strength == StrengthOfEvidenceProvided.MODERATE
    assert result.points == 2

    # Verify it was added to the session
    session_objects = [obj for obj in session.new if isinstance(obj, ACMGClassification)]
    assert len(session_objects) == 1
    assert session_objects[0] == result


def test_find_or_create_acmg_classification_finds_existing_entry(session):
    """Test that function finds and returns existing ACMGClassification."""

    # Create an existing entry
    existing_classification = ACMGClassification(
        criterion=ACMGCriterion.BS3, evidence_strength=StrengthOfEvidenceProvided.STRONG, points=-5
    )
    session.add(existing_classification)
    session.commit()

    result = find_or_create_acmg_classification(session, ACMGCriterion.BS3, StrengthOfEvidenceProvided.STRONG, -5)

    assert result is not None
    assert result == existing_classification
    assert result.criterion == ACMGCriterion.BS3
    assert result.evidence_strength == StrengthOfEvidenceProvided.STRONG
    assert result.points == -5

    # Verify no new objects were added to the session
    assert len(session.new) == 0


def test_find_or_create_acmg_classification_with_zero_points(session):
    """Test function behavior with zero points."""

    result = find_or_create_acmg_classification(session, None, None, 0)
    assert result is None


@pytest.mark.parametrize("points", [-8, -4, -1, 1, 3, 8])
def test_find_or_create_acmg_classification_points_integration(session, points):
    """Test that function works correctly with various point values."""

    result = find_or_create_acmg_classification(session, None, None, points)

    expected_criterion, expected_strength = points_evidence_strength_equivalent(points)

    assert result is not None
    assert result.criterion == expected_criterion
    assert result.evidence_strength == expected_strength
    assert result.points == points


def test_find_or_create_acmg_classification_does_not_commit(session):
    """Test that function does not commit the session."""

    find_or_create_acmg_classification(session, ACMGCriterion.PS3, StrengthOfEvidenceProvided.SUPPORTING, 1)

    # Rollback the session
    session.rollback()

    # Verify the object is no longer in the database
    existing = session.execute(
        select(ACMGClassification)
        .where(ACMGClassification.criterion == ACMGCriterion.PS3)
        .where(ACMGClassification.evidence_strength == StrengthOfEvidenceProvided.SUPPORTING)
        .where(ACMGClassification.points == 1)
    ).scalar_one_or_none()

    assert existing is None

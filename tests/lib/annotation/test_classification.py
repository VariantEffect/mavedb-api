import pytest
from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

from mavedb.lib.annotation.classification import (
    ExperimentalVariantFunctionalImpactClassification,
    functional_classification_of_variant,
    pathogenicity_classification_of_variant,
)


@pytest.mark.parametrize(
    "score,expected_classification",
    [
        (
            50000,
            ExperimentalVariantFunctionalImpactClassification.INDETERMINATE,
        ),
        (
            0,
            ExperimentalVariantFunctionalImpactClassification.INDETERMINATE,
        ),
        (
            2,
            ExperimentalVariantFunctionalImpactClassification.NORMAL,
        ),
        (
            -2,
            ExperimentalVariantFunctionalImpactClassification.ABNORMAL,
        ),
    ],
)
def test_functional_classification_of_variant_with_ranges(
    mock_mapped_variant_with_functional_calibration_score_set, score, expected_classification
):
    mock_mapped_variant_with_functional_calibration_score_set.variant.data["score_data"]["score"] = score

    result = functional_classification_of_variant(mock_mapped_variant_with_functional_calibration_score_set)
    assert result == expected_classification


def test_functional_classification_of_variant_without_ranges(mock_mapped_variant):
    with pytest.raises(ValueError) as exc:
        functional_classification_of_variant(mock_mapped_variant)

    assert f"Variant {mock_mapped_variant.variant.urn} does not have a score set with score calibrations" in str(
        exc.value
    )


def test_functional_classification_of_variant_without_score(mock_mapped_variant_with_functional_calibration_score_set):
    mock_mapped_variant_with_functional_calibration_score_set.variant.data["score_data"]["score"] = None

    with pytest.raises(ValueError) as exc:
        functional_classification_of_variant(mock_mapped_variant_with_functional_calibration_score_set)

    assert (
        f"Variant {mock_mapped_variant_with_functional_calibration_score_set.variant.urn} does not have a functional score"
        in str(exc.value)
    )


def test_functional_classification_of_variant_without_primary_calibration(
    mock_mapped_variant_with_functional_calibration_score_set,
):
    for cal in mock_mapped_variant_with_functional_calibration_score_set.variant.score_set.score_calibrations:
        cal.primary = False

    with pytest.raises(ValueError) as exc:
        functional_classification_of_variant(mock_mapped_variant_with_functional_calibration_score_set)

    assert (
        f"Variant {mock_mapped_variant_with_functional_calibration_score_set.variant.urn} does not have a primary score calibration"
        in str(exc.value)
    )


def test_functional_classification_of_variant_without_ranges_in_primary_calibration(
    mock_mapped_variant_with_functional_calibration_score_set,
):
    primary_cal = next(
        (
            c
            for c in mock_mapped_variant_with_functional_calibration_score_set.variant.score_set.score_calibrations
            if c.primary
        ),
        None,
    )
    assert primary_cal is not None
    primary_cal.functional_ranges = None

    with pytest.raises(ValueError) as exc:
        functional_classification_of_variant(mock_mapped_variant_with_functional_calibration_score_set)

    assert (
        f"Variant {mock_mapped_variant_with_functional_calibration_score_set.variant.urn} does not have ranges defined in its primary score calibration"
        in str(exc.value)
    )


@pytest.mark.parametrize(
    "score,expected_classification,expected_strength_of_evidence",
    [
        (0, VariantPathogenicityEvidenceLine.Criterion.PS3, None),
        (-2, VariantPathogenicityEvidenceLine.Criterion.PS3, StrengthOfEvidenceProvided.STRONG),
        (2, VariantPathogenicityEvidenceLine.Criterion.BS3, StrengthOfEvidenceProvided.STRONG),
    ],
)
def test_pathogenicity_classification_of_variant_with_thresholds(
    score,
    mock_mapped_variant_with_pathogenicity_calibration_score_set,
    expected_classification,
    expected_strength_of_evidence,
):
    mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.data["score_data"]["score"] = score

    classification, strength = pathogenicity_classification_of_variant(
        mock_mapped_variant_with_pathogenicity_calibration_score_set
    )
    assert classification == expected_classification
    assert strength == expected_strength_of_evidence


def test_pathogenicity_classification_of_variant_without_thresholds(mock_mapped_variant):
    with pytest.raises(ValueError) as exc:
        pathogenicity_classification_of_variant(mock_mapped_variant)

    assert f"Variant {mock_mapped_variant.variant.urn} does not have a score set with score calibrations" in str(
        exc.value
    )


def test_pathogenicity_classification_of_variant_without_score(
    mock_mapped_variant_with_pathogenicity_calibration_score_set,
):
    mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.data["score_data"]["score"] = None

    with pytest.raises(ValueError) as exc:
        pathogenicity_classification_of_variant(mock_mapped_variant_with_pathogenicity_calibration_score_set)

    assert (
        f"Variant {mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.urn} does not have a functional score"
        in str(exc.value)
    )


def test_pathogenicity_classification_of_variant_without_primary_calibration(
    mock_mapped_variant_with_pathogenicity_calibration_score_set,
):
    for cal in mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations:
        cal.primary = False

    with pytest.raises(ValueError) as exc:
        pathogenicity_classification_of_variant(mock_mapped_variant_with_pathogenicity_calibration_score_set)

    assert (
        f"Variant {mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.urn} does not have a primary score calibration"
        in str(exc.value)
    )


def test_pathogenicity_classification_of_variant_without_ranges_in_primary_calibration(
    mock_mapped_variant_with_pathogenicity_calibration_score_set,
):
    primary_cal = next(
        (
            c
            for c in mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations
            if c.primary
        ),
        None,
    )
    assert primary_cal is not None
    primary_cal.functional_ranges = None

    with pytest.raises(ValueError) as exc:
        pathogenicity_classification_of_variant(mock_mapped_variant_with_pathogenicity_calibration_score_set)

    assert (
        f"Variant {mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.urn} does not have ranges defined in its primary score calibration"
        in str(exc.value)
    )


def test_pathogenicity_classification_of_variant_without_acmg_classification_in_ranges(
    mock_mapped_variant_with_pathogenicity_calibration_score_set,
):
    primary_cal = next(
        (
            c
            for c in mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations
            if c.primary
        ),
        None,
    )
    assert primary_cal is not None
    for r in primary_cal.functional_ranges:
        r["acmgClassification"] = None

    criterion, strength = pathogenicity_classification_of_variant(
        mock_mapped_variant_with_pathogenicity_calibration_score_set
    )

    assert criterion == VariantPathogenicityEvidenceLine.Criterion.PS3
    assert strength is None


def test_pathogenicity_classification_of_variant_with_invalid_evidence_strength_in_acmg_classification(
    mock_mapped_variant_with_pathogenicity_calibration_score_set,
):
    primary_cal = next(
        (
            c
            for c in mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations
            if c.primary
        ),
        None,
    )
    assert primary_cal is not None
    for r in primary_cal.functional_ranges:
        r["acmgClassification"]["evidenceStrength"] = "MODERATE_PLUS"
        r["oddspathsRatio"] = None

    with pytest.raises(ValueError) as exc:
        pathogenicity_classification_of_variant(mock_mapped_variant_with_pathogenicity_calibration_score_set)

    assert (
        f"Variant {mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.urn} is contained in a clinical calibration range with an invalid evidence strength"
        in str(exc.value)
    )

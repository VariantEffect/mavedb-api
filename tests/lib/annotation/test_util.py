from copy import deepcopy
from unittest.mock import patch

import pytest

from mavedb.lib.annotation.exceptions import MappingDataDoesntExistException
from mavedb.lib.annotation.util import (
    _can_annotate_variant_base_assumptions,
    _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation,
    can_annotate_variant_for_functional_statement,
    can_annotate_variant_for_pathogenicity_evidence,
    variation_from_mapped_variant,
)
from tests.helpers.constants import TEST_SEQUENCE_LOCATION_ACCESSION, TEST_VALID_POST_MAPPED_VRS_ALLELE


@pytest.mark.parametrize(
    "variation_version", [{"variation": TEST_VALID_POST_MAPPED_VRS_ALLELE}, TEST_VALID_POST_MAPPED_VRS_ALLELE]
)
def test_variation_from_mapped_variant_post_mapped_variation(mock_mapped_variant, variation_version):
    mock_mapped_variant.post_mapped = variation_version

    result = variation_from_mapped_variant(mock_mapped_variant).model_dump()

    assert result["location"]["id"] == TEST_SEQUENCE_LOCATION_ACCESSION
    assert result["location"]["start"] == 5
    assert result["location"]["end"] == 6


def test_variation_from_mapped_variant_no_post_mapped(mock_mapped_variant):
    mock_mapped_variant.post_mapped = None

    with pytest.raises(MappingDataDoesntExistException):
        variation_from_mapped_variant(mock_mapped_variant)


## Test base annotation assumptions


def test_base_assumption_check_returns_false_when_score_is_none(mock_mapped_variant):
    mock_mapped_variant.variant.data = {"score_data": {"score": None}}

    assert _can_annotate_variant_base_assumptions(mock_mapped_variant) is False


def test_base_assumption_check_returns_true_when_all_conditions_met(mock_mapped_variant):
    assert _can_annotate_variant_base_assumptions(mock_mapped_variant) is True


## Test variant score ranges have required keys for annotation


@pytest.mark.parametrize("kind", ["functional", "pathogenicity"])
def test_score_range_check_returns_false_when_no_calibrations_present(mock_mapped_variant, kind):
    assert (
        _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(mock_mapped_variant, kind)
        is False
    )


@pytest.mark.parametrize(
    "kind,variant_fixture",
    [
        ("functional", "mock_mapped_variant_with_functional_calibration_score_set"),
        ("pathogenicity", "mock_mapped_variant_with_pathogenicity_calibration_score_set"),
    ],
)
def test_score_range_check_returns_false_when_no_primary_calibration(kind, variant_fixture, request):
    mock_mapped_variant = request.getfixturevalue(variant_fixture)
    for calibration in mock_mapped_variant.variant.score_set.score_calibrations:
        calibration.primary = False

    assert (
        _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(mock_mapped_variant, kind)
        is False
    )


@pytest.mark.parametrize(
    "kind,variant_fixture",
    [
        ("functional", "mock_mapped_variant_with_functional_calibration_score_set"),
        ("pathogenicity", "mock_mapped_variant_with_pathogenicity_calibration_score_set"),
    ],
)
def test_score_range_check_returns_false_when_calibrations_present_with_empty_ranges(kind, variant_fixture, request):
    mock_mapped_variant = request.getfixturevalue(variant_fixture)

    for calibration in mock_mapped_variant.variant.score_set.score_calibrations:
        calibration.functional_classifications = None

    assert (
        _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(mock_mapped_variant, kind)
        is False
    )


def test_pathogenicity_range_check_returns_false_when_no_acmg_calibration(
    mock_mapped_variant_with_pathogenicity_calibration_score_set,
):
    for (
        calibration
    ) in mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations:
        acmg_classification_removed = [deepcopy(r) for r in calibration.functional_classifications]
        for fr in acmg_classification_removed:
            fr["acmgClassification"] = None

        calibration.functional_classifications = acmg_classification_removed

    assert (
        _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
            mock_mapped_variant_with_pathogenicity_calibration_score_set, "pathogenicity"
        )
        is False
    )


def test_pathogenicity_range_check_returns_true_when_some_acmg_calibration(
    mock_mapped_variant_with_pathogenicity_calibration_score_set,
):
    for (
        calibration
    ) in mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations:
        acmg_classification_removed = [deepcopy(r) for r in calibration.functional_classifications]
        acmg_classification_removed[0]["acmgClassification"] = None

        calibration.functional_classifications = acmg_classification_removed

    assert (
        _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
            mock_mapped_variant_with_pathogenicity_calibration_score_set, "pathogenicity"
        )
        is True
    )


@pytest.mark.parametrize(
    "kind,variant_fixture",
    [
        ("functional", "mock_mapped_variant_with_functional_calibration_score_set"),
        ("pathogenicity", "mock_mapped_variant_with_pathogenicity_calibration_score_set"),
    ],
)
def test_score_range_check_returns_true_when_calibration_kind_exists_with_ranges(kind, variant_fixture, request):
    mock_mapped_variant = request.getfixturevalue(variant_fixture)

    assert (
        _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(mock_mapped_variant, kind)
        is True
    )


## Test clinical range check


def test_pathogenicity_range_check_returns_false_when_base_assumptions_fail(mock_mapped_variant):
    with patch("mavedb.lib.annotation.util._can_annotate_variant_base_assumptions", return_value=False):
        result = can_annotate_variant_for_pathogenicity_evidence(mock_mapped_variant)

    assert result is False


def test_pathogenicity_range_check_returns_false_when_pathogenicity_ranges_check_fails(mock_mapped_variant):
    with patch(
        "mavedb.lib.annotation.util._variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation",
        return_value=False,
    ):
        result = can_annotate_variant_for_pathogenicity_evidence(mock_mapped_variant)

    assert result is False


# The default mock_mapped_variant object should be valid
def test_pathogenicity_range_check_returns_true_when_all_conditions_met(
    mock_mapped_variant_with_pathogenicity_calibration_score_set,
):
    assert (
        can_annotate_variant_for_pathogenicity_evidence(mock_mapped_variant_with_pathogenicity_calibration_score_set)
        is True
    )


## Test functional range check


def test_functional_range_check_returns_false_when_base_assumptions_fail(mock_mapped_variant):
    with patch(
        "mavedb.lib.annotation.util._can_annotate_variant_base_assumptions",
        return_value=False,
    ):
        result = can_annotate_variant_for_functional_statement(mock_mapped_variant)

    assert result is False


def test_functional_range_check_returns_false_when_functional_classifications_check_fails(mock_mapped_variant):
    with patch(
        "mavedb.lib.annotation.util._variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation",
        return_value=False,
    ):
        result = can_annotate_variant_for_functional_statement(mock_mapped_variant)

    assert result is False


# The default mock_mapped_variant object should be valid
def test_functional_range_check_returns_true_when_all_conditions_met(
    mock_mapped_variant_with_functional_calibration_score_set,
):
    assert (
        can_annotate_variant_for_functional_statement(mock_mapped_variant_with_functional_calibration_score_set) is True
    )

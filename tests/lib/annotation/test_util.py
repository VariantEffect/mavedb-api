import pytest

from mavedb.lib.annotation.exceptions import MappingDataDoesntExistException
from mavedb.lib.annotation.util import (
    variation_from_mapped_variant,
    _can_annotate_variant_base_assumptions,
    _variant_score_ranges_have_required_keys_for_annotation,
    can_annotate_variant_for_functional_statement,
    can_annotate_variant_for_pathogenicity_evidence,
)

from tests.helpers.constants import TEST_VALID_POST_MAPPED_VRS_ALLELE, TEST_SEQUENCE_LOCATION_ACCESSION
from unittest.mock import patch


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


def test_score_range_check_returns_false_when_keys_are_none(mock_mapped_variant):
    mock_mapped_variant.variant.score_set.score_ranges = None
    key_options = ["required_key1", "required_key2"]

    assert _variant_score_ranges_have_required_keys_for_annotation(mock_mapped_variant, key_options) is False


def test_score_range_check_returns_false_when_no_keys_present(mock_mapped_variant):
    mock_mapped_variant.variant.score_set.score_ranges = {"other_key": "value"}
    key_options = ["required_key1", "required_key2"]

    assert _variant_score_ranges_have_required_keys_for_annotation(mock_mapped_variant, key_options) is False


def test_score_range_check_returns_false_when_key_present_but_value_is_none(mock_mapped_variant):
    mock_mapped_variant.variant.score_set.score_ranges = {"required_key1": None}
    key_options = ["required_key1", "required_key2"]

    assert _variant_score_ranges_have_required_keys_for_annotation(mock_mapped_variant, key_options) is False


def test_score_range_check_returns_none_when_at_least_one_key_has_value(mock_mapped_variant):
    mock_mapped_variant.variant.score_set.score_ranges = {"required_key1": "value"}
    key_options = ["required_key1", "required_key2"]

    assert _variant_score_ranges_have_required_keys_for_annotation(mock_mapped_variant, key_options) is True


## Test clinical range check


def test_clinical_range_check_returns_false_when_base_assumptions_fail(mock_mapped_variant):
    mock_mapped_variant.variant.score_set.score_ranges = None
    result = can_annotate_variant_for_pathogenicity_evidence(mock_mapped_variant)

    assert result is False


@pytest.mark.parametrize("clinical_ranges", [["clinical_range"], ["other_clinical_range"]])
def test_clinical_range_check_returns_false_when_clinical_ranges_check_fails(mock_mapped_variant, clinical_ranges):
    mock_mapped_variant.variant.score_set.score_ranges = {"unrelated_key": "value"}

    with patch("mavedb.lib.annotation.util.CLINICAL_RANGES", clinical_ranges):
        result = can_annotate_variant_for_pathogenicity_evidence(mock_mapped_variant)

    assert result is False


# The default mock_mapped_variant object should be valid
def test_clinical_range_check_returns_true_when_all_conditions_met(mock_mapped_variant):
    assert can_annotate_variant_for_pathogenicity_evidence(mock_mapped_variant) is True


## Test functional range check


def test_functional_range_check_returns_false_when_base_assumptions_fail(mock_mapped_variant):
    mock_mapped_variant.variant.score_set.score_ranges = None
    result = can_annotate_variant_for_functional_statement(mock_mapped_variant)

    assert result is False


@pytest.mark.parametrize("functional_ranges", [["functional_range"], ["other_functional_range"]])
def test_functional_range_check_returns_false_when_functional_ranges_check_fails(
    mock_mapped_variant, functional_ranges
):
    mock_mapped_variant.variant.score_set.score_ranges = {"unrelated_key": "value"}

    with patch("mavedb.lib.annotation.util.FUNCTIONAL_RANGES", functional_ranges):
        result = can_annotate_variant_for_functional_statement(mock_mapped_variant)

    assert result is False


# The default mock_mapped_variant object should be valid
def test_functional_range_check_returns_true_when_all_conditions_met(mock_mapped_variant):
    assert can_annotate_variant_for_functional_statement(mock_mapped_variant) is True

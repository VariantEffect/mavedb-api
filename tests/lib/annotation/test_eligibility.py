# ruff: noqa: E402

"""Tests for mavedb.lib.annotation.eligibility — variant annotatability predicates."""

from unittest.mock import patch

import pytest

pytest.importorskip("psycopg2")

from mavedb.lib.annotation.eligibility import (
    _can_annotate_variant_base_assumptions,
    can_annotate_variant_for_functional_statement,
    can_annotate_variant_for_pathogenicity_evidence,
)
from tests.helpers.constants import TEST_VALID_POST_MAPPED_VRS_ALLELE
from tests.lib.annotation.conftest import admin_principal, annotation_context_for, make_private


@pytest.mark.unit
class TestBaseAnnotationAssumptionsUnit:
    def test_base_assumption_check_returns_false_when_score_is_none(self, mock_annotation_context):
        mock_annotation_context.variant.data = {"score_data": {"score": None}}

        assert _can_annotate_variant_base_assumptions(mock_annotation_context) is False

    def test_base_assumption_check_returns_true_when_all_conditions_met(self, mock_annotation_context):
        assert _can_annotate_variant_base_assumptions(mock_annotation_context) is True


@pytest.mark.unit
class TestPathogenicityAnnotationEligibilityUnit:
    def test_pathogenicity_range_check_returns_false_when_base_assumptions_fail(self, mock_annotation_context):
        with patch("mavedb.lib.annotation.eligibility._can_annotate_variant_base_assumptions", return_value=False):
            result = can_annotate_variant_for_pathogenicity_evidence(mock_annotation_context)

        assert result is False

    def test_pathogenicity_range_check_returns_false_when_no_calibrations_available(self, mock_annotation_context):
        with patch(
            "mavedb.lib.annotation.eligibility.calibrations_available_for_annotation",
            return_value=[],
        ):
            result = can_annotate_variant_for_pathogenicity_evidence(mock_annotation_context)

        assert result is False

    def test_pathogenicity_range_check_returns_true_when_all_conditions_met(
        self, mock_annotation_context_with_pathogenicity_calibration_score_set
    ):
        assert (
            can_annotate_variant_for_pathogenicity_evidence(
                mock_annotation_context_with_pathogenicity_calibration_score_set
            )
            is True
        )

    def test_a_private_only_calibration_is_ineligible_for_an_anonymous_caller(
        self,
        mock_mapped_variant_with_pathogenicity_calibration_score_set,
        mock_annotation_context_with_pathogenicity_calibration_score_set,
    ):
        make_private(mock_mapped_variant_with_pathogenicity_calibration_score_set)

        assert (
            can_annotate_variant_for_pathogenicity_evidence(
                mock_annotation_context_with_pathogenicity_calibration_score_set
            )
            is False
        )

    def test_a_private_only_calibration_is_eligible_for_an_entitled_caller(
        self,
        mock_mapped_variant_with_pathogenicity_calibration_score_set,
        mock_annotation_context_with_pathogenicity_calibration_score_set,
    ):
        make_private(mock_mapped_variant_with_pathogenicity_calibration_score_set)

        assert (
            can_annotate_variant_for_pathogenicity_evidence(
                mock_annotation_context_with_pathogenicity_calibration_score_set, principal=admin_principal()
            )
            is True
        )


@pytest.mark.unit
class TestFunctionalAnnotationEligibilityUnit:
    def test_functional_range_check_returns_false_when_base_assumptions_fail(self, mock_annotation_context):
        with patch(
            "mavedb.lib.annotation.eligibility._can_annotate_variant_base_assumptions",
            return_value=False,
        ):
            result = can_annotate_variant_for_functional_statement(mock_annotation_context)

        assert result is False

    def test_functional_range_check_returns_false_when_no_calibrations_available(self, mock_annotation_context):
        with patch(
            "mavedb.lib.annotation.eligibility.calibrations_available_for_annotation",
            return_value=[],
        ):
            result = can_annotate_variant_for_functional_statement(mock_annotation_context)

        assert result is False

    def test_functional_range_check_returns_true_when_all_conditions_met(
        self, mock_annotation_context_with_functional_calibration_score_set
    ):
        assert (
            can_annotate_variant_for_functional_statement(mock_annotation_context_with_functional_calibration_score_set)
            is True
        )

    def test_a_private_only_calibration_is_ineligible_for_an_anonymous_caller(
        self,
        mock_mapped_variant_with_functional_calibration_score_set,
        mock_annotation_context_with_functional_calibration_score_set,
    ):
        make_private(mock_mapped_variant_with_functional_calibration_score_set)

        assert (
            can_annotate_variant_for_functional_statement(mock_annotation_context_with_functional_calibration_score_set)
            is False
        )

    def test_a_private_only_calibration_is_eligible_for_an_entitled_caller(
        self,
        mock_mapped_variant_with_functional_calibration_score_set,
        mock_annotation_context_with_functional_calibration_score_set,
    ):
        make_private(mock_mapped_variant_with_functional_calibration_score_set)

        assert (
            can_annotate_variant_for_functional_statement(
                mock_annotation_context_with_functional_calibration_score_set, principal=admin_principal()
            )
            is True
        )


@pytest.mark.integration
class TestAnnotationEligibilityIntegration:
    def test_annotation_eligibility_returns_boolean_for_persisted_variant(self, setup_lib_db_with_mapped_variant):
        # Make score presence explicit so a negative result is due to missing calibrations, and give the
        # context a real VRS allele to build its subject variant from.
        setup_lib_db_with_mapped_variant.variant.data = {"score_data": {"score": 1.0}}
        setup_lib_db_with_mapped_variant.post_mapped = TEST_VALID_POST_MAPPED_VRS_ALLELE
        context = annotation_context_for(setup_lib_db_with_mapped_variant)

        pathogenicity_allowed = can_annotate_variant_for_pathogenicity_evidence(context)
        functional_allowed = can_annotate_variant_for_functional_statement(context)

        # DB fixture score sets do not include calibrations by default, so both should be False.
        assert setup_lib_db_with_mapped_variant.variant.score_set.score_calibrations == []
        assert pathogenicity_allowed is False
        assert functional_allowed is False

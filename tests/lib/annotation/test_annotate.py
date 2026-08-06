"""
Tests for mavedb.lib.annotation.annotate module.

This module tests the main annotation functions that create statements and study results
for variants, focusing on object structure and validation.
"""

# ruff: noqa: E402

from copy import deepcopy

import pytest

pytest.importorskip("psycopg2")

from mavedb.lib.annotation.annotate import (
    variant_functional_impact_statement,
    variant_highest_level_annotation,
    variant_pathogenicity_statement,
    variant_study_result,
)
from mavedb.lib.annotation.util import CALIBRATION_SCOPE_EXTENSION_NAME
from tests.lib.annotation.conftest import admin_principal, make_private, owner_principal


def scope_of(annotation) -> str:
    """The disclosed principal of an annotation, which every emitted object must carry."""
    scopes = [
        extension.value
        for extension in (annotation.extensions or [])
        if extension.name == CALIBRATION_SCOPE_EXTENSION_NAME
    ]
    assert len(scopes) == 1, f"expected exactly one calibration scope extension, found {scopes}"
    return scopes[0]


@pytest.mark.unit
class TestVariantStudyResult:
    """Unit tests for variant study result creation."""

    def test_variant_study_result_creates_valid_result(self, mock_mapped_variant):
        """Test that variant study result creates a valid result object."""
        result = variant_study_result(mock_mapped_variant)

        assert result is not None
        assert result.type == "ExperimentalVariantFunctionalImpactStudyResult"

    def test_a_study_result_discloses_a_calibration_scope(self, mock_mapped_variant):
        # Emitted unconditionally so that a record with no scope is never ambiguous between "public" and
        # "produced before disclosure existed".
        assert scope_of(variant_study_result(mock_mapped_variant)) == "public"


@pytest.mark.unit
class TestVariantFunctionalImpactStatement:
    """Unit tests for variant functional impact statement creation."""

    def test_no_calibrations_returns_none(self, mock_mapped_variant):
        """Test that statement returns None when no calibrations exist."""
        result = variant_functional_impact_statement(mock_mapped_variant)

        assert result is None

    def test_only_research_use_only_calibrations_returns_none(
        self, mock_mapped_variant_with_functional_calibration_score_set
    ):
        """Test that statement returns None when only research use only primary calibrations exist."""
        # Set all calibrations to research use only
        for (
            calibration
        ) in mock_mapped_variant_with_functional_calibration_score_set.variant.score_set.score_calibrations:
            calibration.research_use_only = True

        result = variant_functional_impact_statement(mock_mapped_variant_with_functional_calibration_score_set)
        assert result is None

    def test_no_score_returns_none(self, mock_mapped_variant_with_functional_calibration_score_set):
        """Test that statement returns None when variant has no score."""
        mock_mapped_variant_with_functional_calibration_score_set.variant.data = {"score_data": {"score": None}}
        result = variant_functional_impact_statement(mock_mapped_variant_with_functional_calibration_score_set)

        assert result is None

    def test_valid_statement_creation(self, mock_mapped_variant_with_functional_calibration_score_set):
        """Test creating valid functional impact statement with proper structure."""
        result = variant_functional_impact_statement(mock_mapped_variant_with_functional_calibration_score_set)

        assert result is not None
        assert result.type == "Statement"
        assert all(evidence_item.type == "EvidenceLine" for evidence_item in result.hasEvidenceLines)
        assert all(
            study_result.root.type == "ExperimentalVariantFunctionalImpactStudyResult"
            for evidence_line in [evidence_line for evidence_line in result.hasEvidenceLines]
            for study_result in evidence_line.hasEvidenceItems
        )

    def test_skips_research_use_only_calibrations_when_mixed(
        self, mock_mapped_variant_with_functional_calibration_score_set
    ):
        """Test that research-use-only calibrations are skipped when mixed with regular calibrations."""
        calibrations = mock_mapped_variant_with_functional_calibration_score_set.variant.score_set.score_calibrations
        mixed_calibrations = [deepcopy(calibrations[0]), deepcopy(calibrations[0])]
        mixed_calibrations[0].research_use_only = True
        mixed_calibrations[1].research_use_only = False
        mock_mapped_variant_with_functional_calibration_score_set.variant.score_set.score_calibrations = (
            mixed_calibrations
        )

        result = variant_functional_impact_statement(mock_mapped_variant_with_functional_calibration_score_set)

        assert result is not None
        assert len(result.hasEvidenceLines) == 1

    def test_variant_not_in_any_range_returns_indeterminate(
        self, mock_mapped_variant_with_functional_calibration_score_set
    ):
        """Test that variant not in any functional range gets INDETERMINATE classification."""
        from unittest.mock import patch

        from mavedb.lib.annotation.classification import ExperimentalVariantFunctionalImpactClassification

        mapped_variant = mock_mapped_variant_with_functional_calibration_score_set

        # Mock functional_classification_of_variant to return None range (variant not in any range)
        with patch(
            "mavedb.lib.annotation.annotate.functional_classification_of_variant",
            return_value=(None, ExperimentalVariantFunctionalImpactClassification.INDETERMINATE),
        ):
            result = variant_functional_impact_statement(mapped_variant)

            assert result is not None
            assert result.type == "Statement"
            # Classification should be INDETERMINATE
            assert result.classification.primaryCoding.code.root == "indeterminate"

    def test_no_statement_is_built_from_a_private_calibration(
        self, mock_mapped_variant_with_functional_calibration_score_set
    ):
        """A private calibration's thresholds and baseline scores must not reach an anonymous caller."""
        mapped_variant = make_private(mock_mapped_variant_with_functional_calibration_score_set)

        assert variant_functional_impact_statement(mapped_variant) is None

    def test_an_entitled_caller_receives_a_statement_from_a_private_calibration(
        self, mock_mapped_variant_with_functional_calibration_score_set
    ):
        """Viewer-scoped emission: an export shows each principal what that principal may see."""
        mapped_variant = make_private(mock_mapped_variant_with_functional_calibration_score_set)

        assert variant_functional_impact_statement(mapped_variant, principal=admin_principal()) is not None

    def test_a_public_statement_discloses_a_public_calibration_scope(
        self, mock_mapped_variant_with_functional_calibration_score_set
    ):
        # VA-Spec statements carry no stable id, so a viewer-scoped statement must say that it is one.
        statement = variant_functional_impact_statement(mock_mapped_variant_with_functional_calibration_score_set)

        assert scope_of(statement) == "public"

    def test_a_statement_widened_by_entitlement_discloses_a_restricted_scope(
        self, mock_mapped_variant_with_functional_calibration_score_set
    ):
        mapped_variant = make_private(mock_mapped_variant_with_functional_calibration_score_set)

        statement = variant_functional_impact_statement(mapped_variant, principal=admin_principal())

        assert scope_of(statement) == "restricted"


@pytest.mark.unit
class TestVariantPathogenicityStatement:
    """Unit tests for variant pathogenicity statement creation."""

    def test_no_calibrations_returns_none(self, mock_mapped_variant):
        """Test that statement returns None when no calibrations exist."""
        result = variant_pathogenicity_statement(mock_mapped_variant)

        assert result is None

    def test_no_score_returns_none(self, mock_mapped_variant_with_pathogenicity_calibration_score_set):
        """Test that statement returns None when variant has no score."""
        mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.data = {"score_data": {"score": None}}
        result = variant_pathogenicity_statement(mock_mapped_variant_with_pathogenicity_calibration_score_set)

        assert result is None

    def test_only_research_use_only_calibration_returns_none(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        """Test that statement returns None when only research use only primary calibrations exist."""
        # Set all calibrations to research use only
        for (
            calibration
        ) in mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations:
            calibration.research_use_only = True

        result = variant_pathogenicity_statement(mock_mapped_variant_with_pathogenicity_calibration_score_set)
        assert result is None

    def test_no_acmg_classifications_returns_none(self, mock_mapped_variant_with_pathogenicity_calibration_score_set):
        """Test that statement returns None when no ACMG classifications exist."""
        # Remove ACMG classifications from all calibrations
        for (
            calibration
        ) in mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations:
            acmg_removed = [deepcopy(r) for r in calibration.functional_classifications]
            for functional_classification in acmg_removed:
                functional_classification["acmgClassification"] = None
            calibration.functional_classifications = acmg_removed

        result = variant_pathogenicity_statement(mock_mapped_variant_with_pathogenicity_calibration_score_set)
        assert result is None

    def test_valid_pathogenicity_statement_creation(self, mock_mapped_variant_with_pathogenicity_calibration_score_set):
        """Test creating valid pathogenicity statement with proper structure."""
        result = variant_pathogenicity_statement(mock_mapped_variant_with_pathogenicity_calibration_score_set)

        assert result is not None
        assert result.proposition.type == "VariantPathogenicityProposition"
        assert result.type == "Statement"

        pathogenicity_evidence_lines = [evidence_item for evidence_item in result.hasEvidenceLines]
        statements = [
            statement for evidence_item in pathogenicity_evidence_lines for statement in evidence_item.hasEvidenceItems
        ]
        functional_evidence_lines = [
            evidence_item for statement in statements for evidence_item in statement.hasEvidenceLines
        ]

        assert all(ei.type == "EvidenceLine" for ei in pathogenicity_evidence_lines)
        assert all(s.type == "Statement" for s in statements)
        assert all(ei.type == "EvidenceLine" for ei in functional_evidence_lines)
        assert all(
            study_result.root.type == "ExperimentalVariantFunctionalImpactStudyResult"
            for evidence_item in functional_evidence_lines
            for study_result in evidence_item.hasEvidenceItems
        )

    def test_skips_research_use_only_calibrations_when_mixed(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        """Test that research-use-only pathogenicity calibrations are skipped when mixed with regular calibrations."""
        calibrations = mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations
        mixed_calibrations = [deepcopy(calibrations[0]), deepcopy(calibrations[0])]
        mixed_calibrations[0].research_use_only = True
        mixed_calibrations[1].research_use_only = False
        mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations = (
            mixed_calibrations
        )

        result = variant_pathogenicity_statement(mock_mapped_variant_with_pathogenicity_calibration_score_set)

        assert result is not None
        assert len(result.hasEvidenceLines) == 1

    def test_skips_invalid_calibrations_when_functional_annotation(
        self, mock_mapped_variant_with_functional_calibration_score_set
    ):
        """Test that functional annotation skips calibrations invalid under score_calibration_may_be_used_for_annotation."""
        calibrations = mock_mapped_variant_with_functional_calibration_score_set.variant.score_set.score_calibrations
        mixed_calibrations = [deepcopy(calibrations[0]), deepcopy(calibrations[0])]

        # Invalid: no functional classifications
        mixed_calibrations[0].functional_classifications = []
        # Valid: retain default functional classifications
        mixed_calibrations[1].functional_classifications = deepcopy(calibrations[0].functional_classifications)

        mock_mapped_variant_with_functional_calibration_score_set.variant.score_set.score_calibrations = (
            mixed_calibrations
        )

        result = variant_functional_impact_statement(mock_mapped_variant_with_functional_calibration_score_set)

        assert result is not None
        assert len(result.hasEvidenceLines) == 1

    def test_skips_invalid_calibrations_when_pathogenicity_annotation(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        """Test that pathogenicity annotation skips calibrations invalid under score_calibration_may_be_used_for_annotation."""
        calibrations = mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations
        mixed_calibrations = [deepcopy(calibrations[0]), deepcopy(calibrations[0])]

        # Invalid calibration: no functional classifications
        mixed_calibrations[0].functional_classifications = []

        # Valid calibration retained
        mixed_calibrations[1].functional_classifications = deepcopy(calibrations[0].functional_classifications)

        mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations = (
            mixed_calibrations
        )

        result = variant_pathogenicity_statement(mock_mapped_variant_with_pathogenicity_calibration_score_set)

        assert result is not None
        assert len(result.hasEvidenceLines) == 1

    def test_variant_not_in_any_range_returns_uncertain_significance(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        """Test that variant not in any range gets UNCERTAIN_SIGNIFICANCE classification."""
        from unittest.mock import patch

        from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine

        from mavedb.lib.annotation.classification import ExperimentalVariantFunctionalImpactClassification

        mapped_variant = mock_mapped_variant_with_pathogenicity_calibration_score_set

        # Mock both classification functions to return None range (variant not in any range)
        with (
            patch(
                "mavedb.lib.annotation.annotate.functional_classification_of_variant",
                return_value=(None, ExperimentalVariantFunctionalImpactClassification.INDETERMINATE),
            ),
            patch(
                "mavedb.lib.annotation.util.pathogenicity_classification_of_variant",
                return_value=(None, VariantPathogenicityEvidenceLine.Criterion.PS3, None),
            ),
        ):
            result = variant_pathogenicity_statement(mapped_variant)

            assert result is not None
            assert result.type == "Statement"
            # Classification should be UNCERTAIN_SIGNIFICANCE
            assert result.classification.primaryCoding.code.root == "uncertain significance"

    def test_pathogenicity_evidence_line_has_evidence_items_are_statement_instances(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        """Regression test: hasEvidenceItems on VariantPathogenicityEvidenceLine must be model instances.

        Passing serialized dict representations of Statement objects to hasEvidenceItems caused
        VariantPathogenicityEvidenceLine validation to fail when reconstructing nested VRS objects
        (e.g. Allele with production genomic coordinates). Model instances must be stored directly.
        """
        result = variant_pathogenicity_statement(mock_mapped_variant_with_pathogenicity_calibration_score_set)

        assert result is not None
        for evidence_line in result.hasEvidenceLines:
            assert evidence_line.hasEvidenceItems is not None
            for evidence_item in evidence_line.hasEvidenceItems:
                # Must be a model instance, not a raw dict
                assert not isinstance(
                    evidence_item, dict
                ), "hasEvidenceItems contained a raw dict instead of a model instance"
                assert evidence_item.type == "Statement"

    def test_no_statement_is_built_from_a_private_calibration(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        """A private calibration's ACMG criteria must not reach an anonymous caller."""
        mapped_variant = make_private(mock_mapped_variant_with_pathogenicity_calibration_score_set)

        assert variant_pathogenicity_statement(mapped_variant) is None

    def test_the_owner_receives_a_statement_from_their_private_calibration(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        mapped_variant = make_private(mock_mapped_variant_with_pathogenicity_calibration_score_set)

        assert variant_pathogenicity_statement(mapped_variant, principal=owner_principal()) is not None

    def test_a_statement_widened_by_entitlement_discloses_a_restricted_scope(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        mapped_variant = make_private(mock_mapped_variant_with_pathogenicity_calibration_score_set)

        statement = variant_pathogenicity_statement(mapped_variant, principal=admin_principal())

        assert scope_of(statement) == "restricted"


@pytest.mark.unit
class TestVariantHighestLevelAnnotation:
    """Unit tests for the highest-materialized-layer resolver used by the public data dump."""

    def test_study_result_when_uncalibrated(self, mock_mapped_variant):
        result = variant_highest_level_annotation(mock_mapped_variant)

        assert result is not None
        assert result.type == "ExperimentalVariantFunctionalImpactStudyResult"

    def test_functional_statement_when_functional_only(self, mock_mapped_variant_with_functional_calibration_score_set):
        # The functional calibration fixture has no ACMG classifications, so the variant qualifies for the
        # functional layer but not pathogenicity.
        result = variant_highest_level_annotation(mock_mapped_variant_with_functional_calibration_score_set)

        assert result is not None
        assert result.type == "Statement"
        assert result.proposition.type == "ExperimentalVariantFunctionalImpactProposition"

    def test_pathogenicity_statement_when_calibrated(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        result = variant_highest_level_annotation(mock_mapped_variant_with_pathogenicity_calibration_score_set)

        assert result is not None
        assert result.type == "Statement"
        assert result.proposition.type == "VariantPathogenicityProposition"

    def test_none_when_unmapped(self, mock_mapped_variant):
        mock_mapped_variant.post_mapped = None

        result = variant_highest_level_annotation(mock_mapped_variant)
        assert result is None

    def test_degrades_to_a_study_result_when_the_calibration_is_private(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        # A study result reports the measured score, which publishing the score set did make public. The
        # variant is still described; only the calibration-derived interpretation is withheld.
        mapped_variant = make_private(mock_mapped_variant_with_pathogenicity_calibration_score_set)

        result = variant_highest_level_annotation(mapped_variant)

        assert result is not None
        assert result.type == "ExperimentalVariantFunctionalImpactStudyResult"

    def test_reaches_the_statement_layer_for_an_entitled_caller(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        mapped_variant = make_private(mock_mapped_variant_with_pathogenicity_calibration_score_set)

        result = variant_highest_level_annotation(mapped_variant, principal=admin_principal())

        assert result is not None
        assert result.type != "ExperimentalVariantFunctionalImpactStudyResult"

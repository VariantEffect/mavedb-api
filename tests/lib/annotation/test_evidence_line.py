# ruff: noqa: E402

"""
Tests for mavedb.lib.annotation.evidence_line module.

This module tests evidence line creation functions for ACMG and functional
evidence lines, including pathogenicity classification and strength handling.
"""

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("psycopg2")

from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
from ga4gh.va_spec.base.core import Direction, EvidenceLine
from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

from mavedb.lib.annotation.annotate import variant_study_result
from mavedb.lib.annotation.classification import ExperimentalVariantFunctionalImpactClassification
from mavedb.lib.annotation.evidence_line import acmg_evidence_line, functional_evidence_line
from mavedb.lib.annotation.proposition import (
    variant_functional_impact_proposition,
    variant_pathogenicity_proposition,
)
from mavedb.lib.annotation.statement import functional_statement


@pytest.mark.unit
class TestAcmgEvidenceLine:
    """Unit tests for ACMG evidence line creation."""

    @pytest.mark.parametrize(
        "expected_outcome, expected_direction",
        [
            (VariantPathogenicityEvidenceLine.Criterion.BS3, Direction.DISPUTES),
            (VariantPathogenicityEvidenceLine.Criterion.PS3, Direction.SUPPORTS),
        ],
    )
    @pytest.mark.parametrize(
        "expected_strength",
        [
            StrengthOfEvidenceProvided.SUPPORTING,
            StrengthOfEvidenceProvided.MODERATE,
            StrengthOfEvidenceProvided.STRONG,
            StrengthOfEvidenceProvided.VERY_STRONG,
        ],
    )
    def test_acmg_evidence_line_with_met_valid_clinical_classification(
        self,
        mock_annotation_context_with_pathogenicity_calibration_score_set,
        expected_outcome,
        expected_strength,
        expected_direction,
    ):
        """Test ACMG evidence line creation with met valid clinical classification."""
        context = mock_annotation_context_with_pathogenicity_calibration_score_set
        score_calibration = context.variant.score_set.score_calibrations[0]

        with patch(
            "mavedb.lib.annotation.evidence_line.pathogenicity_classification_of_variant",
            return_value=(MagicMock(label="Test Range"), expected_outcome, expected_strength),
        ):
            proposition = variant_pathogenicity_proposition(context)
            study_result = variant_study_result(context)
            evidence = functional_evidence_line(context, score_calibration, [study_result])
            result = acmg_evidence_line(context, score_calibration, proposition, [evidence])

        if expected_strength == StrengthOfEvidenceProvided.STRONG:
            expected_evidence_outcome = expected_outcome.value
        else:
            expected_evidence_outcome = f"{expected_outcome.value}_{expected_strength.name.lower()}"

        assert isinstance(result, VariantPathogenicityEvidenceLine)
        assert result.description == f"Pathogenicity evidence line for {context.variant.urn}."
        assert result.evidenceOutcome.primaryCoding.code.root == expected_evidence_outcome
        assert result.evidenceOutcome.primaryCoding.system == "ACMG Guidelines, 2015"
        assert result.evidenceOutcome.name == f"ACMG 2015 {expected_outcome.name} Criterion Met"
        assert result.strengthOfEvidenceProvided.primaryCoding.code.root == expected_strength
        assert result.strengthOfEvidenceProvided.primaryCoding.system == "ACMG Guidelines, 2015"
        assert result.directionOfEvidenceProvided == expected_direction if expected_strength else None
        assert result.contributions
        assert result.specifiedBy
        assert result.targetProposition == proposition
        assert len(result.hasEvidenceItems) == 1

    def test_acmg_evidence_line_with_not_met_clinical_classification(
        self,
        mock_annotation_context_with_pathogenicity_calibration_score_set,
    ):
        """Test ACMG evidence line creation with not met clinical classification."""
        context = mock_annotation_context_with_pathogenicity_calibration_score_set
        score_calibration = context.variant.score_set.score_calibrations[0]
        expected_outcome = VariantPathogenicityEvidenceLine.Criterion.PS3
        expected_strength = None
        expected_evidence_outcome = f"{expected_outcome.value}_not_met"

        with patch(
            "mavedb.lib.annotation.evidence_line.pathogenicity_classification_of_variant",
            return_value=(MagicMock(label="Test Range"), expected_outcome, expected_strength),
        ):
            proposition = variant_pathogenicity_proposition(context)
            study_result = variant_study_result(context)
            evidence = functional_evidence_line(context, score_calibration, [study_result])
            result = acmg_evidence_line(context, score_calibration, proposition, [evidence])

        assert isinstance(result, VariantPathogenicityEvidenceLine)
        assert result.description == f"Pathogenicity evidence line for {context.variant.urn}."
        assert result.evidenceOutcome.primaryCoding.code.root == expected_evidence_outcome
        assert result.evidenceOutcome.primaryCoding.system == "ACMG Guidelines, 2015"
        assert result.evidenceOutcome.name == f"ACMG 2015 {expected_outcome.name} Criterion Not Met"
        assert result.strengthOfEvidenceProvided is None
        assert result.directionOfEvidenceProvided == Direction.NEUTRAL
        assert result.contributions
        assert result.specifiedBy
        assert result.targetProposition == proposition
        assert len(result.hasEvidenceItems) == 1

    def test_acmg_evidence_line_with_no_calibrations_raises_error(self, mock_annotation_context):
        """Test that ACMG evidence line creation raises error when no calibrations exist."""
        context = mock_annotation_context
        context.variant.score_set.score_calibrations = None
        score_calibration = MagicMock()

        with pytest.raises(ValueError, match="does not have a score set with score calibrations"):
            proposition = variant_pathogenicity_proposition(context)
            study_result = variant_study_result(context)
            acmg_evidence_line(context, score_calibration, proposition, [study_result])

    def test_acmg_evidence_line_accepts_statement_evidence_without_serialization_error(
        self,
        mock_annotation_context_with_pathogenicity_calibration_score_set,
    ):
        """Regression test: VariantPathogenicityEvidenceLine must accept Statement instances directly.

        Previously, acmg_evidence_line serialized Statement evidence to dicts via model_dump,
        which caused VariantPathogenicityEvidenceLine validation to fail when reconstructing
        nested VRS objects from those dicts (e.g. Allele with real genomic coordinates).
        Evidence model instances must be passed directly, not serialized.
        """
        context = mock_annotation_context_with_pathogenicity_calibration_score_set
        score_calibration = context.variant.score_set.score_calibrations[0]

        study_result = variant_study_result(context)
        functional_proposition = variant_functional_impact_proposition(context)
        functional_evidence = functional_evidence_line(context, score_calibration, [study_result])
        statement = functional_statement(
            context,
            functional_proposition,
            [functional_evidence],
            score_calibration,
            ExperimentalVariantFunctionalImpactClassification.NORMAL,
        )
        clinical_proposition = variant_pathogenicity_proposition(context)

        result = acmg_evidence_line(context, score_calibration, clinical_proposition, [statement])

        assert isinstance(result, VariantPathogenicityEvidenceLine)
        assert len(result.hasEvidenceItems) == 1
        # Evidence items must be Statement model instances, not raw dicts
        assert result.hasEvidenceItems[0].type == "Statement"


@pytest.mark.unit
class TestFunctionalEvidenceLine:
    """Unit tests for functional evidence line creation."""

    def test_functional_evidence_line_with_valid_functional_evidence(
        self, mock_annotation_context_with_functional_calibration_score_set
    ):
        """Test functional evidence line creation with valid evidence."""
        context = mock_annotation_context_with_functional_calibration_score_set
        score_calibration = context.variant.score_set.score_calibrations[0]
        study_result = variant_study_result(context)
        result = functional_evidence_line(context, score_calibration, [study_result])

        assert isinstance(result, EvidenceLine)
        assert result.description == f"Functional evidence line for {context.variant.urn}"
        assert result.directionOfEvidenceProvided is not None
        assert result.specifiedBy
        assert result.contributions
        assert result.reportedIn
        assert len(result.hasEvidenceItems) == 1

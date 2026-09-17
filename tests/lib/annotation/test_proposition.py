# ruff: noqa: E402

"""
Tests for mavedb.lib.annotation.proposition module.

This module tests proposition creation functions for experimental variant
clinical and functional impact propositions.
"""

import pytest

pytest.importorskip("psycopg2")

from ga4gh.va_spec.base.core import (
    ExperimentalVariantFunctionalImpactProposition,
    VariantPathogenicityProposition,
)
from ga4gh.vrs.models import MolecularVariation

from mavedb.lib.annotation.proposition import (
    mapped_variant_to_experimental_variant_clinical_impact_proposition,
    mapped_variant_to_experimental_variant_functional_impact_proposition,
)
from tests.helpers.mocks.factories import create_mock_mondo_term, create_mock_score_calibration


@pytest.mark.unit
class TestExperimentalVariantClinicalImpactProposition:
    """Unit tests for experimental variant clinical impact proposition creation."""

    def test_mapped_variant_to_experimental_variant_clinical_impact_proposition(self, mock_mapped_variant):
        """The proposition's condition defaults to the calibration's generic disease term."""
        calibration = create_mock_score_calibration()
        result = mapped_variant_to_experimental_variant_clinical_impact_proposition(mock_mapped_variant, calibration)

        assert isinstance(result, VariantPathogenicityProposition)
        assert result.description == f"Variant pathogenicity proposition for {mock_mapped_variant.variant.urn}."
        assert isinstance(result.subjectVariant, MolecularVariation)
        assert result.predicate == "isCausalFor"
        assert result.objectCondition.root.conceptType == "Disease"
        assert result.objectCondition.root.primaryCoding.code.root == "MONDO:0000001"
        assert result.objectCondition.root.primaryCoding.system == "https://purl.obolibrary.org/obo/mondo.owl"

    def test_clinical_impact_proposition_reflects_the_calibration_disease(self, mock_mapped_variant):
        """A calibration with a specific disease term drives the proposition's condition."""
        calibration = create_mock_score_calibration(
            disease_term=create_mock_mondo_term(code="MONDO:0015263", label="Brugada syndrome")
        )
        result = mapped_variant_to_experimental_variant_clinical_impact_proposition(mock_mapped_variant, calibration)

        assert result.objectCondition.root.primaryCoding.code.root == "MONDO:0015263"
        assert result.objectCondition.root.name == "Brugada syndrome"


@pytest.mark.unit
class TestExperimentalVariantFunctionalImpactProposition:
    """Unit tests for experimental variant functional impact proposition creation."""

    def test_mapped_variant_to_experimental_variant_functional_impact_proposition(self, mock_mapped_variant):
        """Test creation of functional impact proposition from mapped variant."""
        result = mapped_variant_to_experimental_variant_functional_impact_proposition(mock_mapped_variant)

        assert isinstance(result, ExperimentalVariantFunctionalImpactProposition)
        assert result.description == f"Variant functional impact proposition for {mock_mapped_variant.variant.urn}."
        assert isinstance(result.subjectVariant, MolecularVariation)
        assert result.predicate == "impactsFunctionOf"
        assert result.objectSequenceFeature.primaryCoding.code.root == "BRCA1"
        assert result.objectSequenceFeature.primaryCoding.system == "https://www.genenames.org/"
        assert result.experimentalContextQualifier is not None

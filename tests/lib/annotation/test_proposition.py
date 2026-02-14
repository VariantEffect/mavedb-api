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


@pytest.mark.unit
class TestExperimentalVariantClinicalImpactProposition:
    """Unit tests for experimental variant clinical impact proposition creation."""

    def test_mapped_variant_to_experimental_variant_clinical_impact_proposition(self, mock_mapped_variant):
        """Test creation of clinical impact proposition from mapped variant."""
        result = mapped_variant_to_experimental_variant_clinical_impact_proposition(mock_mapped_variant)

        assert isinstance(result, VariantPathogenicityProposition)
        assert result.description == f"Variant pathogenicity proposition for {mock_mapped_variant.variant.urn}."
        assert isinstance(result.subjectVariant, MolecularVariation)
        assert result.predicate == "isCausalFor"
        assert result.objectCondition.root.conceptType == "Disease"
        assert result.objectCondition.root.primaryCoding.code.root == "C0012634"
        assert result.objectCondition.root.primaryCoding.system == "https://www.ncbi.nlm.nih.gov/medgen/"


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

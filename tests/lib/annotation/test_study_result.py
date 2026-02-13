"""
Tests for mavedb.lib.annotation.study_result module.

This module tests study result creation functions for experimental variant
functional impact study results.
"""

import pytest
from ga4gh.va_spec.base.core import ExperimentalVariantFunctionalImpactStudyResult
from ga4gh.vrs.models import MolecularVariation

from mavedb.lib.annotation.document import mapped_variant_as_iri, variant_as_iri
from mavedb.lib.annotation.study_result import (
    mapped_variant_to_experimental_variant_impact_study_result,
)


@pytest.mark.unit
class TestExperimentalVariantImpactStudyResult:
    """Unit tests for experimental variant impact study result creation."""

    def test_mapped_variant_to_experimental_variant_impact_study_result(self, mock_mapped_variant):
        """Test creation of experimental variant impact study result from mapped variant."""
        result = mapped_variant_to_experimental_variant_impact_study_result(mock_mapped_variant)

        assert isinstance(result, ExperimentalVariantFunctionalImpactStudyResult)
        assert result.description == f"Variant effect study result for {mock_mapped_variant.variant.urn}."
        assert isinstance(result.focusVariant, MolecularVariation)
        assert result.functionalImpactScore == mock_mapped_variant.variant.data["score_data"]["score"]
        # Verify all expected contribution types are present
        contribution_types = {c.name for c in result.contributions}
        expected_types = {"MaveDB API", "MaveDB VRS Mapper", "MaveDB Dataset Creator", "MaveDB Dataset Modifier"}
        assert contribution_types == expected_types, f"Expected {expected_types}, got {contribution_types}"
        # specifiedBy will be None when no primary publications exist
        assert result.sourceDataSet is not None
        assert result.reportedIn is not None
        assert mapped_variant_as_iri(mock_mapped_variant) in result.reportedIn
        assert variant_as_iri(mock_mapped_variant.variant) in result.reportedIn

    def test_no_mapped_variant_is_filtered_properly(self, mock_mapped_variant):
        """Test that study result handles missing mapped variant (no ClinGen allele ID)."""
        mock_mapped_variant.clingen_allele_id = None
        result = mapped_variant_to_experimental_variant_impact_study_result(mock_mapped_variant)

        assert isinstance(result, ExperimentalVariantFunctionalImpactStudyResult)
        assert result.description == f"Variant effect study result for {mock_mapped_variant.variant.urn}."
        assert isinstance(result.focusVariant, MolecularVariation)
        assert result.functionalImpactScore == mock_mapped_variant.variant.data["score_data"]["score"]
        # Verify all expected contribution types are present
        contribution_types = {c.name for c in result.contributions}
        expected_types = {"MaveDB API", "MaveDB VRS Mapper", "MaveDB Dataset Creator", "MaveDB Dataset Modifier"}
        assert contribution_types == expected_types, f"Expected {expected_types}, got {contribution_types}"
        # specifiedBy will be None when no primary publications exist
        assert result.sourceDataSet is not None
        assert result.reportedIn is not None
        assert variant_as_iri(mock_mapped_variant.variant) in result.reportedIn
        assert len(result.reportedIn) == 1

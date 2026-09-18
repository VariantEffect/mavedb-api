# ruff: noqa: E402

"""
Tests for mavedb.lib.annotation.study_result module.

This module tests study result creation functions for experimental variant
functional impact study results.
"""

import pytest

pytest.importorskip("psycopg2")

from ga4gh.va_spec.base.core import ExperimentalVariantFunctionalImpactStudyResult
from ga4gh.vrs.models import MolecularVariation

from mavedb.lib.annotation.document import measured_allele_as_iri, variant_as_iri
from mavedb.lib.annotation.study_result import variant_impact_study_result


@pytest.mark.unit
class TestExperimentalVariantImpactStudyResult:
    """Unit tests for experimental variant impact study result creation."""

    def test_variant_impact_study_result(self, mock_annotation_context):
        """Test creation of experimental variant impact study result from an annotation context."""
        result = variant_impact_study_result(mock_annotation_context)

        assert isinstance(result, ExperimentalVariantFunctionalImpactStudyResult)
        assert result.description == f"Variant effect study result for {mock_annotation_context.variant.urn}."
        assert isinstance(result.focusVariant, MolecularVariation)
        assert result.functionalImpactScore == mock_annotation_context.variant.data["score_data"]["score"]
        # Verify all expected contribution types are present
        contribution_types = {c.name for c in result.contributions}
        expected_types = {"MaveDB API", "MaveDB VRS Mapper", "MaveDB Dataset Creator", "MaveDB Dataset Modifier"}
        assert contribution_types == expected_types, f"Expected {expected_types}, got {contribution_types}"
        # specifiedBy will be None when no primary publications exist
        assert result.sourceDataSet is not None
        assert result.reportedIn is not None
        assert measured_allele_as_iri(mock_annotation_context.measured_allele) in result.reportedIn
        assert variant_as_iri(mock_annotation_context.variant) in result.reportedIn

    def test_no_clingen_allele_id_is_filtered_properly(self, mock_annotation_context):
        """Test that study result handles a measured allele with no ClinGen allele ID."""
        mock_annotation_context.measured_allele.clingen_allele_id = None
        result = variant_impact_study_result(mock_annotation_context)

        assert isinstance(result, ExperimentalVariantFunctionalImpactStudyResult)
        assert result.description == f"Variant effect study result for {mock_annotation_context.variant.urn}."
        assert isinstance(result.focusVariant, MolecularVariation)
        assert result.functionalImpactScore == mock_annotation_context.variant.data["score_data"]["score"]
        # Verify all expected contribution types are present
        contribution_types = {c.name for c in result.contributions}
        expected_types = {"MaveDB API", "MaveDB VRS Mapper", "MaveDB Dataset Creator", "MaveDB Dataset Modifier"}
        assert contribution_types == expected_types, f"Expected {expected_types}, got {contribution_types}"
        # specifiedBy will be None when no primary publications exist
        assert result.sourceDataSet is not None
        assert result.reportedIn is not None
        assert variant_as_iri(mock_annotation_context.variant) in result.reportedIn
        assert len(result.reportedIn) == 1

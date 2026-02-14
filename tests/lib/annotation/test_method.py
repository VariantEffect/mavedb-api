# ruff: noqa: E402

"""
Tests for mavedb.lib.annotation.method module.

This module tests method creation functions for publications, APIs, and guidelines,
ensuring proper IRI generation and method metadata.
"""

from unittest.mock import Mock

import pytest

pytest.importorskip("psycopg2")

from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine

from mavedb.lib.annotation.method import (
    functional_score_calibration_as_iri,
    functional_score_calibration_as_method,
    mavedb_api_as_method,
    mavedb_api_releases_as_iri,
    mavedb_vrs_as_method,
    mavedb_vrs_releases_as_iri,
    pathogenicity_score_calibration_as_iri,
    pathogenicity_score_calibration_as_method,
    publication_as_iri,
    publication_identifier_to_method,
    publication_identifiers_to_method,
    variant_interpretation_functional_guideline_as_iri,
    variant_interpretation_functional_guideline_method,
)
from mavedb.models.enums.score_calibration_relation import ScoreCalibrationRelation
from mavedb.models.score_set_publication_identifier import ScoreSetPublicationIdentifierAssociation

MAVEDB_API_RELEASES_URL = "https://github.com/VariantEffect/mavedb-api/releases"
MAVEDB_MAPPER_RELEASES_URL = "https://github.com/VariantEffect/dcd_mapping2/releases"
MAVEDB_CALIBRATION_URL = "https://github.com/Dzeiberg/mave_calibration"
FUNCTIONAL_GUIDELINES_URL = "https://pubmed.ncbi.nlm.nih.gov/29785012/"
CLINICAL_GUIDELINES_URL = "https://pubmed.ncbi.nlm.nih.gov/29785012/"


@pytest.mark.unit
class TestPublicationMethods:
    """Unit tests for publication-related method functions."""

    def test_publication_as_iri(self, mock_publication):
        """Test IRI generation for publications."""
        result = publication_as_iri(mock_publication)
        assert result.root == mock_publication.url

    def test_publication_as_iri_no_url(self, mock_publication):
        """Test IRI generation for publications without URL returns None."""
        mock_publication.url = None
        result = publication_as_iri(mock_publication)
        assert result is None

    def test_publication_identifier_to_method(self, mock_publication):
        """Test method creation from publication identifier."""
        subtype = "Test subtype"
        method = publication_identifier_to_method(mock_publication, subtype=subtype)

        assert method.name == subtype
        assert method.reportedIn.root == mock_publication.url

    def test_publication_identifier_to_method_no_url(self, mock_publication):
        """Test method creation from publication identifier without URL."""
        subtype = "Test subtype"
        mock_publication.url = None
        method = publication_identifier_to_method(mock_publication, subtype=subtype)

        assert method.name == subtype
        assert method.reportedIn is None

    def test_publication_identifiers_to_method(self, mock_publication):
        """Test method creation from publication identifier associations."""
        mock_publication.primary = True

        association = Mock(spec=ScoreSetPublicationIdentifierAssociation)
        association.publication = mock_publication

        method = publication_identifiers_to_method([association])

        assert method.name == "Experimental protocol"
        assert method.reportedIn.root == mock_publication.url

    def test_empty_publication_identifiers_to_method(self):
        """Test method creation from empty publication identifiers returns None."""
        method = publication_identifiers_to_method([])
        assert method is None

    def test_nonexistent_primary_publication_identifiers_to_method(self, mock_publication):
        """Test method creation when no primary publication exists returns None."""
        association = Mock(spec=ScoreSetPublicationIdentifierAssociation)
        association.publication = mock_publication
        association.primary = False

        method = publication_identifiers_to_method([association])
        assert method is None


@pytest.mark.unit
class TestMavedbApiMethods:
    """Unit tests for MaveDB API method functions."""

    def test_mavedb_api_releases_as_iri(self):
        """Test IRI generation for MaveDB API releases."""
        result = mavedb_api_releases_as_iri()
        assert result.root == MAVEDB_API_RELEASES_URL

    def test_mavedb_api_as_method(self):
        """Test method creation for MaveDB API."""
        method = mavedb_api_as_method()

        assert method.name == "Software version"
        assert method.reportedIn.root == MAVEDB_API_RELEASES_URL


@pytest.mark.unit
class TestMavedbVrsMethods:
    """Unit tests for MaveDB VRS method functions."""

    def test_mavedb_vrs_releases_as_iri(self):
        """Test IRI generation for MaveDB VRS releases."""
        result = mavedb_vrs_releases_as_iri()
        assert result.root == MAVEDB_MAPPER_RELEASES_URL

    def test_mavedb_vrs_as_method(self):
        """Test method creation for MaveDB VRS."""
        method = mavedb_vrs_as_method()

        assert method.name == "Software version"
        assert method.reportedIn.root == MAVEDB_MAPPER_RELEASES_URL


@pytest.mark.unit
class TestVariantInterpretationMethods:
    """Unit tests for variant interpretation guideline method functions."""

    def test_variant_interpretation_functional_guideline_as_iri(self):
        """Test IRI generation for functional interpretation guidelines."""
        result = variant_interpretation_functional_guideline_as_iri()
        assert result.root == FUNCTIONAL_GUIDELINES_URL

    def test_variant_interpretation_functional_guideline_method(self):
        """Test method creation for functional interpretation guidelines."""
        method = variant_interpretation_functional_guideline_method()

        assert method.name == "Variant interpretation guideline"
        assert method.reportedIn.root == FUNCTIONAL_GUIDELINES_URL


@pytest.mark.unit
class TestCalibrationMethods:
    """Unit tests for score calibration method and IRI helpers."""

    def test_functional_score_calibration_as_iri_returns_none_without_threshold_relation(self, mock_publication):
        """Test that functional calibration IRI is None when no threshold relation publication is present."""
        association = Mock()
        association.publication = mock_publication
        association.relation = ScoreCalibrationRelation.classification

        score_calibration = Mock()
        score_calibration.publication_identifier_associations = [association]

        assert functional_score_calibration_as_iri(score_calibration) is None

    def test_functional_score_calibration_as_iri_returns_publication_for_threshold_relation(self, mock_publication):
        """Test that functional calibration IRI resolves with threshold relation."""
        association = Mock()
        association.publication = mock_publication
        association.relation = ScoreCalibrationRelation.threshold

        score_calibration = Mock()
        score_calibration.publication_identifier_associations = [association]

        iri = functional_score_calibration_as_iri(score_calibration)
        assert iri is not None
        assert iri.root == mock_publication.url

    def test_functional_score_calibration_as_iri_accepts_string_relation_value(self, mock_publication):
        """Test that functional calibration IRI resolves when relation is raw enum value string."""
        association = Mock()
        association.publication = mock_publication
        association.relation = ScoreCalibrationRelation.threshold.value

        score_calibration = Mock()
        score_calibration.publication_identifier_associations = [association]

        iri = functional_score_calibration_as_iri(score_calibration)
        assert iri is not None
        assert iri.root == mock_publication.url

    def test_pathogenicity_score_calibration_as_iri_returns_none_without_classification_relation(
        self, mock_publication
    ):
        """Test that pathogenicity calibration IRI is None when no classification relation publication is present."""
        association = Mock()
        association.publication = mock_publication
        association.relation = ScoreCalibrationRelation.threshold

        score_calibration = Mock()
        score_calibration.publication_identifier_associations = [association]

        assert pathogenicity_score_calibration_as_iri(score_calibration) is None

    def test_pathogenicity_score_calibration_as_iri_returns_publication_for_classification_relation(
        self, mock_publication
    ):
        """Test that pathogenicity calibration IRI resolves with classification relation."""
        association = Mock()
        association.publication = mock_publication
        association.relation = ScoreCalibrationRelation.classification

        score_calibration = Mock()
        score_calibration.publication_identifier_associations = [association]

        iri = pathogenicity_score_calibration_as_iri(score_calibration)
        assert iri is not None
        assert iri.root == mock_publication.url

    def test_functional_score_calibration_as_method_falls_back_to_not_provided(self):
        """Test functional calibration method fallback when no publication can be resolved."""
        score_calibration = Mock()
        score_calibration.publication_identifier_associations = []

        method = functional_score_calibration_as_method(score_calibration)

        assert method.name == "Calibration method"
        assert method.reportedIn.root == "Not Provided"

    def test_pathogenicity_score_calibration_as_method_sets_method_type(self):
        """Test pathogenicity calibration method includes criterion-based methodType."""
        score_calibration = Mock()
        score_calibration.publication_identifier_associations = []

        method = pathogenicity_score_calibration_as_method(
            score_calibration,
            VariantPathogenicityEvidenceLine.Criterion.PS3,
        )

        assert method.name == "Calibration method"
        assert method.reportedIn.root == "Not Provided"
        assert method.methodType == VariantPathogenicityEvidenceLine.Criterion.PS3.value

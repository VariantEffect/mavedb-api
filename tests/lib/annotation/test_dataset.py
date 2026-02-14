# ruff: noqa: E402

"""
Tests for mavedb.lib.annotation.dataset module.

This module tests functions for converting MaveDB ScoreSet objects to GA4GH DataSet objects,
ensuring proper mapping of metadata, licensing, and publication information.
"""

from datetime import date

import pytest

pytest.importorskip("psycopg2")

from ga4gh.core.models import Coding, MappableConcept
from ga4gh.core.models import iriReference as IRI
from ga4gh.va_spec.base import DataSet

from mavedb.lib.annotation.dataset import score_set_to_data_set
from tests.helpers.mocks.factories import create_mock_license, create_mock_score_set


@pytest.mark.unit
class TestScoreSetToDataSetUnit:
    """Unit tests for score_set_to_data_set function."""

    def test_returns_dataset_object(self):
        """Test that function returns proper DataSet object."""
        score_set = create_mock_score_set()
        data_set = score_set_to_data_set(score_set)

        assert isinstance(data_set, DataSet)

    def test_maps_basic_properties(self):
        """Test that basic score set properties are mapped correctly."""
        score_set = create_mock_score_set(
            urn="urn:mavedb:00000002-b-2", title="Custom Score Set", short_description="Custom description for testing"
        )
        data_set = score_set_to_data_set(score_set)

        assert data_set.id == "urn:mavedb:00000002-b-2"
        assert data_set.name == "Custom Score Set"
        assert data_set.description == "Custom description for testing"

    def test_maps_license_information(self):
        """Test that license information is mapped correctly."""
        license_obj = create_mock_license(
            short_name="MIT", long_name="MIT License", version="1.0", link="https://opensource.org/licenses/MIT"
        )
        score_set = create_mock_score_set(license=license_obj)
        data_set = score_set_to_data_set(score_set)

        assert isinstance(data_set.license, MappableConcept)
        assert data_set.license.name == "MIT License"

    def test_maps_license_primary_coding(self):
        """Test that license primary coding is mapped correctly."""
        license_obj = create_mock_license(
            short_name="CC BY 4.0",
            long_name="Creative Commons Attribution 4.0 International",
            version="4.0",
            link="https://creativecommons.org/licenses/by/4.0/",
        )
        score_set = create_mock_score_set(license=license_obj)
        data_set = score_set_to_data_set(score_set)

        coding = data_set.license.primaryCoding
        assert coding.system == "https://spdx.org/licenses/"
        assert coding.code.root == "CC-BY-4.0"
        assert coding.systemVersion == "4.0"
        assert len(coding.iris) == 1
        assert coding.iris[0].root == "https://creativecommons.org/licenses/by/4.0/"

    def test_maps_license_without_link(self):
        """Test that license mapping works when link is None."""
        license_obj = create_mock_license(
            short_name="Custom License", long_name="Custom License Name", version="1.0", link=None
        )
        score_set = create_mock_score_set(license=license_obj)
        data_set = score_set_to_data_set(score_set)

        # The mock always sets link to a default value unless explicitly set to None,
        # but our dataset code checks for truthy values, so we need to properly test this
        license_obj.link = None  # Explicitly set to None
        data_set = score_set_to_data_set(score_set)
        assert data_set.license is None

    def test_maps_published_date_correctly(self):
        """Test that published date is formatted correctly."""
        test_date = date(2024, 6, 15)
        score_set = create_mock_score_set(published_date=test_date)
        data_set = score_set_to_data_set(score_set)

        assert data_set.releaseDate == test_date

    def test_handles_none_published_date(self):
        """Test that None published date is handled correctly."""
        score_set = create_mock_score_set(published_date=None)
        data_set = score_set_to_data_set(score_set)

        # When published_date is None, our helper still sets a default date
        # If we want to test None, we need to explicitly set it
        score_set.published_date = None
        data_set = score_set_to_data_set(score_set)
        assert data_set.releaseDate is None

    def test_includes_reported_in_iri(self):
        """Test that reportedIn IRI is included."""
        score_set = create_mock_score_set()
        data_set = score_set_to_data_set(score_set)

        assert data_set.reportedIn is not None
        assert isinstance(data_set.reportedIn, IRI)

    @pytest.mark.parametrize(
        "short_name,long_name,version",
        [
            ("CC BY 4.0", "Creative Commons Attribution 4.0 International", "4.0"),
            ("MIT", "MIT License", "1.0"),
            ("Apache-2.0", "Apache License 2.0", "2.0"),
            ("GPL-3.0", "GNU General Public License v3.0", "3.0"),
        ],
    )
    def test_various_license_types(self, short_name, long_name, version):
        """Test function works with various license types."""
        license_obj = create_mock_license(short_name=short_name, long_name=long_name, version=version)
        score_set = create_mock_score_set(license=license_obj)
        data_set = score_set_to_data_set(score_set)

        assert data_set.license.name == long_name
        expected_code = short_name.replace(" ", "-")
        if expected_code.upper().startswith("CC-BY") and version not in expected_code:
            expected_code = f"{expected_code}-{version}"
        elif expected_code.upper() == "CC0":
            expected_code = f"CC0-{version}"

        assert data_set.license.primaryCoding.system == "https://spdx.org/licenses/"
        assert data_set.license.primaryCoding.code.root == expected_code
        assert data_set.license.primaryCoding.systemVersion == version

    @pytest.mark.parametrize("title", ["Simple Title", "Complex Title with Special Characters !@#", ""])
    def test_various_title_formats(self, title):
        """Test function handles various title formats."""
        score_set = create_mock_score_set(title=title)
        data_set = score_set_to_data_set(score_set)

        assert data_set.name == title

    def test_date_formatting_precision(self):
        """Test that date passthrough is preserved exactly."""
        test_dates = [
            date(2024, 3, 10),
            date(2024, 3, 11),
            date(2024, 3, 12),
        ]

        for test_date in test_dates:
            score_set = create_mock_score_set(published_date=test_date)
            data_set = score_set_to_data_set(score_set)
            assert data_set.releaseDate == test_date

    def test_urn_passthrough(self):
        """Test that URN is passed through unchanged."""
        test_urns = ["urn:mavedb:00000001-a-1", "urn:mavedb:00000999-z-999", "custom:urn:format"]

        for urn in test_urns:
            score_set = create_mock_score_set(urn=urn)
            data_set = score_set_to_data_set(score_set)
            assert data_set.id == urn

    def test_consistency(self):
        """Test that multiple calls with same input produce consistent results."""
        score_set = create_mock_score_set()

        data_set1 = score_set_to_data_set(score_set)
        data_set2 = score_set_to_data_set(score_set)

        assert data_set1.id == data_set2.id
        assert data_set1.name == data_set2.name
        assert data_set1.description == data_set2.description
        assert data_set1.license.name == data_set2.license.name
        assert data_set1.license.primaryCoding.code.root == data_set2.license.primaryCoding.code.root
        assert data_set1.license.primaryCoding.systemVersion == data_set2.license.primaryCoding.systemVersion
        assert data_set1.releaseDate == data_set2.releaseDate


@pytest.mark.integration
class TestDataSetIntegration:
    """Integration tests for dataset conversion with external dependencies."""

    def test_score_set_to_dataset_with_real_db_object(self, setup_lib_db_with_score_set):
        """Test dataset conversion from a persisted SQLAlchemy ScoreSet instance."""
        score_set = setup_lib_db_with_score_set
        score_set.published_date = date(2024, 1, 15)

        data_set = score_set_to_data_set(score_set)

        assert isinstance(data_set, DataSet)
        assert data_set.id == score_set.urn
        assert data_set.name == score_set.title
        assert data_set.description == score_set.short_description
        assert data_set.releaseDate == score_set.published_date

    def test_complete_dataset_structure_compliance(self):
        """Test that the complete DataSet structure complies with GA4GH spec."""
        score_set = create_mock_score_set(
            urn="urn:mavedb:00000123-x-1",
            title="Integration Test Score Set",
            short_description="Comprehensive test for GA4GH compliance",
            published_date=date(2024, 12, 1),
        )

        data_set = score_set_to_data_set(score_set)

        # Verify all required GA4GH DataSet properties are present and correct
        assert isinstance(data_set, DataSet)
        assert data_set.id == "urn:mavedb:00000123-x-1"
        assert data_set.name == "Integration Test Score Set"
        assert data_set.description == "Comprehensive test for GA4GH compliance"
        assert isinstance(data_set.license, MappableConcept)
        assert data_set.license.name is not None
        assert isinstance(data_set.license.primaryCoding, Coding)
        assert data_set.license.primaryCoding.system == "https://spdx.org/licenses/"
        assert isinstance(data_set.reportedIn, IRI)
        assert data_set.releaseDate == date(2024, 12, 1)

    def test_dataset_with_document_integration(self):
        """Test that dataset includes proper document IRI integration."""
        score_set = create_mock_score_set()
        data_set = score_set_to_data_set(score_set)

        # Verify that the reportedIn field properly integrates with document module
        assert data_set.reportedIn is not None
        assert isinstance(data_set.reportedIn, IRI)
        # The IRI should be created by the score_set_as_iri function from document module

    def test_edge_case_combinations(self):
        """Test various edge case combinations."""
        # Test with minimal data
        minimal_score_set = create_mock_score_set(title="", short_description="", published_date=None)
        minimal_data_set = score_set_to_data_set(minimal_score_set)

        assert isinstance(minimal_data_set, DataSet)
        assert minimal_data_set.name == ""
        assert minimal_data_set.description == ""
        # Test with explicitly None published_date
        minimal_score_set.published_date = None
        minimal_data_set = score_set_to_data_set(minimal_score_set)
        assert minimal_data_set.releaseDate is None
        assert minimal_data_set.license is not None
        assert minimal_data_set.reportedIn is not None

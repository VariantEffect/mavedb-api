"""
Tests for mavedb.lib.annotation.contribution module.

This module tests functions for creating GA4GH VA Contribution objects,
including API, VRS mapping, score calibration, creator, and modifier contributions.
"""

# ruff: noqa: E402

from datetime import datetime

import pytest

pytest.importorskip("psycopg2")

from ga4gh.core.models import Extension
from ga4gh.va_spec.base import Contribution

from mavedb.lib.annotation.contribution import (
    mavedb_api_contribution,
    mavedb_creator_contribution,
    mavedb_modifier_contribution,
    mavedb_score_calibration_contribution,
    mavedb_vrs_contribution,
)
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.user import User
from tests.helpers.mocks.factories import (
    create_mock_mapped_variant,
    create_mock_resource_with_dates,
    create_mock_score_calibration,
    create_mock_user,
)


@pytest.mark.unit
class TestMavedbApiContributionUnit:
    """Unit tests for mavedb_api_contribution function."""

    def test_returns_contribution_object(self):
        """Test that function returns proper Contribution object."""
        contribution = mavedb_api_contribution()

        assert isinstance(contribution, Contribution)

    def test_has_correct_name_and_description(self):
        """Test that contribution has correct name and description."""
        contribution = mavedb_api_contribution()

        assert contribution.name == "MaveDB API"
        assert contribution.description == "Contribution from the MaveDB API"

    def test_has_correct_activity_type(self):
        """Test that contribution has correct activity type."""
        contribution = mavedb_api_contribution()

        assert contribution.activityType == "software application programming interface"

    def test_has_current_date(self):
        """Test that contribution uses current date."""
        contribution = mavedb_api_contribution()

        assert contribution.date.date() == datetime.today().date()

    def test_has_contributor(self):
        """Test that contribution has a contributor."""
        contribution = mavedb_api_contribution()

        assert contribution.contributor is not None

    def test_consistency(self):
        """Test that multiple calls produce consistent results."""
        contribution1 = mavedb_api_contribution()
        contribution2 = mavedb_api_contribution()

        assert contribution1.name == contribution2.name
        assert contribution1.description == contribution2.description
        assert contribution1.activityType == contribution2.activityType


@pytest.mark.unit
class TestMavedbVrsContributionUnit:
    """Unit tests for mavedb_vrs_contribution function."""

    def test_returns_contribution_object(self):
        """Test that function returns proper Contribution object."""
        mapped_variant = create_mock_mapped_variant()
        contribution = mavedb_vrs_contribution(mapped_variant)

        assert isinstance(contribution, Contribution)

    def test_has_correct_name_and_description(self):
        """Test that contribution has correct name and description."""
        mapped_variant = create_mock_mapped_variant()
        contribution = mavedb_vrs_contribution(mapped_variant)

        assert contribution.name == "MaveDB VRS Mapper"
        assert contribution.description == "Contribution from the MaveDB VRS mapping software"

    def test_has_correct_activity_type(self):
        """Test that contribution has correct activity type."""
        mapped_variant = create_mock_mapped_variant()
        contribution = mavedb_vrs_contribution(mapped_variant)

        assert contribution.activityType == "human genome sequence mapping process"

    def test_uses_mapped_variant_date(self):
        """Test that contribution uses mapped variant date."""
        test_date = datetime(2024, 3, 15, 12, 0, 0)
        mapped_variant = create_mock_mapped_variant(mapped_date=test_date)
        contribution = mavedb_vrs_contribution(mapped_variant)

        assert contribution.date == test_date

    def test_has_contributor(self):
        """Test that contribution has a contributor."""
        mapped_variant = create_mock_mapped_variant()
        contribution = mavedb_vrs_contribution(mapped_variant)

        assert contribution.contributor is not None

    @pytest.mark.parametrize("api_version", ["1.0", "1.1", "2.0"])
    def test_various_api_versions(self, api_version):
        """Test function works with various API versions."""
        mapped_variant = create_mock_mapped_variant(mapping_api_version=api_version)
        contribution = mavedb_vrs_contribution(mapped_variant)

        assert isinstance(contribution, Contribution)
        assert contribution.name == "MaveDB VRS Mapper"


@pytest.mark.unit
class TestMavedbScoreCalibrationContributionUnit:
    """Unit tests for mavedb_score_calibration_contribution function."""

    def test_returns_contribution_object(self):
        """Test that function returns proper Contribution object."""
        calibration = create_mock_score_calibration()
        contribution = mavedb_score_calibration_contribution(calibration)

        assert isinstance(contribution, Contribution)

    def test_uses_calibration_properties(self):
        """Test that contribution uses calibration properties."""
        calibration = create_mock_score_calibration(urn="test:cal:123", title="Test Score Calibration")
        contribution = mavedb_score_calibration_contribution(calibration)

        assert contribution.id == "test:cal:123"
        assert contribution.name == "Test Score Calibration"

    def test_has_correct_description_and_activity_type(self):
        """Test that contribution has correct description and activity type."""
        calibration = create_mock_score_calibration()
        contribution = mavedb_score_calibration_contribution(calibration)

        assert contribution.description == "Contribution from a score calibration."
        assert contribution.activityType == "variant specific calibration"

    def test_uses_calibration_date(self):
        """Test that contribution uses calibration creation date."""
        test_date = datetime(2024, 4, 20, 16, 45, 0)
        calibration = create_mock_score_calibration(creation_date=test_date)
        contribution = mavedb_score_calibration_contribution(calibration)

        assert contribution.date == test_date

    def test_has_contributor(self):
        """Test that contribution has a contributor."""
        calibration = create_mock_score_calibration()
        contribution = mavedb_score_calibration_contribution(calibration)

        assert contribution.contributor is not None


@pytest.mark.unit
class TestMavedbCreatorContributionUnit:
    """Unit tests for mavedb_creator_contribution function."""

    def test_returns_contribution_object(self):
        """Test that function returns proper Contribution object."""
        resource = create_mock_resource_with_dates()
        creator = create_mock_user()
        contribution = mavedb_creator_contribution(resource, creator)

        assert isinstance(contribution, Contribution)

    def test_has_correct_name_description_and_activity_type(self):
        """Test that contribution has correct metadata."""
        resource = create_mock_resource_with_dates()
        creator = create_mock_user()
        contribution = mavedb_creator_contribution(resource, creator)

        assert contribution.name == "MaveDB Dataset Creator"
        assert contribution.description == "When this resource was first submitted, and by whom."
        assert contribution.activityType == "http://purl.obolibrary.org/obo/CRO_0000105"

    def test_uses_creation_date(self):
        """Test that contribution uses resource creation date."""
        test_date = datetime(2024, 5, 10, 8, 15, 0)
        resource = create_mock_resource_with_dates(creation_date=test_date)
        creator = create_mock_user()
        contribution = mavedb_creator_contribution(resource, creator)

        assert contribution.date == test_date

    def test_has_contributor(self):
        """Test that contribution has a contributor."""
        resource = create_mock_resource_with_dates()
        creator = create_mock_user()
        contribution = mavedb_creator_contribution(resource, creator)

        assert contribution.contributor is not None

    def test_has_resource_type_extension(self):
        """Test that contribution includes resource type extension."""
        resource = create_mock_resource_with_dates(class_name="ExperimentSet")
        creator = create_mock_user()
        contribution = mavedb_creator_contribution(resource, creator)

        assert len(contribution.extensions) == 1
        extension = contribution.extensions[0]
        assert isinstance(extension, Extension)
        assert extension.name == "resourceType"
        assert extension.value == "ExperimentSet"

    @pytest.mark.parametrize("resource_type", ["ExperimentSet", "Experiment", "ScoreSet"])
    def test_various_resource_types(self, resource_type):
        """Test function works with various resource types."""
        resource = create_mock_resource_with_dates(class_name=resource_type)
        creator = create_mock_user()
        contribution = mavedb_creator_contribution(resource, creator)

        assert contribution.extensions[0].value == resource_type


@pytest.mark.unit
class TestMavedbModifierContributionUnit:
    """Unit tests for mavedb_modifier_contribution function."""

    def test_returns_contribution_object(self):
        """Test that function returns proper Contribution object."""
        resource = create_mock_resource_with_dates()
        modifier = create_mock_user()
        contribution = mavedb_modifier_contribution(resource, modifier)

        assert isinstance(contribution, Contribution)

    def test_has_correct_name_description_and_activity_type(self):
        """Test that contribution has correct metadata."""
        resource = create_mock_resource_with_dates()
        modifier = create_mock_user()
        contribution = mavedb_modifier_contribution(resource, modifier)

        assert contribution.name == "MaveDB Dataset Modifier"
        assert contribution.description == "When this resource was last modified, and by whom."
        assert contribution.activityType == "http://purl.obolibrary.org/obo/CRO_0000103"

    def test_uses_modification_date(self):
        """Test that contribution uses resource modification date."""
        test_date = datetime(2024, 6, 12, 14, 30, 0)
        resource = create_mock_resource_with_dates(modification_date=test_date)
        modifier = create_mock_user()
        contribution = mavedb_modifier_contribution(resource, modifier)

        assert contribution.date == test_date

    def test_has_contributor(self):
        """Test that contribution has a contributor."""
        resource = create_mock_resource_with_dates()
        modifier = create_mock_user()
        contribution = mavedb_modifier_contribution(resource, modifier)

        assert contribution.contributor is not None

    def test_has_resource_type_extension(self):
        """Test that contribution includes resource type in extensions."""
        resource = create_mock_resource_with_dates(class_name="Experiment")
        modifier = create_mock_user()
        contribution = mavedb_modifier_contribution(resource, modifier)

        assert contribution.extensions is not None
        assert len(contribution.extensions) == 1
        assert isinstance(contribution.extensions[0], Extension)
        assert contribution.extensions[0].name == "resourceType"
        assert contribution.extensions[0].value == "Experiment"

    def test_different_dates_for_creator_and_modifier(self):
        """Test that creator and modifier contributions use different dates."""
        creation_date = datetime(2024, 1, 1, 10, 0, 0)
        modification_date = datetime(2024, 6, 15, 16, 45, 0)
        resource = create_mock_resource_with_dates(creation_date=creation_date, modification_date=modification_date)
        creator = create_mock_user(username="creator")
        modifier = create_mock_user(username="modifier")

        creator_contribution = mavedb_creator_contribution(resource, creator)
        modifier_contribution = mavedb_modifier_contribution(resource, modifier)

        assert creator_contribution.date == creation_date
        assert modifier_contribution.date == modification_date
        assert creator_contribution.date != modifier_contribution.date

    @pytest.mark.parametrize("resource_type", ["ExperimentSet", "Experiment", "ScoreSet"])
    def test_various_resource_types(self, resource_type):
        """Test function works with various resource types."""
        resource = create_mock_resource_with_dates(class_name=resource_type)
        modifier = create_mock_user()
        contribution = mavedb_modifier_contribution(resource, modifier)

        assert contribution.extensions[0].value == resource_type


@pytest.mark.integration
class TestContributionIntegration:
    """Integration tests for contribution functions working together."""

    def test_contributions_with_real_db_objects(self, session, setup_lib_db_with_mapped_variant):
        """Test contribution creation from persisted SQLAlchemy objects."""
        mapped_variant = setup_lib_db_with_mapped_variant
        vrs_contribution = mavedb_vrs_contribution(mapped_variant)

        creator = session.query(User).first()
        score_set = mapped_variant.variant.score_set
        score_calibration = ScoreCalibration(
            score_set_id=score_set.id,
            title="DB Calibration",
            primary=True,
            investigator_provided=True,
            private=False,
            created_by_id=creator.id,
            modified_by_id=creator.id,
        )
        score_calibration.created_by = creator
        score_calibration.modified_by = creator
        session.add(score_calibration)
        session.commit()
        session.refresh(score_calibration)

        calibration_contribution = mavedb_score_calibration_contribution(score_calibration)
        creator_contribution = mavedb_creator_contribution(score_set, creator)
        modifier_contribution = mavedb_modifier_contribution(score_set, creator)

        assert isinstance(vrs_contribution, Contribution)
        assert isinstance(calibration_contribution, Contribution)
        assert calibration_contribution.id == score_calibration.urn
        assert creator_contribution.extensions[0].value == "ScoreSet"
        assert modifier_contribution.extensions[0].value == "ScoreSet"

    def test_all_contributions_return_consistent_structure(self):
        """Test that all contribution functions return consistent Contribution objects."""
        api_contrib = mavedb_api_contribution()

        mapped_variant = create_mock_mapped_variant()
        vrs_contrib = mavedb_vrs_contribution(mapped_variant)

        calibration = create_mock_score_calibration()
        cal_contrib = mavedb_score_calibration_contribution(calibration)

        resource = create_mock_resource_with_dates()
        user = create_mock_user()
        creator_contrib = mavedb_creator_contribution(resource, user)
        modifier_contrib = mavedb_modifier_contribution(resource, user)

        # All should be Contribution objects
        contributions = [api_contrib, vrs_contrib, cal_contrib, creator_contrib, modifier_contrib]
        for contrib in contributions:
            assert isinstance(contrib, Contribution)
            assert contrib.name is not None
            assert contrib.description is not None
            assert contrib.activityType is not None
            assert contrib.contributor is not None
            assert contrib.date is not None

    def test_date_formatting_consistency(self):
        """Test that all functions format dates consistently."""
        test_date = datetime(2024, 12, 31, 23, 59, 59)

        # VRS contribution
        mapped_variant = create_mock_mapped_variant(mapped_date=test_date)
        vrs_contrib = mavedb_vrs_contribution(mapped_variant)
        assert vrs_contrib.date == test_date

        # Score calibration contribution
        calibration = create_mock_score_calibration(creation_date=test_date)
        cal_contrib = mavedb_score_calibration_contribution(calibration)
        assert cal_contrib.date == test_date

        # Creator and modifier contributions
        resource = create_mock_resource_with_dates(creation_date=test_date, modification_date=test_date)
        user = create_mock_user()
        creator_contrib = mavedb_creator_contribution(resource, user)
        modifier_contrib = mavedb_modifier_contribution(resource, user)
        assert creator_contrib.date == test_date
        assert modifier_contrib.date == test_date

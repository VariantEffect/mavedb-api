from datetime import datetime
import pytest

from ga4gh.va_spec.base import Contribution

from mavedb.lib.annotation.contribution import (
    mavedb_api_contribution,
    mavedb_vrs_contribution,
    pillar_project_calibration_contribution,
    mavedb_creator_contribution,
    mavedb_modifier_contribution,
)


def test_mavedb_api_contribution():
    contribution = mavedb_api_contribution()
    assert isinstance(contribution, Contribution)
    assert contribution.activityType == "software application programming interface"
    assert contribution.name == "MaveDB API"
    assert contribution.description == "Contribution from the MaveDB API"
    assert contribution.date.strftime("%Y-%m-%d") == datetime.today().strftime("%Y-%m-%d")
    assert contribution.contributor is not None


def test_mavedb_vrs_contribution(mock_mapped_variant):
    contribution = mavedb_vrs_contribution(mock_mapped_variant)
    assert isinstance(contribution, Contribution)
    assert contribution.activityType == "human genome sequence mapping process"
    assert contribution.name == "MaveDB VRS Mapper"
    assert contribution.description == "Contribution from the MaveDB VRS mapping software"
    assert contribution.contributor is not None
    assert contribution.date.strftime("%Y-%m-%d") == mock_mapped_variant.mapped_date.strftime("%Y-%m-%d")


def test_pillar_project_calibration_contribution():
    contribution = pillar_project_calibration_contribution()
    assert isinstance(contribution, Contribution)
    assert contribution.activityType == "variant specific calibration software"
    assert contribution.name == "MaveDB Pillar Project Calibration"
    assert contribution.description == "Contribution from the MaveDB Pillar Project Calibration software"
    assert contribution.contributor is not None


@pytest.mark.parametrize("mock_resource", ["mock_experiment_set", "mock_experiment", "mock_score_set"])
def test_mavedb_creator_contribution(mock_resource, mock_user, request):
    mocked_resource = request.getfixturevalue(mock_resource)
    contribution = mavedb_creator_contribution(mocked_resource, mock_user)
    assert isinstance(contribution, Contribution)
    assert contribution.activityType == "http://purl.obolibrary.org/obo/CRO_0000105"
    assert contribution.contributor is not None
    assert contribution.date.strftime("%Y-%m-%d") == mocked_resource.creation_date.strftime("%Y-%m-%d")
    assert contribution.extensions[0].value == mocked_resource.__class__.__name__


@pytest.mark.parametrize("mock_resource", ["mock_experiment_set", "mock_experiment", "mock_score_set"])
def test_mavedb_modifier_contribution(mock_resource, mock_user, request):
    mocked_resource = request.getfixturevalue(mock_resource)
    contribution = mavedb_modifier_contribution(mocked_resource, mock_user)
    assert isinstance(contribution, Contribution)
    assert contribution.activityType == "http://purl.obolibrary.org/obo/CRO_0000103"
    assert contribution.contributor is not None
    assert contribution.date.strftime("%Y-%m-%d") == mocked_resource.modification_date.strftime("%Y-%m-%d")
    assert contribution.extensions[0].value == mocked_resource.__class__.__name__

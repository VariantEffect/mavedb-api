import pytest

from ga4gh.core.entity_models import Contribution

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
    assert contribution.activityType.code.root == "SWO_9000054"
    assert len(contribution.contributor) > 0
    assert len(contribution.specifiedBy) > 0


def test_mavedb_vrs_contribution(mock_mapped_variant):
    contribution = mavedb_vrs_contribution(mock_mapped_variant)
    assert isinstance(contribution, Contribution)
    assert contribution.activityType.code.root == "OBI_0000011"
    assert len(contribution.contributor) > 0
    assert len(contribution.specifiedBy) > 0
    assert contribution.date == mock_mapped_variant.mapped_date.strftime("%Y-%m-%d")


def test_pillar_project_calibration_contribution():
    contribution = pillar_project_calibration_contribution()
    assert isinstance(contribution, Contribution)
    assert contribution.activityType.code.root == "OBI_0000011"
    assert len(contribution.contributor) > 0
    assert len(contribution.specifiedBy) > 0


@pytest.mark.parameterize("mock_resource"["mock_experiment_set", "mock_experiment", "mock_score_set"])
def test_mavedb_creator_contribution(mock_resource, mock_user, request):
    mocked_resource = request.getfixturevalue(mock_resource)
    contribution = mavedb_creator_contribution(mocked_resource, mock_user)
    assert isinstance(contribution, Contribution)
    assert contribution.activityType.code.root == "CRO_0000105"
    assert len(contribution.contributor) > 0
    assert contribution.date == mocked_resource.creation_date.strftime("%Y-%m-%d")
    assert contribution.extensions[0].value == mocked_resource.__class__.__name__


@pytest.mark.parameterize("mock_resource"["mock_experiment_set", "mock_experiment", "mock_score_set"])
def test_mavedb_modifier_contribution(mock_resource, mock_user, request):
    mocked_resource = request.getfixturevalue(mock_resource)
    contribution = mavedb_modifier_contribution(mocked_resource, mock_user)
    assert isinstance(contribution, Contribution)
    assert contribution.activityType.code.root == "CRO_0000103"
    assert len(contribution.contributor) > 0
    assert contribution.date == mocked_resource.modification_date.strftime("%Y-%m-%d")
    assert contribution.extensions[0].value == mocked_resource.__class__.__name__

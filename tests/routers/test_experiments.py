from datetime import date
from copy import deepcopy
import jsonschema
import re

import mavedb.view_models.experiment
from mavedb.lib.validation.urn_re import MAVEDB_TMP_URN_RE
from tests.conftest import (
    client,
    TEST_USER,
    TEST_MINIMAL_EXPERIMENT,
    TEST_MINIMAL_EXPERIMENT_RESPONSE,
    change_ownership,
)
from mavedb.view_models.experiment import Experiment, ExperimentCreate
from mavedb.models.experiment import Experiment as ExperimentDbModel
from mavedb.models.experiment_set import ExperimentSet as ExperimentSetDbModel
import pytest


def test_test_minimal_experiment_is_valid():
    jsonschema.validate(instance=TEST_MINIMAL_EXPERIMENT, schema=ExperimentCreate.schema())


def test_create_minimal_experiment(test_empty_db):
    response = client.post("/api/v1/experiments/", json=TEST_MINIMAL_EXPERIMENT)
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["urn"]), re.Match)
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["experimentSetUrn"]), re.Match)
    expected_response = deepcopy(TEST_MINIMAL_EXPERIMENT_RESPONSE)
    expected_response["urn"] = response_data["urn"]
    expected_response["experimentSetUrn"] = response_data["experimentSetUrn"]
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert expected_response[key] == response_data[key]


@pytest.mark.parametrize(
    "test_field,test_value",
    [
        ("urn", "tmp:33df10c9-78b3-4e04-bafb-2446078573d7"),
        ("numScoreSets", 2),
        ("createdBy", {"firstName": "Sneaky", "lastName": "User", "orcidId": "0000-9999-9999-0000"}),
        ("modifiedBy", {"firstName": "Sneaky", "lastName": "User", "orcidId": "0000-9999-9999-0000"}),
        ("creationDate", date(2020, 4, 1).isoformat()),
        ("modificationDate", date(2020, 4, 1).isoformat()),
        ("publishedDate", date(2020, 4, 1).isoformat()),
    ],
)
def test_cannot_create_special_fields(test_empty_db, test_field, test_value):
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    experiment_post_payload[test_field] = test_value
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    assert response.status_code == 200
    response_data = response.json()
    if test_field in response_data:
        assert response_data[test_field] != test_value


@pytest.mark.parametrize(
    "test_field,test_value",
    [
        ("urn", "tmp:33df10c9-78b3-4e04-bafb-2446078573d7"),
        ("experimentSetUrn", "tmp:33df10c9-78b3-4e04-bafb-2446078573d7"),
        ("numScoreSets", 2),
        ("createdBy", {"firstName": "Sneaky", "lastName": "User", "orcidId": "0000-9999-9999-0000"}),
        ("modifiedBy", {"firstName": "Sneaky", "lastName": "User", "orcidId": "0000-9999-9999-0000"}),
        ("creationDate", date(2020, 4, 1).isoformat()),
        ("modificationDate", date(2020, 4, 1).isoformat()),
        ("publishedDate", date(2020, 4, 1).isoformat()),
    ],
)
def test_cannot_edit_special_fields(test_empty_db, test_field, test_value):
    response = client.post("/api/v1/experiments/", json=TEST_MINIMAL_EXPERIMENT)
    response_data = response.json()
    urn = response_data["urn"]
    edited_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    edited_post_payload[test_field] = test_value
    response = client.put(f"/api/v1/experiments/{urn}", json=edited_post_payload)
    assert response.status_code == 200
    response_data = response.json()
    if test_field in response_data:
        assert response_data[test_field] != test_value


def test_edit_preserves_optional_metadata(test_empty_db):
    pass


@pytest.mark.parametrize(
    "test_field,test_value",
    [
        ("title", "Edited Title"),
        ("shortDescription", "Edited Short Description"),
        ("abstractText", "Edited Abstract"),
        ("methodText", "Edited Methods"),
    ],
)
def test_can_edit_private_experiment(test_empty_db, test_field, test_value):
    response = client.post("/api/v1/experiments/", json=TEST_MINIMAL_EXPERIMENT)
    response_data = response.json()
    urn = response_data["urn"]
    edited_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    edited_post_payload[test_field] = test_value
    response = client.put(f"/api/v1/experiments/{urn}", json=edited_post_payload)
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    assert response_data[test_field] == test_value


def test_can_edit_published_experiment(test_empty_db):
    pass


def test_cannot_edit_unowned_experiment(test_empty_db):
    pass


@pytest.mark.parametrize("test_field", ["title", "shortDescription", "abstractText", "methodText"])
def test_required_fields(test_empty_db, test_field):
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    del experiment_post_payload[test_field]
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    response_data = response.json()
    assert response.status_code == 422
    assert "field required" in response_data["detail"][0]["msg"]


def test_create_experiment_with_new_primary_publication(test_empty_db):
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    experiment_post_payload["primaryPublicationIdentifiers"] = [{"identifier": "20711194"}]
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    assert len(response_data["primaryPublicationIdentifiers"]) == 1
    assert sorted(response_data["primaryPublicationIdentifiers"][0]) == sorted(["identifier", "url", "referenceHtml"])
    # TODO: add separate tests for generating the publication url and referenceHtml


def test_create_experiment_with_invalid_doi(test_empty_db):
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    experiment_post_payload["doiIdentifiers"] = [{"identifier": "20711194"}]
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    assert response.status_code == 422
    response_data = response.json()
    assert (
        f"'{experiment_post_payload['doiIdentifiers'][0]['identifier']}' is not a valid DOI identifier"
        in response_data["detail"][0]["msg"]
    )


def test_create_experiment_with_invalid_primary_publication(test_empty_db):
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    experiment_post_payload["primaryPublicationIdentifiers"] = [{"identifier": "abcdefg"}]
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    assert response.status_code == 422
    response_data = response.json()
    assert (
        f"'{experiment_post_payload['primaryPublicationIdentifiers'][0]['identifier']}' is not a valid PubMed identifier"
        in response_data["detail"][0]["msg"]
    )


def test_get_own_private_experiment(test_empty_db):
    response = client.post("/api/v1/experiments/", json=TEST_MINIMAL_EXPERIMENT)
    response_data = response.json()
    expected_response = deepcopy(TEST_MINIMAL_EXPERIMENT_RESPONSE)
    expected_response["urn"] = response_data["urn"]
    expected_response["experimentSetUrn"] = response_data["experimentSetUrn"]
    response = client.get(f"/api/v1/experiments/{response_data['urn']}")
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert expected_response[key] == response_data[key]


def test_cannot_get_other_user_private_experiment(test_empty_db):
    response = client.post("/api/v1/experiments/", json=TEST_MINIMAL_EXPERIMENT)
    response_data = response.json()
    experiment_urn = response_data["urn"]
    experiment_set_urn = response_data["experimentSetUrn"]
    change_ownership(experiment_urn, ExperimentDbModel)
    change_ownership(experiment_set_urn, ExperimentSetDbModel)
    response = client.get(f"/api/v1/experiments/{experiment_urn}")
    assert response.status_code == 404
    response_data = response.json()
    assert f"experiment with URN '{experiment_urn}' not found" in response_data["detail"]

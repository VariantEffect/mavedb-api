from datetime import date
from copy import deepcopy
import jsonschema
import re
from mavedb.lib.validation.urn_re import MAVEDB_TMP_URN_RE
from tests.conftest import client, TEST_USER
from mavedb.view_models.experiment import Experiment, ExperimentCreate
import pytest

TEST_EXPERIMENT_POST_PAYLOAD = {
    "title": "Test Experiment Title",
    "shortDescription": "Test experiment",
    "abstractText": "Abstract",
    "methodText": "Methods",
}

TEST_EXPERIMENT_RESPONSE_PAYLOAD = {
    "title": "Test Experiment Title",
    "shortDescription": "Test experiment",
    "abstractText": "Abstract",
    "methodText": "Methods",
    "numScoreSets": 0,
    "createdBy": {
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "modifiedBy": {
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "creationDate": date.today().isoformat(),
    "modificationDate": date.today().isoformat(),
    "keywords": [],
    "doiIdentifiers": [],
    "primaryPublicationIdentifiers": [],
    "secondaryPublicationIdentifiers": [],
    "rawReadIdentifiers": [],
    # keys to be set after receiving response
    "urn": None,
    "experimentSetUrn": None,
}


def test_test_experiment_post_payload_is_valid():
    jsonschema.validate(instance=TEST_EXPERIMENT_POST_PAYLOAD, schema=ExperimentCreate.schema())


def test_create_minimal_experiment(test_with_empty_db):
    response = client.post("/api/v1/experiments/", json=TEST_EXPERIMENT_POST_PAYLOAD)
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["urn"]), re.Match)
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["experimentSetUrn"]), re.Match)
    expected_response = deepcopy(TEST_EXPERIMENT_RESPONSE_PAYLOAD)
    expected_response["urn"] = response_data["urn"]
    expected_response["experimentSetUrn"] = response_data["experimentSetUrn"]
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert expected_response[key] == response_data[key]


def test_can_edit_private_experiment(test_with_empty_db):
    pass


def test_can_edit_published_experiment(test_with_empty_db):
    pass


def test_cannot_edit_unowned_experiment(test_with_empty_db):
    pass


@pytest.mark.parametrize("test_field", ["title", "shortDescription", "abstractText", "methodText"])
def test_required_fields(test_with_empty_db, test_field):
    experiment_post_payload = deepcopy(TEST_EXPERIMENT_POST_PAYLOAD)
    del experiment_post_payload[test_field]
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    response_data = response.json()
    assert response.status_code == 422
    assert "field required" in response_data["detail"][0]["msg"]


def test_create_experiment_with_new_primary_publication(test_with_empty_db):
    experiment_post_payload = deepcopy(TEST_EXPERIMENT_POST_PAYLOAD)
    experiment_post_payload["primaryPublicationIdentifiers"] = [{"identifier": "20711194"}]
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    assert len(response_data["primaryPublicationIdentifiers"]) == 1
    assert sorted(response_data["primaryPublicationIdentifiers"][0]) == sorted(["identifier", "url", "referenceHtml"])
    # TODO: add separate tests for generating the publication url and referenceHtml


def test_create_experiment_with_invalid_doi(test_with_empty_db):
    experiment_post_payload = deepcopy(TEST_EXPERIMENT_POST_PAYLOAD)
    experiment_post_payload["doiIdentifiers"] = [{"identifier": "20711194"}]
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    assert response.status_code == 422
    assert f"'{experiment_post_payload['doiIdentifiers'][0]['identifier']}' is not a valid DOI identifier" in str(
        response.text
    )


def test_create_experiment_with_invalid_primary_publication(test_with_empty_db):
    experiment_post_payload = deepcopy(TEST_EXPERIMENT_POST_PAYLOAD)
    experiment_post_payload["primaryPublicationIdentifiers"] = [{"identifier": "abcdefg"}]
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    assert response.status_code == 422
    assert (
        f"'{experiment_post_payload['primaryPublicationIdentifiers'][0]['identifier']}' is not a valid PubMed identifier"
        in str(response.text)
    )

from datetime import date
from copy import deepcopy
import json
import jsonschema
import re
from mavedb.lib.validation.urn_re import MAVEDB_TMP_URN_RE
from tests.conftest import client, TEST_USER
from mavedb.view_models.experiment import Experiment, ExperimentCreate

TEST_EXPERIMENT_POST_PAYLOAD = {
    "title": "Test Experiment Title",
    "methodText": "Methods",
    "abstractText": "Abstract",
    "shortDescription": "Test experiment",
    "extraMetadata": {},
    "keywords": [],
    "primaryPublicationIdentifiers": [],
}

TEST_EXPERIMENT_RESPONSE_PAYLOAD = {
    "title": "Test Experiment Title",
    "methodText": "Methods",
    "abstractText": "Abstract",
    "shortDescription": "Test experiment",
    "extraMetadata": {},
    "keywords": [],
    "primaryPublicationIdentifiers": [],
    "numScoreSets": 0,
    "creationDate": date.today().isoformat(),
    "modificationDate": date.today().isoformat(),
    "publishedDate": None,
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
    "processingState": None,
    "doiIdentifiers": [],
    "secondaryPublicationIdentifiers": [],
    "rawReadIdentifiers": [],
    # keys to be set after receiving response
    "urn": None,
    "experimentSetUrn": None,
}


def test_test_experiment_post_payload_is_valid():
    jsonschema.validate(instance=TEST_EXPERIMENT_POST_PAYLOAD, schema=ExperimentCreate.schema())


def test_create_experiment(test_with_empty_db):
    experiment_post_payload = deepcopy(TEST_EXPERIMENT_POST_PAYLOAD)
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["urn"]), re.Match)
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["experimentSetUrn"]), re.Match)
    expected_response = deepcopy(TEST_EXPERIMENT_RESPONSE_PAYLOAD)
    expected_response["urn"] = response_data["urn"]
    expected_response["experimentSetUrn"] = response_data["experimentSetUrn"]
    assert json.dumps(response_data, sort_keys=True) == json.dumps(expected_response, sort_keys=True)


def test_create_experiment_with_new_primary_publication(test_with_empty_db):
    experiment_post_payload = deepcopy(TEST_EXPERIMENT_POST_PAYLOAD)
    experiment_post_payload["primaryPublicationIdentifiers"] = [{"identifier": "20711194"}]
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    assert response.status_code == 200
    response_data = response.json()
    expected_response = deepcopy(TEST_EXPERIMENT_RESPONSE_PAYLOAD)
    expected_response["urn"] = response_data["urn"]
    expected_response["experimentSetUrn"] = response_data["experimentSetUrn"]
    expected_response["primaryPublicationIdentifiers"] = [
        {
            "identifier": "20711194",
            "id": 1,
            "url": "http://www.ncbi.nlm.nih.gov/pubmed/20711194",
            "referenceHtml": "Fowler DM, <i>et al</i>. High-resolution mapping of protein sequence-function relationships. High-resolution mapping of protein sequence-function relationships. 2010; 7:741-6. doi: 10.1038/nmeth.1492",
        },
    ]
    assert json.dumps(response_data, sort_keys=True) == json.dumps(expected_response, sort_keys=True)


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

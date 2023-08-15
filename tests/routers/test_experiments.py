from datetime import date
from copy import deepcopy
import jsonschema
import re

from mavedb.lib.validation.urn_re import MAVEDB_TMP_URN_RE
from tests.helpers.constants import (
    TEST_MINIMAL_EXPERIMENT,
    TEST_MINIMAL_EXPERIMENT_RESPONSE,
)
from mavedb.view_models.experiment import Experiment, ExperimentCreate
from mavedb.models.experiment import Experiment as ExperimentDbModel
from mavedb.models.experiment_set import ExperimentSet as ExperimentSetDbModel
import pytest
from tests.helpers.util import change_ownership, create_experiment, create_score_set_with_variants


def test_test_minimal_experiment_is_valid():
    jsonschema.validate(instance=TEST_MINIMAL_EXPERIMENT, schema=ExperimentCreate.schema())


def test_create_minimal_experiment(client, setup_router_db):
    response = client.post("/api/v1/experiments/", json=TEST_MINIMAL_EXPERIMENT)
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["urn"]), re.Match)
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["experimentSetUrn"]), re.Match)
    expected_response = deepcopy(TEST_MINIMAL_EXPERIMENT_RESPONSE)
    expected_response.update({"urn": response_data["urn"], "experimentSetUrn": response_data["experimentSetUrn"]})
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


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
def test_cannot_create_special_fields(client, setup_router_db, test_field, test_value):
    response_data = create_experiment(client, {test_field: test_value})
    if test_field in response_data:
        assert (test_field, response_data[test_field]) != (test_field, test_value)


@pytest.mark.parametrize(
    "test_field,test_value",
    [
        ("createdBy", {"firstName": "Sneaky", "lastName": "User", "orcidId": "0000-9999-9999-0000"}),
        ("modifiedBy", {"firstName": "Sneaky", "lastName": "User", "orcidId": "0000-9999-9999-0000"}),
        ("creationDate", date(2020, 4, 1).isoformat()),
        ("modificationDate", date(2020, 4, 1).isoformat()),
        ("publishedDate", date(2020, 4, 1).isoformat()),
    ],
)
def test_cannot_edit_special_fields(client, setup_router_db, test_field, test_value):
    experiment = create_experiment(client)
    response_data = create_experiment(client, {test_field: test_value, "urn": experiment["urn"]})
    if test_field in response_data:
        assert (test_field, response_data[test_field]) != (test_field, test_value)


def test_cannot_assign_to_missing_experiment_set(client, setup_router_db):
    experiment_set_urn = "tmp:33df10c9-78b3-4e04-bafb-2446078573d7"
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    experiment_post_payload.update({"experimentSetUrn": experiment_set_urn})
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    assert response.status_code == 404
    response_data = response.json()
    assert f"experiment set with URN '{experiment_set_urn}' not found" in response_data["detail"]


def test_cannot_assign_to_other_user_private_experiment_set(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)
    change_ownership(session, experiment["experimentSetUrn"], ExperimentSetDbModel)
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    experiment_post_payload.update({"experimentSetUrn": experiment["experimentSetUrn"]})
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    assert response.status_code == 404
    response_data = response.json()
    assert f"experiment set with URN '{experiment['experimentSetUrn']}' not found" in response_data["detail"]


def test_can_assign_to_own_public_experiment_set(client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_score_set_with_variants(client, experiment["urn"], data_files / "scores.csv")
    published_score_set = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish").json()
    response_data = create_experiment(
        client,
        {"experimentSetUrn": published_score_set["experiment"]["experimentSetUrn"], "title": "Second Experiment"},
    )
    assert response_data["experimentSetUrn"] == published_score_set["experiment"]["experimentSetUrn"]
    assert response_data["title"] == "Second Experiment"


def test_cannot_assign_to_other_user_public_experiment_set(session, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_score_set_with_variants(client, experiment["urn"], data_files / "scores.csv")
    published_score_set = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish").json()
    published_experiment_set_urn = published_score_set["experiment"]["experimentSetUrn"]
    change_ownership(session, published_experiment_set_urn, ExperimentSetDbModel)
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    experiment_post_payload.update({"experimentSetUrn": published_experiment_set_urn, "title": "Second Experiment"})
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    assert response.status_code == 403
    response_data = response.json()
    assert f"insufficient permissions for URN '{published_experiment_set_urn}'" in response_data["detail"]


def test_edit_preserves_optional_metadata(client, setup_router_db):
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
def test_can_edit_private_experiment(client, setup_router_db, test_field, test_value):
    experiment = create_experiment(client)
    response_data = create_experiment(client, {test_field: test_value, "urn": experiment["urn"]})
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    assert (test_field, response_data[test_field]) == (test_field, test_value)


def test_can_edit_published_experiment(client, setup_router_db):
    pass


def test_cannot_edit_unowned_experiment(client, setup_router_db):
    pass


@pytest.mark.parametrize("test_field", ["title", "shortDescription", "abstractText", "methodText"])
def test_required_fields(client, setup_router_db, test_field):
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    del experiment_post_payload[test_field]
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    response_data = response.json()
    assert response.status_code == 422
    assert "field required" in response_data["detail"][0]["msg"]


def test_create_experiment_with_new_primary_pubmed_publication(client, setup_router_db):
    response_data = create_experiment(client, {"primaryPublicationIdentifiers": [{"identifier": "20711194"}]})
    assert len(response_data["primaryPublicationIdentifiers"]) == 1
    assert sorted(response_data["primaryPublicationIdentifiers"][0]) == sorted(
        [
            "abstract",
            "id",
            "authors",
            "dbName",
            "identifier",
            "title",
            "url",
            "referenceHtml",
            "publicationDoi",
            "publicationYear",
            "publicationJournal",
        ]
    )
    # TODO: add separate tests for generating the publication url and referenceHtml


def test_create_experiment_with_new_primary_biorxiv_publication(client, setup_router_db):
    response_data = create_experiment(
        client, {"primaryPublicationIdentifiers": [{"identifier": "2021.06.21.21259225"}]}
    )
    assert len(response_data["primaryPublicationIdentifiers"]) == 1
    assert sorted(response_data["primaryPublicationIdentifiers"][0]) == sorted(
        [
            "abstract",
            "id",
            "authors",
            "dbName",
            "identifier",
            "title",
            "url",
            "referenceHtml",
            "preprintDoi",
            "preprintDate",
            "publicationJournal",
        ]
    )
    # TODO: add separate tests for generating the publication url and referenceHtml


def test_create_experiment_with_invalid_doi(client, setup_router_db):
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    experiment_post_payload.update({"doiIdentifiers": [{"identifier": "20711194"}]})
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    assert response.status_code == 422
    response_data = response.json()
    assert (
        f"'{experiment_post_payload['doiIdentifiers'][0]['identifier']}' is not a valid DOI identifier"
        in response_data["detail"][0]["msg"]
    )


def test_create_experiment_with_invalid_primary_publication(client, setup_router_db):
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    experiment_post_payload.update({"primaryPublicationIdentifiers": [{"identifier": "abcdefg"}]})
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    assert response.status_code == 422
    response_data = response.json()
    assert (
        f"'{experiment_post_payload['primaryPublicationIdentifiers'][0]['identifier']}' is not a valid PubMed, bioRxiv, or medRxiv identifier"
        in response_data["detail"][0]["msg"]
    )


def test_get_own_private_experiment(client, setup_router_db):
    experiment = create_experiment(client)
    expected_response = deepcopy(TEST_MINIMAL_EXPERIMENT_RESPONSE)
    expected_response.update({"urn": experiment["urn"], "experimentSetUrn": experiment["experimentSetUrn"]})
    response = client.get(f"/api/v1/experiments/{experiment['urn']}")
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_cannot_get_other_user_private_experiment(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)
    change_ownership(session, experiment["experimentSetUrn"], ExperimentSetDbModel)
    response = client.get(f"/api/v1/experiments/{experiment['urn']}")
    assert response.status_code == 404
    response_data = response.json()
    assert f"experiment with URN '{experiment['urn']}' not found" in response_data["detail"]

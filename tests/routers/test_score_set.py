import os
from datetime import date
import json
import jsonschema
import re
from copy import deepcopy

from mavedb.view_models.score_set import ScoreSet, ScoreSetCreate
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from tests.conftest import (
    client,
    change_ownership,
    TEST_MINIMAL_SCORE_SET,
    TEST_MINIMAL_SCORE_SET_RESPONSE,
)
from mavedb.lib.validation.urn_re import MAVEDB_TMP_URN_RE


def test_test_minimal_score_set_is_valid():
    jsonschema.validate(instance=TEST_MINIMAL_SCORE_SET, schema=ScoreSetCreate.schema())


def test_create_minimal_score_set(test_score_set_db):
    experiment = test_score_set_db
    score_set_post_payload = deepcopy(TEST_MINIMAL_SCORE_SET)
    score_set_post_payload["experimentUrn"] = experiment["urn"]
    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["urn"]), re.Match)
    expected_response = deepcopy(TEST_MINIMAL_SCORE_SET_RESPONSE)
    expected_response["urn"] = response_data["urn"]
    expected_response["experiment"]["urn"] = experiment["urn"]
    expected_response["experiment"]["experimentSetUrn"] = experiment["experimentSetUrn"]
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert expected_response[key] == response_data[key]
    response = client.get(f"/api/v1/score-sets/{response_data['urn']}")
    assert response.status_code == 200


def test_get_own_private_score_set(test_score_set_db):
    experiment = test_score_set_db
    score_set_post_payload = deepcopy(TEST_MINIMAL_SCORE_SET)
    score_set_post_payload["experimentUrn"] = experiment["urn"]
    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    response_data = response.json()
    expected_response = deepcopy(TEST_MINIMAL_SCORE_SET_RESPONSE)
    expected_response["urn"] = response_data["urn"]
    expected_response["experiment"]["urn"] = experiment["urn"]
    expected_response["experiment"]["experimentSetUrn"] = experiment["experimentSetUrn"]
    response = client.get(f"/api/v1/score-sets/{response_data['urn']}")
    assert response.status_code == 200
    response_data = response.json()
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert expected_response[key] == response_data[key]


def test_get_other_user_private_score_set(test_score_set_db):
    experiment = test_score_set_db
    score_set_post_payload = deepcopy(TEST_MINIMAL_SCORE_SET)
    score_set_post_payload["experimentUrn"] = experiment["urn"]
    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    response_data = response.json()
    score_set_urn = response_data["urn"]
    change_ownership(score_set_urn, ScoreSetDbModel)
    response = client.get(f"/api/v1/score-sets/{score_set_urn}")
    assert response.status_code == 404
    response_data = response.json()
    assert f"score set with URN '{score_set_urn}' not found" in response_data["detail"]


def test_cannot_publish_score_set_without_variants(test_score_set_db):
    experiment = test_score_set_db
    score_set_post_payload = deepcopy(TEST_MINIMAL_SCORE_SET)
    score_set_post_payload["experimentUrn"] = experiment["urn"]
    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    response_data = response.json()
    score_set_urn = response_data["urn"]
    response = client.post(f"/api/v1/score-sets/{score_set_urn}/publish", json=score_set_post_payload)
    assert response.status_code == 422
    assert f"cannot publish score set without variant scores" in response_data["detail"]

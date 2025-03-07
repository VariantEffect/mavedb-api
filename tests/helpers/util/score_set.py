import cdot.hgvs.dataproviders
import jsonschema
from copy import deepcopy
from unittest.mock import patch
from typing import Any, Dict, Optional

from mavedb.view_models.score_set import ScoreSet, ScoreSetCreate

from tests.helpers.constants import TEST_MINIMAL_ACC_SCORESET, TEST_MINIMAL_SEQ_SCORESET, TEST_NT_CDOT_TRANSCRIPT
from fastapi.testclient import TestClient


def create_seq_score_set(
    client: TestClient, experiment_urn: Optional[str], update: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    score_set_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    if experiment_urn is not None:
        score_set_payload["experimentUrn"] = experiment_urn
    if update is not None:
        score_set_payload.update(update)
    jsonschema.validate(instance=score_set_payload, schema=ScoreSetCreate.schema())

    response = client.post("/api/v1/score-sets/", json=score_set_payload)
    assert response.status_code == 200, "Could not create sequence based score set"

    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    return response_data


def create_acc_score_set(
    client: TestClient, experiment_urn: Optional[str], update: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    score_set_payload = deepcopy(TEST_MINIMAL_ACC_SCORESET)
    if experiment_urn is not None:
        score_set_payload["experimentUrn"] = experiment_urn
    if update is not None:
        score_set_payload.update(update)

    jsonschema.validate(instance=score_set_payload, schema=ScoreSetCreate.schema())

    with patch.object(
        cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_NT_CDOT_TRANSCRIPT
    ):
        response = client.post("/api/v1/score-sets/", json=score_set_payload)

    assert response.status_code == 200, "Could not create accession based score set"

    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    return response_data


def publish_score_set(client: TestClient, score_set_urn: str) -> Dict[str, Any]:
    response = client.post(f"/api/v1/score-sets/{score_set_urn}/publish")
    assert response.status_code == 200, f"Could not publish score set {score_set_urn}"

    response_data = response.json()
    return response_data

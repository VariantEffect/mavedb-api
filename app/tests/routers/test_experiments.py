from datetime import date
import json
import re

from app.tests.setup import client, test_empty_db


def test_create_experiment(test_empty_db):
    experiment_to_create = {
        "title": "Test Experiment Title",
        "methodText": "Methods",
        "abstractText": "Abstract",
        "shortDescription": "Test experiment",
        "extraMetadata": {"key": "value"}
    }
    response = client.post("/api/v1/experiments/", json=experiment_to_create)
    assert response.status_code == 200
    response_data = response.json()
    experiment_urn = response_data['urn']
    experiment_set_urn = response_data['experimentSetUrn']
    assert experiment_urn is not None
    assert experiment_set_urn is not None
    assert re.match(r'tmp:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', experiment_urn)
    assert re.match(r'tmp:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', experiment_set_urn)
    added_fields = {
        'urn': experiment_urn,
        'experimentSetUrn': experiment_set_urn,
        'numScoresets': 0,
        'creationDate': date.today().isoformat(),
        'modificationDate': date.today().isoformat(),
        'publishedDate': None
    }
    expected_response = experiment_to_create | added_fields
    assert json.dumps(response_data, sort_keys=True) == json.dumps(expected_response, sort_keys=True)

import jsonschema
from copy import deepcopy
from typing import Any, Dict, Optional

from mavedb.view_models.experiment import Experiment, ExperimentCreate

from tests.helpers.constants import TEST_MINIMAL_EXPERIMENT
from fastapi.testclient import TestClient


def create_experiment(client: TestClient, update: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    experiment_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    if update is not None:
        experiment_payload.update(update)
    jsonschema.validate(instance=experiment_payload, schema=ExperimentCreate.schema())

    response = client.post("/api/v1/experiments/", json=experiment_payload)
    assert response.status_code == 200, "Could not create experiment."

    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    return response_data

import jsonschema
from copy import deepcopy
from typing import Any, Dict, Optional

from mavedb.view_models.collection import Collection

from tests.helpers.constants import TEST_COLLECTION
from fastapi.testclient import TestClient


def create_collection(client: TestClient, update: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    collection_payload = deepcopy(TEST_COLLECTION)
    if update is not None:
        collection_payload.update(update)

    response = client.post("/api/v1/collections/", json=collection_payload)
    assert response.status_code == 200, "Could not create collection."

    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Collection.model_json_schema())
    return response_data

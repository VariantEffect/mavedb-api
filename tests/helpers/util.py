from copy import deepcopy
import jsonschema

from mavedb.models.user import User
from mavedb.view_models.experiment import Experiment, ExperimentCreate
from mavedb.view_models.score_set import ScoreSet, ScoreSetCreate
from tests.helpers.constants import EXTRA_USER, TEST_MINIMAL_EXPERIMENT, TEST_MINIMAL_SCORE_SET


def change_ownership(db, urn, model):
    """Change the ownership of the record with given urn and model to the extra user."""
    item = db.query(model).filter(model.urn == urn).one_or_none()
    assert item is not None
    extra_user = db.query(User).filter(User.username == EXTRA_USER["username"]).one_or_none()
    assert extra_user is not None
    item.created_by_id = extra_user.id
    item.modified_by_id = extra_user.id
    db.add(item)
    db.commit()


def create_experiment(client, update=None):
    experiment_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    if update is not None:
        experiment_payload.update(update)
    jsonschema.validate(instance=experiment_payload, schema=ExperimentCreate.schema())
    response = client.post("/api/v1/experiments/", json=experiment_payload)
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    return response_data


def create_score_set(client, experiment_urn, update=None):
    score_set_payload = deepcopy(TEST_MINIMAL_SCORE_SET)
    if experiment_urn is not None:
        score_set_payload["experimentUrn"] = experiment_urn
    if update is not None:
        score_set_payload.update(update)
    jsonschema.validate(instance=score_set_payload, schema=ScoreSetCreate.schema())
    response = client.post("/api/v1/score-sets/", json=score_set_payload)
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    return response_data


def create_score_set_with_variants(client, experiment_urn, scores_csv_path, update=None, counts_csv_path=None):
    score_set = create_score_set(client, experiment_urn, update)
    score_file = open(scores_csv_path, "rb")
    files = {"scores_file": (scores_csv_path.name, score_file, "rb")}
    if counts_csv_path is not None:
        counts_file = open(counts_csv_path, "rb")
        files["counts_file"] = (counts_csv_path.name, counts_file, "text/csv")
    else:
        counts_file = None
    response = client.post(f"/api/v1/score-sets/{score_set['urn']}/variants/data", files=files)

    score_file.close()
    if counts_file is not None:
        counts_file.close()

    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    return response_data

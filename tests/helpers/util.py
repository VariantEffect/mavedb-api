from copy import deepcopy
import jsonschema

from mavedb.models.user import User
from mavedb.view_models.experiment import Experiment, ExperimentCreate
from mavedb.view_models.score_set import ScoreSet, ScoreSetCreate
from tests.helpers.constants import (
    EXTRA_USER,
    TEST_MINIMAL_EXPERIMENT,
    TEST_MINIMAL_SEQ_SCORESET,
    TEST_MINIMAL_ACC_SCORESET,
)


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
    assert response.status_code == 200, "Could not create experiment."
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    return response_data


def create_seq_score_set(client, experiment_urn, update=None):
    score_set_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    if experiment_urn is not None:
        score_set_payload["experimentUrn"] = experiment_urn
    if update is not None:
        score_set_payload.update(update)
    jsonschema.validate(instance=score_set_payload, schema=ScoreSetCreate.schema())
    response = client.post("/api/v1/score-sets/", json=score_set_payload)
    assert (
        response.status_code == 200
    ), f"Could not create sequence based score set (no variants) within experiment {experiment_urn}"
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    return response_data


def create_acc_score_set(client, experiment_urn, update=None):
    score_set_payload = deepcopy(TEST_MINIMAL_ACC_SCORESET)
    if experiment_urn is not None:
        score_set_payload["experimentUrn"] = experiment_urn
    if update is not None:
        score_set_payload.update(update)
    jsonschema.validate(instance=score_set_payload, schema=ScoreSetCreate.schema())
    response = client.post("/api/v1/score-sets/", json=score_set_payload)
    assert (
        response.status_code == 200
    ), f"Could not create accession based score set (no variants) within experiment {experiment_urn}"
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    return response_data


def create_seq_score_set_with_variants(client, experiment_urn, scores_csv_path, update=None, counts_csv_path=None):
    score_set = create_seq_score_set(client, experiment_urn, update)
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

    assert (
        response.status_code == 200
    ), f"Could not create sequence based score set with variants within experiment {experiment_urn}"
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    return response_data


def create_acc_score_set_with_variants(client, experiment_urn, scores_csv_path, update=None, counts_csv_path=None):
    score_set = create_acc_score_set(client, experiment_urn, update)
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

    assert (
        response.status_code == 200
    ), f"Could not create accession based score set with variants within experiment {experiment_urn}"
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    return response_data


def publish_score_set(client, score_set_urn):
    response = client.post(f"/api/v1/score-sets/{score_set_urn}/publish")
    assert response.status_code == 200, f"Could not publish score set {score_set_urn}"
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    return response_data

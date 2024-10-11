from copy import deepcopy
from unittest.mock import patch

import cdot.hgvs.dataproviders
import jsonschema
from arq import ArqRedis
from sqlalchemy import select

from mavedb.lib.score_sets import columns_for_dataset, create_variants, create_variants_data, csv_data_to_df
from mavedb.lib.validation.dataframe import validate_and_standardize_dataframe_pair
from mavedb.models.contributor import Contributor
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.user import User
from mavedb.view_models.experiment import Experiment, ExperimentCreate
from mavedb.view_models.score_set import ScoreSet, ScoreSetCreate
from tests.helpers.constants import (
    EXTRA_USER,
    TEST_CDOT_TRANSCRIPT,
    TEST_MINIMAL_ACC_SCORESET,
    TEST_MINIMAL_EXPERIMENT,
    TEST_MINIMAL_SEQ_SCORESET,
)


def add_contributor(db, urn, model, orcid_id: str, given_name: str, family_name: str):
    """Without making an API call, add a new contributor to the record (experiment or score set) with given urn and model."""
    item = db.query(model).filter(model.urn == urn).one_or_none()
    assert item is not None
    contributor = Contributor(orcid_id=orcid_id, given_name=given_name, family_name=family_name)
    db.add(contributor)
    item.contributors = [contributor]
    db.add(item)
    db.commit()


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

    with patch.object(cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT):
        response = client.post("/api/v1/score-sets/", json=score_set_payload)

    assert (
        response.status_code == 200
    ), f"Could not create accession based score set (no variants) within experiment {experiment_urn}"

    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    return response_data


def mock_worker_variant_insertion(client, db, data_provider, score_set, scores_csv_path, counts_csv_path):
    with (
        open(scores_csv_path, "rb") as score_file,
        patch.object(ArqRedis, "enqueue_job", return_value=None) as worker_queue,
    ):
        files = {"scores_file": (scores_csv_path.name, score_file, "rb")}

        if counts_csv_path is not None:
            counts_file = open(counts_csv_path, "rb")
            files["counts_file"] = (counts_csv_path.name, counts_file, "rb")
        else:
            counts_file = None

        response = client.post(f"/api/v1/score-sets/{score_set['urn']}/variants/data", files=files)

        # Assert we have mocked a job being added to the queue, and that the request succeeded. The
        # response value here isn't important- we will add variants to the score set manually.
        worker_queue.assert_called_once()
        assert response.status_code == 200

        if counts_file is not None:
            counts_file.close()

    # Reopen files since their buffers are consumed while mocking the variant data post request.
    with open(scores_csv_path, "rb") as score_file:
        score_df = csv_data_to_df(score_file)

    if counts_csv_path is not None:
        with open(scores_csv_path, "rb") as counts_file:
            counts_df = csv_data_to_df(counts_file)
    else:
        counts_df = None

    # Insert variant manually, worker jobs are tested elsewhere separately.
    item = db.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set["urn"])).one_or_none()
    assert item is not None

    scores, counts = validate_and_standardize_dataframe_pair(score_df, counts_df, item.target_genes, data_provider)
    variants = create_variants_data(scores, counts, None)
    num_variants = create_variants(db, item, variants)
    assert num_variants == 3

    item.processing_state = ProcessingState.success
    item.dataset_columns = {
        "score_columns": columns_for_dataset(scores),
        "count_columns": columns_for_dataset(counts),
    }

    db.add(item)
    db.commit()

    return client.get(f"/api/v1/score-sets/{score_set['urn']}").json()


def create_seq_score_set_with_variants(
    client, db, data_provider, experiment_urn, scores_csv_path, update=None, counts_csv_path=None
):
    score_set = create_seq_score_set(client, experiment_urn, update)
    score_set = mock_worker_variant_insertion(client, db, data_provider, score_set, scores_csv_path, counts_csv_path)

    assert (
        score_set["numVariants"] == 3
    ), f"Could not create sequence based score set with variants within experiment {experiment_urn}"

    jsonschema.validate(instance=score_set, schema=ScoreSet.schema())
    return score_set


def create_acc_score_set_with_variants(
    client, db, data_provider, experiment_urn, scores_csv_path, update=None, counts_csv_path=None
):
    score_set = create_acc_score_set(client, experiment_urn, update)
    score_set = mock_worker_variant_insertion(client, db, data_provider, score_set, scores_csv_path, counts_csv_path)

    assert (
        score_set["numVariants"] == 3
    ), f"Could not create sequence based score set with variants within experiment {experiment_urn}"

    jsonschema.validate(instance=score_set, schema=ScoreSet.schema())
    return score_set


def publish_score_set(client, score_set_urn):
    response = client.post(f"/api/v1/score-sets/{score_set_urn}/publish")
    assert response.status_code == 200, f"Could not publish score set {score_set_urn}"

    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    return response_data


def create_api_key_for_current_user(client):
    response = client.post("api/v1/users/me/access-keys")
    assert response.status_code == 200
    return response.json()["keyId"]


def create_admin_key_for_current_user(client):
    response = client.post("api/v1/users/me/access-keys/admin")
    assert response.status_code == 200
    return response.json()["keyId"]


def mark_user_inactive(session, username):
    user = session.query(User).where(User.username == username).one()
    user.is_active = False

    session.add(user)
    session.commit()
    session.refresh(user)

    return user


async def awaitable_exception():
    return Exception()

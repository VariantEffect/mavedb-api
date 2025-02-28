from typing import Optional

from arq import ArqRedis
from cdot.hgvs.dataproviders import RESTDataProvider
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from sqlalchemy import select
from unittest.mock import patch

from mavedb.lib.score_sets import create_variants, create_variants_data, csv_data_to_df
from mavedb.lib.validation.dataframe.dataframe import validate_and_standardize_dataframe_pair
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.score_set import ScoreSet


def mock_worker_variant_insertion(
    client: TestClient,
    db: Session,
    data_provider: RESTDataProvider,
    score_set: dict,
    scores_csv_path: str,
    counts_csv_path: Optional[str] = None,
) -> None:
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
        with open(counts_csv_path, "rb") as counts_file:
            counts_df = csv_data_to_df(counts_file)
    else:
        counts_df = None

    # Insert variant manually, worker jobs are tested elsewhere separately.
    item = db.scalars(select(ScoreSet).where(ScoreSet.urn == score_set["urn"])).one_or_none()
    assert item is not None

    scores, counts = validate_and_standardize_dataframe_pair(score_df, counts_df, item.target_genes, data_provider)
    variants = create_variants_data(scores, counts, None)
    num_variants = create_variants(db, item, variants)
    assert num_variants == 3

    item.processing_state = ProcessingState.success

    db.add(item)
    db.commit()

    return client.get(f"api/v1/score-sets/{score_set['urn']}").json()

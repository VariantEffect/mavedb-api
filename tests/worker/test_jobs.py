from copy import deepcopy
from unittest.mock import patch

import cdot.hgvs.dataproviders
import jsonschema
import pandas as pd
import pytest
from mavedb.lib.mave.constants import HGVS_NT_COLUMN
from mavedb.lib.score_sets import csv_data_to_df
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.variant import Variant
from mavedb.view_models.experiment import Experiment, ExperimentCreate
from mavedb.view_models.score_set import ScoreSet, ScoreSetCreate
from mavedb.worker.jobs import create_variants_for_score_set
from sqlalchemy import select

from tests.helpers.constants import (
    TEST_CDOT_TRANSCRIPT,
    TEST_MINIMAL_ACC_SCORESET,
    TEST_MINIMAL_EXPERIMENT,
    TEST_MINIMAL_SEQ_SCORESET,
)


async def setup_records_and_files(async_client, data_files, input_score_set):
    experiment_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    jsonschema.validate(instance=experiment_payload, schema=ExperimentCreate.schema())
    experiment_response = await async_client.post("/api/v1/experiments/", json=experiment_payload)
    assert experiment_response.status_code == 200
    experiment = experiment_response.json()
    jsonschema.validate(instance=experiment, schema=Experiment.schema())

    score_set_payload = deepcopy(input_score_set)
    score_set_payload["experimentUrn"] = experiment["urn"]
    jsonschema.validate(instance=score_set_payload, schema=ScoreSetCreate.schema())
    score_set_response = await async_client.post("/api/v1/score-sets/", json=score_set_payload)
    assert score_set_response.status_code == 200
    score_set = score_set_response.json()
    jsonschema.validate(instance=score_set, schema=ScoreSet.schema())

    scores_fp = "scores.csv" if "targetSequence" in score_set["targetGenes"][0] else "scores_acc.csv"
    counts_fp = "counts.csv" if "targetSequence" in score_set["targetGenes"][0] else "counts_acc.csv"
    with (
        open(data_files / scores_fp, "rb") as score_file,
        open(data_files / counts_fp, "rb") as count_file,
    ):
        scores = csv_data_to_df(score_file)
        counts = csv_data_to_df(count_file)

    return score_set["urn"], scores, counts


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "input_score_set,validation_error",
    [
        (
            TEST_MINIMAL_SEQ_SCORESET,
            {
                "exception": "encountered 1 invalid variant strings.",
                "detail": ["target sequence mismatch for 'c.1T>A' at row 0 for sequence TEST1"],
            },
        ),
        (
            TEST_MINIMAL_ACC_SCORESET,
            {
                "exception": "encountered 1 invalid variant strings.",
                "detail": [
                    "Failed to parse row 0 with HGVS exception: NM_001637.3:c.1T>A: Variant reference (T) does not agree with reference sequence (G)"
                ],
            },
        ),
    ],
)
async def test_create_variants_for_score_set_with_validation_error(
    input_score_set, validation_error, setup_worker_db, async_client, standalone_worker_context, session, data_files
):
    score_set_urn, scores, counts = await setup_records_and_files(async_client, data_files, input_score_set)

    # This is invalid for both data sets.
    scores.loc[:, HGVS_NT_COLUMN].iloc[0] = "c.1T>A"

    with (
        patch.object(
            cdot.hgvs.dataproviders.RESTDataProvider,
            "_get_transcript",
            return_value=TEST_CDOT_TRANSCRIPT,
        ) as hdp,
    ):
        score_set = await create_variants_for_score_set(standalone_worker_context, score_set_urn, 1, scores, counts)

        # Call data provider _get_transcript method if this is an accession based score set, otherwise do not.
        if all([target.target_sequence is not None for target in score_set.target_genes]):
            hdp.assert_not_called()
        else:
            hdp.assert_called_once()

    db_variants = session.scalars(select(Variant)).all()

    assert score_set.num_variants == 0
    assert len(db_variants) == 0
    assert score_set.processing_state == ProcessingState.failed
    assert score_set.processing_errors == validation_error

    # Have to commit at the end of async tests for DB threads to be released. Otherwise pytest
    # thinks we are still using the session fixture and will hang indefinitely.
    session.commit()


@pytest.mark.asyncio
@pytest.mark.parametrize("input_score_set", (TEST_MINIMAL_SEQ_SCORESET, TEST_MINIMAL_ACC_SCORESET))
async def test_create_variants_for_score_set_with_caught_exception(
    input_score_set, setup_worker_db, async_client, standalone_worker_context, session, data_files
):
    score_set_urn, scores, counts = await setup_records_and_files(async_client, data_files, input_score_set)

    # This is somewhat dumb and wouldn't actually happen like this, but it serves as an effective way to guarantee
    # some exception will be raised no matter what in the async job.
    with (patch.object(pd.DataFrame, "isnull", side_effect=Exception) as mocked_exc,):
        score_set = await create_variants_for_score_set(standalone_worker_context, score_set_urn, 1, scores, counts)
        mocked_exc.assert_called()

    db_variants = session.scalars(select(Variant)).all()

    assert score_set.num_variants == 0
    assert len(db_variants) == 0
    assert score_set.processing_state == ProcessingState.failed
    assert score_set.processing_errors == {"detail": [], "exception": ""}

    # Have to commit at the end of async tests for DB threads to be released. Otherwise pytest
    # thinks we are still using the session fixture and will hang indefinitely.
    session.commit()


@pytest.mark.asyncio
@pytest.mark.parametrize("input_score_set", (TEST_MINIMAL_SEQ_SCORESET, TEST_MINIMAL_ACC_SCORESET))
async def test_create_variants_for_score_set_with_raised_exception(
    input_score_set, setup_worker_db, async_client, standalone_worker_context, session, data_files
):
    score_set_urn, scores, counts = await setup_records_and_files(async_client, data_files, input_score_set)

    # This is somewhat (extra) dumb and wouldn't actually happen like this, but it serves as an effective way to guarantee
    # some exception will be raised no matter what in the async job.
    with (patch.object(pd.DataFrame, "isnull", side_effect=BaseException),):
        with pytest.raises(BaseException):
            score_set = await create_variants_for_score_set(standalone_worker_context, score_set_urn, 1, scores, counts)

    db_variants = session.scalars(select(Variant)).all()
    score_set = session.scalars(select(ScoreSetDbModel)).first()

    assert score_set.num_variants == 0
    assert len(db_variants) == 0
    assert score_set.processing_state == ProcessingState.failed
    assert score_set.processing_errors is None

    # Have to commit at the end of async tests for DB threads to be released. Otherwise pytest
    # thinks we are still using the session fixture and will hang indefinitely.
    session.commit()


@pytest.mark.asyncio
@pytest.mark.parametrize("input_score_set", (TEST_MINIMAL_SEQ_SCORESET, TEST_MINIMAL_ACC_SCORESET))
async def test_create_variants_for_score_set_with_existing_variants(
    input_score_set, setup_worker_db, async_client, standalone_worker_context, session, data_files
):
    score_set_urn, scores, counts = await setup_records_and_files(async_client, data_files, input_score_set)

    with patch.object(
        cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT
    ) as hdp:
        score_set = await create_variants_for_score_set(standalone_worker_context, score_set_urn, 1, scores, counts)

        # Call data provider _get_transcript method if this is an accession based score set, otherwise do not.
        if all([target.target_sequence is not None for target in score_set.target_genes]):
            hdp.assert_not_called()
        else:
            hdp.assert_called_once()

    session = standalone_worker_context["db"]
    db_variants = session.scalars(select(Variant)).all()

    assert score_set.num_variants == 3
    assert len(db_variants) == 3
    assert score_set.processing_state == ProcessingState.success

    with patch.object(
        cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT
    ) as hdp:
        score_set = await create_variants_for_score_set(standalone_worker_context, score_set_urn, 1, scores, counts)

    db_variants = session.scalars(select(Variant)).all()

    assert score_set.num_variants == 3
    assert len(db_variants) == 3
    assert score_set.processing_state == ProcessingState.success
    assert score_set.processing_errors is None

    # Have to commit at the end of async tests for DB threads to be released. Otherwise pytest
    # thinks we are still using the session fixture and will hang indefinitely.
    session.commit()


@pytest.mark.asyncio
@pytest.mark.parametrize("input_score_set", (TEST_MINIMAL_SEQ_SCORESET, TEST_MINIMAL_ACC_SCORESET))
async def test_create_variants_for_score_set_with_existing_exceptions(
    input_score_set, setup_worker_db, async_client, standalone_worker_context, session, data_files
):
    score_set_urn, scores, counts = await setup_records_and_files(async_client, data_files, input_score_set)

    # This is somewhat dumb and wouldn't actually happen like this, but it serves as an effective way to guarantee
    # some exception will be raised no matter what in the async job.
    with (
        patch.object(
            pd.DataFrame, "isnull", side_effect=ValidationError("Test Exception", triggers=["exc_1", "exc_2"])
        ) as mocked_exc,
    ):
        score_set = await create_variants_for_score_set(standalone_worker_context, score_set_urn, 1, scores, counts)
        mocked_exc.assert_called()

    db_variants = session.scalars(select(Variant)).all()

    assert score_set.num_variants == 0
    assert len(db_variants) == 0
    assert score_set.processing_state == ProcessingState.failed
    assert score_set.processing_errors == {"exception": "Test Exception", "detail": ["exc_1", "exc_2"]}

    with patch.object(
        cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT
    ) as hdp:
        score_set = await create_variants_for_score_set(standalone_worker_context, score_set_urn, 1, scores, counts)

        # Call data provider _get_transcript method if this is an accession based score set, otherwise do not.
        if all([target.target_sequence is not None for target in score_set.target_genes]):
            hdp.assert_not_called()
        else:
            hdp.assert_called_once()

    db_variants = session.scalars(select(Variant)).all()

    assert score_set.num_variants == 3
    assert len(db_variants) == 3
    assert score_set.processing_state == ProcessingState.success
    assert score_set.processing_errors is None

    # Have to commit at the end of async tests for DB threads to be released. Otherwise pytest
    # thinks we are still using the session fixture and will hang indefinitely.
    session.commit()


@pytest.mark.asyncio
@pytest.mark.parametrize("input_score_set", (TEST_MINIMAL_SEQ_SCORESET, TEST_MINIMAL_ACC_SCORESET))
async def test_create_variants_for_score_set(
    input_score_set, setup_worker_db, async_client, standalone_worker_context, session, data_files
):
    score_set_urn, scores, counts = await setup_records_and_files(async_client, data_files, input_score_set)

    with patch.object(
        cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT
    ) as hdp:
        score_set = await create_variants_for_score_set(standalone_worker_context, score_set_urn, 1, scores, counts)

        # Call data provider _get_transcript method if this is an accession based score set, otherwise do not.
        if all([target.target_sequence is not None for target in score_set.target_genes]):
            hdp.assert_not_called()
        else:
            hdp.assert_called_once()

    db_variants = session.scalars(select(Variant)).all()

    assert score_set.num_variants == 3
    assert len(db_variants) == 3
    assert score_set.processing_state == ProcessingState.success

    # Have to commit at the end of async tests for DB threads to be released. Otherwise pytest
    # thinks we are still using the session fixture and will hang indefinitely.
    session.commit()

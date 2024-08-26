from datetime import date

from asyncio.unix_events import _UnixSelectorEventLoop
from copy import deepcopy
from requests import HTTPError
from uuid import uuid4
from unittest.mock import patch

import arq.jobs
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
from mavedb.models.mapped_variant import MappedVariant
from mavedb.view_models.experiment import Experiment, ExperimentCreate
from mavedb.view_models.score_set import ScoreSet, ScoreSetCreate
from mavedb.worker.jobs import (
    create_variants_for_score_set,
    map_variants_for_score_set,
    variant_mapper_manager,
    MAPPING_QUEUE_NAME,
)
from sqlalchemy import select

from tests.helpers.constants import (
    TEST_CDOT_TRANSCRIPT,
    TEST_MINIMAL_ACC_SCORESET,
    TEST_MINIMAL_EXPERIMENT,
    TEST_MINIMAL_SEQ_SCORESET,
    TEST_VARIANT_MAPPING_SCAFFOLD,
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


async def setup_records_files_and_variants(async_client, data_files, input_score_set, worker_ctx):
    urn, scores, counts = await setup_records_and_files(async_client, data_files, input_score_set)

    # Patch CDOT `_get_transcript`, in the event this function is called on an accesssion based scoreset.
    with patch.object(cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT):
        score_set = await create_variants_for_score_set(worker_ctx, urn, 1, scores, counts)

    assert score_set.processing_state is ProcessingState.success
    assert score_set.num_variants == 3

    return score_set


async def setup_mapping_output(async_client, session, score_set, empty=False):
    score_set_response = await async_client.get(f"/api/v1/score-set/{score_set.urn}")

    mapping_output = deepcopy(TEST_VARIANT_MAPPING_SCAFFOLD)
    mapping_output["metadata"] = score_set_response.json()

    if empty:
        return mapping_output

    variants = session.scalars(select(Variant).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).all()
    for variant in variants:
        mapped_score = {
            "pre_mapped": {"test": "pre_mapped_output"},
            "pre_mapped_2_0": {"test": "pre_mapped_output (2.0)"},
            "post_mapped": {"test": "post_mapped_output"},
            "post_mapped_2_0": {"test": "post_mapped_output (2.0)"},
            "mavedb_id": variant.urn,
        }

        mapping_output["mapped_scores"].append(mapped_score)

    return mapping_output


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
        success = await create_variants_for_score_set(
            standalone_worker_context, uuid4().hex, score_set_urn, 1, scores, counts
        )

        # Call data provider _get_transcript method if this is an accession based score set, otherwise do not.
        if all(["targetSequence" in target for target in input_score_set["targetGenes"]]):
            hdp.assert_not_called()
        else:
            hdp.assert_called_once()

    db_variants = session.scalars(select(Variant)).all()

    score_set = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set_urn).one()
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
        success = await create_variants_for_score_set(
            standalone_worker_context, uuid4().hex, score_set_urn, 1, scores, counts
        )
        mocked_exc.assert_called()

    db_variants = session.scalars(select(Variant)).all()
    score_set = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set_urn).one()

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
            success = await create_variants_for_score_set(
                standalone_worker_context, uuid4().hex, score_set_urn, 1, scores, counts
            )

    db_variants = session.scalars(select(Variant)).all()
    score_set = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set_urn).one()

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
        success = await create_variants_for_score_set(
            standalone_worker_context, uuid4().hex, score_set_urn, 1, scores, counts
        )

        # Call data provider _get_transcript method if this is an accession based score set, otherwise do not.
        if all(["targetSequence" in target for target in input_score_set["targetGenes"]]):
            hdp.assert_not_called()
        else:
            hdp.assert_called_once()

    db_variants = session.scalars(select(Variant)).all()
    score_set = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set_urn).one()

    assert score_set.num_variants == 3
    assert len(db_variants) == 3
    assert score_set.processing_state == ProcessingState.success

    with patch.object(
        cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT
    ) as hdp:
        success = await create_variants_for_score_set(
            standalone_worker_context, uuid4().hex, score_set_urn, 1, scores, counts
        )

    db_variants = session.scalars(select(Variant)).all()
    score_set = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set_urn).one()

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
        success = await create_variants_for_score_set(
            standalone_worker_context, uuid4().hex, score_set_urn, 1, scores, counts
        )
        mocked_exc.assert_called()

    db_variants = session.scalars(select(Variant)).all()
    score_set = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set_urn).one()

    assert score_set.num_variants == 0
    assert len(db_variants) == 0
    assert score_set.processing_state == ProcessingState.failed
    assert score_set.processing_errors == {"exception": "Test Exception", "detail": ["exc_1", "exc_2"]}

    with patch.object(
        cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT
    ) as hdp:
        success = await create_variants_for_score_set(
            standalone_worker_context, uuid4().hex, score_set_urn, 1, scores, counts
        )

        # Call data provider _get_transcript method if this is an accession based score set, otherwise do not.
        if all(["targetSequence" in target for target in input_score_set["targetGenes"]]):
            hdp.assert_not_called()
        else:
            hdp.assert_called_once()

    db_variants = session.scalars(select(Variant)).all()
    score_set = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set_urn).one()

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
        success = await create_variants_for_score_set(
            standalone_worker_context, uuid4().hex, score_set_urn, 1, scores, counts
        )

        # Call data provider _get_transcript method if this is an accession based score set, otherwise do not.
        if all(["targetSequence" in target for target in input_score_set["targetGenes"]]):
            hdp.assert_not_called()
        else:
            hdp.assert_called_once()

    db_variants = session.scalars(select(Variant)).all()
    score_set = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set_urn).one()

    assert score_set.num_variants == 3
    assert len(db_variants) == 3
    assert score_set.processing_state == ProcessingState.success

    # Have to commit at the end of async tests for DB threads to be released. Otherwise pytest
    # thinks we are still using the session fixture and will hang indefinitely.
    session.commit()


# NOTE: These tests operate under the assumption that mapping output is consistent between accession based and sequence based score sets. If
# this assumption changes in the future, tests reflecting this difference in output should be added for accession based score sets.


@pytest.mark.asyncio
async def test_create_mapped_variants_for_scoreset(
    setup_worker_db,
    async_client,
    standalone_worker_context,
    session,
    data_files,
):
    score_set = await setup_records_files_and_variants(
        async_client, data_files, TEST_MINIMAL_SEQ_SCORESET, standalone_worker_context
    )

    # Do not await, we need a co-routine object to be the return value of our `run_in_executor` mock.
    mapping_test_output_for_score_set = setup_mapping_output(async_client, session, score_set)

    # We seem unable to mock requests via requests_mock that occur inside another event loop. Workaround
    # this limitation by instead patching the _UnixSelectorEventLoop 's executor function, with a coroutine
    # object that sets up test mappingn output.
    #
    # TODO: Does this work on non-unix based machines.
    # TODO: Is it even a safe operation to patch this event loop method?
    with patch.object(_UnixSelectorEventLoop, "run_in_executor", return_value=mapping_test_output_for_score_set):
        await map_variants_for_score_set(standalone_worker_context, score_set.urn)

    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()
    assert len(mapped_variants_for_score_set) == score_set.num_variants

    # Have to commit at the end of async tests for DB threads to be released. Otherwise pytest
    # thinks we are still using the session fixture and will hang indefinitely.
    session.commit()


@pytest.mark.asyncio
async def test_create_mapped_variants_for_scoreset_with_existing_mapped_variants(
    setup_worker_db, async_client, standalone_worker_context, session, data_files
):
    score_set = await setup_records_files_and_variants(
        async_client, data_files, TEST_MINIMAL_SEQ_SCORESET, standalone_worker_context
    )

    # Do not await, we need a co-routine object to be the return value of our `run_in_executor` mock.
    mapping_test_output_for_score_set = setup_mapping_output(async_client, session, score_set)

    # We seem unable to mock requests via requests_mock that occur inside another event loop. Workaround
    # this limitation by instead patching the _UnixSelectorEventLoop 's executor function, with a coroutine
    # object that sets up test mappingn output.
    #
    # TODO: Does this work on non-unix based machines.
    # TODO: Is it even a safe operation to patch this event loop method?
    with patch.object(_UnixSelectorEventLoop, "run_in_executor", return_value=mapping_test_output_for_score_set):
        existing_variant = session.scalars(select(Variant)).first()

        if not existing_variant:
            raise ValueError

        session.add(
            MappedVariant(
                pre_mapped={"preexisting": "variant"},
                post_mapped={"preexisting": "variant"},
                variant_id=existing_variant.id,
                modification_date=date.today(),
                mapped_date=date.today(),
                vrs_version="2.0",
                mapping_api_version="0.0.0",
            )
        )
        session.commit()

        await map_variants_for_score_set(standalone_worker_context, score_set.urn)

    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()
    assert len(mapped_variants_for_score_set) == score_set.num_variants

    # Have to commit at the end of async tests for DB threads to be released. Otherwise pytest
    # thinks we are still using the session fixture and will hang indefinitely.
    session.commit()


@pytest.mark.asyncio
async def test_create_mapped_variants_for_scoreset_mapping_exception(
    setup_worker_db, async_client, standalone_worker_context, session, data_files
):
    async def awaitable_http_error():
        raise HTTPError

    score_set = await setup_records_files_and_variants(
        async_client, data_files, TEST_MINIMAL_SEQ_SCORESET, standalone_worker_context
    )

    # Do not await, we need a co-routine object which raises an http error once awaited.
    mapping_test_output_for_score_set = awaitable_http_error()

    # We seem unable to mock requests via requests_mock that occur inside another event loop. Workaround
    # this limitation by instead patching the _UnixSelectorEventLoop 's executor function, with a coroutine
    # object that sets up test mappingn output.
    #
    # TODO: Does this work on non-unix based machines?
    # TODO: Is it even a safe operation to patch this event loop method?
    with patch.object(_UnixSelectorEventLoop, "run_in_executor", return_value=mapping_test_output_for_score_set):
        await map_variants_for_score_set(standalone_worker_context, score_set.urn)

    # TODO: How are errors persisted? Test persistence mechanism.
    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()
    assert len(mapped_variants_for_score_set) == 0

    # Have to commit at the end of async tests for DB threads to be released. Otherwise pytest
    # thinks we are still using the session fixture and will hang indefinitely.
    session.commit()


@pytest.mark.asyncio
async def test_create_mapped_variants_for_scoreset_no_mapping_output(
    setup_worker_db, async_client, standalone_worker_context, session, data_files
):
    score_set = await setup_records_files_and_variants(
        async_client, data_files, TEST_MINIMAL_SEQ_SCORESET, standalone_worker_context
    )

    # Do not await, we need a co-routine object to be the return value of our `run_in_executor` mock.
    mapping_test_output_for_score_set = setup_mapping_output(async_client, session, score_set, empty=True)

    # We seem unable to mock requests via requests_mock that occur inside another event loop. Workaround
    # this limitation by instead patching the _UnixSelectorEventLoop 's executor function, with a coroutine
    # object that sets up test mappingn output.
    #
    # TODO: Does this work on non-unix based machines.
    # TODO: Is it even a safe operation to patch this event loop method?
    with patch.object(_UnixSelectorEventLoop, "run_in_executor", return_value=mapping_test_output_for_score_set):
        await map_variants_for_score_set(standalone_worker_context, score_set.urn)

    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()
    assert len(mapped_variants_for_score_set) == 0

    # Have to commit at the end of async tests for DB threads to be released. Otherwise pytest
    # thinks we are still using the session fixture and will hang indefinitely.
    session.commit()


@pytest.mark.asyncio
async def test_mapping_manager_empty_queue(setup_worker_db, standalone_worker_context, session):
    queued_job = await variant_mapper_manager(standalone_worker_context)

    # No new jobs should have been created if nothing is in the queue.
    assert queued_job is None
    session.commit()


@pytest.mark.asyncio
async def test_mapping_manager_occupied_queue_mapping_in_progress(setup_worker_db, standalone_worker_context, session):
    await standalone_worker_context["redis"].lpush(MAPPING_QUEUE_NAME, "mavedb:test-urn")

    with patch.object(arq.jobs.Job, "status", return_value=arq.jobs.JobStatus.in_progress):
        queued_job = await variant_mapper_manager(standalone_worker_context)

    # Execution should be deferred if a job is in progress.
    assert await queued_job.status() is arq.jobs.JobStatus.deferred
    session.commit()


@pytest.mark.asyncio
async def test_mapping_manager_occupied_queue_mapping_not_in_progress(
    setup_worker_db, standalone_worker_context, session
):
    await standalone_worker_context["redis"].lpush(MAPPING_QUEUE_NAME, "mavedb:test-urn")

    with patch.object(arq.jobs.Job, "status", return_value=arq.jobs.JobStatus.not_found):
        queued_job = await variant_mapper_manager(standalone_worker_context)

    # VRS Mapping jobs have the same ID.
    assert queued_job.job_id == "vrs_map"
    session.commit()

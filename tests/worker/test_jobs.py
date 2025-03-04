from asyncio.unix_events import _UnixSelectorEventLoop
from copy import deepcopy
from datetime import date
from unittest.mock import patch
from uuid import uuid4

import arq.jobs
import cdot.hgvs.dataproviders
import jsonschema
import pandas as pd
import pytest
from arq import ArqRedis
from sqlalchemy import not_, select

from mavedb.data_providers.services import VRSMap
from mavedb.lib.mave.constants import HGVS_NT_COLUMN
from mavedb.lib.score_sets import csv_data_to_df
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.variant import Variant
from mavedb.view_models.experiment import Experiment, ExperimentCreate
from mavedb.view_models.score_set import ScoreSet, ScoreSetCreate
from mavedb.worker.jobs import (
    BACKOFF_LIMIT,
    MAPPING_CURRENT_ID_NAME,
    MAPPING_QUEUE_NAME,
    create_variants_for_score_set,
    map_variants_for_score_set,
    variant_mapper_manager,
)
from tests.helpers.constants import (
    TEST_CDOT_TRANSCRIPT,
    TEST_MINIMAL_ACC_SCORESET,
    TEST_MINIMAL_EXPERIMENT,
    TEST_MINIMAL_SEQ_SCORESET,
    TEST_VARIANT_MAPPING_SCAFFOLD,
    VALID_ACCESSION,
)
from tests.helpers.util import awaitable_exception


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


async def setup_records_files_and_variants(session, async_client, data_files, input_score_set, worker_ctx):
    score_set_urn, scores, counts = await setup_records_and_files(async_client, data_files, input_score_set)
    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()

    # Patch CDOT `_get_transcript`, in the event this function is called on an accesssion based scoreset.
    with patch.object(
        cdot.hgvs.dataproviders.RESTDataProvider,
        "_get_transcript",
        return_value=TEST_CDOT_TRANSCRIPT,
    ):
        result = await create_variants_for_score_set(worker_ctx, uuid4().hex, score_set.id, 1, scores, counts)

    score_set_with_variants = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()

    assert result["success"]
    assert score_set.processing_state is ProcessingState.success
    assert score_set_with_variants.num_variants == 3

    return score_set_with_variants


async def sanitize_mapping_queue(standalone_worker_context, score_set):
    queued_job = await standalone_worker_context["redis"].rpop(MAPPING_QUEUE_NAME)
    assert int(queued_job.decode("utf-8")) == score_set.id


async def setup_mapping_output(async_client, session, score_set, empty=False):
    score_set_response = await async_client.get(f"/api/v1/score-sets/{score_set.urn}")

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
    input_score_set,
    validation_error,
    setup_worker_db,
    async_client,
    standalone_worker_context,
    session,
    data_files,
):
    score_set_urn, scores, counts = await setup_records_and_files(async_client, data_files, input_score_set)
    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()

    if input_score_set == TEST_MINIMAL_SEQ_SCORESET:
        scores.loc[:, HGVS_NT_COLUMN].iloc[0] = "c.1T>A"
    else:
        scores.loc[:, HGVS_NT_COLUMN].iloc[0] = f"{VALID_ACCESSION}:c.1T>A"

    with (
        patch.object(
            cdot.hgvs.dataproviders.RESTDataProvider,
            "_get_transcript",
            return_value=TEST_CDOT_TRANSCRIPT,
        ) as hdp,
    ):
        result = await create_variants_for_score_set(
            standalone_worker_context, uuid4().hex, score_set.id, 1, scores, counts
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
    assert not result["success"]
    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("input_score_set", (TEST_MINIMAL_SEQ_SCORESET, TEST_MINIMAL_ACC_SCORESET))
async def test_create_variants_for_score_set_with_caught_exception(
    input_score_set,
    setup_worker_db,
    async_client,
    standalone_worker_context,
    session,
    data_files,
):
    score_set_urn, scores, counts = await setup_records_and_files(async_client, data_files, input_score_set)
    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()

    # This is somewhat dumb and wouldn't actually happen like this, but it serves as an effective way to guarantee
    # some exception will be raised no matter what in the async job.
    with (
        patch.object(pd.DataFrame, "isnull", side_effect=Exception) as mocked_exc,
    ):
        result = await create_variants_for_score_set(
            standalone_worker_context, uuid4().hex, score_set.id, 1, scores, counts
        )
        mocked_exc.assert_called()

    db_variants = session.scalars(select(Variant)).all()
    score_set = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set_urn).one()

    assert score_set.num_variants == 0
    assert len(db_variants) == 0
    assert score_set.processing_state == ProcessingState.failed
    assert score_set.processing_errors == {"detail": [], "exception": ""}
    assert not result["success"]
    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("input_score_set", (TEST_MINIMAL_SEQ_SCORESET, TEST_MINIMAL_ACC_SCORESET))
async def test_create_variants_for_score_set_with_caught_base_exception(
    input_score_set,
    setup_worker_db,
    async_client,
    standalone_worker_context,
    session,
    data_files,
):
    score_set_urn, scores, counts = await setup_records_and_files(async_client, data_files, input_score_set)
    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()

    # This is somewhat (extra) dumb and wouldn't actually happen like this, but it serves as an effective way to guarantee
    # some base exception will be handled no matter what in the async job.
    with (
        patch.object(pd.DataFrame, "isnull", side_effect=BaseException),
    ):
        result = await create_variants_for_score_set(
            standalone_worker_context, uuid4().hex, score_set.id, 1, scores, counts
        )

    db_variants = session.scalars(select(Variant)).all()
    score_set = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set_urn).one()

    assert score_set.num_variants == 0
    assert len(db_variants) == 0
    assert score_set.processing_state == ProcessingState.failed
    assert score_set.processing_errors is None
    assert not result["success"]
    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("input_score_set", (TEST_MINIMAL_SEQ_SCORESET, TEST_MINIMAL_ACC_SCORESET))
async def test_create_variants_for_score_set_with_existing_variants(
    input_score_set,
    setup_worker_db,
    async_client,
    standalone_worker_context,
    session,
    data_files,
):
    score_set_urn, scores, counts = await setup_records_and_files(async_client, data_files, input_score_set)
    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()

    with patch.object(
        cdot.hgvs.dataproviders.RESTDataProvider,
        "_get_transcript",
        return_value=TEST_CDOT_TRANSCRIPT,
    ) as hdp:
        result = await create_variants_for_score_set(
            standalone_worker_context, uuid4().hex, score_set.id, 1, scores, counts
        )

        # Call data provider _get_transcript method if this is an accession based score set, otherwise do not.
        if all(["targetSequence" in target for target in input_score_set["targetGenes"]]):
            hdp.assert_not_called()
        else:
            hdp.assert_called_once()

    await sanitize_mapping_queue(standalone_worker_context, score_set)
    db_variants = session.scalars(select(Variant)).all()
    score_set = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set_urn).one()

    assert score_set.num_variants == 3
    assert len(db_variants) == 3
    assert score_set.processing_state == ProcessingState.success

    with patch.object(
        cdot.hgvs.dataproviders.RESTDataProvider,
        "_get_transcript",
        return_value=TEST_CDOT_TRANSCRIPT,
    ) as hdp:
        result = await create_variants_for_score_set(
            standalone_worker_context, uuid4().hex, score_set.id, 1, scores, counts
        )

    db_variants = session.scalars(select(Variant)).all()
    score_set = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set_urn).one()

    assert score_set.num_variants == 3
    assert len(db_variants) == 3
    assert score_set.processing_state == ProcessingState.success
    assert score_set.processing_errors is None
    assert result["success"]
    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("input_score_set", (TEST_MINIMAL_SEQ_SCORESET, TEST_MINIMAL_ACC_SCORESET))
async def test_create_variants_for_score_set_with_existing_exceptions(
    input_score_set,
    setup_worker_db,
    async_client,
    standalone_worker_context,
    session,
    data_files,
):
    score_set_urn, scores, counts = await setup_records_and_files(async_client, data_files, input_score_set)
    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()

    # This is somewhat dumb and wouldn't actually happen like this, but it serves as an effective way to guarantee
    # some exception will be raised no matter what in the async job.
    with (
        patch.object(
            pd.DataFrame,
            "isnull",
            side_effect=ValidationError("Test Exception", triggers=["exc_1", "exc_2"]),
        ) as mocked_exc,
    ):
        result = await create_variants_for_score_set(
            standalone_worker_context, uuid4().hex, score_set.id, 1, scores, counts
        )
        mocked_exc.assert_called()

    db_variants = session.scalars(select(Variant)).all()
    score_set = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set_urn).one()

    assert score_set.num_variants == 0
    assert len(db_variants) == 0
    assert score_set.processing_state == ProcessingState.failed
    assert score_set.processing_errors == {
        "exception": "Test Exception",
        "detail": ["exc_1", "exc_2"],
    }

    with patch.object(
        cdot.hgvs.dataproviders.RESTDataProvider,
        "_get_transcript",
        return_value=TEST_CDOT_TRANSCRIPT,
    ) as hdp:
        result = await create_variants_for_score_set(
            standalone_worker_context, uuid4().hex, score_set.id, 1, scores, counts
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
    assert result["success"]
    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("input_score_set", (TEST_MINIMAL_SEQ_SCORESET, TEST_MINIMAL_ACC_SCORESET))
async def test_create_variants_for_score_set(
    input_score_set,
    setup_worker_db,
    async_client,
    standalone_worker_context,
    session,
    data_files,
):
    score_set_urn, scores, counts = await setup_records_and_files(async_client, data_files, input_score_set)
    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()

    with patch.object(
        cdot.hgvs.dataproviders.RESTDataProvider,
        "_get_transcript",
        return_value=TEST_CDOT_TRANSCRIPT,
    ) as hdp:
        result = await create_variants_for_score_set(
            standalone_worker_context, uuid4().hex, score_set.id, 1, scores, counts
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
    assert result["success"]
    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("input_score_set", (TEST_MINIMAL_SEQ_SCORESET, TEST_MINIMAL_ACC_SCORESET))
async def test_create_variants_for_score_set_enqueues_manager_and_successful_mapping(
    input_score_set,
    setup_worker_db,
    session,
    async_client,
    data_files,
    arq_worker,
    arq_redis,
):
    score_set_urn, scores, counts = await setup_records_and_files(async_client, data_files, input_score_set)
    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()

    async def dummy_mapping_job():
        return await setup_mapping_output(async_client, session, score_set)

    with (
        patch.object(
            cdot.hgvs.dataproviders.RESTDataProvider,
            "_get_transcript",
            return_value=TEST_CDOT_TRANSCRIPT,
        ) as hdp,
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            return_value=dummy_mapping_job(),
        ),
        patch("mavedb.worker.jobs.BACKOFF_IN_SECONDS", 0),
    ):
        await arq_redis.enqueue_job("create_variants_for_score_set", uuid4().hex, score_set.id, 1, scores, counts)
        await arq_worker.async_run()
        await arq_worker.run_check()

        # Call data provider _get_transcript method if this is an accession based score set, otherwise do not.
        if all(["targetSequence" in target for target in input_score_set["targetGenes"]]):
            hdp.assert_not_called()
        else:
            hdp.assert_called_once()

    db_variants = session.scalars(select(Variant)).all()
    score_set = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set_urn).one()
    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()

    assert score_set.num_variants == 3
    assert len(db_variants) == 3
    assert score_set.processing_state == ProcessingState.success
    assert (await arq_redis.llen(MAPPING_QUEUE_NAME)) == 0
    assert (await arq_redis.get(MAPPING_CURRENT_ID_NAME)).decode("utf-8") == ""
    assert len(mapped_variants_for_score_set) == score_set.num_variants
    assert score_set.mapping_state == MappingState.complete
    assert score_set.mapping_errors is None


@pytest.mark.asyncio
@pytest.mark.parametrize("input_score_set", (TEST_MINIMAL_SEQ_SCORESET, TEST_MINIMAL_ACC_SCORESET))
async def test_create_variants_for_score_set_exception_skips_mapping(
    input_score_set,
    setup_worker_db,
    session,
    async_client,
    data_files,
    arq_worker,
    arq_redis,
):
    score_set_urn, scores, counts = await setup_records_and_files(async_client, data_files, input_score_set)
    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()

    with patch.object(pd.DataFrame, "isnull", side_effect=Exception) as mocked_exc:
        await arq_redis.enqueue_job("create_variants_for_score_set", uuid4().hex, score_set.id, 1, scores, counts)
        await arq_worker.async_run()
        await arq_worker.run_check()

        mocked_exc.assert_called()

    db_variants = session.scalars(select(Variant)).all()
    score_set = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set_urn).one()
    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()

    assert score_set.num_variants == 0
    assert len(db_variants) == 0
    assert score_set.processing_state == ProcessingState.failed
    assert score_set.processing_errors == {"detail": [], "exception": ""}
    assert (await arq_redis.llen(MAPPING_QUEUE_NAME)) == 0
    assert len(mapped_variants_for_score_set) == 0
    assert score_set.mapping_state == MappingState.not_attempted
    assert score_set.mapping_errors is None


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
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )
    # The call to `create_variants_from_score_set` within the above `setup_records_files_and_variants` will
    # add a score set to the queue. Since we are executing the mapping independent of the manager job, we should
    # sanitize the queue as if the mananger process had run.
    await sanitize_mapping_queue(standalone_worker_context, score_set)

    async def dummy_mapping_job():
        return await setup_mapping_output(async_client, session, score_set)

    # We seem unable to mock requests via requests_mock that occur inside another event loop. Workaround
    # this limitation by instead patching the _UnixSelectorEventLoop 's executor function, with a coroutine
    # object that sets up test mappingn output.
    with patch.object(
        _UnixSelectorEventLoop,
        "run_in_executor",
        return_value=dummy_mapping_job(),
    ):
        result = await map_variants_for_score_set(standalone_worker_context, uuid4().hex, score_set.id, 1)

    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()
    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()
    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 0
    assert (await standalone_worker_context["redis"].get(MAPPING_CURRENT_ID_NAME)).decode("utf-8") == ""
    assert result["success"]
    assert not result["retried"]
    assert len(mapped_variants_for_score_set) == score_set.num_variants
    assert score_set.mapping_state == MappingState.complete
    assert score_set.mapping_errors is None


@pytest.mark.asyncio
async def test_create_mapped_variants_for_scoreset_with_existing_mapped_variants(
    setup_worker_db, async_client, standalone_worker_context, session, data_files
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )
    # The call to `create_variants_from_score_set` within the above `setup_records_files_and_variants` will
    # add a score set to the queue. Since we are executing the mapping independent of the manager job, we should
    # sanitize the queue as if the mananger process had run.
    await sanitize_mapping_queue(standalone_worker_context, score_set)

    async def dummy_mapping_job():
        return await setup_mapping_output(async_client, session, score_set)

    # We seem unable to mock requests via requests_mock that occur inside another event loop. Workaround
    # this limitation by instead patching the _UnixSelectorEventLoop 's executor function, with a coroutine
    # object that sets up test mappingn output.
    with patch.object(
        _UnixSelectorEventLoop,
        "run_in_executor",
        return_value=dummy_mapping_job(),
    ):
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
                current=True,
            )
        )
        session.commit()

        result = await map_variants_for_score_set(standalone_worker_context, uuid4().hex, score_set.id, 1)

    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()
    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()
    preexisting_variants = session.scalars(
        select(MappedVariant)
        .join(Variant)
        .join(ScoreSetDbModel)
        .filter(ScoreSetDbModel.urn == score_set.urn, not_(MappedVariant.current))
    ).all()
    new_variants = session.scalars(
        select(MappedVariant)
        .join(Variant)
        .join(ScoreSetDbModel)
        .filter(ScoreSetDbModel.urn == score_set.urn, MappedVariant.current)
    ).all()
    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 0
    assert (await standalone_worker_context["redis"].get(MAPPING_CURRENT_ID_NAME)).decode("utf-8") == ""
    assert result["success"]
    assert not result["retried"]
    assert len(mapped_variants_for_score_set) == score_set.num_variants + 1
    assert len(preexisting_variants) == 1
    assert len(new_variants) == score_set.num_variants
    assert score_set.mapping_state == MappingState.complete
    assert score_set.mapping_errors is None


@pytest.mark.asyncio
async def test_create_mapped_variants_for_scoreset_exception_in_mapping_setup_score_set_selection(
    setup_worker_db, async_client, standalone_worker_context, session, data_files
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )
    # The call to `create_variants_from_score_set` within the above `setup_records_files_and_variants` will
    # add a score set to the queue. Since we are executing the mapping independent of the manager job, we should
    # sanitize the queue as if the mananger process had run.
    await sanitize_mapping_queue(standalone_worker_context, score_set)

    # We seem unable to mock requests via requests_mock that occur inside another event loop. Workaround
    # this limitation by instead patching the _UnixSelectorEventLoop 's executor function, with a coroutine
    # object that sets up test mappingn output.
    with patch.object(
        _UnixSelectorEventLoop,
        "run_in_executor",
        return_value=awaitable_exception(),
    ):
        result = await map_variants_for_score_set(standalone_worker_context, uuid4().hex, score_set.id + 5, 1)

    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()
    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()

    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 0
    assert (await standalone_worker_context["redis"].get(MAPPING_CURRENT_ID_NAME)).decode("utf-8") == ""
    assert not result["success"]
    assert not result["retried"]
    assert len(mapped_variants_for_score_set) == 0
    # When we cannot fetch a score set, these fields are unable to be updated.
    assert score_set.mapping_state == MappingState.queued
    assert score_set.mapping_errors is None


@pytest.mark.asyncio
async def test_create_mapped_variants_for_scoreset_exception_in_mapping_setup_vrs_object(
    setup_worker_db, async_client, standalone_worker_context, session, data_files
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )
    # The call to `create_variants_from_score_set` within the above `setup_records_files_and_variants` will
    # add a score set to the queue. Since we are executing the mapping independent of the manager job, we should
    # sanitize the queue as if the mananger process had run.
    await sanitize_mapping_queue(standalone_worker_context, score_set)

    with patch.object(
        VRSMap,
        "__init__",
        return_value=Exception(),
    ):
        result = await map_variants_for_score_set(standalone_worker_context, uuid4().hex, score_set.id, 1)

    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()
    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()

    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 0
    assert (await standalone_worker_context["redis"].get(MAPPING_CURRENT_ID_NAME)).decode("utf-8") == ""
    assert not result["success"]
    assert not result["retried"]
    assert len(mapped_variants_for_score_set) == 0
    assert score_set.mapping_state == MappingState.failed
    assert score_set.mapping_errors is not None


@pytest.mark.asyncio
async def test_create_mapped_variants_for_scoreset_mapping_exception(
    setup_worker_db, async_client, standalone_worker_context, session, data_files
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )
    # The call to `create_variants_from_score_set` within the above `setup_records_files_and_variants` will
    # add a score set to the queue. Since we are executing the mapping independent of the manager job, we should
    # sanitize the queue as if the mananger process had run.
    await sanitize_mapping_queue(standalone_worker_context, score_set)

    # We seem unable to mock requests via requests_mock that occur inside another event loop. Workaround
    # this limitation by instead patching the _UnixSelectorEventLoop 's executor function, with a coroutine
    # object that sets up test mappingn output.
    with patch.object(
        _UnixSelectorEventLoop,
        "run_in_executor",
        return_value=awaitable_exception(),
    ):
        result = await map_variants_for_score_set(standalone_worker_context, uuid4().hex, score_set.id, 1)

    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()
    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()

    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 1
    assert (await standalone_worker_context["redis"].get(MAPPING_CURRENT_ID_NAME)).decode("utf-8") == ""
    assert not result["success"]
    assert result["retried"]
    assert len(mapped_variants_for_score_set) == 0
    assert score_set.mapping_state == MappingState.queued
    assert score_set.mapping_errors is not None


@pytest.mark.asyncio
async def test_create_mapped_variants_for_scoreset_mapping_exception_retry_limit_reached(
    setup_worker_db, async_client, standalone_worker_context, session, data_files
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )
    # The call to `create_variants_from_score_set` within the above `setup_records_files_and_variants` will
    # add a score set to the queue. Since we are executing the mapping independent of the manager job, we should
    # sanitize the queue as if the mananger process had run.
    await sanitize_mapping_queue(standalone_worker_context, score_set)

    # We seem unable to mock requests via requests_mock that occur inside another event loop. Workaround
    # this limitation by instead patching the _UnixSelectorEventLoop 's executor function, with a coroutine
    # object that sets up test mappingn output.
    with patch.object(
        _UnixSelectorEventLoop,
        "run_in_executor",
        return_value=awaitable_exception(),
    ):
        result = await map_variants_for_score_set(
            standalone_worker_context, uuid4().hex, score_set.id, 1, BACKOFF_LIMIT + 1
        )

    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()
    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()

    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 0
    assert (await standalone_worker_context["redis"].get(MAPPING_CURRENT_ID_NAME)).decode("utf-8") == ""
    assert not result["success"]
    assert not result["retried"]
    assert len(mapped_variants_for_score_set) == 0
    assert score_set.mapping_state == MappingState.failed
    assert score_set.mapping_errors is not None


@pytest.mark.asyncio
async def test_create_mapped_variants_for_scoreset_mapping_exception_retry_failed(
    setup_worker_db, async_client, standalone_worker_context, session, data_files
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )
    # The call to `create_variants_from_score_set` within the above `setup_records_files_and_variants` will
    # add a score set to the queue. Since we are executing the mapping independent of the manager job, we should
    # sanitize the queue as if the mananger process had run.
    await sanitize_mapping_queue(standalone_worker_context, score_set)

    # We seem unable to mock requests via requests_mock that occur inside another event loop. Workaround
    # this limitation by instead patching the _UnixSelectorEventLoop 's executor function, with a coroutine
    # object that sets up test mappingn output.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            return_value=awaitable_exception(),
        ),
        patch.object(ArqRedis, "lpush", awaitable_exception()),
    ):
        result = await map_variants_for_score_set(standalone_worker_context, uuid4().hex, score_set.id, 1)

    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()
    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()

    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 0
    assert (await standalone_worker_context["redis"].get(MAPPING_CURRENT_ID_NAME)).decode("utf-8") == ""
    assert not result["success"]
    assert not result["retried"]
    assert len(mapped_variants_for_score_set) == 0
    # Behavior for exception in mapping is retried job
    assert score_set.mapping_state == MappingState.failed
    assert score_set.mapping_errors is not None


@pytest.mark.asyncio
async def test_create_mapped_variants_for_scoreset_parsing_exception_with_retry(
    setup_worker_db, async_client, standalone_worker_context, session, data_files
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )
    # The call to `create_variants_from_score_set` within the above `setup_records_files_and_variants` will
    # add a score set to the queue. Since we are executing the mapping independent of the manager job, we should
    # sanitize the queue as if the mananger process had run.
    await sanitize_mapping_queue(standalone_worker_context, score_set)

    async def dummy_mapping_job():
        mapping_test_output_for_score_set = await setup_mapping_output(async_client, session, score_set)
        mapping_test_output_for_score_set.pop("computed_genomic_reference_sequence")
        return mapping_test_output_for_score_set

    # We seem unable to mock requests via requests_mock that occur inside another event loop. Workaround
    # this limitation by instead patching the _UnixSelectorEventLoop 's executor function, with a coroutine
    # object that sets up test mappingn output.
    with patch.object(
        _UnixSelectorEventLoop,
        "run_in_executor",
        return_value=dummy_mapping_job(),
    ):
        result = await map_variants_for_score_set(standalone_worker_context, uuid4().hex, score_set.id, 1)

    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()
    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()

    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 1
    assert (await standalone_worker_context["redis"].get(MAPPING_CURRENT_ID_NAME)).decode("utf-8") == ""
    assert not result["success"]
    assert result["retried"]
    assert len(mapped_variants_for_score_set) == 0
    assert score_set.mapping_state == MappingState.queued
    assert score_set.mapping_errors is not None


@pytest.mark.asyncio
async def test_create_mapped_variants_for_scoreset_parsing_exception_retry_failed(
    setup_worker_db, async_client, standalone_worker_context, session, data_files
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )
    # The call to `create_variants_from_score_set` within the above `setup_records_files_and_variants` will
    # add a score set to the queue. Since we are executing the mapping independent of the manager job, we should
    # sanitize the queue as if the mananger process had run.
    await sanitize_mapping_queue(standalone_worker_context, score_set)

    async def dummy_mapping_job():
        mapping_test_output_for_score_set = await setup_mapping_output(async_client, session, score_set)
        mapping_test_output_for_score_set.pop("computed_genomic_reference_sequence")
        return mapping_test_output_for_score_set

    # We seem unable to mock requests via requests_mock that occur inside another event loop. Workaround
    # this limitation by instead patching the _UnixSelectorEventLoop 's executor function, with a coroutine
    # object that sets up test mappingn output.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            return_value=dummy_mapping_job(),
        ),
        patch.object(ArqRedis, "lpush", awaitable_exception()),
    ):
        result = await map_variants_for_score_set(standalone_worker_context, uuid4().hex, score_set.id, 1)

    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()
    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()

    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 0
    assert (await standalone_worker_context["redis"].get(MAPPING_CURRENT_ID_NAME)).decode("utf-8") == ""
    assert not result["success"]
    assert not result["retried"]
    assert len(mapped_variants_for_score_set) == 0
    # Behavior for exception outside mapping is failed job
    assert score_set.mapping_state == MappingState.failed
    assert score_set.mapping_errors is not None


@pytest.mark.asyncio
async def test_create_mapped_variants_for_scoreset_parsing_exception_retry_limit_reached(
    setup_worker_db, async_client, standalone_worker_context, session, data_files
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )
    # The call to `create_variants_from_score_set` within the above `setup_records_files_and_variants` will
    # add a score set to the queue. Since we are executing the mapping independent of the manager job, we should
    # sanitize the queue as if the mananger process had run.
    await sanitize_mapping_queue(standalone_worker_context, score_set)

    async def dummy_mapping_job():
        mapping_test_output_for_score_set = await setup_mapping_output(async_client, session, score_set)
        mapping_test_output_for_score_set.pop("computed_genomic_reference_sequence")
        return mapping_test_output_for_score_set

    # We seem unable to mock requests via requests_mock that occur inside another event loop. Workaround
    # this limitation by instead patching the _UnixSelectorEventLoop 's executor function, with a coroutine
    # object that sets up test mappingn output.
    with patch.object(
        _UnixSelectorEventLoop,
        "run_in_executor",
        return_value=dummy_mapping_job(),
    ):
        result = await map_variants_for_score_set(
            standalone_worker_context, uuid4().hex, score_set.id, 1, BACKOFF_LIMIT + 1
        )

    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()
    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()

    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 0
    assert (await standalone_worker_context["redis"].get(MAPPING_CURRENT_ID_NAME)).decode("utf-8") == ""
    assert not result["success"]
    assert not result["retried"]
    assert len(mapped_variants_for_score_set) == 0
    # Behavior for exception outside mapping is failed job
    assert score_set.mapping_state == MappingState.failed
    assert score_set.mapping_errors is not None


@pytest.mark.asyncio
async def test_create_mapped_variants_for_scoreset_no_mapping_output(
    setup_worker_db, async_client, standalone_worker_context, session, data_files
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )
    # The call to `create_variants_from_score_set` within the above `setup_records_files_and_variants` will
    # add a score set to the queue. Since we are executing the mapping independent of the manager job, we should
    # sanitize the queue as if the mananger process had run.
    await sanitize_mapping_queue(standalone_worker_context, score_set)

    # Do not await, we need a co-routine object to be the return value of our `run_in_executor` mock.
    async def dummy_mapping_job():
        return await setup_mapping_output(async_client, session, score_set, empty=True)

    # We seem unable to mock requests via requests_mock that occur inside another event loop. Workaround
    # this limitation by instead patching the _UnixSelectorEventLoop 's executor function, with a coroutine
    # object that sets up test mappingn output.
    with patch.object(
        _UnixSelectorEventLoop,
        "run_in_executor",
        return_value=dummy_mapping_job(),
    ):
        result = await map_variants_for_score_set(standalone_worker_context, uuid4().hex, score_set.id, 1)

    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()
    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()

    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 0
    assert (await standalone_worker_context["redis"].get(MAPPING_CURRENT_ID_NAME)).decode("utf-8") == ""
    assert result["success"]
    assert not result["retried"]
    assert len(mapped_variants_for_score_set) == 0
    assert score_set.mapping_state == MappingState.failed


@pytest.mark.asyncio
async def test_mapping_manager_empty_queue(setup_worker_db, standalone_worker_context):
    result = await variant_mapper_manager(standalone_worker_context, uuid4().hex, 1)

    # No new jobs should have been created if nothing is in the queue, and the queue should remain empty.
    assert result["enqueued_job"] is None
    assert result["success"]
    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 0
    assert (await standalone_worker_context["redis"].get(MAPPING_CURRENT_ID_NAME)).decode("utf-8") == ""


@pytest.mark.asyncio
async def test_mapping_manager_empty_queue_error_during_setup(setup_worker_db, standalone_worker_context):
    await standalone_worker_context["redis"].set(MAPPING_CURRENT_ID_NAME, "")
    with patch.object(ArqRedis, "rpop", Exception()):
        result = await variant_mapper_manager(standalone_worker_context, uuid4().hex, 1)

    # No new jobs should have been created if nothing is in the queue, and the queue should remain empty.
    assert result["enqueued_job"] is None
    assert not result["success"]
    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 0
    assert (await standalone_worker_context["redis"].get(MAPPING_CURRENT_ID_NAME)).decode("utf-8") == ""


@pytest.mark.asyncio
async def test_mapping_manager_occupied_queue_mapping_in_progress(
    setup_worker_db, standalone_worker_context, session, async_client, data_files
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    await standalone_worker_context["redis"].set(MAPPING_CURRENT_ID_NAME, "5")
    with patch.object(arq.jobs.Job, "status", return_value=arq.jobs.JobStatus.in_progress):
        result = await variant_mapper_manager(standalone_worker_context, uuid4().hex, 1)

    # Execution should be deferred if a job is in progress, and the queue should contain one entry which is the deferred ID.
    assert result["enqueued_job"] is not None
    assert (
        await arq.jobs.Job(result["enqueued_job"], standalone_worker_context["redis"]).status()
    ) == arq.jobs.JobStatus.deferred
    assert result["success"]
    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 1
    assert (await standalone_worker_context["redis"].rpop(MAPPING_QUEUE_NAME)).decode("utf-8") == str(score_set.id)
    assert (await standalone_worker_context["redis"].get(MAPPING_CURRENT_ID_NAME)).decode("utf-8") == "5"
    assert score_set.mapping_state == MappingState.queued
    assert score_set.mapping_errors is None


@pytest.mark.asyncio
async def test_mapping_manager_occupied_queue_mapping_not_in_progress(
    setup_worker_db, standalone_worker_context, session, async_client, data_files
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    await standalone_worker_context["redis"].set(MAPPING_CURRENT_ID_NAME, "")
    with patch.object(arq.jobs.Job, "status", return_value=arq.jobs.JobStatus.not_found):
        result = await variant_mapper_manager(standalone_worker_context, uuid4().hex, 1)

    # Mapping job should be queued if none is currently running, and the queue should now be empty.
    assert result["enqueued_job"] is not None
    assert (
        await arq.jobs.Job(result["enqueued_job"], standalone_worker_context["redis"]).status()
    ) == arq.jobs.JobStatus.queued
    assert result["success"]
    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 0
    # We don't actually start processing these score sets.
    assert score_set.mapping_state == MappingState.queued
    assert score_set.mapping_errors is None


@pytest.mark.asyncio
async def test_mapping_manager_occupied_queue_mapping_in_progress_error_during_enqueue(
    setup_worker_db, standalone_worker_context, session, async_client, data_files
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    await standalone_worker_context["redis"].set(MAPPING_CURRENT_ID_NAME, "5")
    with (
        patch.object(arq.jobs.Job, "status", return_value=arq.jobs.JobStatus.in_progress),
        patch.object(ArqRedis, "enqueue_job", return_value=awaitable_exception()),
    ):
        result = await variant_mapper_manager(standalone_worker_context, uuid4().hex, 1)

    # Execution should be deferred if a job is in progress, and the queue should contain one entry which is the deferred ID.
    assert result["enqueued_job"] is None
    assert not result["success"]
    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 0
    assert (await standalone_worker_context["redis"].get(MAPPING_CURRENT_ID_NAME)).decode("utf-8") == "5"
    assert score_set.mapping_state == MappingState.failed
    assert score_set.mapping_errors is not None


@pytest.mark.asyncio
async def test_mapping_manager_occupied_queue_mapping_not_in_progress_error_during_enqueue(
    setup_worker_db, standalone_worker_context, session, async_client, data_files
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    await standalone_worker_context["redis"].set(MAPPING_CURRENT_ID_NAME, "")
    with (
        patch.object(arq.jobs.Job, "status", return_value=arq.jobs.JobStatus.not_found),
        patch.object(ArqRedis, "enqueue_job", return_value=awaitable_exception()),
    ):
        result = await variant_mapper_manager(standalone_worker_context, uuid4().hex, 1)

    # Enqueue would have failed, the job is unsuccessful, and we remove the queued item.
    assert result["enqueued_job"] is None
    assert not result["success"]
    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 0
    assert score_set.mapping_state == MappingState.failed
    assert score_set.mapping_errors is not None


@pytest.mark.asyncio
async def test_mapping_manager_multiple_score_sets_occupy_queue_mapping_in_progress(
    setup_worker_db, standalone_worker_context, session, async_client, data_files
):
    score_set_id_1 = (
        await setup_records_files_and_variants(
            session,
            async_client,
            data_files,
            TEST_MINIMAL_SEQ_SCORESET,
            standalone_worker_context,
        )
    ).id
    score_set_id_2 = (
        await setup_records_files_and_variants(
            session,
            async_client,
            data_files,
            TEST_MINIMAL_SEQ_SCORESET,
            standalone_worker_context,
        )
    ).id
    score_set_id_3 = (
        await setup_records_files_and_variants(
            session,
            async_client,
            data_files,
            TEST_MINIMAL_SEQ_SCORESET,
            standalone_worker_context,
        )
    ).id

    await standalone_worker_context["redis"].set(MAPPING_CURRENT_ID_NAME, "5")
    with patch.object(arq.jobs.Job, "status", return_value=arq.jobs.JobStatus.in_progress):
        result1 = await variant_mapper_manager(standalone_worker_context, uuid4().hex, 1)
        result2 = await variant_mapper_manager(standalone_worker_context, uuid4().hex, 1)
        result3 = await variant_mapper_manager(standalone_worker_context, uuid4().hex, 1)

    # All three jobs should complete successfully...
    assert result1["success"]
    assert result2["success"]
    assert result3["success"]

    # ...with a new job enqueued...
    assert result1["enqueued_job"] is not None
    assert result2["enqueued_job"] is not None
    assert result3["enqueued_job"] is not None

    # ...of which all should be deferred jobs of the "variant_mapper_manager" variety...
    assert (
        await arq.jobs.Job(result1["enqueued_job"], standalone_worker_context["redis"]).status()
    ) == arq.jobs.JobStatus.deferred
    assert (
        await arq.jobs.Job(result2["enqueued_job"], standalone_worker_context["redis"]).status()
    ) == arq.jobs.JobStatus.deferred
    assert (
        await arq.jobs.Job(result3["enqueued_job"], standalone_worker_context["redis"]).status()
    ) == arq.jobs.JobStatus.deferred

    assert (
        await arq.jobs.Job(result1["enqueued_job"], standalone_worker_context["redis"]).info()
    ).function == "variant_mapper_manager"
    assert (
        await arq.jobs.Job(result2["enqueued_job"], standalone_worker_context["redis"]).info()
    ).function == "variant_mapper_manager"
    assert (
        await arq.jobs.Job(result3["enqueued_job"], standalone_worker_context["redis"]).info()
    ).function == "variant_mapper_manager"

    # ...and the queue state should have three jobs, each of our three created score sets.
    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 3
    assert (await standalone_worker_context["redis"].rpop(MAPPING_QUEUE_NAME)).decode("utf-8") == str(score_set_id_1)
    assert (await standalone_worker_context["redis"].rpop(MAPPING_QUEUE_NAME)).decode("utf-8") == str(score_set_id_2)
    assert (await standalone_worker_context["redis"].rpop(MAPPING_QUEUE_NAME)).decode("utf-8") == str(score_set_id_3)

    score_set1 = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.id == score_set_id_1)).one()
    score_set2 = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.id == score_set_id_2)).one()
    score_set3 = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.id == score_set_id_3)).one()
    # Each score set should remain queued with no mapping errors.
    assert score_set1.mapping_state == MappingState.queued
    assert score_set2.mapping_state == MappingState.queued
    assert score_set3.mapping_state == MappingState.queued
    assert score_set1.mapping_errors is None
    assert score_set2.mapping_errors is None
    assert score_set3.mapping_errors is None


@pytest.mark.asyncio
async def test_mapping_manager_multiple_score_sets_occupy_queue_mapping_not_in_progress(
    setup_worker_db, standalone_worker_context, session, async_client, data_files
):
    score_set_id_1 = (
        await setup_records_files_and_variants(
            session,
            async_client,
            data_files,
            TEST_MINIMAL_SEQ_SCORESET,
            standalone_worker_context,
        )
    ).id
    score_set_id_2 = (
        await setup_records_files_and_variants(
            session,
            async_client,
            data_files,
            TEST_MINIMAL_SEQ_SCORESET,
            standalone_worker_context,
        )
    ).id
    score_set_id_3 = (
        await setup_records_files_and_variants(
            session,
            async_client,
            data_files,
            TEST_MINIMAL_SEQ_SCORESET,
            standalone_worker_context,
        )
    ).id

    await standalone_worker_context["redis"].set(MAPPING_CURRENT_ID_NAME, "")
    with patch.object(arq.jobs.Job, "status", return_value=arq.jobs.JobStatus.not_found):
        result1 = await variant_mapper_manager(standalone_worker_context, uuid4().hex, 1)

    # Mock the first job being in-progress
    await standalone_worker_context["redis"].set(MAPPING_CURRENT_ID_NAME, str(score_set_id_1))
    with patch.object(arq.jobs.Job, "status", return_value=arq.jobs.JobStatus.in_progress):
        result2 = await variant_mapper_manager(standalone_worker_context, uuid4().hex, 1)
        result3 = await variant_mapper_manager(standalone_worker_context, uuid4().hex, 1)

    # All three jobs should complete successfully...
    assert result1["success"]
    assert result2["success"]
    assert result3["success"]

    # ...with a new job enqueued...
    assert result1["enqueued_job"] is not None
    assert result2["enqueued_job"] is not None
    assert result3["enqueued_job"] is not None

    # ...of which the first should be a queued job of the "map_variants_for_score_set" variety and the other two should be
    # deferred jobs of the "variant_mapper_manager" variety...
    assert (
        await arq.jobs.Job(result1["enqueued_job"], standalone_worker_context["redis"]).status()
    ) == arq.jobs.JobStatus.queued
    assert (
        await arq.jobs.Job(result2["enqueued_job"], standalone_worker_context["redis"]).status()
    ) == arq.jobs.JobStatus.deferred
    assert (
        await arq.jobs.Job(result3["enqueued_job"], standalone_worker_context["redis"]).status()
    ) == arq.jobs.JobStatus.deferred

    assert (
        await arq.jobs.Job(result1["enqueued_job"], standalone_worker_context["redis"]).info()
    ).function == "map_variants_for_score_set"
    assert (
        await arq.jobs.Job(result2["enqueued_job"], standalone_worker_context["redis"]).info()
    ).function == "variant_mapper_manager"
    assert (
        await arq.jobs.Job(result3["enqueued_job"], standalone_worker_context["redis"]).info()
    ).function == "variant_mapper_manager"

    # ...and the queue state should have two jobs, neither of which should be the first score set.
    assert (await standalone_worker_context["redis"].llen(MAPPING_QUEUE_NAME)) == 2
    assert (await standalone_worker_context["redis"].rpop(MAPPING_QUEUE_NAME)).decode("utf-8") == str(score_set_id_2)
    assert (await standalone_worker_context["redis"].rpop(MAPPING_QUEUE_NAME)).decode("utf-8") == str(score_set_id_3)

    score_set1 = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.id == score_set_id_1)).one()
    score_set2 = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.id == score_set_id_2)).one()
    score_set3 = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.id == score_set_id_3)).one()
    # We don't actually process any score sets in the manager job, and each should have no mapping errors.
    assert score_set1.mapping_state == MappingState.queued
    assert score_set2.mapping_state == MappingState.queued
    assert score_set3.mapping_state == MappingState.queued
    assert score_set1.mapping_errors is None
    assert score_set2.mapping_errors is None
    assert score_set3.mapping_errors is None


@pytest.mark.asyncio
async def test_mapping_manager_enqueues_mapping_process_with_successful_mapping(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    async def dummy_mapping_job():
        return await setup_mapping_output(async_client, session, score_set)

    # We seem unable to mock requests via requests_mock that occur inside another event loop. Workaround
    # this limitation by instead patching the _UnixSelectorEventLoop 's executor function, with a coroutine
    # object that sets up test mappingn output.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            return_value=dummy_mapping_job(),
        ),
        patch("mavedb.worker.jobs.BACKOFF_IN_SECONDS", 0),
    ):
        await arq_redis.enqueue_job("variant_mapper_manager", uuid4().hex, 1)
        await arq_worker.async_run()
        await arq_worker.run_check()

    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()
    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()
    assert (await arq_redis.llen(MAPPING_QUEUE_NAME)) == 0
    assert (await arq_redis.get(MAPPING_CURRENT_ID_NAME)).decode("utf-8") == ""
    assert len(mapped_variants_for_score_set) == score_set.num_variants
    assert score_set.mapping_state == MappingState.complete
    assert score_set.mapping_errors is None


@pytest.mark.asyncio
async def test_mapping_manager_enqueues_mapping_process_with_retried_mapping_successful_mapping_on_retry(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    async def failed_mapping_job():
        return Exception()

    async def dummy_mapping_job():
        return await setup_mapping_output(async_client, session, score_set)

    # We seem unable to mock requests via requests_mock that occur inside another event loop. Workaround
    # this limitation by instead patching the _UnixSelectorEventLoop 's executor function, with a coroutine
    # object that sets up test mappingn output.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            side_effect=[failed_mapping_job(), dummy_mapping_job()],
        ),
        patch("mavedb.worker.jobs.BACKOFF_IN_SECONDS", 0),
    ):
        await arq_redis.enqueue_job("variant_mapper_manager", uuid4().hex, 1)
        await arq_worker.async_run()
        await arq_worker.run_check()

    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()
    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()
    assert (await arq_redis.llen(MAPPING_QUEUE_NAME)) == 0
    assert (await arq_redis.get(MAPPING_CURRENT_ID_NAME)).decode("utf-8") == ""
    assert len(mapped_variants_for_score_set) == score_set.num_variants
    assert score_set.mapping_state == MappingState.complete
    assert score_set.mapping_errors is None


@pytest.mark.asyncio
async def test_mapping_manager_enqueues_mapping_process_with_unsuccessful_mapping(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    async def failed_mapping_job():
        return Exception()

    # We seem unable to mock requests via requests_mock that occur inside another event loop. Workaround
    # this limitation by instead patching the _UnixSelectorEventLoop 's executor function, with a coroutine
    # object that sets up test mappingn output.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            side_effect=[failed_mapping_job()] * 5,
        ),
        patch("mavedb.worker.jobs.BACKOFF_IN_SECONDS", 0),
    ):
        await arq_redis.enqueue_job("variant_mapper_manager", uuid4().hex, 1)
        await arq_worker.async_run()
        await arq_worker.run_check()

    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()
    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()
    assert (await arq_redis.llen(MAPPING_QUEUE_NAME)) == 0
    assert (await arq_redis.get(MAPPING_CURRENT_ID_NAME)).decode("utf-8") == ""
    assert len(mapped_variants_for_score_set) == 0
    assert score_set.mapping_state == MappingState.failed
    assert score_set.mapping_errors is not None

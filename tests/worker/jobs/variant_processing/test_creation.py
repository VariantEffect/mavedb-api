# ruff: noqa: E402

from asyncio.unix_events import _UnixSelectorEventLoop
from unittest.mock import patch
from uuid import uuid4

import pandas as pd
import pytest
from sqlalchemy import select

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")

from mavedb.lib.clingen.services import (
    ClinGenLdhService,
)
from mavedb.lib.mave.constants import HGVS_NT_COLUMN
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.variant import Variant
from mavedb.worker.jobs import (
    create_variants_for_score_set,
)
from mavedb.worker.jobs.utils.constants import MAPPING_CURRENT_ID_NAME, MAPPING_QUEUE_NAME
from tests.helpers.constants import (
    TEST_CLINGEN_ALLELE_OBJECT,
    TEST_CLINGEN_LDH_LINKING_RESPONSE,
    TEST_CLINGEN_SUBMISSION_RESPONSE,
    TEST_MINIMAL_ACC_SCORESET,
    TEST_MINIMAL_MULTI_TARGET_SCORESET,
    TEST_MINIMAL_SEQ_SCORESET,
    TEST_NT_CDOT_TRANSCRIPT,
    VALID_NT_ACCESSION,
)
from tests.helpers.util.mapping import sanitize_mapping_queue
from tests.helpers.util.setup.worker import setup_mapping_output, setup_records_and_files


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
                    "Failed to parse row 0 with HGVS exception: NM_001637.3:c.1T>A: Variant reference (T) does not agree with reference sequence (G)."
                ],
            },
        ),
        (
            TEST_MINIMAL_MULTI_TARGET_SCORESET,
            {
                "exception": "encountered 1 invalid variant strings.",
                "detail": ["target sequence mismatch for 'n.1T>A' at row 0 for sequence TEST3"],
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
    score_set_urn, scores, counts, score_columns_metadata, count_columns_metadata = await setup_records_and_files(
        async_client, data_files, input_score_set
    )
    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()

    if input_score_set == TEST_MINIMAL_SEQ_SCORESET:
        scores.loc[:, HGVS_NT_COLUMN].iloc[0] = "c.1T>A"
    elif input_score_set == TEST_MINIMAL_ACC_SCORESET:
        scores.loc[:, HGVS_NT_COLUMN].iloc[0] = f"{VALID_NT_ACCESSION}:c.1T>A"
    elif input_score_set == TEST_MINIMAL_MULTI_TARGET_SCORESET:
        scores.loc[:, HGVS_NT_COLUMN].iloc[0] = "TEST3:n.1T>A"

    with (
        patch.object(
            cdot.hgvs.dataproviders.RESTDataProvider,
            "_get_transcript",
            return_value=TEST_NT_CDOT_TRANSCRIPT,
        ) as hdp,
    ):
        result = await create_variants_for_score_set(
            standalone_worker_context,
            uuid4().hex,
            score_set.id,
            1,
            scores,
            counts,
            score_columns_metadata,
            count_columns_metadata,
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
@pytest.mark.parametrize(
    "input_score_set", (TEST_MINIMAL_SEQ_SCORESET, TEST_MINIMAL_ACC_SCORESET, TEST_MINIMAL_MULTI_TARGET_SCORESET)
)
async def test_create_variants_for_score_set_with_caught_exception(
    input_score_set,
    setup_worker_db,
    async_client,
    standalone_worker_context,
    session,
    data_files,
):
    score_set_urn, scores, counts, score_columns_metadata, count_columns_metadata = await setup_records_and_files(
        async_client, data_files, input_score_set
    )
    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()

    # This is somewhat dumb and wouldn't actually happen like this, but it serves as an effective way to guarantee
    # some exception will be raised no matter what in the async job.
    with (
        patch.object(pd.DataFrame, "isnull", side_effect=Exception) as mocked_exc,
    ):
        result = await create_variants_for_score_set(
            standalone_worker_context,
            uuid4().hex,
            score_set.id,
            1,
            scores,
            counts,
            score_columns_metadata,
            count_columns_metadata,
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
@pytest.mark.parametrize(
    "input_score_set", (TEST_MINIMAL_SEQ_SCORESET, TEST_MINIMAL_ACC_SCORESET, TEST_MINIMAL_MULTI_TARGET_SCORESET)
)
async def test_create_variants_for_score_set_with_caught_base_exception(
    input_score_set,
    setup_worker_db,
    async_client,
    standalone_worker_context,
    session,
    data_files,
):
    score_set_urn, scores, counts, score_columns_metadata, count_columns_metadata = await setup_records_and_files(
        async_client, data_files, input_score_set
    )
    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()

    # This is somewhat (extra) dumb and wouldn't actually happen like this, but it serves as an effective way to guarantee
    # some base exception will be handled no matter what in the async job.
    with (
        patch.object(pd.DataFrame, "isnull", side_effect=BaseException),
    ):
        result = await create_variants_for_score_set(
            standalone_worker_context,
            uuid4().hex,
            score_set.id,
            1,
            scores,
            counts,
            score_columns_metadata,
            count_columns_metadata,
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
@pytest.mark.parametrize(
    "input_score_set", (TEST_MINIMAL_SEQ_SCORESET, TEST_MINIMAL_ACC_SCORESET, TEST_MINIMAL_MULTI_TARGET_SCORESET)
)
async def test_create_variants_for_score_set_with_existing_variants(
    input_score_set,
    setup_worker_db,
    async_client,
    standalone_worker_context,
    session,
    data_files,
):
    score_set_urn, scores, counts, score_columns_metadata, count_columns_metadata = await setup_records_and_files(
        async_client, data_files, input_score_set
    )
    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()

    with patch.object(
        cdot.hgvs.dataproviders.RESTDataProvider,
        "_get_transcript",
        return_value=TEST_NT_CDOT_TRANSCRIPT,
    ) as hdp:
        result = await create_variants_for_score_set(
            standalone_worker_context,
            uuid4().hex,
            score_set.id,
            1,
            scores,
            counts,
            score_columns_metadata,
            count_columns_metadata,
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
        return_value=TEST_NT_CDOT_TRANSCRIPT,
    ) as hdp:
        result = await create_variants_for_score_set(
            standalone_worker_context,
            uuid4().hex,
            score_set.id,
            1,
            scores,
            counts,
            score_columns_metadata,
            count_columns_metadata,
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
@pytest.mark.parametrize(
    "input_score_set", (TEST_MINIMAL_SEQ_SCORESET, TEST_MINIMAL_ACC_SCORESET, TEST_MINIMAL_MULTI_TARGET_SCORESET)
)
async def test_create_variants_for_score_set_with_existing_exceptions(
    input_score_set,
    setup_worker_db,
    async_client,
    standalone_worker_context,
    session,
    data_files,
):
    score_set_urn, scores, counts, score_columns_metadata, count_columns_metadata = await setup_records_and_files(
        async_client, data_files, input_score_set
    )
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
            standalone_worker_context,
            uuid4().hex,
            score_set.id,
            1,
            scores,
            counts,
            score_columns_metadata,
            count_columns_metadata,
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
        return_value=TEST_NT_CDOT_TRANSCRIPT,
    ) as hdp:
        result = await create_variants_for_score_set(
            standalone_worker_context,
            uuid4().hex,
            score_set.id,
            1,
            scores,
            counts,
            score_columns_metadata,
            count_columns_metadata,
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
@pytest.mark.parametrize(
    "input_score_set", (TEST_MINIMAL_SEQ_SCORESET, TEST_MINIMAL_ACC_SCORESET, TEST_MINIMAL_MULTI_TARGET_SCORESET)
)
async def test_create_variants_for_score_set(
    input_score_set,
    setup_worker_db,
    async_client,
    standalone_worker_context,
    session,
    data_files,
):
    score_set_urn, scores, counts, score_columns_metadata, count_columns_metadata = await setup_records_and_files(
        async_client, data_files, input_score_set
    )
    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()

    with patch.object(
        cdot.hgvs.dataproviders.RESTDataProvider,
        "_get_transcript",
        return_value=TEST_NT_CDOT_TRANSCRIPT,
    ) as hdp:
        result = await create_variants_for_score_set(
            standalone_worker_context,
            uuid4().hex,
            score_set.id,
            1,
            scores,
            counts,
            score_columns_metadata,
            count_columns_metadata,
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
@pytest.mark.parametrize(
    "input_score_set", (TEST_MINIMAL_SEQ_SCORESET, TEST_MINIMAL_ACC_SCORESET, TEST_MINIMAL_MULTI_TARGET_SCORESET)
)
async def test_create_variants_for_score_set_enqueues_manager_and_successful_mapping(
    input_score_set,
    setup_worker_db,
    session,
    async_client,
    data_files,
    arq_worker,
    arq_redis,
):
    score_set_is_seq = all(["targetSequence" in target for target in input_score_set["targetGenes"]])
    score_set_is_multi_target = len(input_score_set["targetGenes"]) > 1
    score_set_urn, scores, counts, score_columns_metadata, count_columns_metadata = await setup_records_and_files(
        async_client, data_files, input_score_set
    )
    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()

    async def dummy_mapping_job():
        return await setup_mapping_output(async_client, session, score_set, score_set_is_seq, score_set_is_multi_target)

    async def dummy_car_submission_job():
        return TEST_CLINGEN_ALLELE_OBJECT

    async def dummy_ldh_submission_job():
        return [TEST_CLINGEN_SUBMISSION_RESPONSE, None]

    # Variants have not yet been created, so infer their URNs.
    async def dummy_linking_job():
        return [(f"{score_set_urn}#{i}", TEST_CLINGEN_LDH_LINKING_RESPONSE) for i in range(1, len(scores) + 1)]

    with (
        patch.object(
            cdot.hgvs.dataproviders.RESTDataProvider,
            "_get_transcript",
            return_value=TEST_NT_CDOT_TRANSCRIPT,
        ) as hdp,
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            side_effect=[
                dummy_mapping_job(),
                dummy_car_submission_job(),
                dummy_ldh_submission_job(),
                dummy_linking_job(),
            ],
        ),
        patch.object(ClinGenLdhService, "_existing_jwt", return_value="test_jwt"),
        patch("mavedb.worker.jobs.variant_processing.mapping.MAPPING_BACKOFF_IN_SECONDS", 0),
        patch("mavedb.worker.jobs.external_services.clingen.LINKING_BACKOFF_IN_SECONDS", 0),
        patch("mavedb.worker.jobs.variant_processing.mapping.CLIN_GEN_SUBMISSION_ENABLED", True),
    ):
        await arq_redis.enqueue_job(
            "create_variants_for_score_set",
            uuid4().hex,
            score_set.id,
            1,
            scores,
            counts,
            score_columns_metadata,
            count_columns_metadata,
        )
        await arq_worker.async_run()
        await arq_worker.run_check()

        # Call data provider _get_transcript method if this is an accession based score set, otherwise do not.
        if score_set_is_seq:
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
@pytest.mark.parametrize(
    "input_score_set", (TEST_MINIMAL_SEQ_SCORESET, TEST_MINIMAL_ACC_SCORESET, TEST_MINIMAL_MULTI_TARGET_SCORESET)
)
async def test_create_variants_for_score_set_exception_skips_mapping(
    input_score_set,
    setup_worker_db,
    session,
    async_client,
    data_files,
    arq_worker,
    arq_redis,
):
    score_set_urn, scores, counts, score_columns_metadata, count_columns_metadata = await setup_records_and_files(
        async_client, data_files, input_score_set
    )
    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()

    with patch.object(pd.DataFrame, "isnull", side_effect=Exception) as mocked_exc:
        await arq_redis.enqueue_job(
            "create_variants_for_score_set",
            uuid4().hex,
            score_set.id,
            1,
            scores,
            counts,
            score_columns_metadata,
            count_columns_metadata,
        )
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

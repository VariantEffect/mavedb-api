# ruff: noqa: E402

from asyncio.unix_events import _UnixSelectorEventLoop
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import select

arq = pytest.importorskip("arq")

from mavedb.lib.clingen.services import (
    ClinGenAlleleRegistryService,
    ClinGenLdhService,
)
from mavedb.lib.uniprot.id_mapping import UniProtIDMappingAPI
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.variant import Variant
from mavedb.worker.jobs import (
    variant_mapper_manager,
)
from mavedb.worker.jobs.utils.constants import MAPPING_CURRENT_ID_NAME, MAPPING_QUEUE_NAME
from tests.helpers.constants import (
    TEST_CLINGEN_ALLELE_OBJECT,
    TEST_CLINGEN_LDH_LINKING_RESPONSE,
    TEST_CLINGEN_SUBMISSION_RESPONSE,
    TEST_GNOMAD_DATA_VERSION,
    TEST_MINIMAL_SEQ_SCORESET,
    TEST_UNIPROT_ID_MAPPING_SWISS_PROT_RESPONSE,
    TEST_UNIPROT_JOB_SUBMISSION_RESPONSE,
)
from tests.helpers.util.exceptions import awaitable_exception
from tests.helpers.util.setup.worker import setup_mapping_output, setup_records_files_and_variants


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
    with patch.object(arq.ArqRedis, "rpop", Exception()):
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
        patch.object(arq.ArqRedis, "enqueue_job", return_value=awaitable_exception()),
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
        patch.object(arq.ArqRedis, "enqueue_job", return_value=awaitable_exception()),
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

    async def dummy_ldh_submission_job():
        return [TEST_CLINGEN_SUBMISSION_RESPONSE, None]

    async def dummy_linking_job():
        return [
            (variant_urn, TEST_CLINGEN_LDH_LINKING_RESPONSE)
            for variant_urn in session.scalars(
                select(Variant.urn).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
            ).all()
        ]

    # We seem unable to mock requests via requests_mock that occur inside another event loop. Workaround
    # this limitation by instead patching the _UnixSelectorEventLoop 's executor function, with a coroutine
    # object that sets up test mapping output.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            side_effect=[dummy_mapping_job(), dummy_ldh_submission_job(), dummy_linking_job()],
        ),
        patch.object(ClinGenAlleleRegistryService, "dispatch_submissions", return_value=[TEST_CLINGEN_ALLELE_OBJECT]),
        patch.object(ClinGenLdhService, "_existing_jwt", return_value="test_jwt"),
        patch.object(UniProtIDMappingAPI, "submit_id_mapping", return_value=TEST_UNIPROT_JOB_SUBMISSION_RESPONSE),
        patch.object(UniProtIDMappingAPI, "check_id_mapping_results_ready", return_value=True),
        patch.object(
            UniProtIDMappingAPI, "get_id_mapping_results", return_value=TEST_UNIPROT_ID_MAPPING_SWISS_PROT_RESPONSE
        ),
        patch("mavedb.worker.jobs.variant_processing.mapping.MAPPING_BACKOFF_IN_SECONDS", 0),
        patch("mavedb.worker.jobs.external_services.clingen.LINKING_BACKOFF_IN_SECONDS", 0),
        patch("mavedb.worker.jobs.variant_processing.mapping.UNIPROT_ID_MAPPING_ENABLED", True),
        patch("mavedb.worker.jobs.variant_processing.mapping.CLIN_GEN_SUBMISSION_ENABLED", True),
        patch(
            "mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT",
            "https://reg.test.genome.network/pytest",
        ),
        patch("mavedb.lib.clingen.services.GENBOREE_ACCOUNT_NAME", "testuser"),
        patch("mavedb.lib.clingen.services.GENBOREE_ACCOUNT_PASSWORD", "testpassword"),
        patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION),
        patch.object(ClinGenAlleleRegistryService, "dispatch_submissions", return_value=[TEST_CLINGEN_ALLELE_OBJECT]),
    ):
        await arq_worker.async_run()
        num_completed_jobs = await arq_worker.run_check()

    # We should have completed all jobs exactly once.
    assert num_completed_jobs == 8

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
async def test_mapping_manager_enqueues_mapping_process_with_successful_mapping_linking_disabled_uniprot_disabled(
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
    # object that sets up test mapping output.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            side_effect=[dummy_mapping_job()],
        ),
        patch.object(ClinGenLdhService, "_existing_jwt", return_value="test_jwt"),
        patch("mavedb.worker.jobs.variant_processing.mapping.MAPPING_BACKOFF_IN_SECONDS", 0),
        patch("mavedb.worker.jobs.external_services.clingen.LINKING_BACKOFF_IN_SECONDS", 0),
        patch("mavedb.worker.jobs.variant_processing.mapping.UNIPROT_ID_MAPPING_ENABLED", False),
        patch("mavedb.worker.jobs.variant_processing.mapping.CLIN_GEN_SUBMISSION_ENABLED", False),
    ):
        await arq_worker.async_run()
        num_completed_jobs = await arq_worker.run_check()

    # We should have completed the manager and mapping jobs, but not the submission, linking, or uniprot mapping jobs.
    assert num_completed_jobs == 2

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
async def test_mapping_manager_enqueues_mapping_process_with_successful_mapping_linking_disabled_uniprot_enabled(
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
    # object that sets up test mapping output.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            side_effect=[dummy_mapping_job()],
        ),
        patch.object(ClinGenLdhService, "_existing_jwt", return_value="test_jwt"),
        patch.object(UniProtIDMappingAPI, "submit_id_mapping", return_value=TEST_UNIPROT_JOB_SUBMISSION_RESPONSE),
        patch.object(UniProtIDMappingAPI, "check_id_mapping_results_ready", return_value=True),
        patch.object(
            UniProtIDMappingAPI, "get_id_mapping_results", return_value=TEST_UNIPROT_ID_MAPPING_SWISS_PROT_RESPONSE
        ),
        patch("mavedb.worker.jobs.variant_processing.mapping.MAPPING_BACKOFF_IN_SECONDS", 0),
        patch("mavedb.worker.jobs.external_services.clingen.LINKING_BACKOFF_IN_SECONDS", 0),
        patch("mavedb.worker.jobs.variant_processing.mapping.UNIPROT_ID_MAPPING_ENABLED", True),
        patch("mavedb.worker.jobs.variant_processing.mapping.CLIN_GEN_SUBMISSION_ENABLED", False),
    ):
        await arq_worker.async_run()
        num_completed_jobs = await arq_worker.run_check()

    # We should have completed the manager, mapping, and uniprot jobs, but not the submission or linking jobs.
    assert num_completed_jobs == 4

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
async def test_mapping_manager_enqueues_mapping_process_with_successful_mapping_linking_enabled_uniprot_disabled(
    setup_worker_db,
    standalone_worker_context,
    session,
    async_client,
    data_files,
    arq_worker,
    arq_redis,
    mocked_gnomad_variant_row,
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

    async def dummy_submission_job():
        return [TEST_CLINGEN_SUBMISSION_RESPONSE, None]

    async def dummy_linking_job():
        return [
            (variant_urn, TEST_CLINGEN_LDH_LINKING_RESPONSE)
            for variant_urn in session.scalars(
                select(Variant.urn).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
            ).all()
        ]

    # We seem unable to mock requests via requests_mock that occur inside another event loop. Workaround
    # this limitation by instead patching the _UnixSelectorEventLoop 's executor function, with a coroutine
    # object that sets up test mapping output.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            side_effect=[dummy_mapping_job(), dummy_submission_job(), dummy_linking_job()],
        ),
        patch.object(ClinGenLdhService, "_existing_jwt", return_value="test_jwt"),
        patch("mavedb.worker.jobs.variant_processing.mapping.MAPPING_BACKOFF_IN_SECONDS", 0),
        patch("mavedb.worker.jobs.external_services.clingen.LINKING_BACKOFF_IN_SECONDS", 0),
        patch("mavedb.worker.jobs.variant_processing.mapping.UNIPROT_ID_MAPPING_ENABLED", False),
        patch("mavedb.worker.jobs.variant_processing.mapping.CLIN_GEN_SUBMISSION_ENABLED", True),
        patch(
            "mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT",
            "https://reg.test.genome.network/pytest",
        ),
        patch("mavedb.lib.clingen.services.GENBOREE_ACCOUNT_NAME", "testuser"),
        patch("mavedb.lib.clingen.services.GENBOREE_ACCOUNT_PASSWORD", "testpassword"),
        patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION),
        patch.object(ClinGenAlleleRegistryService, "dispatch_submissions", return_value=[TEST_CLINGEN_ALLELE_OBJECT]),
        patch(
            "mavedb.worker.jobs.external_services.gnomad.gnomad_variant_data_for_caids",
            return_value=[mocked_gnomad_variant_row],
        ),
    ):
        await arq_worker.async_run()
        num_completed_jobs = await arq_worker.run_check()

    # We should have completed the manager, mapping, submission, and linking jobs, but not the uniprot jobs.
    assert num_completed_jobs == 6

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

    async def dummy_ldh_submission_job():
        return [TEST_CLINGEN_SUBMISSION_RESPONSE, None]

    async def dummy_linking_job():
        return [
            (variant_urn, TEST_CLINGEN_LDH_LINKING_RESPONSE)
            for variant_urn in session.scalars(
                select(Variant.urn).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
            ).all()
        ]

    # We seem unable to mock requests via requests_mock that occur inside another event loop. Workaround
    # this limitation by instead patching the _UnixSelectorEventLoop 's executor function, with a coroutine
    # object that sets up test mapping output.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            side_effect=[failed_mapping_job(), dummy_mapping_job(), dummy_ldh_submission_job(), dummy_linking_job()],
        ),
        patch.object(ClinGenLdhService, "_existing_jwt", return_value="test_jwt"),
        patch.object(ClinGenAlleleRegistryService, "dispatch_submissions", return_value=[TEST_CLINGEN_ALLELE_OBJECT]),
        patch("mavedb.worker.jobs.variant_processing.mapping.MAPPING_BACKOFF_IN_SECONDS", 0),
        patch("mavedb.worker.jobs.external_services.clingen.LINKING_BACKOFF_IN_SECONDS", 0),
        patch("mavedb.worker.jobs.variant_processing.mapping.UNIPROT_ID_MAPPING_ENABLED", False),
        patch("mavedb.worker.jobs.variant_processing.mapping.CLIN_GEN_SUBMISSION_ENABLED", True),
        patch(
            "mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT",
            "https://reg.test.genome.network/pytest",
        ),
        patch("mavedb.lib.clingen.services.GENBOREE_ACCOUNT_NAME", "testuser"),
        patch("mavedb.lib.clingen.services.GENBOREE_ACCOUNT_PASSWORD", "testpassword"),
        patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION),
        patch.object(ClinGenAlleleRegistryService, "dispatch_submissions", return_value=[TEST_CLINGEN_ALLELE_OBJECT]),
    ):
        await arq_worker.async_run()
        num_completed_jobs = await arq_worker.run_check()

    # We should have completed the mapping manager job twice, the mapping job twice, the two submission jobs, and both linking jobs.
    assert num_completed_jobs == 8

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
    # object that sets up test mapping output.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            side_effect=[failed_mapping_job()] * 5,
        ),
        patch("mavedb.worker.jobs.variant_processing.mapping.MAPPING_BACKOFF_IN_SECONDS", 0),
    ):
        await arq_worker.async_run()
        num_completed_jobs = await arq_worker.run_check()

    # We should have completed 6 mapping jobs and 6 management jobs.
    assert num_completed_jobs == 12

    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()
    mapped_variants_for_score_set = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set.urn)
    ).all()
    assert (await arq_redis.llen(MAPPING_QUEUE_NAME)) == 0
    assert (await arq_redis.get(MAPPING_CURRENT_ID_NAME)).decode("utf-8") == ""
    assert len(mapped_variants_for_score_set) == 0
    assert score_set.mapping_state == MappingState.failed
    assert score_set.mapping_errors is not None

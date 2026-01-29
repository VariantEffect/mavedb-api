# ruff : noqa: E402

"""
Unit tests for the with_pipeline_management async decorator.
Covers orchestration steps, error handling, and PipelineManager interaction.
"""

import pytest

pytest.importorskip("arq")  # Skip tests if arq is not installed

import asyncio
from unittest.mock import patch

from sqlalchemy import select

from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.job_run import JobRun
from mavedb.models.pipeline import Pipeline
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.managers.pipeline_manager import PipelineManager
from tests.helpers.transaction_spy import TransactionSpy

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


async def sample_job(ctx=None, job_id=None):
    """Sample job function to test the decorator. When called, it patches
    the with_job_management decorator to be a no-op so we can test the
    with_pipeline_management decorator in isolation.

    NOTE: The job_manager parameter is normally injected by the with_job_management
          decorator. Since we are patching that decorator to be a no-op here,
          we do not include it in the function signature.

    Args:
        ctx (dict): Worker context dictionary.
        job_id (int): ID of the JobRun record created by the decorator.
    """
    # patch the with_job_management decorator to be a no-op
    with patch(
        "mavedb.worker.lib.decorators.pipeline_management.with_job_management", wraps=lambda f: f
    ) as mock_job_mgmt:

        @with_pipeline_management
        async def patched_sample_job(ctx: dict, job_id: int):
            return {"status": "ok"}

        return await patched_sample_job(ctx, job_id)

    # Ensure the mock was called
    mock_job_mgmt.assert_called_once()


async def sample_raise(ctx: dict, job_id: int):
    """Sample job function to test the decorator when a job raises.
    When called, it patches the with_job_management decorator to be
    a no-op so we can test the with_pipeline_management decorator in isolation.

    NOTE: The job_manager parameter is normally injected by the with_job_management
          decorator. Since we are patching that decorator to be a no-op here,
          we do not include it in the function signature.

    Args:
        ctx (dict): Worker context dictionary.
        job_id (int): ID of the JobRun record created by the decorator.
    """
    # patch the with_job_management decorator to be a no-op
    with patch(
        "mavedb.worker.lib.decorators.pipeline_management.with_job_management", wraps=lambda f: f
    ) as mock_job_mgmt:

        @with_pipeline_management
        async def patched_sample_job(ctx: dict, job_id: int):
            raise RuntimeError("error in wrapped function")

        return await patched_sample_job(ctx, job_id)

    # Ensure the mock was called
    mock_job_mgmt.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.unit
class TestPipelineManagementDecoratorUnit:
    """Unit tests for the with_pipeline_management decorator."""

    async def test_decorator_must_receive_ctx_as_first_argument(self, mock_pipeline_manager):
        with pytest.raises(ValueError) as exc_info, TransactionSpy.spy(mock_pipeline_manager.db):
            await sample_job()

        assert "Managed functions must receive context as first argument" in str(exc_info.value)

    @pytest.mark.parametrize("missing_key", ["redis"])
    async def test_decorator_raises_value_error_if_required_context_missing(
        self, mock_pipeline_manager, mock_worker_ctx, missing_key
    ):
        del mock_worker_ctx[missing_key]

        with (
            pytest.raises(ValueError) as exc_info,
            TransactionSpy.spy(mock_pipeline_manager.db),
            patch("mavedb.worker.lib.decorators.pipeline_management.send_slack_error") as mock_send_slack_error,
        ):
            await sample_job(mock_worker_ctx, 999)

        assert missing_key.replace("_", " ") in str(exc_info.value).lower()
        assert "not found in pipeline context" in str(exc_info.value).lower()
        mock_send_slack_error.assert_called_once()

    async def test_decorator_raises_value_error_if_job_id_missing(self, mock_pipeline_manager, mock_worker_ctx):
        # Remove job_id from args to simulate missing job_id
        with (
            pytest.raises(ValueError) as exc_info,
            TransactionSpy.spy(mock_pipeline_manager.db),
            patch("mavedb.worker.lib.decorators.pipeline_management.send_slack_error") as mock_send_slack_error,
        ):
            await sample_job(mock_worker_ctx)

        assert "job id not found in function arguments" in str(exc_info.value).lower()
        mock_send_slack_error.assert_called_once()

    async def test_decorator_swallows_exception_if_cant_fetch_pipeline_id(
        self, session, mock_pipeline_manager, mock_worker_ctx
    ):
        with (
            TransactionSpy.mock_database_execution_failure(
                session,
                exception=ValueError("job id not found in pipeline context"),
                expect_rollback=True,
            ),
            patch("mavedb.worker.lib.decorators.pipeline_management.send_slack_error") as mock_send_slack_error,
        ):
            await sample_job(mock_worker_ctx, 999)
        mock_send_slack_error.assert_called_once()

    async def test_decorator_fetches_pipeline_from_db_and_constructs_pipeline_manager(
        self, session, mock_pipeline_manager, mock_worker_ctx, sample_job_run, with_populated_job_data
    ):
        with (
            patch("mavedb.worker.lib.decorators.pipeline_management.PipelineManager") as mock_pipeline_manager_class,
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None),
            patch.object(mock_pipeline_manager, "start_pipeline", return_value=None),
            TransactionSpy.spy(session, expect_commit=True),
        ):
            mock_pipeline_manager_class.return_value = mock_pipeline_manager
            result = await sample_job(mock_worker_ctx, sample_job_run.id)

        assert result == {"status": "ok"}

    async def test_decorator_skips_coordination_and_start_when_no_pipeline_exists(
        self, session, mock_pipeline_manager, mock_worker_ctx, sample_independent_job_run, with_populated_job_data
    ):
        with (
            patch("mavedb.worker.lib.decorators.pipeline_management.PipelineManager") as mock_pipeline_manager_class,
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None) as mock_coordinate_pipeline,
            patch.object(mock_pipeline_manager, "start_pipeline", return_value=None) as mock_start_pipeline,
            # We shouldn't expect any commits since no pipeline coordination occurs
            TransactionSpy.spy(session),
        ):
            mock_pipeline_manager_class.return_value = mock_pipeline_manager
            result = await sample_job(mock_worker_ctx, sample_independent_job_run.id)

        mock_coordinate_pipeline.assert_not_called()
        mock_start_pipeline.assert_not_called()
        assert result == {"status": "ok"}

    async def test_decorator_starts_pipeline_when_in_created_state(
        self, session, mock_pipeline_manager, mock_worker_ctx, sample_job_run, with_populated_job_data
    ):
        with (
            patch("mavedb.worker.lib.decorators.pipeline_management.PipelineManager") as mock_pipeline_manager_class,
            patch.object(mock_pipeline_manager, "get_pipeline_status", return_value=PipelineStatus.CREATED),
            patch.object(mock_pipeline_manager, "start_pipeline", return_value=None) as mock_start_pipeline,
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None),
            TransactionSpy.spy(session, expect_commit=True),
        ):
            mock_pipeline_manager_class.return_value = mock_pipeline_manager
            result = await sample_job(mock_worker_ctx, sample_job_run.id)

        mock_start_pipeline.assert_called_once()
        assert result == {"status": "ok"}

    @pytest.mark.parametrize(
        "pipeline_state",
        [status for status in PipelineStatus._member_map_.values() if status != PipelineStatus.CREATED],
    )
    async def test_decorator_does_not_start_pipeline_when_in_not_in_created_state(
        self, session, mock_pipeline_manager, mock_worker_ctx, sample_job_run, with_populated_job_data, pipeline_state
    ):
        with (
            patch("mavedb.worker.lib.decorators.pipeline_management.PipelineManager") as mock_pipeline_manager_class,
            patch.object(mock_pipeline_manager, "get_pipeline_status", return_value=pipeline_state),
            patch.object(mock_pipeline_manager, "start_pipeline", return_value=None) as mock_start_pipeline,
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None),
            TransactionSpy.spy(session, expect_commit=True),
        ):
            mock_pipeline_manager_class.return_value = mock_pipeline_manager
            result = await sample_job(mock_worker_ctx, sample_job_run.id)

        mock_start_pipeline.assert_not_called()
        assert result == {"status": "ok"}

    async def test_decorator_calls_pipeline_manager_coordinate_pipeline_after_wrapped_function(
        self, session, mock_pipeline_manager, mock_worker_ctx, sample_job_run, with_populated_job_data
    ):
        with (
            patch("mavedb.worker.lib.decorators.pipeline_management.PipelineManager") as mock_pipeline_manager_class,
            patch.object(mock_pipeline_manager, "get_pipeline_status", return_value=PipelineStatus.CREATED),
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None) as mock_coordinate_pipeline,
            patch.object(mock_pipeline_manager, "start_pipeline", return_value=None),
            TransactionSpy.spy(session, expect_commit=True),
        ):
            mock_pipeline_manager_class.return_value = mock_pipeline_manager
            await sample_job(mock_worker_ctx, sample_job_run.id)

        mock_coordinate_pipeline.assert_called_once()

    async def test_decorator_swallows_exception_from_wrapped_function(
        self, session, mock_pipeline_manager, mock_worker_ctx, sample_job_run, with_populated_job_data
    ):
        with (
            patch("mavedb.worker.lib.decorators.pipeline_management.PipelineManager") as mock_pipeline_manager_class,
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None),
            patch.object(mock_pipeline_manager, "start_pipeline", return_value=None),
            patch.object(mock_pipeline_manager, "get_pipeline_status", return_value=PipelineStatus.CREATED),
            TransactionSpy.spy(session, expect_commit=True, expect_rollback=True),
            patch("mavedb.worker.lib.decorators.pipeline_management.send_slack_error") as mock_send_slack_error,
        ):
            mock_pipeline_manager_class.return_value = mock_pipeline_manager
            await sample_raise(mock_worker_ctx, sample_job_run.id)

        mock_send_slack_error.assert_called_once()

    async def test_decorator_swallows_exception_from_pipeline_manager_coordinate_pipeline(
        self, session, mock_pipeline_manager, mock_worker_ctx, sample_job_run, with_populated_job_data
    ):
        with (
            patch("mavedb.worker.lib.decorators.pipeline_management.PipelineManager") as mock_pipeline_manager_class,
            patch.object(
                mock_pipeline_manager,
                "coordinate_pipeline",
                side_effect=RuntimeError("error in coordinate_pipeline"),
            ),
            patch.object(mock_pipeline_manager, "start_pipeline", return_value=None),
            patch.object(mock_pipeline_manager, "get_pipeline_status", return_value=PipelineStatus.CREATED),
            # Exception raised from coordinate_pipeline should trigger rollback,
            # and commit will be called when pipeline status is set to running
            TransactionSpy.spy(session, expect_commit=True, expect_rollback=True),
            patch("mavedb.worker.lib.decorators.pipeline_management.send_slack_error") as mock_send_slack_error,
        ):
            mock_pipeline_manager_class.return_value = mock_pipeline_manager
            await sample_job(mock_worker_ctx, sample_job_run.id)

        assert mock_send_slack_error.call_count == 2

    async def test_decorator_swallows_exception_from_job_management_decorator(
        self, session, mock_pipeline_manager, mock_worker_ctx, sample_job_run, with_populated_job_data
    ):
        def passthrough_decorator(f):
            return f

        with (
            # patch the with_job_management decorator to raise an error
            patch(
                "mavedb.worker.lib.decorators.pipeline_management.with_job_management",
                wraps=passthrough_decorator,
                side_effect=ValueError("error in job management decorator"),
            ) as mock_with_job_mgmt,
            patch.object(mock_pipeline_manager, "start_pipeline", return_value=None),
            patch.object(mock_pipeline_manager, "get_pipeline_status", return_value=PipelineStatus.CREATED),
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None),
            patch("mavedb.worker.lib.decorators.pipeline_management.PipelineManager") as mock_pipeline_manager_class,
            TransactionSpy.spy(session, expect_commit=True, expect_rollback=True),
            patch("mavedb.worker.lib.decorators.pipeline_management.send_slack_error") as mock_send_slack_error,
        ):
            mock_pipeline_manager_class.return_value = mock_pipeline_manager

            @with_pipeline_management
            async def sample_job(ctx: dict, job_id: int, pipeline_manager: PipelineManager):
                return {"status": "ok"}

            await sample_job(mock_worker_ctx, sample_job_run.id, pipeline_manager=mock_pipeline_manager)

        mock_with_job_mgmt.assert_called_once()
        mock_send_slack_error.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.integration
class TestPipelineManagementDecoratorIntegration:
    """Integration tests for the with_pipeline_management decorator."""

    @pytest.mark.parametrize("initial_status", [PipelineStatus.CREATED, PipelineStatus.RUNNING])
    async def test_decorator_integrated_pipeline_lifecycle_success(
        self,
        session,
        arq_redis,
        sample_job_run,
        sample_dependent_job_run,
        standalone_worker_context,
        with_populated_job_data,
        sample_pipeline,
        initial_status,
    ):
        # Use an event to control when the job completes
        event = asyncio.Event()
        dep_event = asyncio.Event()

        # Set initial pipeline status to the parameterized value.
        # This allows testing both CREATED and RUNNING start states.
        sample_pipeline.status = initial_status
        session.commit()

        @with_pipeline_management
        async def sample_job(ctx: dict, job_id: int, job_manager: JobManager):
            await event.wait()  # Simulate async work, block until test signals
            return {"status": "ok", "data": {}, "exception": None}

        @with_pipeline_management
        async def sample_dependent_job(ctx: dict, job_id: int, job_manager: JobManager):
            await dep_event.wait()  # Simulate async work, block until test signals
            return {"status": "ok", "data": {}, "exception": None}

        # Start the job (it will block at event.wait())
        job_task = asyncio.create_task(sample_job(standalone_worker_context, sample_job_run.id))

        # At this point, the job should be started but not completed
        await asyncio.sleep(0.1)  # Give the event loop a moment to start the job
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.RUNNING

        pipeline = session.execute(select(Pipeline).where(Pipeline.id == sample_pipeline.id)).scalar_one()
        assert pipeline.status == PipelineStatus.RUNNING

        # Now allow the job to complete and flush the Redis queue. Flush the queue first to ensure
        # we don't mistakenly flush our queued job.
        await arq_redis.flushdb()
        event.set()
        await job_task

        # After completion, status should be SUCCEEDED
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.SUCCEEDED

        pipeline = session.execute(select(Pipeline).where(Pipeline.id == sample_pipeline.id)).scalar_one()

        # Pipeline remains RUNNING after job success, another job was queued.
        assert pipeline.status == PipelineStatus.RUNNING

        queued_jobs = await arq_redis.queued_jobs()
        assert len(queued_jobs) == 1  # Ensure the next job was queued

        # Simulate execution of next job by running the dependent job.
        # Start the job (it will block at event.wait())
        dependent_job_task = asyncio.create_task(
            sample_dependent_job(standalone_worker_context, sample_dependent_job_run.id)
        )

        # At this point, the job should be started but not completed
        await asyncio.sleep(0.1)  # Give the event loop a moment to start the job
        job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert job.status == JobStatus.RUNNING

        pipeline = session.execute(select(Pipeline).where(Pipeline.id == sample_pipeline.id)).scalar_one()
        assert pipeline.status == PipelineStatus.RUNNING

        # Now allow the job to complete and flush the Redis queue. Flush the queue first to ensure
        # we don't mistakenly flush our queued job.
        await arq_redis.flushdb()
        dep_event.set()
        await dependent_job_task

        # After completion, status should be SUCCEEDED
        job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert job.status == JobStatus.SUCCEEDED

        # Now that all jobs are complete, the pipeline should be SUCCEEDED
        pipeline = session.execute(select(Pipeline).where(Pipeline.id == sample_pipeline.id)).scalar_one()
        assert pipeline.status == PipelineStatus.SUCCEEDED

        # No further jobs should be queued
        queued_jobs = await arq_redis.queued_jobs()
        assert len(queued_jobs) == 0

    async def test_decorator_integrated_pipeline_lifecycle_retryable_failure(
        self,
        session,
        arq_redis,
        sample_job_run,
        sample_dependent_job_run,
        standalone_worker_context,
        with_populated_job_data,
        sample_pipeline,
    ):
        # Use an event to control when the job completes
        event = asyncio.Event()
        retry_event = asyncio.Event()
        dep_event = asyncio.Event()

        @with_pipeline_management
        async def sample_job(ctx: dict, job_id: int, job_manager: JobManager):
            await event.wait()  # Simulate async work, block until test signals
            raise RuntimeError("Simulated job failure for retry")

        @with_pipeline_management
        async def sample_retried_job(ctx: dict, job_id: int, job_manager: JobManager):
            await retry_event.wait()  # Simulate async work, block until test signals
            return {"status": "ok", "data": {}, "exception": None}

        @with_pipeline_management
        async def sample_dependent_job(ctx: dict, job_id: int, job_manager: JobManager):
            await dep_event.wait()  # Simulate async work, block until test signals
            return {"status": "ok", "data": {}, "exception": None}

        # job management handles slack alerting in this context
        with patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error:
            # Start the job (it will block at event.wait())
            job_task = asyncio.create_task(sample_job(standalone_worker_context, sample_job_run.id))

            # At this point, the job should be started but not completed
            await asyncio.sleep(0.1)  # Give the event loop a moment to start the job
            job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
            assert job.status == JobStatus.RUNNING

            pipeline = session.execute(select(Pipeline).where(Pipeline.id == sample_pipeline.id)).scalar_one()
            assert pipeline.status == PipelineStatus.RUNNING

            # Now allow the job to complete with failure that triggers a retry. This failure
            # should be swallowed by the job_task.
            with patch.object(JobManager, "should_retry", return_value=True):
                event.set()
                await job_task

            mock_send_slack_error.assert_called_once()

        # After failure with retry, status should be QUEUED
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED
        assert job.retry_count == 1  # Ensure it attempted once before retrying

        # Now start the retried job (it will block at retry_event.wait())
        retried_job_task = asyncio.create_task(sample_retried_job(standalone_worker_context, sample_job_run.id))
        await asyncio.sleep(0.1)  # Give the event loop a moment to start the job
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.RUNNING

        # The pipeline should remain running
        pipeline = session.execute(select(Pipeline).where(Pipeline.id == sample_pipeline.id)).scalar_one()
        assert pipeline.status == PipelineStatus.RUNNING

        # Now allow the retried job to complete successfully
        await arq_redis.flushdb()
        retry_event.set()
        await retried_job_task

        # After completion, status should be SUCCEEDED
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.SUCCEEDED

        pipeline = session.execute(select(Pipeline).where(Pipeline.id == sample_pipeline.id)).scalar_one()
        assert pipeline.status == PipelineStatus.RUNNING

        queued_jobs = await arq_redis.queued_jobs()
        assert len(queued_jobs) == 1  # Ensure the next job was queued

        # Simulate execution of next job by running the dependent job.
        # Start the job (it will block at event.wait())
        dependent_job_task = asyncio.create_task(
            sample_dependent_job(standalone_worker_context, sample_dependent_job_run.id)
        )

        # At this point, the job should be started but not completed
        await asyncio.sleep(0.1)  # Give the event loop a moment to start the job
        job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert job.status == JobStatus.RUNNING

        pipeline = session.execute(select(Pipeline).where(Pipeline.id == sample_pipeline.id)).scalar_one()
        assert pipeline.status == PipelineStatus.RUNNING

        # Now allow the job to complete and flush the Redis queue. Flush the queue first to ensure
        # we don't mistakenly flush our queued job.
        await arq_redis.flushdb()
        dep_event.set()
        await dependent_job_task

        # After completion, status should be SUCCEEDED
        job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert job.status == JobStatus.SUCCEEDED

        # Now that all jobs are complete, the pipeline should be SUCCEEDED
        pipeline = session.execute(select(Pipeline).where(Pipeline.id == sample_pipeline.id)).scalar_one()
        assert pipeline.status == PipelineStatus.SUCCEEDED

        queued_jobs = await arq_redis.queued_jobs()
        assert len(queued_jobs) == 0  # Ensure no further jobs were queued

    async def test_decorator_integrated_pipeline_lifecycle_non_retryable_failure(
        self,
        session,
        arq_redis,
        sample_job_run,
        sample_dependent_job_run,
        standalone_worker_context,
        with_populated_job_data,
        sample_pipeline,
    ):
        # Use an event to control when the job completes
        event = asyncio.Event()

        @with_pipeline_management
        async def sample_job(ctx: dict, job_id: int, job_manager: JobManager):
            await event.wait()  # Simulate async work, block until test signals
            raise RuntimeError("Simulated job failure")

        # job management handles slack alerting in this context
        with patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error:
            # Start the job (it will block at event.wait())
            job_task = asyncio.create_task(sample_job(standalone_worker_context, sample_job_run.id))

            # At this point, the job should be started but not completed
            await asyncio.sleep(0.1)  # Give the event loop a moment to start the job
            job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
            assert job.status == JobStatus.RUNNING

            pipeline = session.execute(select(Pipeline).where(Pipeline.id == sample_pipeline.id)).scalar_one()
            assert pipeline.status == PipelineStatus.RUNNING

            # Now allow the job to complete with failure and flush the Redis queue. This failure
            # should be swallowed by the pipeline manager
            await arq_redis.flushdb()
            event.set()
            await job_task

            mock_send_slack_error.assert_called_once()

        # After failure with no retry, status should be FAILED
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.FAILED

        pipeline = session.execute(select(Pipeline).where(Pipeline.id == sample_pipeline.id)).scalar_one()

        # Pipeline should be marked FAILED after job failure
        assert pipeline.status == PipelineStatus.FAILED

        # No further jobs should be queued
        queued_jobs = await arq_redis.queued_jobs()
        assert len(queued_jobs) == 0

        # Dependent job should transition to skipped since it was never queued
        job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert job.status == JobStatus.SKIPPED

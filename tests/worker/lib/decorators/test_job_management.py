# ruff : noqa: E402

"""
Unit and integration tests for the with_job_management async decorator.
Covers status transitions, error handling, and JobManager interaction.
"""

import pytest

pytest.importorskip("arq")  # Skip tests if arq is not installed

import asyncio
from unittest.mock import patch

from sqlalchemy import select

from mavedb.models.enums.job_pipeline import JobStatus
from mavedb.models.job_run import JobRun
from mavedb.worker.lib.decorators.job_management import with_job_management
from mavedb.worker.lib.managers.constants import RETRYABLE_FAILURE_CATEGORIES
from mavedb.worker.lib.managers.exceptions import JobStateError
from mavedb.worker.lib.managers.job_manager import JobManager
from tests.helpers.transaction_spy import TransactionSpy


@with_job_management
async def sample_job(ctx: dict, job_id: int, job_manager: JobManager):
    """Sample job function to test the decorator.

    NOTE: The job_manager parameter is injected by the decorator
          and is not passed explicitly when calling the function.

    Args:
        ctx (dict): Worker context dictionary.
        job_id (int): ID of the JobRun record created by the decorator.
    """
    return {"status": "ok"}


@with_job_management
async def sample_raise(ctx: dict, job_id: int, job_manager: JobManager):
    """Sample job function to test the decorator in cases where the wrapped function raises an exception.

    NOTE: The job_manager parameter is injected by the decorator
          and is not passed explicitly when calling the function.

    Args:
        ctx (dict): Worker context dictionary.
        job_id (int): ID of the JobRun record created by the decorator.
    """
    raise RuntimeError("error in wrapped function")


@pytest.mark.asyncio
@pytest.mark.unit
class TestManagedJobDecoratorUnit:
    async def test_decorator_must_receive_ctx_as_first_argument(self, mock_job_manager):
        with pytest.raises(ValueError) as exc_info, TransactionSpy.spy(mock_job_manager.db):
            await sample_job()

        assert "Managed job functions must receive context as first argument" in str(exc_info.value)

    async def test_decorator_calls_wrapped_function_and_returns_result(self, mock_job_manager, mock_worker_ctx):
        with (
            patch("mavedb.worker.lib.decorators.job_management.JobManager") as mock_job_manager_class,
            patch.object(mock_job_manager, "start_job", return_value=None),
            patch.object(mock_job_manager, "succeed_job", return_value=None),
            TransactionSpy.spy(mock_worker_ctx["db"], expect_commit=True),
        ):
            mock_job_manager_class.return_value = mock_job_manager

            result = await sample_job(mock_worker_ctx, 999)
            assert result == {"status": "ok"}

    async def test_decorator_calls_start_job_and_succeed_job_when_wrapped_function_succeeds(
        self, mock_worker_ctx, mock_job_manager
    ):
        with (
            patch("mavedb.worker.lib.decorators.job_management.JobManager") as mock_job_manager_class,
            patch.object(mock_job_manager, "start_job", return_value=None) as mock_start_job,
            patch.object(mock_job_manager, "succeed_job", return_value=None) as mock_succeed_job,
            TransactionSpy.spy(mock_worker_ctx["db"], expect_commit=True),
        ):
            mock_job_manager_class.return_value = mock_job_manager
            await sample_job(mock_worker_ctx, 999)

        mock_start_job.assert_called_once()
        mock_succeed_job.assert_called_once()

    async def test_decorator_calls_start_job_and_fail_job_when_wrapped_function_raises_and_no_retry(
        self, mock_worker_ctx, mock_job_manager
    ):
        with (
            patch("mavedb.worker.lib.decorators.job_management.JobManager") as mock_job_manager_class,
            patch.object(mock_job_manager, "start_job", return_value=None) as mock_start_job,
            patch.object(mock_job_manager, "should_retry", return_value=False),
            patch.object(mock_job_manager, "fail_job", return_value=None) as mock_fail_job,
            TransactionSpy.spy(mock_worker_ctx["db"], expect_commit=True, expect_rollback=True),
        ):
            mock_job_manager_class.return_value = mock_job_manager
            await sample_raise(mock_worker_ctx, 999)

        mock_start_job.assert_called_once()
        mock_fail_job.assert_called_once()

    async def test_decorator_calls_start_job_and_retries_job_when_wrapped_function_raises_and_retry(
        self, mock_worker_ctx, mock_job_manager
    ):
        with (
            patch("mavedb.worker.lib.decorators.job_management.JobManager") as mock_job_manager_class,
            patch.object(mock_job_manager, "start_job", return_value=None) as mock_start_job,
            patch.object(mock_job_manager, "should_retry", return_value=True),
            patch.object(mock_job_manager, "prepare_retry", return_value=None) as mock_prepare_retry,
            TransactionSpy.spy(mock_worker_ctx["db"], expect_commit=True, expect_rollback=True),
        ):
            mock_job_manager_class.return_value = mock_job_manager
            await sample_raise(mock_worker_ctx, 999)

        mock_start_job.assert_called_once()
        mock_prepare_retry.assert_called_once_with(reason="error in wrapped function")

    @pytest.mark.parametrize("missing_key", ["db", "redis"])
    async def test_decorator_raises_value_error_if_required_context_missing(
        self, mock_job_manager, mock_worker_ctx, missing_key
    ):
        del mock_worker_ctx[missing_key]

        with pytest.raises(ValueError) as exc_info:
            await sample_job(mock_worker_ctx, 999)

        assert missing_key.replace("_", " ") in str(exc_info.value).lower()
        assert "not found in job context" in str(exc_info.value).lower()

    async def test_decorator_swallows_exception_from_lifecycle_state_outside_except(
        self, mock_job_manager, mock_worker_ctx
    ):
        with (
            patch("mavedb.worker.lib.decorators.job_management.JobManager") as mock_job_manager_class,
            patch.object(mock_job_manager, "start_job", side_effect=JobStateError("error in job start")),
            patch.object(mock_job_manager, "should_retry", return_value=False),
            patch.object(mock_job_manager, "fail_job", return_value=None),
            TransactionSpy.spy(mock_worker_ctx["db"], expect_rollback=True, expect_commit=True),
        ):
            mock_job_manager_class.return_value = mock_job_manager
            result = await sample_job(mock_worker_ctx, 999)

        assert "error in job start" in result["exception_details"]["message"]

    async def test_decorator_raises_value_error_if_job_id_missing(self, mock_job_manager, mock_worker_ctx):
        # Remove job_id from args to simulate missing job_id
        with pytest.raises(ValueError) as exc_info, TransactionSpy.spy(mock_worker_ctx["db"]):
            await sample_job(mock_worker_ctx)

        assert "job id not found in pipeline context" in str(exc_info.value).lower()

    async def test_decorator_swallows_exception_from_wrapped_function_inside_except(
        self, mock_job_manager, mock_worker_ctx
    ):
        with (
            patch("mavedb.worker.lib.decorators.job_management.JobManager") as mock_job_manager_class,
            patch.object(mock_job_manager, "start_job", return_value=None),
            patch.object(mock_job_manager, "should_retry", return_value=False),
            patch.object(mock_job_manager, "fail_job", side_effect=JobStateError("error in job fail")),
            TransactionSpy.spy(mock_worker_ctx["db"], expect_commit=True, expect_rollback=True),
        ):
            mock_job_manager_class.return_value = mock_job_manager
            result = await sample_raise(mock_worker_ctx, 999)

        # Errors within the main try block should take precedence
        assert "error in wrapped function" in result["exception_details"]["message"]

    async def test_decorator_passes_job_manager_to_wrapped(self, mock_job_manager, mock_worker_ctx):
        @with_job_management
        async def assert_manager_passed_job(ctx, job_id: int, job_manager):
            assert isinstance(job_manager, JobManager)
            return True

        with (
            patch("mavedb.worker.lib.decorators.job_management.JobManager") as mock_job_manager_class,
            patch.object(mock_job_manager, "start_job", return_value=None),
            patch.object(mock_job_manager, "succeed_job", return_value=None),
            TransactionSpy.spy(mock_worker_ctx["db"], expect_commit=True),
        ):
            mock_job_manager_class.return_value = mock_job_manager
            assert await assert_manager_passed_job(mock_worker_ctx, 999)


@pytest.mark.asyncio
@pytest.mark.integration
class TestManagedJobDecoratorIntegration:
    """Integration tests for with_job_management decorator."""

    async def test_decorator_integrated_job_lifecycle_success(
        self, session, arq_redis, sample_job_run, standalone_worker_context, with_populated_job_data
    ):
        # Use an event to control when the job completes
        event = asyncio.Event()

        @with_job_management
        async def sample_job(ctx: dict, job_id: int, job_manager: JobManager):
            await event.wait()  # Simulate async work, block until test signals
            return {"status": "ok"}

        # Start the job (it will block at event.wait())
        job_task = asyncio.create_task(sample_job(standalone_worker_context, sample_job_run.id))

        # At this point, the job should be started but not completed
        await asyncio.sleep(0.1)  # Give the event loop a moment to start the job
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.RUNNING

        # Now allow the job to complete
        event.set()
        await job_task

        # After completion, status should be SUCCEEDED
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.SUCCEEDED

    async def test_decorator_integrated_job_lifecycle_failure(
        self, session, arq_redis, sample_job_run, standalone_worker_context, with_populated_job_data
    ):
        # Use an event to control when the job completes
        event = asyncio.Event()

        @with_job_management
        async def sample_job(ctx: dict, job_id: int, job_manager: JobManager):
            await event.wait()  # Simulate async work, block until test signals
            raise RuntimeError("Simulated job failure")

        # Start the job (it will block at event.wait())
        job_task = asyncio.create_task(sample_job(standalone_worker_context, sample_job_run.id))

        # At this point, the job should be started but not in error
        await asyncio.sleep(0.1)  # Give the event loop a moment to start the job
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.RUNNING

        # Now allow the job to complete with failure. This failure
        # should be swallowed by the job_task.
        event.set()
        await job_task

        # After failure, status should be FAILED
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.FAILED
        assert job.error_message == "Simulated job failure"

    async def test_decorator_integrated_job_lifecycle_retry(
        self, session, arq_redis, sample_job_run, standalone_worker_context, with_populated_job_data
    ):
        # Use an event to control when the job completes
        event = asyncio.Event()

        @with_job_management
        async def sample_job(ctx: dict, job_id: int, job_manager: JobManager):
            sample_job_run.failure_category = RETRYABLE_FAILURE_CATEGORIES[0]  # Set a retryable failure category
            await event.wait()  # Simulate async work, block until test signals
            raise RuntimeError("Simulated job failure for retry")

        # Start the job (it will block at event.wait())
        job_task = asyncio.create_task(sample_job(standalone_worker_context, sample_job_run.id))

        # At this point, the job should be started but not in error
        await asyncio.sleep(0.1)  # Give the event loop a moment to start the job
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.RUNNING

        # TODO: We patch `should_retry` to return True to force a retry scenario. After implementing failure
        # categorization in the worker, this patch can be removed and we should directly test retry logic based
        # on failure categories.
        #
        # Now allow the job to complete with failure that triggers a retry. This failure
        # should be swallowed by the job_task.
        with patch.object(JobManager, "should_retry", return_value=True):
            event.set()
            await job_task

        # After failure with retry, status should be PENDING
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING
        assert job.retry_count == 1  # Ensure it attempted once before retrying

# ruff: noqa: E402
"""
Comprehensive test suite for PipelineManager class.

Tests cover all aspects of pipeline coordination, job dependency management,
status updates, error handling, and database interactions including new methods
for pipeline monitoring, job retry management, and restart functionality.
"""

import pytest

pytest.importorskip("arq")

import datetime
from unittest.mock import Mock, PropertyMock, patch

from arq import ArqRedis
from arq.jobs import Job as ArqJob
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from mavedb.models.enums.job_pipeline import DependencyType, JobStatus, PipelineStatus
from mavedb.models.job_dependency import JobDependency
from mavedb.models.job_run import JobRun
from mavedb.models.pipeline import Pipeline
from mavedb.worker.lib.managers import JobManager
from mavedb.worker.lib.managers.constants import (
    ACTIVE_JOB_STATUSES,
    CANCELLED_PIPELINE_STATUSES,
    RUNNING_PIPELINE_STATUSES,
    TERMINAL_PIPELINE_STATUSES,
)
from mavedb.worker.lib.managers.exceptions import (
    DatabaseConnectionError,
    PipelineCoordinationError,
    PipelineStateError,
    PipelineTransitionError,
)
from mavedb.worker.lib.managers.pipeline_manager import PipelineManager
from tests.helpers.transaction_spy import TransactionSpy

HANDLED_EXCEPTIONS_DURING_OBJECT_MANIPULATION = (
    AttributeError("Mock attribute error"),
    KeyError("Mock key error"),
    TypeError("Mock type error"),
    ValueError("Mock value error"),
)


@pytest.mark.integration
class TestPipelineManagerInitialization:
    """Test PipelineManager initialization and setup."""

    def test_init_with_valid_pipeline(self, session, arq_redis, with_populated_job_data, sample_pipeline):
        """Test successful initialization with valid pipeline ID."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        assert manager.db == session
        assert manager.redis == arq_redis
        assert manager.pipeline_id == sample_pipeline.id

    def test_init_with_invalid_pipeline_id(self, session, arq_redis):
        """Test initialization failure with non-existent pipeline ID."""
        pipeline_id = 999  # Assuming this ID does not exist
        with pytest.raises(DatabaseConnectionError, match=f"Failed to get pipeline {pipeline_id}"):
            PipelineManager(session, arq_redis, pipeline_id)

    def test_init_with_database_error(self, session, arq_redis, with_populated_job_data, sample_pipeline):
        """Test initialization failure with database connection error."""
        pipeline_id = sample_pipeline.id

        with (
            TransactionSpy.mock_database_execution_failure(session),
            pytest.raises(DatabaseConnectionError, match=f"Failed to get pipeline {pipeline_id}"),
        ):
            PipelineManager(session, arq_redis, pipeline_id)


@pytest.mark.unit
class TestStartPipelineUnit:
    """Unit tests for starting a pipeline."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "coordinate_after_start",
        [True, False],
    )
    async def test_start_pipeline_successful(self, mock_pipeline_manager, coordinate_after_start):
        """Test successful pipeline start from CREATED state."""
        with (
            patch.object(
                mock_pipeline_manager,
                "get_pipeline",
                return_value=Mock(spec=Pipeline, status=PipelineStatus.CREATED),
            ),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None) as mock_coordinate,
            TransactionSpy.spy(mock_pipeline_manager.db, expect_flush=True),
        ):
            await mock_pipeline_manager.start_pipeline(coordinate=coordinate_after_start)

        mock_set_status.assert_called_once_with(PipelineStatus.RUNNING)
        if coordinate_after_start:
            mock_coordinate.assert_called_once()
        else:
            mock_coordinate.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "current_status",
        [status for status in PipelineStatus._member_map_.values() if status != PipelineStatus.CREATED],
    )
    async def test_start_pipeline_non_created_state(self, mock_pipeline_manager, current_status):
        """Test pipeline start failure when not in CREATED state."""
        with (
            patch.object(
                mock_pipeline_manager,
                "get_pipeline_status",
                return_value=current_status,
            ),
            pytest.raises(
                PipelineTransitionError,
                match=f"Pipeline {mock_pipeline_manager.pipeline_id} is in state {current_status} and may not be started",
            ),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None) as mock_coordinate,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            await mock_pipeline_manager.start_pipeline()

        mock_set_status.assert_not_called()
        mock_coordinate.assert_not_called()


@pytest.mark.integration
class TestStartPipelineIntegration:
    """Integration tests for starting a pipeline."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "coordinate_after_start",
        [True, False],
    )
    async def test_start_pipeline_successful(
        self, session, arq_redis, with_populated_job_data, sample_pipeline, sample_job_run, coordinate_after_start
    ):
        """Test successful pipeline start from CREATED state."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        with TransactionSpy.spy(session, expect_flush=True):
            await manager.start_pipeline(coordinate=coordinate_after_start)

        # Commit the session to persist changes
        session.commit()

        # Verify pipeline status is now RUNNING
        pipeline = session.execute(select(Pipeline).where(Pipeline.id == sample_pipeline.id)).scalar_one()
        assert pipeline.status == PipelineStatus.RUNNING

        # Verify the initial job was queued if we are coordinating after start
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        jobs = await arq_redis.queued_jobs()

        if coordinate_after_start:
            assert job.status == JobStatus.QUEUED
            assert jobs[0].function == sample_job_run.job_function
        else:
            assert job.status == JobStatus.PENDING
            assert len(jobs) == 0

    @pytest.mark.asyncio
    async def test_start_pipeline_no_jobs(self, session, arq_redis, with_populated_job_data, sample_empty_pipeline):
        """Test pipeline start when there are no jobs in the pipeline."""
        manager = PipelineManager(session, arq_redis, sample_empty_pipeline.id)

        with TransactionSpy.spy(session, expect_flush=True):
            await manager.start_pipeline(coordinate=True)

        # Commit the session to persist changes
        session.commit()

        # Verify pipeline status is now SUCCEEDED since there are no jobs
        pipeline = session.execute(select(Pipeline).where(Pipeline.id == sample_empty_pipeline.id)).scalar_one()
        assert pipeline.status == PipelineStatus.SUCCEEDED

        # Verify no jobs were enqueued in Redis
        jobs = await arq_redis.queued_jobs()
        assert len(jobs) == 0


@pytest.mark.unit
class TestCoordinatePipelineUnit:
    """Unit tests for pipeline coordination logic."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "new_status",
        CANCELLED_PIPELINE_STATUSES,
    )
    async def test_coordinate_pipeline_cancels_remaining_jobs_status_transitions_to_cancellable(
        self,
        mock_pipeline_manager,
        new_status,
    ):
        """Test that remaining jobs are cancelled if pipeline transitions to a cancelable status."""
        with (
            patch.object(
                mock_pipeline_manager, "transition_pipeline_status", return_value=new_status
            ) as mock_transition,
            patch.object(mock_pipeline_manager, "cancel_remaining_jobs", return_value=None) as mock_cancel,
            patch.object(mock_pipeline_manager, "enqueue_ready_jobs", return_value=None) as mock_enqueue,
            TransactionSpy.spy(mock_pipeline_manager.db, expect_flush=True),
        ):
            await mock_pipeline_manager.coordinate_pipeline()

        mock_transition.assert_called_once()
        mock_cancel.assert_called_once_with(reason="Pipeline failed or cancelled")
        mock_enqueue.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "new_status",
        RUNNING_PIPELINE_STATUSES,
    )
    async def test_coordinate_pipeline_enqueues_jobs_when_status_transitions_to_running(
        self, mock_pipeline_manager, new_status
    ):
        """Test coordination after successful job completion."""
        with (
            patch.object(
                mock_pipeline_manager, "transition_pipeline_status", return_value=new_status
            ) as mock_transition,
            patch.object(mock_pipeline_manager, "cancel_remaining_jobs", return_value=None) as mock_cancel,
            patch.object(mock_pipeline_manager, "enqueue_ready_jobs", return_value=None) as mock_enqueue,
            TransactionSpy.spy(mock_pipeline_manager.db, expect_flush=True),
        ):
            await mock_pipeline_manager.coordinate_pipeline()

        assert mock_transition.call_count == 2  # Called once before and once after enqueuing jobs
        mock_cancel.assert_not_called()
        mock_enqueue.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "new_status",
        [
            status
            for status in PipelineStatus._member_map_.values()
            if status not in CANCELLED_PIPELINE_STATUSES + RUNNING_PIPELINE_STATUSES
        ],
    )
    async def test_coordinate_pipeline_noop_for_other_status_transitions(self, mock_pipeline_manager, new_status):
        """Test coordination no-op for non-cancelled/running status transitions."""
        with (
            patch.object(
                mock_pipeline_manager, "transition_pipeline_status", return_value=new_status
            ) as mock_transition,
            patch.object(mock_pipeline_manager, "cancel_remaining_jobs", return_value=None) as mock_cancel,
            patch.object(mock_pipeline_manager, "enqueue_ready_jobs", return_value=None) as mock_enqueue,
            TransactionSpy.spy(mock_pipeline_manager.db, expect_flush=True),
        ):
            await mock_pipeline_manager.coordinate_pipeline()

        mock_transition.assert_called_once()
        mock_cancel.assert_not_called()
        mock_enqueue.assert_not_called()


@pytest.mark.integration
class TestCoordinatePipelineIntegration:
    """Test pipeline coordination after job completion."""

    @pytest.mark.asyncio
    async def test_coordinate_pipeline_transitions_pipeline_to_failed_after_job_failure(
        self, session, arq_redis, with_populated_job_data, sample_pipeline, sample_job_run, sample_dependent_job_run
    ):
        """Test successful pipeline coordination and job enqueuing after job completion."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set the job in the pipeline to a terminal status
        sample_job_run.status = JobStatus.FAILED
        session.commit()

        with (
            TransactionSpy.spy(session, expect_flush=True),
            patch.object(manager, "cancel_remaining_jobs", wraps=manager.cancel_remaining_jobs) as mock_cancel,
            patch.object(manager, "enqueue_ready_jobs", wraps=manager.enqueue_ready_jobs) as mock_enqueue,
        ):
            await manager.coordinate_pipeline()

        # Ensure no new jobs were enqueued but that jobs were cancelled
        mock_cancel.assert_called_once()
        mock_enqueue.assert_not_called()

        # Verify that the pipeline status is now FAILED
        assert manager.get_pipeline().status == PipelineStatus.FAILED

        # Verify that the failed job remains failed
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.FAILED

        # Verify that the pending job transitions to skipped
        job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert job.status == JobStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_coordinate_pipeline_transitions_pipeline_to_cancelled_after_pipeline_is_cancelled(
        self, session, arq_redis, with_populated_job_data, sample_pipeline, sample_job_run, sample_dependent_job_run
    ):
        """Test successful pipeline coordination and job enqueuing after pipeline cancellation  ."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set the pipeline to a cancelled status
        manager.set_pipeline_status(PipelineStatus.CANCELLED)
        session.commit()

        # Set the job in the pipeline to a running status
        sample_job_run.status = JobStatus.RUNNING
        session.commit()

        with (
            TransactionSpy.spy(session, expect_flush=True),
            patch.object(manager, "cancel_remaining_jobs", wraps=manager.cancel_remaining_jobs) as mock_cancel,
            patch.object(manager, "enqueue_ready_jobs", wraps=manager.enqueue_ready_jobs) as mock_enqueue,
        ):
            await manager.coordinate_pipeline()

        # Ensure no new jobs were enqueued but that jobs were cancelled
        mock_cancel.assert_called_once()
        mock_enqueue.assert_not_called()

        # Verify that the pipeline status is now CANCELLED
        assert manager.get_pipeline().status == PipelineStatus.CANCELLED

        # Verify that the running job transitions to cancelled
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.CANCELLED

        # Verify that the pending dependent job transitions to skipped
        job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert job.status == JobStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_coordinate_running_pipeline_enqueues_ready_jobs(
        self, session, arq_redis, with_populated_job_data, sample_pipeline, sample_job_run, sample_dependent_job_run
    ):
        """Test successful pipeline coordination and job enqueuing when jobs are still pending."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set the pipeline to a running status
        manager.set_pipeline_status(PipelineStatus.RUNNING)
        session.commit()

        with (
            TransactionSpy.spy(session, expect_flush=True),
            patch.object(manager, "cancel_remaining_jobs", wraps=manager.cancel_remaining_jobs) as mock_cancel,
            patch.object(manager, "enqueue_ready_jobs", wraps=manager.enqueue_ready_jobs) as mock_enqueue,
        ):
            await manager.coordinate_pipeline()

        # Ensure no new jobs were cancelled but that jobs were enqueued
        mock_cancel.assert_not_called()
        mock_enqueue.assert_called_once()

        # Verify that the non-dependent job is now queued
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED

        # Verify that the dependent job is still pending (since its dependency is not yet complete)
        job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "initial_status",
        [PipelineStatus.CREATED, PipelineStatus.PAUSED, PipelineStatus.SUCCEEDED, PipelineStatus.PARTIAL],
    )
    async def test_coordinate_pipeline_noop(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
        initial_status,
    ):
        """Test successful pipeline coordination and job enqueuing when jobs are still pending."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set the pipeline to a cancelled status
        manager.set_pipeline_status(initial_status)
        session.commit()

        with (
            TransactionSpy.spy(session, expect_flush=True),
            patch.object(manager, "cancel_remaining_jobs", wraps=manager.cancel_remaining_jobs) as mock_cancel,
            patch.object(manager, "enqueue_ready_jobs", wraps=manager.enqueue_ready_jobs) as mock_enqueue,
        ):
            await manager.coordinate_pipeline()

        # Ensure no new jobs were enqueued or cancelled
        mock_cancel.assert_not_called()
        mock_enqueue.assert_not_called()

        # Verify that the job is still pending
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING

        # Verify that the dependent job is still pending
        job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING


@pytest.mark.unit
class TestTransitionPipelineStatusUnit:
    """Test pipeline status transition logic."""

    @pytest.mark.parametrize(
        "existing_status",
        TERMINAL_PIPELINE_STATUSES,
    )
    def test_terminal_state_results_in_retention_of_terminal_states(
        self, mock_pipeline_manager, existing_status, mock_pipeline
    ):
        """No jobs in pipeline should result in no status change, so long as the pipeline is in a terminal state."""
        mock_pipeline.status = existing_status

        with (
            patch.object(mock_pipeline_manager, "get_job_counts_by_status", return_value={}),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            result = mock_pipeline_manager.transition_pipeline_status()
            assert result is existing_status

        mock_set_status.assert_not_called()

    def test_paused_state_results_in_retention_of_paused_state(self, mock_pipeline_manager, mock_pipeline):
        """No jobs in pipeline should result in no status change when pipeline is paused."""
        mock_pipeline.status = PipelineStatus.PAUSED

        with (
            patch.object(mock_pipeline_manager, "get_job_counts_by_status", return_value={}),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            result = mock_pipeline_manager.transition_pipeline_status()
            assert result is PipelineStatus.PAUSED

        mock_set_status.assert_not_called()

    @pytest.mark.parametrize(
        "existing_status",
        [
            status
            for status in PipelineStatus._member_map_.values()
            if status not in TERMINAL_PIPELINE_STATUSES + [PipelineStatus.PAUSED]
        ],
    )
    def test_no_jobs_results_in_succeeded_state_if_not_terminal(
        self, mock_pipeline_manager, existing_status, mock_pipeline
    ):
        """No jobs in pipeline should result in SUCCEEDED state if not already terminal."""
        mock_pipeline.status = existing_status
        mock_pipeline.finished_at = None
        with (
            patch.object(mock_pipeline_manager, "get_job_counts_by_status", return_value={}),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            result = mock_pipeline_manager.transition_pipeline_status()
            assert result == PipelineStatus.SUCCEEDED

        mock_set_status.assert_called_once_with(PipelineStatus.SUCCEEDED)

    @pytest.mark.parametrize(
        "job_counts,expected_status",
        [
            # Any failure trumps everything
            ({JobStatus.SUCCEEDED: 10, JobStatus.FAILED: 1}, PipelineStatus.FAILED),
            # Running or queued jobs without failures keep pipeline running
            ({JobStatus.SUCCEEDED: 5, JobStatus.FAILED: 0, JobStatus.RUNNING: 2}, PipelineStatus.RUNNING),
            ({JobStatus.SUCCEEDED: 5, JobStatus.FAILED: 0, JobStatus.QUEUED: 3}, PipelineStatus.RUNNING),
            # All succeeded
            ({JobStatus.SUCCEEDED: 5}, PipelineStatus.SUCCEEDED),
            # Mix of terminal states without failures
            ({JobStatus.SUCCEEDED: 3, JobStatus.SKIPPED: 2}, PipelineStatus.PARTIAL),
            ({JobStatus.SUCCEEDED: 1, JobStatus.CANCELLED: 1}, PipelineStatus.PARTIAL),
            # All cancelled
            ({JobStatus.CANCELLED: 5}, PipelineStatus.CANCELLED),
            # All skipped
            ({JobStatus.SKIPPED: 4}, PipelineStatus.CANCELLED),
            # Some cancelled and skipped
            ({JobStatus.CANCELLED: 2, JobStatus.SKIPPED: 3}, PipelineStatus.CANCELLED),
            # Inconsistent state
            ({JobStatus.CANCELLED: 2, JobStatus.SKIPPED: 1, JobStatus.SUCCEEDED: 1, None: 3}, PipelineStatus.PARTIAL),
        ],
    )
    def test_pipeline_status_determination_based_on_job_counts(
        self, mock_pipeline_manager, job_counts, expected_status, mock_pipeline
    ):
        """Test pipeline status determination based on job counts."""
        mock_pipeline.status = PipelineStatus.CREATED
        mock_pipeline.finished_at = None

        with (
            patch.object(mock_pipeline_manager, "get_job_counts_by_status", return_value=job_counts),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            result = mock_pipeline_manager.transition_pipeline_status()
            assert result == expected_status

        mock_set_status.assert_called_once_with(expected_status)

    @pytest.mark.parametrize(
        "job_counts,existing_status",
        [
            ({JobStatus.PENDING: 5}, PipelineStatus.CREATED),
            ({JobStatus.SUCCEEDED: 5, JobStatus.PENDING: 3}, PipelineStatus.RUNNING),
            ({JobStatus.PENDING: 2, JobStatus.SKIPPED: 4}, PipelineStatus.RUNNING),
            ({JobStatus.PENDING: 1, JobStatus.CANCELLED: 1}, PipelineStatus.RUNNING),
        ],
    )
    def test_pipeline_status_determination_pending_jobs_do_not_change_status(
        self, mock_pipeline_manager, job_counts, existing_status, mock_pipeline
    ):
        """Test that presence of pending jobs does not change pipeline status."""
        mock_pipeline.status = existing_status

        with (
            patch.object(
                mock_pipeline_manager,
                "get_job_counts_by_status",
                return_value=job_counts,
            ),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            result = mock_pipeline_manager.transition_pipeline_status()
            assert result == existing_status

        mock_set_status.assert_not_called()

    @pytest.mark.parametrize(
        "exception",
        HANDLED_EXCEPTIONS_DURING_OBJECT_MANIPULATION,
    )
    def test_pipeline_status_determination_throws_state_error_for_handled_exceptions(
        self, mock_pipeline_manager, exception
    ):
        """Test that handled exceptions during status determination raise PipelineStateError."""

        # Mocks exception in first try/except
        with (
            patch.object(
                mock_pipeline_manager,
                "get_job_counts_by_status",
                return_value=Mock(side_effect=exception),
            ),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            TransactionSpy.spy(mock_pipeline_manager.db),
            pytest.raises(PipelineStateError),
        ):
            mock_pipeline_manager.transition_pipeline_status()
        mock_set_status.assert_not_called()

        # Mocks exception in second try/except
        with (
            patch.object(
                mock_pipeline_manager,
                "get_job_counts_by_status",
                return_value={JobStatus.SUCCEEDED: 5},
            ),
            patch.object(mock_pipeline_manager, "set_pipeline_status", side_effect=exception) as mock_set_status,
            patch.object(
                mock_pipeline_manager, "get_pipeline", return_value=Mock(spec=Pipeline, status=PipelineStatus.CREATED)
            ),
            pytest.raises(PipelineStateError),
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            mock_pipeline_manager.transition_pipeline_status()

    def test_pipeline_status_determination_no_change(self, mock_pipeline_manager, mock_pipeline):
        """Test that no status change occurs if pipeline status remains the same."""
        mock_pipeline.status = PipelineStatus.SUCCEEDED
        with (
            patch.object(mock_pipeline_manager, "get_job_counts_by_status", return_value={JobStatus.SUCCEEDED: 5}),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            result = mock_pipeline_manager.transition_pipeline_status()
            assert result == PipelineStatus.SUCCEEDED

        mock_set_status.assert_not_called()


class TestTransitionPipelineStatusIntegration:
    """Integration tests for pipeline status transition logic."""

    @pytest.mark.parametrize(
        "initial_status",
        TERMINAL_PIPELINE_STATUSES,
    )
    def test_pipeline_status_transition_noop_when_status_is_terminal(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        initial_status,
    ):
        """Test that pipeline status remains unchanged when already in a terminal state."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set initial pipeline status
        manager.set_pipeline_status(initial_status)
        session.commit()

        with TransactionSpy.spy(session):
            new_status = manager.transition_pipeline_status()

        # Commit the transaction
        session.commit()

        # Verify that the pipeline status remains unchanged
        assert new_status == initial_status
        assert manager.get_pipeline_status() == initial_status

    def test_pipeline_status_transition_noop_when_status_is_paused(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
    ):
        """Test that pipeline status remains unchanged when in PAUSED state."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set initial pipeline status to PAUSED
        manager.set_pipeline_status(PipelineStatus.PAUSED)
        session.commit()

        with TransactionSpy.spy(session):
            new_status = manager.transition_pipeline_status()

        # Commit the transaction
        session.commit()

        # Verify that the pipeline status remains unchanged
        assert new_status == PipelineStatus.PAUSED
        assert manager.get_pipeline_status() == PipelineStatus.PAUSED

    @pytest.mark.parametrize(
        "initial_status,expected_status",
        [
            (
                status,
                status if status in TERMINAL_PIPELINE_STATUSES + [PipelineStatus.PAUSED] else PipelineStatus.SUCCEEDED,
            )
            for status in PipelineStatus._member_map_.values()
        ],
    )
    def test_pipeline_status_transition_when_no_jobs_in_pipeline(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        initial_status,
        expected_status,
        sample_empty_pipeline,
    ):
        """Test that pipeline status transitions to SUCCEEDED when there are no jobs in a
        non-terminal pipeline. If the pipeline is already in a terminal state, it should remain unchanged."""
        manager = PipelineManager(session, arq_redis, sample_empty_pipeline.id)

        # Set initial pipeline status
        manager.set_pipeline_status(initial_status)
        session.commit()

        with TransactionSpy.spy(session):
            new_status = manager.transition_pipeline_status()

        # Commit the transaction
        session.commit()

        # Verify that the pipeline status is the expected status and that
        # the status was persisted to the transaction
        assert new_status == expected_status
        assert manager.get_pipeline_status() == expected_status

    @pytest.mark.parametrize(
        "initial_status,job_updates,expected_status",
        [
            # Some failed -> failed
            (PipelineStatus.CREATED, {1: JobStatus.SUCCEEDED, 2: JobStatus.FAILED}, PipelineStatus.FAILED),
            # Some running -> running
            (PipelineStatus.CREATED, {1: JobStatus.SUCCEEDED, 2: JobStatus.RUNNING}, PipelineStatus.RUNNING),
            # Some queued -> running
            (PipelineStatus.CREATED, {1: JobStatus.SUCCEEDED, 2: JobStatus.QUEUED}, PipelineStatus.RUNNING),
            # Some pending => no change (handled separately via a second call to transition after enqueuing jobs)
            (PipelineStatus.CREATED, {1: JobStatus.SUCCEEDED, 2: JobStatus.PENDING}, PipelineStatus.CREATED),
            (PipelineStatus.RUNNING, {1: JobStatus.SUCCEEDED, 2: JobStatus.PENDING}, PipelineStatus.RUNNING),
            # All succeeded -> succeeded
            (PipelineStatus.CREATED, {1: JobStatus.SUCCEEDED, 2: JobStatus.SUCCEEDED}, PipelineStatus.SUCCEEDED),
            # All cancelled -> cancelled
            (PipelineStatus.RUNNING, {1: JobStatus.CANCELLED, 2: JobStatus.CANCELLED}, PipelineStatus.CANCELLED),
            # Mix of succeeded and skipped -> partial
            (PipelineStatus.CREATED, {1: JobStatus.SUCCEEDED, 2: JobStatus.SKIPPED}, PipelineStatus.PARTIAL),
            # Mix of succeeded and cancelled -> partial
            (PipelineStatus.CREATED, {1: JobStatus.SUCCEEDED, 2: JobStatus.CANCELLED}, PipelineStatus.PARTIAL),
            # Mix of cancelled and skipped -> cancelled
            (PipelineStatus.CREATED, {1: JobStatus.CANCELLED, 2: JobStatus.SKIPPED}, PipelineStatus.CANCELLED),
        ],
    )
    def test_pipeline_status_transitions(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        initial_status,
        job_updates,
        expected_status,
    ):
        """Test pipeline status transitions based on job status updates."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set initial pipeline status
        manager.set_pipeline_status(initial_status)
        session.commit()

        # Update job statuses as per test case
        for job_run in sample_pipeline.job_runs:
            if job_run.id in job_updates:
                job_run.status = job_updates[job_run.id]
        session.commit()

        # Perform status transition and verify return state
        with TransactionSpy.spy(session):
            new_status = manager.transition_pipeline_status()
            assert new_status == expected_status
        session.commit()

        # Verify expected pipeline status is persisted
        pipeline = manager.get_pipeline()
        assert pipeline.status == expected_status


@pytest.mark.unit
class TestEnqueueReadyJobsUnit:
    """Test enqueuing of ready jobs (both independent and dependent)."""

    @pytest.mark.parametrize(
        "pipeline_status",
        [status for status in PipelineStatus._member_map_.values() if status not in RUNNING_PIPELINE_STATUSES],
    )
    @pytest.mark.asyncio
    async def test_enqueue_ready_jobs_raises_if_pipeline_not_running(self, mock_pipeline_manager, pipeline_status):
        """Test that job enqueuing raises a state error if pipeline is not in RUNNING status."""
        with (
            patch.object(mock_pipeline_manager, "get_pipeline_status", return_value=pipeline_status),
            pytest.raises(PipelineStateError, match="cannot enqueue jobs"),
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            await mock_pipeline_manager.enqueue_ready_jobs()

    @pytest.mark.asyncio
    async def test_enqueue_ready_jobs_skips_if_no_jobs(self, mock_pipeline_manager):
        """Test that job enqueuing skips if there are no pending jobs."""
        with (
            patch.object(mock_pipeline_manager, "get_pipeline_status", return_value=PipelineStatus.RUNNING),
            patch.object(
                mock_pipeline_manager,
                "get_pending_jobs",
                return_value=[],
            ),
            TransactionSpy.spy(mock_pipeline_manager.db, expect_flush=True),
        ):
            await mock_pipeline_manager.enqueue_ready_jobs()
        # Should complete without error

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "should_skip",
        [False, True],
    )
    async def test_enqueue_ready_jobs_checks_if_jobs_are_reachable_if_cant_enqueue(
        self, mock_pipeline_manager, mock_job_manager, should_skip
    ):
        """Test that job enqueuing skips jobs which are unreachable if any exist."""
        with (
            patch.object(mock_pipeline_manager, "get_pipeline_status", return_value=PipelineStatus.RUNNING),
            patch.object(
                mock_pipeline_manager, "get_pending_jobs", return_value=[Mock(spec=JobRun, id=1, urn="test:job:1")]
            ),
            patch.object(mock_pipeline_manager, "can_enqueue_job", return_value=False),
            patch.object(
                mock_pipeline_manager, "should_skip_job_due_to_dependencies", return_value=(should_skip, "Reason")
            ) as mock_should_skip,
            patch.object(mock_job_manager, "skip_job", return_value=None) as mock_skip_job,
            TransactionSpy.spy(mock_pipeline_manager.db, expect_flush=True),
        ):
            await mock_pipeline_manager.enqueue_ready_jobs()

        mock_should_skip.assert_called_once()
        mock_skip_job.assert_called_once() if should_skip else mock_skip_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_enqueue_ready_jobs_raises_if_arq_enqueue_fails(self, mock_pipeline_manager, mock_job_manager):
        """Test that job enqueuing raises an error if ARQ enqueue fails."""
        with (
            patch.object(mock_pipeline_manager, "get_pipeline_status", return_value=PipelineStatus.RUNNING),
            patch.object(
                mock_pipeline_manager, "get_pending_jobs", return_value=[Mock(spec=JobRun, id=1, urn="test:job:1")]
            ),
            patch.object(mock_pipeline_manager, "can_enqueue_job", return_value=True),
            patch.object(mock_job_manager, "prepare_queue", return_value=None) as mock_prepare_queue,
            patch.object(
                mock_pipeline_manager, "_enqueue_in_arq", side_effect=PipelineCoordinationError("ARQ enqueue failed")
            ),
            pytest.raises(PipelineCoordinationError, match="ARQ enqueue failed"),
            TransactionSpy.spy(mock_pipeline_manager.db, expect_flush=True),
        ):
            await mock_pipeline_manager.enqueue_ready_jobs()

        mock_prepare_queue.assert_called_once()

    @pytest.mark.asyncio
    async def test_enqueue_ready_jobs_successful_enqueue(self, mock_pipeline_manager, mock_job_manager):
        """Test successful job enqueuing."""
        with (
            patch.object(mock_pipeline_manager, "get_pipeline_status", return_value=PipelineStatus.RUNNING),
            patch.object(
                mock_pipeline_manager, "get_pending_jobs", return_value=[Mock(spec=JobRun, id=1, urn="test:job:1")]
            ),
            patch.object(mock_pipeline_manager, "can_enqueue_job", return_value=True),
            patch.object(mock_pipeline_manager, "_enqueue_in_arq", return_value=None) as mock_enqueue,
            patch.object(mock_job_manager, "prepare_queue", return_value=None) as mock_prepare_queue,
            TransactionSpy.spy(mock_pipeline_manager.db, expect_flush=True),
        ):
            await mock_pipeline_manager.enqueue_ready_jobs()

        mock_prepare_queue.assert_called_once()
        mock_enqueue.assert_called_once()


@pytest.mark.integration
class TestEnqueueReadyJobsIntegration:
    """Integration tests for enqueuing of ready jobs."""

    @pytest.mark.asyncio
    async def test_enqueue_ready_jobs_integration(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test successful enqueuing of ready jobs in a pipeline."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set the pipeline to RUNNING status
        manager.set_pipeline_status(PipelineStatus.RUNNING)
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True):
            await manager.enqueue_ready_jobs()

        # Verify that the independent job is now queued
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED

        # Verify that the dependent job is still pending (since its dependency is not yet complete)
        job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING

        # Verify the queued ARQ job exists and is the job we expect
        arq_job = await arq_redis.queued_jobs()
        assert len(arq_job) == 1
        assert arq_job[0].function == sample_job_run.job_function

        # Verify the pipeline is still in RUNNING status
        assert manager.get_pipeline_status() == PipelineStatus.RUNNING

    @pytest.mark.asyncio
    async def test_enqueue_ready_jobs_integration_with_unreachable_job(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
        sample_job_dependency,
    ):
        """Test enqueuing of ready jobs skips unreachable jobs in a pipeline."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set the pipeline to RUNNING status
        manager.set_pipeline_status(PipelineStatus.RUNNING)
        session.commit()

        # Make the dependent job unreachable by setting the sample_job to cancelled.
        sample_job_run.status = JobStatus.CANCELLED
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True):
            await manager.enqueue_ready_jobs()

        # Verify that the dependent job is marked as skipped
        job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert job.status == JobStatus.SKIPPED

        # Verify nothing was enqueued for the dependent job
        arq_job = await arq_redis.queued_jobs()
        assert len(arq_job) == 0

        # Verify the pipeline is still in RUNNING status
        assert manager.get_pipeline_status() == PipelineStatus.RUNNING

    @pytest.mark.asyncio
    async def test_enqueue_ready_jobs_with_empty_pipeline(
        self, session, arq_redis, with_populated_job_data, sample_empty_pipeline
    ):
        """Test enqueuing of ready jobs in an empty pipeline."""
        manager = PipelineManager(session, arq_redis, sample_empty_pipeline.id)

        # Set the pipeline to RUNNING status
        manager.set_pipeline_status(PipelineStatus.RUNNING)
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True):
            await manager.enqueue_ready_jobs()

        # Verify nothing was enqueued
        arq_job = await arq_redis.queued_jobs()
        assert len(arq_job) == 0

        # Verify the pipeline is still in RUNNING status
        assert manager.get_pipeline_status() == PipelineStatus.RUNNING

    @pytest.mark.asyncio
    async def test_enqueue_ready_jobs_bubbles_pipeline_coordination_error_for_any_exception_during_enqueue(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
    ):
        """Test that any exception during job enqueuing raises PipelineCoordinationError."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set the pipeline to RUNNING status
        manager.set_pipeline_status(PipelineStatus.RUNNING)
        session.commit()

        with (
            TransactionSpy.spy(session, expect_flush=True),
            patch.object(
                manager.redis,
                "enqueue_job",
                side_effect=Exception("Unexpected error during enqueue"),
            ),
            pytest.raises(PipelineCoordinationError, match="Failed to enqueue job in ARQ"),
        ):
            await manager.enqueue_ready_jobs()


@pytest.mark.unit
class TestCancelRemainingJobsUnit:
    """Test cancellation of remaining jobs."""

    def test_cancel_remaining_jobs_no_active_jobs(self, mock_pipeline_manager, mock_job_manager):
        """Test job cancellation when there are no active jobs."""
        with (
            patch.object(
                mock_pipeline_manager,
                "get_active_jobs",
                return_value=[],
            ),
            patch.object(mock_job_manager, "cancel_job", return_value=None) as mock_cancel_job,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            mock_pipeline_manager.cancel_remaining_jobs()

        mock_cancel_job.assert_not_called()

    @pytest.mark.parametrize(
        "job_status, expected_status",
        [(JobStatus.QUEUED, JobStatus.CANCELLED), (JobStatus.RUNNING, JobStatus.CANCELLED)],
    )
    def test_cancel_remaining_jobs_cancels_queued_and_running_jobs(
        self, mock_pipeline_manager, mock_job_manager, mock_job_run, job_status, expected_status
    ):
        """Test successful cancellation of remaining jobs."""
        mock_job_run.status = job_status
        cancellation_result = {"status": expected_status, "reason": "Pipeline cancelled"}

        with (
            patch.object(
                mock_pipeline_manager,
                "get_active_jobs",
                return_value=[mock_job_run],
            ),
            patch.object(mock_job_manager, "cancel_job", return_value=None) as mock_cancel_job,
            patch(
                "mavedb.worker.lib.managers.pipeline_manager.construct_bulk_cancellation_result",
                return_value=cancellation_result,
            ),
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            mock_pipeline_manager.cancel_remaining_jobs()

        mock_cancel_job.assert_called_once_with(result=cancellation_result)

    @pytest.mark.parametrize(
        "job_status, expected_status",
        [
            (JobStatus.PENDING, JobStatus.SKIPPED),
        ],
    )
    def test_cancel_remaining_jobs_skips_pending_jobs(
        self, mock_pipeline_manager, mock_job_manager, mock_job_run, job_status, expected_status
    ):
        """Test successful cancellation of remaining jobs."""
        mock_job_run.status = job_status
        cancellation_result = {"status": expected_status, "reason": "Pipeline cancelled"}

        with (
            patch.object(
                mock_pipeline_manager,
                "get_active_jobs",
                return_value=[mock_job_run],
            ),
            patch.object(mock_job_manager, "skip_job", return_value=None) as mock_skip_job,
            patch(
                "mavedb.worker.lib.managers.pipeline_manager.construct_bulk_cancellation_result",
                return_value=cancellation_result,
            ),
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            mock_pipeline_manager.cancel_remaining_jobs()

        mock_skip_job.assert_called_once_with(result=cancellation_result)


@pytest.mark.integration
class TestCancelRemainingJobsIntegration:
    """Integration tests for cancellation of remaining jobs."""

    def test_cancel_remaining_jobs_integration(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test successful cancellation of remaining jobs in a pipeline."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set the job statuses
        sample_job_run.status = JobStatus.RUNNING
        sample_dependent_job_run.status = JobStatus.PENDING
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            manager.cancel_remaining_jobs()

        # Commit the transaction
        session.commit()

        # Verify that the running job transitions to cancelled
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.CANCELLED

        # Verify that the pending dependent job transitions to skipped
        job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert job.status == JobStatus.SKIPPED

    def test_cancel_remaining_jobs_integration_no_active_jobs(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_empty_pipeline,
    ):
        """Test cancellation of remaining jobs when there are no active jobs."""
        manager = PipelineManager(session, arq_redis, sample_empty_pipeline.id)

        with (
            TransactionSpy.spy(session),
        ):
            manager.cancel_remaining_jobs()

        # Commit the transaction
        session.commit()

        # Should complete without error


@pytest.mark.unit
class TestCancelPipelineUnit:
    """Test cancellation of pipelines."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "pipeline_status",
        TERMINAL_PIPELINE_STATUSES,
    )
    async def test_cancel_pipeline_raises_transition_error_if_already_in_terminal_status(
        self, mock_pipeline_manager, pipeline_status
    ):
        """Test that pipeline cancellation raises an error if already in terminal status."""
        with (
            patch.object(mock_pipeline_manager, "get_pipeline_status", return_value=pipeline_status),
            pytest.raises(
                PipelineTransitionError,
                match=f"Pipeline {mock_pipeline_manager.pipeline_id} is in terminal state",
            ),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None) as mock_coordinate,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            await mock_pipeline_manager.cancel_pipeline(reason="Testing cancellation")

        mock_set_status.assert_not_called()
        mock_coordinate.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "pipeline_status",
        [status for status in PipelineStatus._member_map_.values() if status not in TERMINAL_PIPELINE_STATUSES],
    )
    async def test_cancel_pipeline_successful_cancellation_if_not_in_terminal_status(
        self, mock_pipeline_manager, pipeline_status
    ):
        """Test successful pipeline cancellation if not already in terminal status."""
        with (
            patch.object(mock_pipeline_manager, "get_pipeline_status", return_value=pipeline_status),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None) as mock_coordinate,
            TransactionSpy.spy(mock_pipeline_manager.db, expect_flush=True),
        ):
            await mock_pipeline_manager.cancel_pipeline(reason="Testing cancellation")

        mock_coordinate.assert_called_once()
        mock_set_status.assert_called_once_with(PipelineStatus.CANCELLED)


@pytest.mark.integration
class TestCancelPipelineIntegration:
    """Integration tests for cancellation of pipelines."""

    @pytest.mark.asyncio
    async def test_cancel_pipeline_integration(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test successful cancellation of a pipeline."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set the pipeline to RUNNING status
        manager.set_pipeline_status(PipelineStatus.RUNNING)
        session.commit()

        # Set the job statuses
        sample_job_run.status = JobStatus.RUNNING
        sample_dependent_job_run.status = JobStatus.PENDING
        session.commit()

        with (
            TransactionSpy.spy(session, expect_flush=True),
        ):
            await manager.cancel_pipeline(reason="Testing cancellation")

        # Commit the transaction
        session.commit()

        # Verify that the pipeline is now in CANCELLED status
        assert manager.get_pipeline_status() == PipelineStatus.CANCELLED

        # Verify that the running job transitions to cancelled
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.CANCELLED

        # Verify that the pending dependent job transitions to skipped
        job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert job.status == JobStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_cancel_pipeline_integration_already_terminal(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
    ):
        """Test that cancelling a pipeline already in terminal status raises an error."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set the pipeline to SUCCEEDED status
        manager.set_pipeline_status(PipelineStatus.SUCCEEDED)
        session.commit()

        # Set the job status to something that would normally be cancellable
        sample_job_run.status = JobStatus.PENDING
        session.commit()

        with (
            pytest.raises(
                PipelineTransitionError,
                match=f"Pipeline {manager.pipeline_id} is in terminal state",
            ),
            TransactionSpy.spy(session),
        ):
            await manager.cancel_pipeline(reason="Testing cancellation")

        # Commit the transaction
        session.commit()

        # Verify the pipeline status remains SUCCEEDED
        assert manager.get_pipeline_status() == PipelineStatus.SUCCEEDED

        # Verify that the job status remains unchanged
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING


@pytest.mark.unit
class TestPausePipelineUnit:
    """Test pausing of pipelines."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "pipeline_status",
        TERMINAL_PIPELINE_STATUSES,
    )
    async def test_pause_pipeline_raises_transition_error_if_already_in_terminal_status(
        self, mock_pipeline_manager, pipeline_status
    ):
        """Test that pipeline pausing raises an error if already in terminal status."""
        with (
            patch.object(mock_pipeline_manager, "get_pipeline_status", return_value=pipeline_status),
            pytest.raises(
                PipelineTransitionError,
                match=f"Pipeline {mock_pipeline_manager.pipeline_id} is in terminal state",
            ),
            TransactionSpy.spy(mock_pipeline_manager.db),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None) as mock_coordinate,
        ):
            await mock_pipeline_manager.pause_pipeline()

        mock_set_status.assert_not_called()
        mock_coordinate.assert_not_called()

    @pytest.mark.asyncio
    async def test_pause_pipeline_raises_transition_error_if_already_paused(self, mock_pipeline_manager):
        """Test that pipeline pausing raises an error if already paused."""
        with (
            patch.object(mock_pipeline_manager, "get_pipeline_status", return_value=PipelineStatus.PAUSED),
            pytest.raises(
                PipelineTransitionError,
                match=f"Pipeline {mock_pipeline_manager.pipeline_id} is already paused",
            ),
            TransactionSpy.spy(mock_pipeline_manager.db),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None) as mock_coordinate,
        ):
            await mock_pipeline_manager.pause_pipeline()

        mock_set_status.assert_not_called()
        mock_coordinate.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "pipeline_status",
        [
            status
            for status in PipelineStatus._member_map_.values()
            if status not in TERMINAL_PIPELINE_STATUSES and status != PipelineStatus.PAUSED
        ],
    )
    async def test_pause_pipeline_successful_pausing_if_not_in_terminal_status(
        self, mock_pipeline_manager, pipeline_status
    ):
        """Test successful pipeline pausing if not already in terminal status."""
        with (
            patch.object(mock_pipeline_manager, "get_pipeline_status", return_value=pipeline_status),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None) as mock_coordinate,
            TransactionSpy.spy(mock_pipeline_manager.db, expect_flush=True),
        ):
            await mock_pipeline_manager.pause_pipeline()

        mock_coordinate.assert_called_once()
        mock_set_status.assert_called_once_with(PipelineStatus.PAUSED)


@pytest.mark.integration
class TestPausePipelineIntegration:
    """Integration tests for pausing of pipelines."""

    @pytest.mark.asyncio
    async def test_pause_pipeline_integration(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
    ):
        """Test successful pausing of a pipeline."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set the pipeline to RUNNING status
        manager.set_pipeline_status(PipelineStatus.RUNNING)
        session.commit()

        with (
            TransactionSpy.spy(session, expect_flush=True),
        ):
            await manager.pause_pipeline()

        # Commit the transaction
        session.commit()

        # Verify that the pipeline is now in PAUSED status
        assert manager.get_pipeline_status() == PipelineStatus.PAUSED

        # Verify that all jobs remain in their original statuses
        # (coordinate_pipeline is called by pause_pipeline but should not change job statuses
        # while paused).
        for job_run in sample_pipeline.job_runs:
            assert job_run.status == JobStatus.PENDING


@pytest.mark.unit
class TestUnpausePipelineUnit:
    """Test unpausing of pipelines."""

    @pytest.mark.asyncio
    async def test_unpause_pipeline_raises_transition_error_if_not_paused(self, mock_pipeline_manager):
        """Test that pipeline unpausing raises an error if not currently paused."""
        with (
            patch.object(mock_pipeline_manager, "get_pipeline_status", return_value=PipelineStatus.RUNNING),
            pytest.raises(
                PipelineTransitionError,
                match=f"Pipeline {mock_pipeline_manager.pipeline_id} is not paused",
            ),
            TransactionSpy.spy(mock_pipeline_manager.db),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None) as mock_coordinate,
        ):
            await mock_pipeline_manager.unpause_pipeline()

        mock_set_status.assert_not_called()
        mock_coordinate.assert_not_called()

    @pytest.mark.asyncio
    async def test_unpause_pipeline_successful_unpausing_if_currently_paused(self, mock_pipeline_manager):
        """Test successful pipeline unpausing if currently paused."""
        with (
            patch.object(mock_pipeline_manager, "get_pipeline_status", return_value=PipelineStatus.PAUSED),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None) as mock_coordinate,
            TransactionSpy.spy(mock_pipeline_manager.db, expect_flush=True),
        ):
            await mock_pipeline_manager.unpause_pipeline()

        mock_coordinate.assert_called_once()
        mock_set_status.assert_called_once_with(PipelineStatus.RUNNING)


@pytest.mark.integration
class TestUnpausePipelineIntegration:
    """Integration tests for unpausing of pipelines."""

    @pytest.mark.asyncio
    async def test_unpause_pipeline_integration(
        self, session, arq_redis, with_populated_job_data, sample_pipeline, sample_job_run, sample_dependent_job_run
    ):
        """Test successful unpausing of a pipeline."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set the pipeline to PAUSED status
        manager.set_pipeline_status(PipelineStatus.PAUSED)
        session.commit()

        with (
            TransactionSpy.spy(session, expect_flush=True),
        ):
            await manager.unpause_pipeline()

        # Commit the transaction
        session.commit()

        # Verify that the pipeline is now in RUNNING status
        assert manager.get_pipeline_status() == PipelineStatus.RUNNING

        # Verify that the non-dependent job was queued
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED


@pytest.mark.unit
class TestRestartPipelineUnit:
    """Test restarting of pipelines."""

    @pytest.mark.asyncio
    async def test_restart_pipeline_skips_if_no_jobs_in_pipeline(self, mock_pipeline_manager):
        """Test that pipeline restart skips if there are no jobs in the pipeline."""
        with (
            patch.object(
                mock_pipeline_manager,
                "get_all_jobs",
                return_value=[],
            ),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None) as mock_coordinate,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            await mock_pipeline_manager.restart_pipeline()

        mock_set_status.assert_not_called()
        mock_coordinate.assert_not_called()

    @pytest.mark.asyncio
    async def test_restart_pipeline_successful_restart(self, mock_pipeline_manager, mock_job_manager):
        """Test successful pipeline restart."""
        with (
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            patch.object(mock_pipeline_manager, "start_pipeline", return_value=None) as mock_start_pipeline,
            patch.object(
                mock_pipeline_manager,
                "get_all_jobs",
                return_value=[Mock(spec=JobRun, id=1), Mock(spec=JobRun, id=2)],
            ),
            patch.object(
                mock_job_manager,
                "reset_job",
                return_value=None,
            ) as mock_reset_job,
            TransactionSpy.spy(mock_pipeline_manager.db, expect_flush=True),
        ):
            await mock_pipeline_manager.restart_pipeline()

        assert mock_reset_job.call_count == 2
        mock_set_status.assert_called_once_with(PipelineStatus.CREATED)
        mock_start_pipeline.assert_called_once()


@pytest.mark.integration
class TestRestartPipelineIntegration:
    """Integration tests for restarting of pipelines."""

    @pytest.mark.asyncio
    async def test_restart_pipeline_integration(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test successful restarting of a pipeline."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set the job statuses to terminal states
        sample_job_run.status = JobStatus.SUCCEEDED
        sample_dependent_job_run.status = JobStatus.FAILED
        session.commit()

        with (
            TransactionSpy.spy(session, expect_flush=True),
        ):
            await manager.restart_pipeline()

        # Commit the transaction
        session.commit()

        # Verify that the pipeline is now in RUNNING status
        assert manager.get_pipeline_status() == PipelineStatus.RUNNING

        # Verify that the non-dependent job is now queued
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED

        # Verify that the dependent job is now pending
        job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_restart_pipeline_integration_skips_if_no_jobs(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_empty_pipeline,
    ):
        """Test that restarting a pipeline with no jobs skips without error."""
        manager = PipelineManager(session, arq_redis, sample_empty_pipeline.id)

        # Set the pipeline to a terminal status
        manager.set_pipeline_status(PipelineStatus.SUCCEEDED)
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            await manager.restart_pipeline()

        # Commit the transaction
        session.commit()

        # Verify that the pipeline status remains unchanged
        assert manager.get_pipeline_status() == PipelineStatus.SUCCEEDED


@pytest.mark.unit
class TestCanEnqueueJobUnit:
    """Test job dependency checking."""

    def test_can_enqueue_job_with_no_dependencies(self, mock_pipeline_manager):
        """Test that a job with no dependencies can be enqueued."""
        mock_job = Mock(spec=JobRun, id=1)

        with (
            patch.object(
                mock_pipeline_manager,
                "get_dependencies_for_job",
                return_value=[],
            ),
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            result = mock_pipeline_manager.can_enqueue_job(mock_job)

        assert result is True

    def test_cannot_enqueue_job_with_unmet_dependencies(self, mock_pipeline_manager):
        """Test that a job with unmet dependencies cannot be enqueued."""
        mock_job = Mock(spec=JobRun, id=1, status=JobStatus.PENDING)
        mock_dependency = Mock(spec=JobDependency, dependency_type=DependencyType.COMPLETION_REQUIRED)

        with (
            patch.object(
                mock_pipeline_manager,
                "get_dependencies_for_job",
                return_value=[(mock_dependency, mock_job)],
            ),
            patch(
                "mavedb.worker.lib.managers.pipeline_manager.job_dependency_is_met", return_value=False
            ) as mock_job_dependency_is_met,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            result = mock_pipeline_manager.can_enqueue_job(mock_job)

        mock_job_dependency_is_met.assert_called_once_with(
            dependency_type=DependencyType.COMPLETION_REQUIRED, dependent_job_status=JobStatus.PENDING
        )
        assert result is False

    def test_can_enqueue_job_with_met_dependencies(self, mock_pipeline_manager):
        """Test that a job with met dependencies can be enqueued."""
        mock_job = Mock(spec=JobRun, id=1, status=JobStatus.SUCCEEDED)
        mock_dependency = Mock(spec=JobDependency, dependency_type=DependencyType.COMPLETION_REQUIRED)

        with (
            patch.object(
                mock_pipeline_manager,
                "get_dependencies_for_job",
                return_value=[(mock_dependency, mock_job)],
            ),
            patch(
                "mavedb.worker.lib.managers.pipeline_manager.job_dependency_is_met", return_value=True
            ) as mock_job_dependency_is_met,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            result = mock_pipeline_manager.can_enqueue_job(mock_job)

        mock_job_dependency_is_met.assert_called_once_with(
            dependency_type=DependencyType.COMPLETION_REQUIRED, dependent_job_status=JobStatus.SUCCEEDED
        )
        assert result is True

    @pytest.mark.parametrize(
        "exception",
        HANDLED_EXCEPTIONS_DURING_OBJECT_MANIPULATION,
    )
    def test_can_enqueue_job_raises_pipeline_state_error_on_handled_exceptions(self, mock_pipeline_manager, exception):
        """Test that handled exceptions during dependency checking raise PipelineStateError."""
        mock_job = Mock(spec=JobRun, id=1, status=JobStatus.SUCCEEDED)
        mock_dependency = Mock(spec=JobDependency, dependency_type=DependencyType.COMPLETION_REQUIRED)

        with (
            patch.object(
                mock_pipeline_manager,
                "get_dependencies_for_job",
                return_value=[(mock_dependency, mock_job)],
            ),
            patch("mavedb.worker.lib.managers.pipeline_manager.job_dependency_is_met", side_effect=exception),
            pytest.raises(PipelineStateError, match="Corrupted dependency data"),
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            mock_pipeline_manager.can_enqueue_job(mock_job)


@pytest.mark.integration
class TestCanEnqueueJobIntegration:
    """Integration tests for job dependency checking."""

    def test_can_enqueue_job_integration_with_no_dependencies(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
    ):
        """Test that a job with no dependencies can be enqueued."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        with (
            TransactionSpy.spy(session),
        ):
            result = manager.can_enqueue_job(sample_job_run)

        assert result is True

    def test_can_enqueue_job_integration_with_unmet_dependencies(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_dependent_job_run,
    ):
        """Test that a job with unmet dependencies cannot be enqueued."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        with (
            TransactionSpy.spy(session),
        ):
            result = manager.can_enqueue_job(sample_dependent_job_run)

        assert result is False

    def test_can_enqueue_job_integration_with_met_dependencies(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test that a job with met dependencies can be enqueued."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set the dependency job to a succeeded status
        sample_job_run.status = JobStatus.SUCCEEDED
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            result = manager.can_enqueue_job(sample_dependent_job_run)

        assert result is True


@pytest.mark.unit
class TestShouldSkipJobDueToDependenciesUnit:
    """Test job skipping due to unmet dependencies."""

    def test_should_not_skip_job_with_no_dependencies(self, mock_pipeline_manager):
        """Test that a job with no dependencies should not be skipped."""
        mock_job = Mock(spec=JobRun, id=1)

        with (
            patch.object(
                mock_pipeline_manager,
                "get_dependencies_for_job",
                return_value=[],
            ),
            patch(
                "mavedb.worker.lib.managers.pipeline_manager.job_should_be_skipped_due_to_unfulfillable_dependency",
                return_value=(False, ""),
            ) as mock_job_should_be_skipped,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            should_skip, reason = mock_pipeline_manager.should_skip_job_due_to_dependencies(mock_job)

        mock_job_should_be_skipped.assert_not_called()
        assert should_skip is False
        assert reason == ""

    def test_should_skip_job_with_unreachable_dependency(self, mock_pipeline_manager):
        """Test that a job with unreachable dependencies should be skipped."""
        mock_job = Mock(spec=JobRun, id=1, status=JobStatus.FAILED)
        mock_dependency = Mock(spec=JobDependency, dependency_type=DependencyType.SUCCESS_REQUIRED)

        with (
            patch.object(
                mock_pipeline_manager,
                "get_dependencies_for_job",
                return_value=[(mock_dependency, mock_job)],
            ),
            patch(
                "mavedb.worker.lib.managers.pipeline_manager.job_should_be_skipped_due_to_unfulfillable_dependency",
                return_value=(True, "Unfulfillable dependency detected"),
            ) as mock_job_should_be_skipped,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            should_skip, reason = mock_pipeline_manager.should_skip_job_due_to_dependencies(mock_job)

        mock_job_should_be_skipped.assert_called_once_with(
            dependency_type=DependencyType.SUCCESS_REQUIRED, dependent_job_status=JobStatus.FAILED
        )
        assert should_skip is True
        assert reason == "Unfulfillable dependency detected"

    def test_should_not_skip_job_with_reachable(self, mock_pipeline_manager):
        """Test that a job with met dependencies can be enqueued."""
        mock_job = Mock(spec=JobRun, id=1, status=JobStatus.SUCCEEDED)
        mock_dependency = Mock(spec=JobDependency, dependency_type=DependencyType.COMPLETION_REQUIRED)

        with (
            patch.object(
                mock_pipeline_manager,
                "get_dependencies_for_job",
                return_value=[(mock_dependency, mock_job)],
            ),
            patch(
                "mavedb.worker.lib.managers.pipeline_manager.job_should_be_skipped_due_to_unfulfillable_dependency",
                return_value=(False, ""),
            ) as mock_job_should_be_skipped,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            should_skip, reason = mock_pipeline_manager.should_skip_job_due_to_dependencies(mock_job)
        mock_job_should_be_skipped.assert_called_once_with(
            dependency_type=DependencyType.COMPLETION_REQUIRED, dependent_job_status=JobStatus.SUCCEEDED
        )
        assert should_skip is False
        assert reason == ""

    @pytest.mark.parametrize(
        "exception",
        HANDLED_EXCEPTIONS_DURING_OBJECT_MANIPULATION,
    )
    def test_should_skip_job_due_to_dependencies_raises_pipeline_state_error_on_handled_exceptions(
        self, mock_pipeline_manager, exception
    ):
        """Test that handled exceptions during dependency checking raise PipelineStateError."""
        mock_job = Mock(spec=JobRun, id=1, status=JobStatus.SUCCEEDED)
        mock_dependency = Mock(spec=JobDependency, dependency_type=DependencyType.COMPLETION_REQUIRED)

        with (
            patch.object(
                mock_pipeline_manager,
                "get_dependencies_for_job",
                return_value=[(mock_dependency, mock_job)],
            ),
            patch(
                "mavedb.worker.lib.managers.pipeline_manager.job_should_be_skipped_due_to_unfulfillable_dependency",
                side_effect=exception,
            ),
            pytest.raises(PipelineStateError, match="Corrupted dependency data"),
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            mock_pipeline_manager.should_skip_job_due_to_dependencies(mock_job)


@pytest.mark.integration
class TestShouldSkipJobDueToDependenciesIntegration:
    """Integration tests for job skipping due to unmet dependencies."""

    def test_should_not_skip_job_with_no_dependencies(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
    ):
        """Test that a job with no dependencies should not be skipped."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        with (
            TransactionSpy.spy(session),
        ):
            should_skip, reason = manager.should_skip_job_due_to_dependencies(sample_job_run)

        assert should_skip is False
        assert reason == ""

    def test_should_skip_job_with_unreachable_dependency(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test that a job with unreachable dependencies should be skipped."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set the job the dependency depends on to a failed status
        sample_job_run.status = JobStatus.FAILED
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            should_skip, reason = manager.should_skip_job_due_to_dependencies(sample_dependent_job_run)

        assert should_skip is True
        assert reason == "Dependency did not succeed (failed)"

    def test_should_not_skip_job_with_reachable_dependency(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test that a job with met dependencies can be enqueued."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set the dependency job to a succeeded status
        sample_job_run.status = JobStatus.PENDING
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            should_skip, reason = manager.should_skip_job_due_to_dependencies(sample_dependent_job_run)

        assert should_skip is False
        assert reason == ""


@pytest.mark.unit
class TestRetryFailedJobsUnit:
    """Test retrying of failed jobs."""

    @pytest.mark.asyncio
    async def test_retry_failed_jobs_no_failed_jobs(self, mock_pipeline_manager, mock_job_manager):
        """Test that retrying failed jobs skips if there are no failed jobs."""
        with (
            patch.object(
                mock_pipeline_manager,
                "get_failed_jobs",
                return_value=[],
            ),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None) as mock_coordinate,
            patch.object(mock_job_manager, "prepare_retry", return_value=None) as mock_prepare_retry,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            await mock_pipeline_manager.retry_failed_jobs()

        mock_prepare_retry.assert_not_called()
        mock_set_status.assert_not_called()
        mock_coordinate.assert_not_called()

    @pytest.mark.asyncio
    async def test_retry_failed_jobs_successful_retry(self, mock_pipeline_manager, mock_job_manager):
        """Test successful retrying of failed jobs."""
        mock_failed_job1 = Mock(spec=JobRun, id=1)
        mock_failed_job2 = Mock(spec=JobRun, id=2)

        with (
            patch.object(
                mock_pipeline_manager,
                "get_failed_jobs",
                return_value=[mock_failed_job1, mock_failed_job2],
            ),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None) as mock_coordinate,
            patch.object(
                mock_job_manager,
                "prepare_retry",
                return_value=None,
            ) as mock_prepare_retry,
            TransactionSpy.spy(mock_pipeline_manager.db, expect_flush=True),
        ):
            await mock_pipeline_manager.retry_failed_jobs()

        assert mock_prepare_retry.call_count == 2
        mock_set_status.assert_called_once_with(PipelineStatus.RUNNING)
        mock_coordinate.assert_called_once()


@pytest.mark.integration
class TestRetryFailedJobsIntegration:
    """Integration tests for retrying of failed jobs."""

    @pytest.mark.asyncio
    async def test_retry_failed_jobs_integration(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test successful retrying of failed jobs in a pipeline."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set the pipeline to RUNNING status
        manager.set_pipeline_status(PipelineStatus.RUNNING)
        session.commit()

        # Set the job statuses
        sample_job_run.status = JobStatus.FAILED
        sample_dependent_job_run.status = JobStatus.PENDING
        session.commit()

        with (
            TransactionSpy.spy(session, expect_flush=True),
        ):
            await manager.retry_failed_jobs()

        # Commit the transaction
        session.commit()

        # Verify that the pipeline is now in RUNNING status
        assert manager.get_pipeline_status() == PipelineStatus.RUNNING

        # Verify that the failed job is now queued
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED

        # Verify that the dependent job is still pending
        job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_retry_failed_jobs_integration_no_failed_jobs(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_empty_pipeline,
    ):
        """Test that retrying failed jobs skips if there are no failed jobs."""
        manager = PipelineManager(session, arq_redis, sample_empty_pipeline.id)

        # Set the pipeline to RUNNING status
        manager.set_pipeline_status(PipelineStatus.RUNNING)
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            await manager.retry_failed_jobs()

        # Commit the transaction
        session.commit()

        # Verify that the pipeline status is not changed
        assert manager.get_pipeline_status() == PipelineStatus.RUNNING


@pytest.mark.unit
class TestRetryUnsuccessfulJobsUnit:
    """Test retrying of unsuccessful jobs."""

    @pytest.mark.asyncio
    async def test_retry_unsuccessful_jobs_no_unsuccessful_jobs(self, mock_pipeline_manager, mock_job_manager):
        """Test that retrying unsuccessful jobs skips if there are no unsuccessful jobs."""
        with (
            patch.object(
                mock_pipeline_manager,
                "get_unsuccessful_jobs",
                return_value=[],
            ),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None) as mock_coordinate,
            patch.object(mock_job_manager, "prepare_retry", return_value=None) as mock_prepare_retry,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            await mock_pipeline_manager.retry_unsuccessful_jobs()

        mock_prepare_retry.assert_not_called()
        mock_set_status.assert_not_called()
        mock_coordinate.assert_not_called()

    @pytest.mark.asyncio
    async def test_retry_failed_jobs_successful_retry(self, mock_pipeline_manager, mock_job_manager):
        """Test successful retrying of failed jobs."""
        mock_failed_job1 = Mock(spec=JobRun, id=1)
        mock_failed_job2 = Mock(spec=JobRun, id=2)

        with (
            patch.object(
                mock_pipeline_manager,
                "get_unsuccessful_jobs",
                return_value=[mock_failed_job1, mock_failed_job2],
            ),
            patch.object(mock_pipeline_manager, "set_pipeline_status", return_value=None) as mock_set_status,
            patch.object(mock_pipeline_manager, "coordinate_pipeline", return_value=None) as mock_coordinate,
            patch.object(
                mock_job_manager,
                "prepare_retry",
                return_value=None,
            ) as mock_prepare_retry,
            TransactionSpy.spy(mock_pipeline_manager.db, expect_flush=True),
        ):
            await mock_pipeline_manager.retry_unsuccessful_jobs()

        assert mock_prepare_retry.call_count == 2
        mock_set_status.assert_called_once_with(PipelineStatus.RUNNING)
        mock_coordinate.assert_called_once()


@pytest.mark.integration
class TestRetryUnsuccessfulJobsIntegration:
    """Integration tests for retrying of unsuccessful jobs."""

    @pytest.mark.asyncio
    async def test_retry_unsuccessful_jobs_integration(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test successful retrying of unsuccessful jobs in a pipeline."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set the pipeline to RUNNING status
        manager.set_pipeline_status(PipelineStatus.RUNNING)
        session.commit()

        # Set the job statuses
        sample_job_run.status = JobStatus.FAILED
        sample_dependent_job_run.status = JobStatus.CANCELLED
        session.commit()

        with (
            TransactionSpy.spy(session, expect_flush=True),
        ):
            await manager.retry_unsuccessful_jobs()

        # Commit the transaction
        session.commit()

        # Verify that the pipeline is now in RUNNING status
        assert manager.get_pipeline_status() == PipelineStatus.RUNNING

        # Verify that the failed job is now queued
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED

        # Verify that the cancelled dependent job is now queued
        job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_retry_unsuccessful_jobs_integration_no_unsuccessful_jobs(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_empty_pipeline,
    ):
        """Test that retrying unsuccessful jobs skips if there are no unsuccessful jobs."""
        manager = PipelineManager(session, arq_redis, sample_empty_pipeline.id)

        # Set the pipeline to RUNNING status
        manager.set_pipeline_status(PipelineStatus.RUNNING)
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            await manager.retry_unsuccessful_jobs()

        # Commit the transaction
        session.commit()

        # Verify that the pipeline status is not changed
        assert manager.get_pipeline_status() == PipelineStatus.RUNNING


@pytest.mark.unit
class TestRetryPipelineUnit:
    """Test retrying of entire pipelines."""

    @pytest.mark.asyncio
    async def test_retry_pipeline_calls_retry_unsuccessful_jobs(self, mock_pipeline_manager, mock_job_manager):
        """Test that retrying a pipeline calls retrying unsuccessful jobs."""
        with (
            patch.object(
                mock_pipeline_manager,
                "retry_unsuccessful_jobs",
                return_value=None,
            ) as mock_retry_unsuccessful_jobs,
            TransactionSpy.spy(mock_pipeline_manager.db),  # flush is handled in retry_unsuccessful_jobs, which we mock
        ):
            await mock_pipeline_manager.retry_pipeline()

        mock_retry_unsuccessful_jobs.assert_called_once()


@pytest.mark.integration
class TestRetryPipelineIntegration:
    """Integration tests for retrying of entire pipelines."""

    @pytest.mark.asyncio
    async def test_retry_pipeline_integration(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test successful retrying of an entire pipeline."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set the pipeline to RUNNING status
        manager.set_pipeline_status(PipelineStatus.RUNNING)
        session.commit()

        # Set the job statuses
        sample_job_run.status = JobStatus.CANCELLED
        sample_dependent_job_run.status = JobStatus.SKIPPED
        session.commit()

        with (
            TransactionSpy.spy(session, expect_flush=True),
        ):
            await manager.retry_pipeline()

        # Commit the transaction
        session.commit()

        # Verify that the pipeline is now in RUNNING status
        assert manager.get_pipeline_status() == PipelineStatus.RUNNING

        # Verify that the failed job is now queued
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED

        # Verify that the cancelled dependent job is now queued
        job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING


@pytest.mark.unit
class TestGetJobsByStatusUnit:
    """Test job retrieval by status with mocked database."""

    def test_get_jobs_by_status_wraps_sqlalchemy_error_with_database_error(self, mock_pipeline_manager):
        """Test database error handling."""
        with (
            patch.object(mock_pipeline_manager.db, "execute", side_effect=SQLAlchemyError("DB error")),
            pytest.raises(DatabaseConnectionError, match="Failed to get jobs with status"),
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            mock_pipeline_manager.get_jobs_by_status([JobStatus.RUNNING])


@pytest.mark.integration
class TestGetJobsByStatusIntegration:
    """Integration tests for job retrieval by status."""

    @pytest.mark.parametrize(
        "status",
        JobStatus._member_map_.values(),
    )
    def test_get_jobs_by_status_integration(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
        status,
    ):
        """Test retrieval of jobs by status."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set job statuses
        sample_job_run.status = status
        sample_dependent_job_run.status = [s for s in JobStatus if s != status][0]
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            running_jobs = manager.get_jobs_by_status([status])

        assert len(running_jobs) == 1
        assert running_jobs[0].id == sample_job_run.id

    def test_get_jobs_by_status_integration_no_matching_jobs(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
    ):
        """Test retrieval of jobs by status when no jobs match."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        with (
            TransactionSpy.spy(session),
        ):
            jobs = manager.get_jobs_by_status([JobStatus.SUCCEEDED])

        assert len(jobs) == 0

    def test_get_jobs_by_status_integration_multiple_matching_jobs(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test retrieval of jobs by status when multiple jobs match."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set both job statuses to RUNNING
        sample_job_run.status = JobStatus.RUNNING
        sample_dependent_job_run.status = JobStatus.RUNNING
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            running_jobs = manager.get_jobs_by_status([JobStatus.RUNNING])

        assert len(running_jobs) == 2
        job_ids = {job.id for job in running_jobs}
        assert sample_job_run.id in job_ids
        assert sample_dependent_job_run.id in job_ids

    def test_get_jobs_by_status_integration_no_jobs_in_pipeline(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_empty_pipeline,
    ):
        """Test retrieval of jobs by status when there are no jobs in the pipeline."""
        manager = PipelineManager(session, arq_redis, sample_empty_pipeline.id)

        with (
            TransactionSpy.spy(session),
        ):
            jobs = manager.get_jobs_by_status([JobStatus.RUNNING])

        assert len(jobs) == 0

    def test_get_jobs_by_status_multiple_statuses(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test retrieval of jobs by multiple statuses."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set job statuses
        sample_job_run.status = JobStatus.RUNNING
        sample_dependent_job_run.status = JobStatus.PENDING
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            jobs = manager.get_jobs_by_status([JobStatus.RUNNING, JobStatus.PENDING])

        assert len(jobs) == 2
        job_ids = {job.id for job in jobs}
        assert sample_job_run.id in job_ids
        assert sample_dependent_job_run.id in job_ids

        # Assert jobs are ordered by created by timestamp
        assert jobs[0].created_at <= jobs[1].created_at


@pytest.mark.unit
class TestGetPendingJobsUnit:
    """Test retrieval of pending jobs."""

    def test_get_pending_jobs_success(self, mock_pipeline_manager):
        """Test successful retrieval of pending jobs."""

        with (
            patch.object(
                mock_pipeline_manager, "get_jobs_by_status", return_value=[Mock(), Mock()]
            ) as mock_get_jobs_by_status,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            jobs = mock_pipeline_manager.get_pending_jobs()

            assert len(jobs) == 2
            mock_get_jobs_by_status.assert_called_once_with([JobStatus.PENDING])


@pytest.mark.integration
class TestGetPendingJobsIntegration:
    """Integration tests for retrieval of pending jobs."""

    def test_get_pending_jobs_integration(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test retrieval of pending jobs."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set job statuses
        sample_job_run.status = JobStatus.PENDING
        sample_dependent_job_run.status = JobStatus.RUNNING
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            pending_jobs = manager.get_pending_jobs()

        assert len(pending_jobs) == 1
        assert pending_jobs[0].id == sample_job_run.id

    def test_get_pending_jobs_integration_no_pending_jobs(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test retrieval of pending jobs when there are no pending jobs."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set job statuses
        sample_job_run.status = JobStatus.RUNNING
        sample_dependent_job_run.status = JobStatus.SUCCEEDED
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            pending_jobs = manager.get_pending_jobs()

        assert len(pending_jobs) == 0


@pytest.mark.unit
class TestGetRunningJobsUnit:
    """Test retrieval of running jobs."""

    def test_get_running_jobs_success(self, mock_pipeline_manager):
        """Test successful retrieval of running jobs."""

        with (
            patch.object(mock_pipeline_manager, "get_jobs_by_status") as mock_get_jobs_by_status,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            mock_pipeline_manager.get_running_jobs()
        mock_get_jobs_by_status.assert_called_once_with([JobStatus.RUNNING])


@pytest.mark.unit
class TestGetActiveJobsUnit:
    """Test retrieval of active jobs."""

    def test_get_active_jobs_success(self, mock_pipeline_manager):
        """Test successful retrieval of active jobs."""

        with (
            patch.object(mock_pipeline_manager, "get_jobs_by_status") as mock_get_jobs_by_status,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            mock_pipeline_manager.get_active_jobs()
        mock_get_jobs_by_status.assert_called_once_with(ACTIVE_JOB_STATUSES)


@pytest.mark.integration
class TestGetActiveJobsIntegration:
    """Integration tests for retrieval of active jobs."""

    def test_get_active_jobs_integration(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test retrieval of active jobs."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set job statuses
        sample_job_run.status = JobStatus.RUNNING
        sample_dependent_job_run.status = JobStatus.PENDING
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            active_jobs = manager.get_active_jobs()

        assert len(active_jobs) == 2
        job_ids = {job.id for job in active_jobs}
        assert sample_job_run.id in job_ids
        assert sample_dependent_job_run.id in job_ids

    def test_get_active_jobs_integration_no_active_jobs(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test retrieval of active jobs when there are no active jobs."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set job statuses
        sample_job_run.status = JobStatus.SUCCEEDED
        sample_dependent_job_run.status = JobStatus.FAILED
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            active_jobs = manager.get_active_jobs()

        assert len(active_jobs) == 0


@pytest.mark.integration
class TestGetRunningJobsIntegration:
    """Integration tests for retrieval of running jobs."""

    def test_get_running_jobs_integration(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test retrieval of running jobs."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set job statuses
        sample_job_run.status = JobStatus.RUNNING
        sample_dependent_job_run.status = JobStatus.PENDING
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            running_jobs = manager.get_running_jobs()

        assert len(running_jobs) == 1
        assert running_jobs[0].id == sample_job_run.id

    def test_get_running_jobs_integration_no_running_jobs(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test retrieval of running jobs when there are no running jobs."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set job statuses
        sample_job_run.status = JobStatus.SUCCEEDED
        sample_dependent_job_run.status = JobStatus.PENDING
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            running_jobs = manager.get_running_jobs()

        assert len(running_jobs) == 0


@pytest.mark.unit
class TestGetFailedJobsUnit:
    """Test retrieval of failed jobs."""

    def test_get_failed_jobs_success(self, mock_pipeline_manager):
        """Test successful retrieval of failed jobs."""

        with (
            patch.object(mock_pipeline_manager, "get_jobs_by_status") as mock_get_jobs_by_status,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            mock_pipeline_manager.get_failed_jobs()

        mock_get_jobs_by_status.assert_called_once_with([JobStatus.FAILED])


@pytest.mark.integration
class TestGetFailedJobsIntegration:
    """Integration tests for retrieval of failed jobs."""

    def test_get_failed_jobs_integration(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test retrieval of failed jobs."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set job statuses
        sample_job_run.status = JobStatus.FAILED
        sample_dependent_job_run.status = JobStatus.PENDING
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            failed_jobs = manager.get_failed_jobs()

        assert len(failed_jobs) == 1
        assert failed_jobs[0].id == sample_job_run.id

    def test_get_failed_jobs_integration_no_failed_jobs(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test retrieval of failed jobs when there are no failed jobs."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set job statuses
        sample_job_run.status = JobStatus.SUCCEEDED
        sample_dependent_job_run.status = JobStatus.PENDING
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            failed_jobs = manager.get_failed_jobs()

        assert len(failed_jobs) == 0


@pytest.mark.unit
class TestGetUnsuccessfulJobsUnit:
    """Test retrieval of unsuccessful jobs."""

    def test_get_unsuccessful_jobs_success(self, mock_pipeline_manager):
        """Test successful retrieval of unsuccessful jobs."""

        with (
            patch.object(mock_pipeline_manager, "get_jobs_by_status") as mock_get_jobs_by_status,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            mock_pipeline_manager.get_unsuccessful_jobs()
        mock_get_jobs_by_status.assert_called_once_with([JobStatus.CANCELLED, JobStatus.SKIPPED, JobStatus.FAILED])


@pytest.mark.integration
class TestGetUnsuccessfulJobsIntegration:
    """Integration tests for retrieval of unsuccessful jobs."""

    def test_get_unsuccessful_jobs_integration(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test retrieval of unsuccessful jobs."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set job statuses
        sample_job_run.status = JobStatus.FAILED
        sample_dependent_job_run.status = JobStatus.CANCELLED
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            unsuccessful_jobs = manager.get_unsuccessful_jobs()

        assert len(unsuccessful_jobs) == 2
        job_ids = {job.id for job in unsuccessful_jobs}
        assert sample_job_run.id in job_ids
        assert sample_dependent_job_run.id in job_ids

    def test_get_unsuccessful_jobs_integration_no_unsuccessful_jobs(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test retrieval of unsuccessful jobs when there are no unsuccessful jobs."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set job statuses
        sample_job_run.status = JobStatus.SUCCEEDED
        sample_dependent_job_run.status = JobStatus.PENDING
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            unsuccessful_jobs = manager.get_unsuccessful_jobs()

        assert len(unsuccessful_jobs) == 0


@pytest.mark.unit
class TestGetAllJobsUnit:
    """Test retrieval of all jobs."""

    def test_get_all_jobs_wraps_sqlalchemy_errors_with_database_error(self, mock_pipeline_manager):
        """Test database error handling during retrieval of all jobs."""

        with (
            patch.object(mock_pipeline_manager.db, "execute", side_effect=SQLAlchemyError("DB error")),
            pytest.raises(DatabaseConnectionError, match="Failed to get all jobs"),
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            mock_pipeline_manager.get_all_jobs()


@pytest.mark.integration
class TestGetAllJobsIntegration:
    """Integration tests for retrieval of all jobs."""

    def test_get_all_jobs_integration(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test retrieval of all jobs in a pipeline."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        with (
            TransactionSpy.spy(session),
        ):
            all_jobs = manager.get_all_jobs()

        assert len(all_jobs) == 2
        job_ids = {job.id for job in all_jobs}
        assert sample_job_run.id in job_ids
        assert sample_dependent_job_run.id in job_ids

    def test_get_all_jobs_integration_no_jobs(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_empty_pipeline,
    ):
        """Test retrieval of all jobs when there are no jobs in the pipeline."""
        manager = PipelineManager(session, arq_redis, sample_empty_pipeline.id)

        with (
            TransactionSpy.spy(session),
        ):
            all_jobs = manager.get_all_jobs()

        assert len(all_jobs) == 0

    def test_get_all_jobs_integration_multiple_jobs(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test retrieval of all jobs when there are multiple jobs in the pipeline."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Add an additional job to the pipeline
        new_job = JobRun(
            id=99,
            urn="job:additional_job:999",
            pipeline_id=sample_pipeline.id,
            job_type="Additional Job",
            job_function="additional_function",
            status=JobStatus.PENDING,
        )
        session.add(new_job)
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            all_jobs = manager.get_all_jobs()

        assert len(all_jobs) == 3
        job_ids = {job.id for job in all_jobs}
        assert sample_job_run.id in job_ids
        assert sample_dependent_job_run.id in job_ids
        assert new_job.id in job_ids

        # Assert jobs are ordered by created by timestamp
        assert all_jobs[0].created_at <= all_jobs[1].created_at <= all_jobs[2].created_at


@pytest.mark.unit
class TestGetDependenciesForJobUnit:
    """Test retrieval of job dependencies."""

    def test_get_dependencies_for_job_wraps_sqlalchemy_error_with_database_error(self, mock_pipeline_manager):
        """Test database error handling during retrieval of job dependencies."""
        mock_job = Mock(spec=JobRun)

        with (
            patch.object(mock_pipeline_manager.db, "execute", side_effect=SQLAlchemyError("DB error")),
            pytest.raises(DatabaseConnectionError, match="Failed to get job dependencies for job"),
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            mock_pipeline_manager.get_dependencies_for_job(mock_job)


@pytest.mark.integration
class TestGetDependenciesForJobIntegration:
    """Integration tests for retrieval of job dependencies."""

    def test_get_dependencies_for_job_integration(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
        sample_job_dependency,
    ):
        """Test retrieval of job dependencies."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        with (
            TransactionSpy.spy(session),
        ):
            dependencies = manager.get_dependencies_for_job(sample_dependent_job_run)

        assert len(dependencies) == 1
        dependency, job = dependencies[0]
        assert dependency.id == sample_job_dependency.id
        assert job.id == sample_job_run.id

    def test_get_dependencies_for_job_integration_no_dependencies(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
    ):
        """Test retrieval of job dependencies when there are no dependencies."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        with (
            TransactionSpy.spy(session),
        ):
            dependencies = manager.get_dependencies_for_job(sample_job_run)

        assert len(dependencies) == 0

    def test_get_dependencies_for_job_integration_multiple_dependencies(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test retrieval of job dependencies when there are multiple dependencies."""
        # Create additional job and dependency
        additional_job = JobRun(
            id=99,
            urn="job:additional_job:999",
            pipeline_id=sample_pipeline.id,
            job_type="Additional Job",
            job_function="additional_function",
            status=JobStatus.PENDING,
        )
        session.add(additional_job)
        session.commit()

        additional_dependency = JobDependency(
            id=sample_dependent_job_run.id,
            depends_on_job_id=additional_job.id,
            dependency_type=DependencyType.COMPLETION_REQUIRED,
        )
        session.add(additional_dependency)
        session.commit()

        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        with (
            TransactionSpy.spy(session),
        ):
            dependencies = manager.get_dependencies_for_job(sample_dependent_job_run)

        assert len(dependencies) == 2
        fetched_dependency_ids = {dep.id for dep, job in dependencies}
        implicit_dependency_ids = {dep.id for dep in sample_dependent_job_run.job_dependencies}
        assert fetched_dependency_ids == implicit_dependency_ids


@pytest.mark.unit
class TestGetPipelineUnit:
    """Test retrieval of pipeline."""

    def test_get_pipeline_wraps_sqlalchemy_errors_with_database_error(self, mock_pipeline):
        """Test database error handling during retrieval of pipeline."""

        # Prepare mock PipelineManager with mocked DB session that will raise SQLAlchemyError on query.
        # We don't use the default fixture here since it usually wraps this function.
        mock_db = Mock(spec=Session)
        mock_redis = Mock(spec=ArqRedis)
        manager = object.__new__(PipelineManager)
        manager.db = mock_db
        manager.redis = mock_redis
        manager.pipeline_id = mock_pipeline.id

        with (
            patch.object(manager.db, "execute", side_effect=SQLAlchemyError("DB error")),
            pytest.raises(DatabaseConnectionError, match="Failed to get pipeline"),
            TransactionSpy.spy(manager.db),
        ):
            manager.get_pipeline()


@pytest.mark.integration
class TestGetPipelineIntegration:
    """Integration tests for retrieval of pipeline."""

    def test_get_pipeline_integration(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
    ):
        """Test retrieval of pipeline."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        with (
            TransactionSpy.spy(session),
        ):
            pipeline = manager.get_pipeline()

        assert pipeline.id == sample_pipeline.id
        assert pipeline.name == sample_pipeline.name

    def test_get_pipeline_integration_nonexistent_pipeline(
        self,
        session,
        arq_redis,
        with_populated_job_data,
    ):
        """Test retrieval of a nonexistent pipeline raises PipelineNotFoundError."""
        with (
            pytest.raises(DatabaseConnectionError, match="Failed to get pipeline 9999"),
            TransactionSpy.spy(session),
        ):
            # get_pipeline is called implicitly during PipelineManager initialization
            PipelineManager(session, arq_redis, pipeline_id=9999)


@pytest.mark.unit
class TestGetJobCountsByStatusUnit:
    """Test retrieval of job counts by status."""

    def test_get_job_counts_by_status_wraps_sqlalchemy_errors_with_database_error(self, mock_pipeline_manager):
        """Test database error handling during retrieval of job counts by status."""

        with (
            patch.object(mock_pipeline_manager.db, "execute", side_effect=SQLAlchemyError("DB error")),
            pytest.raises(DatabaseConnectionError, match="Failed to get job counts for pipeline"),
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            mock_pipeline_manager.get_job_counts_by_status()


@pytest.mark.integration
class TestGetJobCountsByStatusIntegration:
    """Integration tests for retrieval of job counts by status."""

    def test_get_job_counts_by_status_integration(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test retrieval of job counts by status."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set job statuses
        sample_job_run.status = JobStatus.RUNNING
        sample_dependent_job_run.status = JobStatus.PENDING
        session.commit()

        with (
            TransactionSpy.spy(session),
        ):
            counts = manager.get_job_counts_by_status()

        assert counts[JobStatus.RUNNING] == 1
        assert counts[JobStatus.PENDING] == 1
        assert counts.get(JobStatus.SUCCEEDED, 0) == 0

    def test_get_job_counts_by_status_integration_no_jobs(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_empty_pipeline,
    ):
        """Test retrieval of job counts by status when there are no jobs in the pipeline."""
        manager = PipelineManager(session, arq_redis, sample_empty_pipeline.id)

        with (
            TransactionSpy.spy(session),
        ):
            counts = manager.get_job_counts_by_status()

        assert counts == {}


@pytest.mark.unit
class TestGetPipelineProgressUnit:
    """Test retrieval of pipeline progress."""

    pass


@pytest.mark.integration
class TestGetPipelineProgressIntegration:
    """Integration tests for retrieval of pipeline progress."""

    pass


@pytest.mark.unit
class TestGetPipelineStatusUnit:
    """Test retrieval of pipeline status."""

    def test_get_pipeline_status_success(self, mock_pipeline_manager):
        """Test successful retrieval of pipeline status."""
        with (
            TransactionSpy.spy(mock_pipeline_manager.db),
            patch.object(
                mock_pipeline_manager,
                "get_pipeline",
                wraps=mock_pipeline_manager.get_pipeline,
            ) as mock_get_pipeline,
        ):
            mock_pipeline_manager.get_pipeline_status()
        mock_get_pipeline.assert_called_once()


@pytest.mark.integration
class TestGetPipelineStatusIntegration:
    """Integration tests for retrieval of pipeline status."""

    def test_get_pipeline_status_integration(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
    ):
        """Test retrieval of pipeline status."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        with (
            TransactionSpy.spy(session),
        ):
            status = manager.get_pipeline_status()

        assert status == sample_pipeline.status


@pytest.mark.unit
class TestSetPipelineStatusUnit:
    """Test setting of pipeline status."""

    @pytest.mark.parametrize("pipeline_status", [status for status in PipelineStatus._member_map_.values()])
    def test_set_pipeline_status_success(self, mock_pipeline_manager, pipeline_status):
        """Test successful setting of pipeline status."""
        mock_pipeline = Mock(spec=Pipeline, status=None)

        with (
            patch.object(
                mock_pipeline_manager,
                "get_pipeline",
                return_value=mock_pipeline,
            ) as mock_get_pipeline,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            mock_pipeline_manager.set_pipeline_status(pipeline_status)
            assert mock_pipeline.status == pipeline_status

        mock_get_pipeline.assert_called_once()

    @pytest.mark.parametrize(
        "pipeline_status",
        TERMINAL_PIPELINE_STATUSES,
    )
    def test_set_pipeline_status_sets_finished_at_property_for_terminal_status(
        self, mock_pipeline_manager, mock_pipeline, pipeline_status
    ):
        """Test that setting a terminal status updates the finished_at property."""
        # Set initial finished_at to None
        mock_pipeline.finished_at = None

        with TransactionSpy.spy(mock_pipeline_manager.db):
            before_update = datetime.datetime.now()
            mock_pipeline_manager.set_pipeline_status(pipeline_status)
            after_update = datetime.datetime.now()

            assert mock_pipeline.status == pipeline_status
            assert mock_pipeline.finished_at is not None
            assert before_update <= mock_pipeline.finished_at <= after_update

    def test_set_pipeline_status_clears_started_at_property_for_created_status(
        self, mock_pipeline_manager, mock_pipeline
    ):
        """Test that setting status to CREATED clears the started_at property."""

        with TransactionSpy.spy(mock_pipeline_manager.db):
            mock_pipeline_manager.set_pipeline_status(PipelineStatus.CREATED)
            assert mock_pipeline.status == PipelineStatus.CREATED
            assert mock_pipeline.started_at is None

    @pytest.mark.parametrize(
        "initial_started_at",
        [None, datetime.datetime.now() - datetime.timedelta(hours=1)],
    )
    def test_set_pipeline_status_sets_started_at_property_for_running_status(
        self, mock_pipeline_manager, mock_pipeline, initial_started_at
    ):
        """Test that setting status to RUNNING sets the started_at property if not already set."""
        mock_pipeline.started_at = initial_started_at
        with TransactionSpy.spy(mock_pipeline_manager.db):
            before_update = datetime.datetime.now()
            mock_pipeline_manager.set_pipeline_status(PipelineStatus.RUNNING)
            after_update = datetime.datetime.now()

            assert mock_pipeline.status == PipelineStatus.RUNNING

            if initial_started_at is None:
                assert mock_pipeline.started_at is not None
                assert before_update <= mock_pipeline.started_at <= after_update
            else:
                assert mock_pipeline.started_at == initial_started_at

    @pytest.mark.parametrize(
        "exception",
        HANDLED_EXCEPTIONS_DURING_OBJECT_MANIPULATION,
    )
    def test_set_pipeline_status_handled_exception_raises_pipeline_state_error(self, mock_pipeline_manager, exception):
        """Test that handled exceptions during setting of pipeline status raise PipelineStateError."""

        def get_or_error(*args):
            if args:
                raise exception
            return PipelineStatus.CREATED

        with (
            patch.object(mock_pipeline_manager, "get_pipeline") as mock_pipeline,
            pytest.raises(PipelineStateError, match="Failed to set pipeline status"),
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            # Mock exception when setting pipeline status
            mock_pipeline.return_value = Mock(spec=Pipeline)
            type(mock_pipeline.return_value).status = PropertyMock(side_effect=get_or_error)

            mock_pipeline_manager.set_pipeline_status(PipelineStatus.RUNNING)


@pytest.mark.integration
class TestSetPipelineStatusIntegration:
    """Integration tests for setting of pipeline status."""

    @pytest.mark.parametrize("pipeline_status", [status for status in PipelineStatus._member_map_.values()])
    def test_set_pipeline_status_integration(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        pipeline_status,
    ):
        """Test setting of pipeline status."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        with (
            TransactionSpy.spy(session),
        ):
            manager.set_pipeline_status(pipeline_status)

        # Commit the transaction
        session.commit()

        # Verify that the pipeline status is updated
        updated_pipeline = session.execute(select(Pipeline).where(Pipeline.id == sample_pipeline.id)).scalar_one()
        assert updated_pipeline.status == pipeline_status

    @pytest.mark.parametrize(
        "pipeline_status",
        TERMINAL_PIPELINE_STATUSES,
    )
    def test_set_pipeline_status_integration_terminal_status_sets_finished_at(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        pipeline_status,
    ):
        """Test that setting a terminal status updates the finished_at property."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        with (
            TransactionSpy.spy(session),
        ):
            before_update = datetime.datetime.now(tz=datetime.timezone.utc)
            manager.set_pipeline_status(pipeline_status)
            after_update = datetime.datetime.now(tz=datetime.timezone.utc)

        # Commit the transaction
        session.commit()

        # Verify that the pipeline status and finished_at are updated
        updated_pipeline = session.execute(select(Pipeline).where(Pipeline.id == sample_pipeline.id)).scalar_one()
        assert updated_pipeline.status == pipeline_status
        assert updated_pipeline.finished_at is not None
        assert before_update <= updated_pipeline.finished_at <= after_update

    def test_set_pipeline_status_integration_created_status_clears_started_at(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
    ):
        """Test that setting status to CREATED clears the started_at property."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        with TransactionSpy.spy(session):
            manager.set_pipeline_status(PipelineStatus.CREATED)

        # Commit the transaction
        session.commit()

        # Verify that the pipeline status is updated and started_at is None
        updated_pipeline = session.execute(select(Pipeline).where(Pipeline.id == sample_pipeline.id)).scalar_one()
        assert updated_pipeline.status == PipelineStatus.CREATED
        assert updated_pipeline.started_at is None

    @pytest.mark.parametrize(
        "initial_started_at",
        [None, datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(hours=1)],
    )
    def test_set_pipeline_status_integration_running_status_sets_started_at(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        initial_started_at,
    ):
        """Test that setting status to RUNNING sets the started_at property if not already set."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Set initial started_at
        sample_pipeline.started_at = initial_started_at
        session.commit()

        with TransactionSpy.spy(session):
            before_update = datetime.datetime.now(tz=datetime.timezone.utc)
            manager.set_pipeline_status(PipelineStatus.RUNNING)
            after_update = datetime.datetime.now(tz=datetime.timezone.utc)

        # Commit the transaction
        session.commit()

        # Verify that the pipeline status and started_at are updated
        updated_pipeline = session.execute(select(Pipeline).where(Pipeline.id == sample_pipeline.id)).scalar_one()
        assert updated_pipeline.status == PipelineStatus.RUNNING

        if initial_started_at is None:
            assert before_update <= updated_pipeline.started_at <= after_update
        else:
            assert updated_pipeline.started_at == initial_started_at


@pytest.mark.unit
class TestEnqueueInArqUnit:
    """Test enqueuing jobs in ARQ."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("enqueud", [Mock(spec=ArqJob), None])
    @pytest.mark.parametrize("retry", [True, False])
    async def test_enqueue_in_arq_success(self, mock_pipeline_manager, retry, enqueud):
        """Test successful enqueuing of a job in ARQ."""
        mock_job = Mock(spec=JobRun, job_function="test_func", id=1, urn="urn:example", retry_delay_seconds=10)
        with (
            patch.object(mock_pipeline_manager.redis, "enqueue_job", return_value=enqueud) as mock_enqueue_job,
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            await mock_pipeline_manager._enqueue_in_arq(job=mock_job, is_retry=retry)

        mock_enqueue_job.assert_called_once_with(
            mock_job.job_function,
            mock_job.id,
            _defer_by=datetime.timedelta(seconds=mock_job.retry_delay_seconds if retry else 0),
            _job_id=mock_job.urn,
        )

    @pytest.mark.asyncio
    async def test_any_enqueue_exception_raises_pipeline_coordination_error(self, mock_pipeline_manager):
        """Test that any exception during enqueuing raises PipelineCoordinationError."""
        mock_job = Mock(spec=JobRun, job_function="test_func", id=1, urn="urn:example", retry_delay_seconds=10)

        with (
            patch.object(
                mock_pipeline_manager.redis,
                "enqueue_job",
                side_effect=Exception("Test exception"),
            ),
            pytest.raises(PipelineCoordinationError, match="Failed to enqueue job in ARQ"),
            TransactionSpy.spy(mock_pipeline_manager.db),
        ):
            await mock_pipeline_manager._enqueue_in_arq(job=mock_job, is_retry=False)


@pytest.mark.integration
class TestEnqueueInArqIntegration:
    """Integration tests for enqueuing jobs in ARQ."""

    @pytest.mark.asyncio
    async def test_enqueue_in_arq_integration(
        self,
        session,
        arq_redis: ArqRedis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
    ):
        """Test enqueuing of a job in ARQ."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        with (
            TransactionSpy.spy(session),
        ):
            await manager._enqueue_in_arq(job=sample_job_run, is_retry=False)

        queued_jobs = await arq_redis.queued_jobs()
        assert len(queued_jobs) == 1
        assert queued_jobs[0].function == sample_job_run.job_function


@pytest.mark.integration
class TestPipelineManagerLifecycle:
    """Integration tests for PipelineManager lifecycle."""

    @pytest.mark.asyncio
    async def test_full_pipeline_lifecycle(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
    ):
        """Test full lifecycle of PipelineManager including initialization and job retrieval."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # pipeline is created with pending jobs
        pipeline = manager.get_pipeline()
        all_jobs = manager.get_all_jobs()

        assert pipeline.id == sample_pipeline.id
        assert len(all_jobs) == 2
        assert all_jobs[0].id == sample_job_run.id
        assert all_jobs[0].status == JobStatus.PENDING

        # pipeline started
        await manager.start_pipeline()
        session.commit()

        # verify pipeline status is running
        updated_pipeline = manager.get_pipeline()
        assert updated_pipeline.status == PipelineStatus.RUNNING

        # Verify job status and enqueued in ARQ
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED
        queued_jobs = await arq_redis.queued_jobs()
        assert len(queued_jobs) == 1
        assert queued_jobs[0].function == sample_job_run.job_function

        # Simulate pipeline lifecycle for a two job sample pipeline. The workflow here should be as follows:
        # - Enter pipeline manager decorator. We don't make any calls when a pipeline begins
        #       - Enter the job manager decorator. This sets the job to RUNNING.
        #           - Job runs...
        #       - Exit the job manager decorator. This sets the job to some terminal state.
        # - Exit the pipeline manager decorator. This coordinates the pipeline, either
        #   enqueuing any newly queueable jobs or terminating it.

        # enter pipeline manager decorator: no work
        pass

        # enter job manager decorator: set job to RUNNING
        job_manager = JobManager(session, arq_redis, sample_job_run.id)
        job_manager.start_job()
        session.commit()

        # job runs... Actual job execution is out of scope for this test. Instead, evict the job from redis to simulate completion.
        await arq_redis.flushdb()

        # exit job manager decorator: set job to SUCCEEDED
        job_manager.succeed_job({"output": "some result", "logs": "some logs", "metadata": {"key": "value"}})
        session.commit()

        # exit pipeline manager decorator: enqueue newly queueable jobs or terminate pipeline
        await manager.coordinate_pipeline()
        session.commit()

        # Verify pipeline status is still RUNNING (since there is a dependent job)
        updated_pipeline = manager.get_pipeline()
        assert updated_pipeline.status == PipelineStatus.RUNNING

        # Verify that the completed job is now SUCCEEDED in the database
        completed_job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert completed_job.status == JobStatus.SUCCEEDED

        # Verify that the dependent job is now QUEUED in the database and ARQ
        dependent_job = session.execute(
            select(JobRun).where(JobRun.pipeline_id == sample_pipeline.id).filter(JobRun.id != sample_job_run.id)
        ).scalar_one()
        assert dependent_job.status == JobStatus.QUEUED
        queued_jobs = await arq_redis.queued_jobs()
        assert len(queued_jobs) == 1
        assert queued_jobs[0].function == dependent_job.job_function

        # Simulate the next iteration of pipeline lifecycle. We've now entered a new context manager with
        # steps identical to those described above but executing in the context of a newly enqueued dependent job.
        job_manager = JobManager(session, arq_redis, dependent_job.id)

        # enter pipeline manager decorator: no work
        pass

        # enter job manager decorator: set dependent job to RUNNING
        dependent_job_manager = JobManager(session, arq_redis, dependent_job.id)
        dependent_job_manager.start_job()
        session.commit()

        # job runs... Actual job execution is out of scope for this test. Instead, evict the job from redis to simulate completion.
        await arq_redis.flushdb()

        # exit job manager decorator: set dependent job to SUCCEEDED
        job_manager.succeed_job({"output": "some result", "logs": "some logs", "metadata": {"key": "value"}})
        session.commit()

        # exit pipeline manager decorator: enqueue newly queueable jobs or terminate pipeline
        await manager.coordinate_pipeline()
        session.commit()

        # Verify pipeline status is now SUCCEEDED
        updated_pipeline = manager.get_pipeline()
        assert updated_pipeline.status == PipelineStatus.SUCCEEDED

        # Verify that the dependent job is now SUCCEEDED in the database
        dependent_job = session.execute(select(JobRun).where(JobRun.id == dependent_job.id)).scalar_one()
        assert dependent_job.status == JobStatus.SUCCEEDED

    @pytest.mark.asyncio
    async def test_paused_pipeline_lifecycle(
        self, session, arq_redis, with_populated_job_data, sample_pipeline, sample_job_run, sample_dependent_job_run
    ):
        """Test lifecycle of a paused pipeline."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Start the pipeline
        await manager.start_pipeline()
        session.commit()

        # Verify pipeline status is running
        updated_pipeline = manager.get_pipeline()
        assert updated_pipeline.status == PipelineStatus.RUNNING

        # Verify job status and enqueued in ARQ
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED
        queued_jobs = await arq_redis.queued_jobs()
        assert len(queued_jobs) == 1
        assert queued_jobs[0].function == sample_job_run.job_function

        # Simulate job start
        job_manager = JobManager(session, arq_redis, sample_job_run.id)
        job_manager.start_job()
        session.commit()

        # Pause the pipeline. Pausing the pipeline while a job is running DOES NOT affect the job.
        await manager.pause_pipeline()
        session.commit()

        # Verify that the pipeline is paused
        updated_pipeline = manager.get_pipeline()
        assert updated_pipeline.status == PipelineStatus.PAUSED

        # Evict the job from redis to simulate completion.
        await arq_redis.flushdb()

        # Simulate job completion
        job_manager.succeed_job({"output": "some result", "logs": "some logs", "metadata": {"key": "value"}})
        session.commit()

        # Coordinate the pipeline
        await manager.coordinate_pipeline()
        session.commit()

        # Verify that the pipeline remains paused
        updated_pipeline = manager.get_pipeline()
        assert updated_pipeline.status == PipelineStatus.PAUSED

        # Verify that no jobs were enqueued in ARQ
        queued_jobs = await arq_redis.queued_jobs()
        assert len(queued_jobs) == 0

        # Verify that the dependent job remains pending in the database
        dependent_job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert dependent_job.status == JobStatus.PENDING

        # Unpause the pipeline
        await manager.unpause_pipeline()
        session.commit()

        # Verify that the pipeline is now running
        updated_pipeline = manager.get_pipeline()
        assert updated_pipeline.status == PipelineStatus.RUNNING

        # Verify that the dependent job is is now queued in ARQ
        dependent_job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert dependent_job.status == JobStatus.QUEUED
        queued_jobs = await arq_redis.queued_jobs()
        assert len(queued_jobs) == 1
        assert queued_jobs[0].function == sample_dependent_job_run.job_function

        # Simulate dependent job start
        dependent_job_manager = JobManager(session, arq_redis, sample_dependent_job_run.id)
        dependent_job_manager.start_job()
        session.commit()

        # Evict the dependent job from redis to simulate completion.
        await arq_redis.flushdb()

        # Simulate dependent job completion
        dependent_job_manager.succeed_job({"output": "some result", "logs": "some logs", "metadata": {"key": "value"}})
        session.commit()

        # Coordinate the pipeline
        await manager.coordinate_pipeline()
        session.commit()

        # Verify that the pipeline is now succeeded
        updated_pipeline = manager.get_pipeline()
        assert updated_pipeline.status == PipelineStatus.SUCCEEDED

        # Verify that the dependent job is now succeeded in the database
        dependent_job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert dependent_job.status == JobStatus.SUCCEEDED

    @pytest.mark.asyncio
    async def test_cancelled_pipeline_lifecycle(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
        sample_dependent_job_run,
    ):
        """Test lifecycle of a cancelled pipeline."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Start the pipeline
        await manager.start_pipeline()
        session.commit()

        # Verify pipeline status is running
        updated_pipeline = manager.get_pipeline()
        assert updated_pipeline.status == PipelineStatus.RUNNING

        # Verify job status and enqueued in ARQ
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED
        queued_jobs = await arq_redis.queued_jobs()
        assert len(queued_jobs) == 1
        assert queued_jobs[0].function == sample_job_run.job_function

        # Simulate job start
        job_manager = JobManager(session, arq_redis, sample_job_run.id)
        job_manager.start_job()
        session.commit()

        # Evict the job from redis to simulate completion.
        await arq_redis.flushdb()

        # Cancel the pipeline. This DOES have an effect on the running job.
        await manager.cancel_pipeline()
        session.commit()

        # Verify that the pipeline is now cancelled
        updated_pipeline = manager.get_pipeline()
        assert updated_pipeline.status == PipelineStatus.CANCELLED

        # Verify that the job is now cancelled in the database
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.CANCELLED

        # Verify that the dependent job is now skipped in the database
        dependent_job = session.execute(select(JobRun).where(JobRun.id == sample_dependent_job_run.id)).scalar_one()
        assert dependent_job.status == JobStatus.SKIPPED

        # Verify that no jobs were enqueued in ARQ
        queued_jobs = await arq_redis.queued_jobs()
        assert len(queued_jobs) == 0

    @pytest.mark.asyncio
    async def test_restart_pipeline_lifecycle(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
    ):
        """Test lifecycle of a restarted pipeline."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Start the pipeline
        await manager.start_pipeline()
        session.commit()

        # Verify pipeline status is running
        updated_pipeline = manager.get_pipeline()
        assert updated_pipeline.status == PipelineStatus.RUNNING

        # Verify job status and enqueued in ARQ
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED
        queued_jobs = await arq_redis.queued_jobs()
        assert len(queued_jobs) == 1
        assert queued_jobs[0].function == sample_job_run.job_function

        # Start the job
        job_manager = JobManager(session, arq_redis, sample_job_run.id)
        job_manager.start_job()
        session.commit()

        # Evict the job from redis to simulate completion.
        await arq_redis.flushdb()

        job_manager.fail_job(
            error=Exception("Simulated job failure"), result={"output": None, "logs": "some logs", "metadata": {}}
        )
        session.commit()

        # Coordinate the pipeline
        await manager.coordinate_pipeline()
        session.commit()

        # Verify the pipeline failed
        updated_pipeline = manager.get_pipeline()
        assert updated_pipeline.status == PipelineStatus.FAILED

        # Verify that the job is now failed in the database
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.FAILED

        # Restart the pipeline
        await manager.restart_pipeline()
        session.commit()

        # Verify that the pipeline is now created
        updated_pipeline = manager.get_pipeline()
        assert updated_pipeline.status == PipelineStatus.RUNNING

        # Verify job status and enqueued in ARQ
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED
        queued_jobs = await arq_redis.queued_jobs()
        assert len(queued_jobs) == 1
        assert queued_jobs[0].function == sample_job_run.job_function

    @pytest.mark.asyncio
    async def test_retry_pipeline_lifecycle(
        self,
        session,
        arq_redis,
        with_populated_job_data,
        sample_pipeline,
        sample_job_run,
    ):
        """Test lifecycle of a restarted pipeline."""
        manager = PipelineManager(session, arq_redis, sample_pipeline.id)

        # Add a cancelled job to the pipeline
        cancelled_job = JobRun(
            id=99,
            pipeline_id=sample_pipeline.id,
            job_function="cancelled_job_function",
            job_type="CANCELLED_JOB",
            status=JobStatus.CANCELLED,
            urn="urn:cancelled_job",
        )
        session.add(cancelled_job)
        session.commit()

        # Start the pipeline
        await manager.start_pipeline()
        session.commit()

        # Verify pipeline status is running
        updated_pipeline = manager.get_pipeline()
        assert updated_pipeline.status == PipelineStatus.RUNNING

        # Verify job status and enqueued in ARQ
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED
        queued_jobs = await arq_redis.queued_jobs()
        assert len(queued_jobs) == 1
        assert queued_jobs[0].function == sample_job_run.job_function

        # Start the job
        job_manager = JobManager(session, arq_redis, sample_job_run.id)
        job_manager.start_job()
        session.commit()

        # Evict the job from redis to simulate completion.
        await arq_redis.flushdb()

        job_manager.fail_job(
            error=Exception("Simulated job failure"), result={"output": None, "logs": "some logs", "metadata": {}}
        )
        session.commit()

        # Coordinate the pipeline
        await manager.coordinate_pipeline()
        session.commit()

        # Verify the pipeline failed
        updated_pipeline = manager.get_pipeline()
        assert updated_pipeline.status == PipelineStatus.FAILED

        # Verify that the job is now failed in the database
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.FAILED

        # Restart the pipeline
        await manager.retry_pipeline()
        session.commit()

        # Verify that the pipeline is now created
        updated_pipeline = manager.get_pipeline()
        assert updated_pipeline.status == PipelineStatus.RUNNING

        # Verify job status of failed job
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED

        # Verify the previously cancelled job is now queued
        job = session.execute(select(JobRun).where(JobRun.id == cancelled_job.id)).scalar_one()
        assert job.status == JobStatus.QUEUED
        queued_jobs = await arq_redis.queued_jobs()
        assert len(queued_jobs) == 2

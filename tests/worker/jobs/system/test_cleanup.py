# ruff: noqa: E402
"""Comprehensive tests for the cleanup_stalled_jobs worker function.

Tests cover:
- Unit tests: Mock database queries and verify cleanup logic
- Integration tests: Use real database and verify end-to-end behavior
- ARQ integration tests: Verify full worker integration
- Edge cases: Empty results, multiple jobs, different states
"""

import pytest

pytest.importorskip("arq")  # Skip tests if arq is not installed

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, call, patch

from sqlalchemy import select

from mavedb.models.enums import DependencyType
from mavedb.models.enums.job_pipeline import FailureCategory, JobStatus, PipelineStatus
from mavedb.models.job_dependency import JobDependency
from mavedb.models.job_run import JobRun
from mavedb.models.pipeline import Pipeline
from mavedb.worker.jobs.system.cleanup import (
    PENDING_TIMEOUT_MINUTES,
    QUEUED_TIMEOUT_MINUTES,
    RUNNING_TIMEOUT_MINUTES,
    cleanup_stalled_jobs,
)
from mavedb.worker.lib.managers.job_manager import JobManager
from tests.helpers.transaction_spy import TransactionSpy

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


############################################################################################################################################
# Unit Tests
############################################################################################################################################


@pytest.mark.asyncio
@pytest.mark.unit
class TestCleanupStalledJobsUnit:
    """Unit tests for the cleanup_stalled_jobs function."""

    async def test_cleanup_with_no_stalled_jobs(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test cleanup when no stalled jobs are found."""
        with (
            patch.object(JobManager, "update_progress") as mock_update_progress,
        ):
            result = await cleanup_stalled_jobs(
                mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
            )

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 0
        assert result["data"]["queued_jobs"] == []
        assert result["data"]["running_jobs"] == []
        assert result["data"]["pending_jobs"] == []

        # Verify progress updates
        assert mock_update_progress.call_count >= 4  # Start, QUEUED, RUNNING, PENDING

    async def test_cleanup_updates_progress_correctly(
        self, mock_worker_ctx, session, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test that cleanup updates progress at each stage."""
        with (
            patch.object(JobManager, "update_progress") as mock_update_progress,
        ):
            await cleanup_stalled_jobs(
                mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
            )

        # Verify progress update calls
        expected_calls = [
            call(0, 100, "Starting cleanup of stalled jobs."),
            call(10, 100, "Found 0 stalled QUEUED jobs to evaluate."),
            call(50, 100, "Found 0 stalled RUNNING jobs to evaluate."),
            call(80, 100, "Found 0 stalled PENDING jobs to evaluate."),
        ]
        mock_update_progress.assert_has_calls(expected_calls)

    async def test_cleanup_stalled_queued_job_with_retries_remaining(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test cleanup of a stalled QUEUED job with retries remaining."""
        # Create a stalled QUEUED job in the database
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.QUEUED,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=QUEUED_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        result = await cleanup_stalled_jobs(
            mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
        )
        mock_worker_ctx["redis"].enqueue_job.assert_called_once()  # Verify a retry job was enqueued

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1
        assert stalled_job.urn in result["data"]["queued_jobs"]

        # Verify job state was updated correctly
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.QUEUED  # job was re-enqueued but not yet started, so it remains QUEUED
        assert stalled_job.retry_count == 1
        assert stalled_job.started_at is None
        assert stalled_job.finished_at is None

    async def test_cleanup_stalled_queued_job_max_retries_reached(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test cleanup of a stalled QUEUED job with max retries reached."""
        # Create a stalled QUEUED job with max retries
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.QUEUED,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=QUEUED_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=3,  # Already at max
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        result = await cleanup_stalled_jobs(
            mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
        )

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1
        assert stalled_job.urn in result["data"]["queued_jobs"]

        # Verify job was marked as FAILED
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.FAILED
        assert stalled_job.failure_category == FailureCategory.SYSTEM_ERROR
        assert "stalled" in stalled_job.error_message.lower()

    async def test_cleanup_stalled_running_job_with_retries(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test cleanup of a stalled RUNNING job with retries remaining."""
        # Create a stalled RUNNING job in the database
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.RUNNING,
            created_at=datetime.now(timezone.utc) - timedelta(hours=2),
            started_at=datetime.now(timezone.utc) - timedelta(minutes=RUNNING_TIMEOUT_MINUTES + 10),
            finished_at=None,
            max_retries=3,
            retry_count=1,
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        result = await cleanup_stalled_jobs(
            mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
        )
        mock_worker_ctx["redis"].enqueue_job.assert_called_once()  # Verify a retry job was enqueued

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1
        assert stalled_job.urn in result["data"]["running_jobs"]

        # Verify job state was updated correctly
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.QUEUED  # Moved back to QUEUED for retry
        assert stalled_job.retry_count == 2  # Incremented from 1
        assert stalled_job.started_at is None  # Cleared for retry
        assert stalled_job.finished_at is None

    async def test_cleanup_stalled_running_job_max_retries_reached(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test cleanup of a stalled RUNNING job with max retries reached."""
        # Create a stalled RUNNING job with max retries
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.RUNNING,
            created_at=datetime.now(timezone.utc) - timedelta(hours=2),
            started_at=datetime.now(timezone.utc) - timedelta(minutes=RUNNING_TIMEOUT_MINUTES + 10),
            finished_at=None,
            max_retries=3,
            retry_count=3,  # Already at max
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        result = await cleanup_stalled_jobs(
            mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
        )

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1
        assert stalled_job.urn in result["data"]["running_jobs"]

        # Verify job was marked as FAILED
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.FAILED
        assert stalled_job.failure_category == FailureCategory.SYSTEM_ERROR
        assert "stalled" in stalled_job.error_message.lower()

    async def test_cleanup_stalled_running_job_missing_started_at(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test cleanup handles RUNNING job with missing started_at timestamp."""
        # Add session to worker context for real DB operations
        mock_worker_ctx["db"] = session

        # Create a RUNNING job without started_at (data inconsistency)
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.RUNNING,
            created_at=datetime.now(timezone.utc) - timedelta(hours=2),
            started_at=None,  # Missing timestamp
            finished_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        with patch("mavedb.worker.jobs.system.cleanup.send_slack_error") as mock_slack:
            result = await cleanup_stalled_jobs(
                mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
            )

        # Job should be skipped (not cleaned up)
        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 0

        # Slack error should have been sent
        mock_slack.assert_called_once()

        # Job should remain unchanged
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.RUNNING
        assert stalled_job.retry_count == 0

    async def test_cleanup_stalled_pending_job_with_retries(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test cleanup of a stalled PENDING job with retries remaining."""
        # Create a stalled PENDING job in the database
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.PENDING,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=PENDING_TIMEOUT_MINUTES + 5),
            started_at=None,
            finished_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        result = await cleanup_stalled_jobs(
            mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
        )
        mock_worker_ctx["redis"].enqueue_job.assert_called_once()  # Verify a retry job was enqueued

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1
        assert stalled_job.urn in result["data"]["pending_jobs"]

        # Verify job state was updated correctly
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.QUEUED  # Moved back to QUEUED for retry
        assert stalled_job.retry_count == 1  # Incremented from 0
        assert stalled_job.started_at is None
        assert stalled_job.finished_at is None

    async def test_cleanup_stalled_pending_job_max_retries_reached(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test cleanup of a stalled PENDING job with max retries reached."""
        # Create a stalled PENDING job with max retries
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.PENDING,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=PENDING_TIMEOUT_MINUTES + 5),
            started_at=None,
            finished_at=None,
            max_retries=3,
            retry_count=3,  # Already at max
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        result = await cleanup_stalled_jobs(
            mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
        )

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1
        assert stalled_job.urn in result["data"]["pending_jobs"]

        # Verify job was marked as FAILED
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.FAILED
        assert stalled_job.failure_category == FailureCategory.SYSTEM_ERROR
        assert "stalled" in stalled_job.error_message.lower()

    async def test_cleanup_stalled_pending_job_enqueue_failure(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test that stalled PENDING job is marked FAILED if ARQ enqueue fails."""
        # Create a stalled PENDING job
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.PENDING,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=PENDING_TIMEOUT_MINUTES + 5),
            started_at=None,
            finished_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        # Mock redis.enqueue_job to raise an exception
        mock_worker_ctx["redis"].enqueue_job = AsyncMock(side_effect=Exception("Redis connection failed"))

        result = await cleanup_stalled_jobs(
            mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
        )

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Verify job was marked as FAILED due to enqueue failure
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.FAILED
        assert stalled_job.failure_category == FailureCategory.SYSTEM_ERROR
        assert "Failed to enqueue after stall recovery" in stalled_job.error_message

    async def test_cleanup_multiple_stalled_jobs_mixed_states(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test cleanup of multiple stalled jobs in different states."""
        # Create a pipeline and stalled jobs in all three states
        test_pipeline = Pipeline(
            urn="test:pipeline:multi",
            name="Test Pipeline Multi",
            description="Pipeline for multi-job test",
            status=PipelineStatus.CREATED,
            correlation_id="test_multi",
        )
        session.add(test_pipeline)
        session.flush()

        stalled_queued = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.QUEUED,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=QUEUED_TIMEOUT_MINUTES + 1),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )

        stalled_running = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.RUNNING,
            created_at=datetime.now(timezone.utc) - timedelta(hours=2),
            started_at=datetime.now(timezone.utc) - timedelta(minutes=RUNNING_TIMEOUT_MINUTES + 1),
            finished_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )

        stalled_pending = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.PENDING,
            pipeline_id=test_pipeline.id,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=PENDING_TIMEOUT_MINUTES + 1),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )

        session.add_all([stalled_queued, stalled_running, stalled_pending])
        session.commit()

        result = await cleanup_stalled_jobs(
            mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
        )

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 3
        assert stalled_queued.urn in result["data"]["queued_jobs"]
        assert stalled_running.urn in result["data"]["running_jobs"]
        assert stalled_pending.urn in result["data"]["pending_jobs"]

        # Verify all jobs were updated correctly
        session.refresh(stalled_queued)
        session.refresh(stalled_running)
        session.refresh(stalled_pending)
        # All jobs should be QUEUED after successful retry and enqueue
        assert stalled_queued.status == JobStatus.QUEUED
        assert stalled_queued.retry_count == 1
        assert stalled_running.status == JobStatus.QUEUED
        assert stalled_running.retry_count == 1
        assert stalled_pending.status == JobStatus.QUEUED
        assert stalled_pending.retry_count == 1

    async def test_cleanup_stalled_queued_standalone_job_enqueue_failure(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test that stalled standalone QUEUED job is marked FAILED if ARQ enqueue fails."""

        # Create a stalled QUEUED job WITHOUT pipeline_id (standalone)
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.QUEUED,
            pipeline_id=None,  # Standalone job
            created_at=datetime.now(timezone.utc) - timedelta(minutes=QUEUED_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        # Mock redis.enqueue_job to raise an exception
        mock_worker_ctx["redis"].enqueue_job = AsyncMock(side_effect=Exception("Redis connection failed"))

        result = await cleanup_stalled_jobs(
            mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
        )

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Verify job was marked as FAILED due to enqueue failure
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.FAILED
        assert stalled_job.failure_category == FailureCategory.SYSTEM_ERROR
        assert "Failed to enqueue after stall recovery" in stalled_job.error_message

    async def test_cleanup_stalled_running_standalone_job_enqueue_failure(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test that stalled standalone RUNNING job is marked FAILED if ARQ enqueue fails."""

        # Create a stalled RUNNING job WITHOUT pipeline_id (standalone)
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.RUNNING,
            pipeline_id=None,  # Standalone job
            created_at=datetime.now(timezone.utc) - timedelta(hours=2),
            started_at=datetime.now(timezone.utc) - timedelta(minutes=RUNNING_TIMEOUT_MINUTES + 10),
            finished_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        # Mock redis.enqueue_job to raise an exception
        mock_worker_ctx["redis"].enqueue_job = AsyncMock(side_effect=Exception("Redis connection failed"))

        result = await cleanup_stalled_jobs(
            mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
        )

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Verify job was marked as FAILED due to enqueue failure
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.FAILED
        assert stalled_job.failure_category == FailureCategory.SYSTEM_ERROR
        assert "Failed to enqueue after stall recovery" in stalled_job.error_message

    async def test_cleanup_stalled_queued_pipeline_job_dependencies_satisfied(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test that stalled pipeline QUEUED job with satisfied dependencies is enqueued."""
        # Create a pipeline with all dependencies satisfied
        test_pipeline = Pipeline(
            urn="test:pipeline:queued_deps_ok",
            name="Test Pipeline Queued Deps OK",
            description="Pipeline for queued job with satisfied dependencies",
            status=PipelineStatus.CREATED,
            correlation_id="test_queued_deps_ok",
        )
        session.add(test_pipeline)
        session.flush()

        # Create a stalled QUEUED job WITH pipeline_id
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.QUEUED,
            pipeline_id=test_pipeline.id,  # Part of pipeline
            created_at=datetime.now(timezone.utc) - timedelta(minutes=QUEUED_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        result = await cleanup_stalled_jobs(
            mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
        )

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Verify job was enqueued (dependencies were satisfied)
        mock_worker_ctx["redis"].enqueue_job.assert_called_once()
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.QUEUED
        assert stalled_job.retry_count == 1

    async def test_cleanup_stalled_running_pipeline_job_dependencies_satisfied(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test that stalled pipeline RUNNING job with satisfied dependencies is enqueued."""
        # Create a pipeline with all dependencies satisfied
        test_pipeline = Pipeline(
            urn="test:pipeline:running_deps_ok",
            name="Test Pipeline Running Deps OK",
            description="Pipeline for running job with satisfied dependencies",
            status=PipelineStatus.CREATED,
            correlation_id="test_running_deps_ok",
        )
        session.add(test_pipeline)
        session.flush()

        # Create a stalled RUNNING job WITH pipeline_id
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.RUNNING,
            pipeline_id=test_pipeline.id,  # Part of pipeline
            created_at=datetime.now(timezone.utc) - timedelta(hours=2),
            started_at=datetime.now(timezone.utc) - timedelta(minutes=RUNNING_TIMEOUT_MINUTES + 10),
            finished_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        result = await cleanup_stalled_jobs(
            mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
        )

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Verify job was enqueued (dependencies were satisfied)
        mock_worker_ctx["redis"].enqueue_job.assert_called_once()
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.QUEUED
        assert stalled_job.retry_count == 1

    async def test_cleanup_stalled_queued_pipeline_job_dependencies_failed(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test that stalled pipeline QUEUED job with failed dependencies is skipped."""
        # Create a pipeline
        test_pipeline = Pipeline(
            urn="test:pipeline:queued_deps_failed",
            name="Test Pipeline Queued Deps Failed",
            description="Pipeline for queued job with failed dependencies",
            status=PipelineStatus.CREATED,
            correlation_id="test_queued_deps_failed",
        )
        session.add(test_pipeline)
        session.flush()

        # Create dependency job that failed
        dependency_job = JobRun(
            job_type="dependency",
            job_function="dependency_function",
            status=JobStatus.FAILED,
            pipeline_id=test_pipeline.id,
            max_retries=3,
            retry_count=3,
            job_params={},
        )
        session.add(dependency_job)
        session.flush()

        # Create stalled job that depends on the failed job
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.QUEUED,
            pipeline_id=test_pipeline.id,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=QUEUED_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.flush()

        # Create dependency relationship
        dependency = JobDependency(
            id=stalled_job.id,
            depends_on_job_id=dependency_job.id,
            dependency_type=DependencyType.SUCCESS_REQUIRED,
        )
        session.add(dependency)
        session.commit()

        result = await cleanup_stalled_jobs(
            mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
        )

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Verify job was NOT enqueued (dependencies failed - should be skipped)
        # Job should remain in PENDING state for pipeline manager to handle skipping
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.PENDING
        assert stalled_job.retry_count == 1

    async def test_cleanup_stalled_queued_pipeline_job_dependencies_not_ready(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test that stalled pipeline QUEUED job with unmet dependencies stays PENDING."""
        # Create a pipeline
        test_pipeline = Pipeline(
            urn="test:pipeline:queued_deps_not_ready",
            name="Test Pipeline Queued Deps Not Ready",
            description="Pipeline for queued job with unmet dependencies",
            status=PipelineStatus.CREATED,
            correlation_id="test_queued_deps_not_ready",
        )
        session.add(test_pipeline)
        session.flush()

        # Create dependency job that's still running
        dependency_job = JobRun(
            job_type="dependency",
            job_function="dependency_function",
            status=JobStatus.RUNNING,
            pipeline_id=test_pipeline.id,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(dependency_job)
        session.flush()

        # Create stalled job that depends on the running job
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.QUEUED,
            pipeline_id=test_pipeline.id,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=QUEUED_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.flush()

        # Create dependency relationship
        dependency = JobDependency(
            id=stalled_job.id,
            depends_on_job_id=dependency_job.id,
            dependency_type=DependencyType.SUCCESS_REQUIRED,
        )
        session.add(dependency)
        session.commit()

        result = await cleanup_stalled_jobs(
            mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
        )

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Verify job was NOT enqueued (dependencies not ready)
        # Job should remain in PENDING state waiting for dependencies
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.PENDING
        assert stalled_job.retry_count == 1

    async def test_cleanup_stalled_running_pipeline_job_dependencies_failed(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test that stalled pipeline RUNNING job with failed dependencies is skipped."""
        # Create a pipeline
        test_pipeline = Pipeline(
            urn="test:pipeline:running_deps_failed",
            name="Test Pipeline Running Deps Failed",
            description="Pipeline for running job with failed dependencies",
            status=PipelineStatus.CREATED,
            correlation_id="test_running_deps_failed",
        )
        session.add(test_pipeline)
        session.flush()

        # Create dependency job that failed
        dependency_job = JobRun(
            job_type="dependency",
            job_function="dependency_function",
            status=JobStatus.FAILED,
            pipeline_id=test_pipeline.id,
            max_retries=3,
            retry_count=3,
            job_params={},
        )
        session.add(dependency_job)
        session.flush()

        # Create stalled job that depends on the failed job
        # Use recent created_at to avoid being detected as stalled PENDING after reset from RUNNING
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.RUNNING,
            pipeline_id=test_pipeline.id,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=PENDING_TIMEOUT_MINUTES - 5),
            started_at=datetime.now(timezone.utc) - timedelta(minutes=RUNNING_TIMEOUT_MINUTES + 10),
            finished_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.flush()

        # Create dependency relationship
        dependency = JobDependency(
            id=stalled_job.id,
            depends_on_job_id=dependency_job.id,
            dependency_type=DependencyType.SUCCESS_REQUIRED,
        )
        session.add(dependency)
        session.commit()

        result = await cleanup_stalled_jobs(
            mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
        )

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Verify job was NOT enqueued (dependencies failed)
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.PENDING
        assert stalled_job.retry_count == 1

    async def test_cleanup_stalled_pending_pipeline_job_dependencies_failed(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test that stalled pipeline PENDING job with failed dependencies is skipped."""
        # Create a pipeline
        test_pipeline = Pipeline(
            urn="test:pipeline:pending_deps_failed",
            name="Test Pipeline Pending Deps Failed",
            description="Pipeline for pending job with failed dependencies",
            status=PipelineStatus.CREATED,
            correlation_id="test_pending_deps_failed",
        )
        session.add(test_pipeline)
        session.flush()

        # Create dependency job that failed
        dependency_job = JobRun(
            job_type="dependency",
            job_function="dependency_function",
            status=JobStatus.FAILED,
            pipeline_id=test_pipeline.id,
            max_retries=3,
            retry_count=3,
            job_params={},
        )
        session.add(dependency_job)
        session.flush()

        # Create stalled job that depends on the failed job
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.PENDING,
            pipeline_id=test_pipeline.id,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=PENDING_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.flush()

        # Create dependency relationship
        dependency = JobDependency(
            id=stalled_job.id,
            depends_on_job_id=dependency_job.id,
            dependency_type=DependencyType.SUCCESS_REQUIRED,
        )
        session.add(dependency)
        session.commit()

        result = await cleanup_stalled_jobs(
            mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
        )

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Verify job was NOT enqueued (dependencies failed)
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.PENDING
        assert stalled_job.retry_count == 1

    async def test_cleanup_stalled_running_pipeline_job_dependencies_not_ready(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test that stalled pipeline RUNNING job with dependencies not ready is skipped."""
        # Create a pipeline
        test_pipeline = Pipeline(
            urn="test:pipeline:running_deps_not_ready",
            name="Test Pipeline Running Deps Not Ready",
            description="Pipeline for running job with dependencies not ready",
            status=PipelineStatus.CREATED,
            correlation_id="test_running_deps_not_ready",
        )
        session.add(test_pipeline)
        session.flush()

        # Create dependency job still running
        dependency_job = JobRun(
            job_type="dependency",
            job_function="dependency_function",
            status=JobStatus.RUNNING,
            pipeline_id=test_pipeline.id,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(dependency_job)
        session.flush()

        # Create stalled job - use recent created_at to avoid double cleanup
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.RUNNING,
            pipeline_id=test_pipeline.id,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=PENDING_TIMEOUT_MINUTES - 5),
            started_at=datetime.now(timezone.utc) - timedelta(minutes=RUNNING_TIMEOUT_MINUTES + 10),
            finished_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.flush()

        # Create dependency relationship
        dependency = JobDependency(
            id=stalled_job.id,
            depends_on_job_id=dependency_job.id,
            dependency_type=DependencyType.SUCCESS_REQUIRED,
        )
        session.add(dependency)
        session.commit()

        result = await cleanup_stalled_jobs(
            mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
        )

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Verify job was NOT enqueued (dependencies not ready)
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.PENDING
        assert stalled_job.retry_count == 1

    async def test_cleanup_stalled_pending_pipeline_job_dependencies_not_ready(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test that stalled pipeline PENDING job with dependencies not ready is skipped."""
        # Create a pipeline
        test_pipeline = Pipeline(
            urn="test:pipeline:pending_deps_not_ready",
            name="Test Pipeline Pending Deps Not Ready",
            description="Pipeline for pending job with dependencies not ready",
            status=PipelineStatus.CREATED,
            correlation_id="test_pending_deps_not_ready",
        )
        session.add(test_pipeline)
        session.flush()

        # Create dependency job still running
        dependency_job = JobRun(
            job_type="dependency",
            job_function="dependency_function",
            status=JobStatus.RUNNING,
            pipeline_id=test_pipeline.id,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(dependency_job)
        session.flush()

        # Create stalled job
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.PENDING,
            pipeline_id=test_pipeline.id,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=PENDING_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.flush()

        # Create dependency relationship
        dependency = JobDependency(
            id=stalled_job.id,
            depends_on_job_id=dependency_job.id,
            dependency_type=DependencyType.SUCCESS_REQUIRED,
        )
        session.add(dependency)
        session.commit()

        result = await cleanup_stalled_jobs(
            mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
        )

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Verify job was NOT enqueued (dependencies not ready)
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.PENDING
        assert stalled_job.retry_count == 1

    async def test_cleanup_jobs_does_not_alter_jobs_in_valid_states(
        self, session, mock_worker_ctx, sample_cleanup_job_run, with_cleanup_job
    ):
        """Test that cleanup does not alter jobs that are not stalled."""
        # Create a non-stalled RUNNING job
        valid_running_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.RUNNING,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=30),
            started_at=datetime.now(timezone.utc) - timedelta(minutes=25),
            finished_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )

        # Create a non-stalled PENDING job in a pipeline (well within timeout)
        test_pipeline = Pipeline(
            urn="test:pipeline:valid",
            name="Test Pipeline Valid",
            description="Pipeline for valid job test",
            status=PipelineStatus.CREATED,
            correlation_id="test_valid",
        )
        session.add(test_pipeline)
        session.flush()
        valid_pending_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.PENDING,
            pipeline_id=test_pipeline.id,
            created_at=datetime.now(timezone.utc)
            - timedelta(minutes=PENDING_TIMEOUT_MINUTES - 5),  # 5 min before timeout
            started_at=None,
            finished_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )

        # Create a non-stalled QUEUED job (well within timeout)
        valid_queued_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.QUEUED,
            created_at=datetime.now(timezone.utc)
            - timedelta(minutes=QUEUED_TIMEOUT_MINUTES - 5),  # 5 min before timeout
            started_at=None,
            finished_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )

        session.add_all([valid_running_job, valid_pending_job, valid_queued_job])
        session.commit()

        result = await cleanup_stalled_jobs(
            mock_worker_ctx, None, JobManager(session, mock_worker_ctx["redis"], sample_cleanup_job_run.id)
        )

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 0

        # Verify the valid job was not altered
        session.refresh(valid_running_job)
        assert valid_running_job.status == JobStatus.RUNNING
        session.refresh(valid_pending_job)
        assert valid_pending_job.status == JobStatus.PENDING
        session.refresh(valid_queued_job)
        assert valid_queued_job.status == JobStatus.QUEUED


############################################################################################################################################
# Integration Tests
############################################################################################################################################


@pytest.mark.asyncio
@pytest.mark.integration
class TestCleanupStalledJobsIntegration:
    """Integration tests for cleanup_stalled_jobs with real database."""

    async def test_cleanup_integration_no_stalled_jobs(self, standalone_worker_context, session):
        """Integration test: cleanup with no stalled jobs."""
        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await cleanup_stalled_jobs(standalone_worker_context)

        # Verify the cleanup job itself was created and succeeded
        cleanup_job = session.execute(
            select(JobRun).where(JobRun.job_function == "cleanup_stalled_jobs")
        ).scalar_one_or_none()

        assert cleanup_job is not None
        assert cleanup_job.status == JobStatus.SUCCEEDED
        assert cleanup_job.job_type == "cron_job"

        # Verify no jobs were cleaned
        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 0

    async def test_cleanup_integration_stalled_queued_job_gets_retried(self, standalone_worker_context, session):
        """Integration test: stalled QUEUED job is retried."""
        # Create a stalled QUEUED job
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.QUEUED,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=QUEUED_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await cleanup_stalled_jobs(standalone_worker_context)

        # Verify cleanup succeeded
        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Verify the stalled job was reset to PENDING for retry
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.QUEUED  # Jobs are enqueued after retry
        assert stalled_job.retry_count == 1

    async def test_cleanup_integration_stalled_running_job_gets_retried(self, standalone_worker_context, session):
        """Integration test: stalled RUNNING job is retried."""
        # Create a stalled RUNNING job (simulating worker crash)
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.RUNNING,
            created_at=datetime.now(timezone.utc) - timedelta(hours=2),
            started_at=datetime.now(timezone.utc) - timedelta(minutes=RUNNING_TIMEOUT_MINUTES + 10),
            finished_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await cleanup_stalled_jobs(standalone_worker_context)

        # Verify cleanup succeeded
        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Verify the stalled job was reset to PENDING for retry
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.QUEUED  # Jobs are enqueued after retry
        assert stalled_job.retry_count == 1
        assert stalled_job.error_message is None  # Cleared on retry
        assert stalled_job.finished_at is None  # Cleared on retry

    async def test_cleanup_integration_max_retries_reached_fails_job(self, standalone_worker_context, session):
        """Integration test: stalled job with max retries is failed."""
        # Create a stalled job at max retries
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.QUEUED,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=QUEUED_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=3,  # Already at max
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await cleanup_stalled_jobs(standalone_worker_context)

        # Verify cleanup succeeded
        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Verify the stalled job was marked as FAILED
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.FAILED
        assert stalled_job.failure_category == FailureCategory.SYSTEM_ERROR
        assert "stalled" in stalled_job.error_message.lower()

    async def test_cleanup_integration_pending_job_in_pipeline(self, standalone_worker_context, session):
        """Integration test: stalled PENDING job in pipeline is retried."""
        test_pipeline = Pipeline(
            urn="test:pipeline:cleanup",
            name="Test Cleanup Pipeline",
            description="Pipeline for cleanup test",
            status=PipelineStatus.CREATED,
            correlation_id="test_cleanup_correlation",
        )
        session.add(test_pipeline)
        session.flush()  # Get the pipeline ID

        # Create a stalled PENDING job in the pipeline
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.PENDING,
            pipeline_id=test_pipeline.id,  # Reference the real pipeline
            created_at=datetime.now(timezone.utc) - timedelta(minutes=PENDING_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await cleanup_stalled_jobs(standalone_worker_context)

        # Verify cleanup succeeded
        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Verify the stalled job was reset for retry
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.QUEUED  # Jobs are enqueued after retry
        assert stalled_job.retry_count == 1

    async def test_cleanup_integration_excludes_recent_jobs(self, standalone_worker_context, session):
        """Integration test: recent jobs are not cleaned up."""
        # Create jobs that are recent (within timeout thresholds)
        recent_queued = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.QUEUED,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=QUEUED_TIMEOUT_MINUTES - 5),  # Within threshold
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )

        recent_running = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.RUNNING,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=30),
            started_at=datetime.now(timezone.utc) - timedelta(minutes=RUNNING_TIMEOUT_MINUTES - 5),  # Within threshold
            finished_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )

        session.add_all([recent_queued, recent_running])
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await cleanup_stalled_jobs(standalone_worker_context)

        # Verify no jobs were cleaned
        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 0

        # Verify jobs remain unchanged
        session.refresh(recent_queued)
        session.refresh(recent_running)
        assert recent_queued.status == JobStatus.QUEUED
        assert recent_running.status == JobStatus.RUNNING
        assert recent_queued.retry_count == 0
        assert recent_running.retry_count == 0

    async def test_cleanup_integration_updates_progress_correctly(self, standalone_worker_context, session):
        """Integration test: cleanup job updates progress correctly and returns proper data."""
        # Create stalled jobs to trigger progress updates across different states
        queued_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.QUEUED,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=QUEUED_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        running_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.RUNNING,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=PENDING_TIMEOUT_MINUTES - 5),
            started_at=datetime.now(timezone.utc) - timedelta(minutes=RUNNING_TIMEOUT_MINUTES + 10),
            finished_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add_all([queued_job, running_job])
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await cleanup_stalled_jobs(standalone_worker_context)

        # Verify cleanup succeeded with progress through all states
        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 2

        # Verify result structure contains detailed breakdown
        assert "queued_jobs" in result["data"]
        assert "running_jobs" in result["data"]
        assert "pending_jobs" in result["data"]

        # Verify both jobs were processed
        assert len(result["data"]["queued_jobs"]) == 1
        assert len(result["data"]["running_jobs"]) == 1
        assert len(result["data"]["pending_jobs"]) == 0

    async def test_cleanup_integration_stalled_running_job_max_retries_reached(
        self, standalone_worker_context, session
    ):
        """Integration test: stalled RUNNING job at max retries is failed."""
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.RUNNING,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=PENDING_TIMEOUT_MINUTES - 5),
            started_at=datetime.now(timezone.utc) - timedelta(minutes=RUNNING_TIMEOUT_MINUTES + 10),
            finished_at=None,
            max_retries=3,
            retry_count=3,  # Already at max
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await cleanup_stalled_jobs(standalone_worker_context)

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.FAILED
        assert stalled_job.retry_count == 3
        assert stalled_job.failure_category == FailureCategory.SYSTEM_ERROR

    async def test_cleanup_integration_stalled_running_job_missing_started_at(self, standalone_worker_context, session):
        """Integration test: stalled RUNNING job without started_at is skipped (not cleaned)."""
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.RUNNING,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=PENDING_TIMEOUT_MINUTES - 5),
            started_at=None,  # Missing started_at - causes job to be skipped
            finished_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await cleanup_stalled_jobs(standalone_worker_context)

        # Job is skipped (not cleaned) when started_at is missing
        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 0

        # Job remains unchanged
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.RUNNING
        assert stalled_job.retry_count == 0

    async def test_cleanup_integration_stalled_pending_job_with_retries(self, standalone_worker_context, session):
        """Integration test: stalled PENDING job is retried."""
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.PENDING,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=PENDING_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await cleanup_stalled_jobs(standalone_worker_context)

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.QUEUED
        assert stalled_job.retry_count == 1

    async def test_cleanup_integration_stalled_pending_job_max_retries_reached(
        self, standalone_worker_context, session
    ):
        """Integration test: stalled PENDING job at max retries is failed."""
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.PENDING,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=PENDING_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=3,
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await cleanup_stalled_jobs(standalone_worker_context)

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.FAILED
        assert stalled_job.retry_count == 3
        assert stalled_job.failure_category == FailureCategory.SYSTEM_ERROR

    async def test_cleanup_integration_multiple_stalled_jobs_mixed_states(self, standalone_worker_context, session):
        """Integration test: cleanup handles multiple jobs in different states."""
        queued_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.QUEUED,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=QUEUED_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )

        running_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.RUNNING,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=PENDING_TIMEOUT_MINUTES - 5),
            started_at=datetime.now(timezone.utc) - timedelta(minutes=RUNNING_TIMEOUT_MINUTES + 10),
            finished_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )

        pending_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.PENDING,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=PENDING_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )

        session.add_all([queued_job, running_job, pending_job])
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await cleanup_stalled_jobs(standalone_worker_context)

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 3

        session.refresh(queued_job)
        session.refresh(running_job)
        session.refresh(pending_job)

        assert queued_job.status == JobStatus.QUEUED
        assert running_job.status == JobStatus.QUEUED
        assert pending_job.status == JobStatus.QUEUED
        assert queued_job.retry_count == 1
        assert running_job.retry_count == 1
        assert pending_job.retry_count == 1

    async def test_cleanup_integration_stalled_queued_pipeline_job_dependencies_satisfied(
        self, standalone_worker_context, session
    ):
        """Integration test: stalled pipeline QUEUED job with satisfied dependencies is enqueued."""
        test_pipeline = Pipeline(
            urn="test:pipeline:queued_deps_ok",
            name="Test Pipeline Queued Deps OK",
            description="Pipeline for queued job with satisfied dependencies",
            status=PipelineStatus.CREATED,
            correlation_id="test_queued_deps_ok",
        )
        session.add(test_pipeline)
        session.flush()

        # Create dependency job that succeeded
        dependency_job = JobRun(
            job_type="dependency",
            job_function="dependency_function",
            status=JobStatus.SUCCEEDED,
            pipeline_id=test_pipeline.id,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(dependency_job)
        session.flush()

        # Create stalled job that depends on successful job
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.QUEUED,
            pipeline_id=test_pipeline.id,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=QUEUED_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.flush()

        # Create dependency relationship
        dependency = JobDependency(
            id=stalled_job.id,
            depends_on_job_id=dependency_job.id,
            dependency_type=DependencyType.SUCCESS_REQUIRED,
        )
        session.add(dependency)
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await cleanup_stalled_jobs(standalone_worker_context)

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.QUEUED
        assert stalled_job.retry_count == 1

    async def test_cleanup_integration_stalled_queued_pipeline_job_dependencies_failed(
        self, standalone_worker_context, session
    ):
        """Integration test: stalled pipeline QUEUED job with failed dependencies is skipped."""
        test_pipeline = Pipeline(
            urn="test:pipeline:queued_deps_failed",
            name="Test Pipeline Queued Deps Failed",
            description="Pipeline for queued job with failed dependencies",
            status=PipelineStatus.CREATED,
            correlation_id="test_queued_deps_failed",
        )
        session.add(test_pipeline)
        session.flush()

        # Create dependency job that failed
        dependency_job = JobRun(
            job_type="dependency",
            job_function="dependency_function",
            status=JobStatus.FAILED,
            pipeline_id=test_pipeline.id,
            max_retries=3,
            retry_count=3,
            job_params={},
        )
        session.add(dependency_job)
        session.flush()

        # Create stalled job
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.QUEUED,
            pipeline_id=test_pipeline.id,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=QUEUED_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.flush()

        # Create dependency relationship
        dependency = JobDependency(
            id=stalled_job.id,
            depends_on_job_id=dependency_job.id,
            dependency_type=DependencyType.SUCCESS_REQUIRED,
        )
        session.add(dependency)
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await cleanup_stalled_jobs(standalone_worker_context)

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Job should be in PENDING, not enqueued
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.PENDING
        assert stalled_job.retry_count == 1

    async def test_cleanup_integration_stalled_queued_pipeline_job_dependencies_not_ready(
        self, standalone_worker_context, session
    ):
        """Integration test: stalled pipeline QUEUED job with dependencies not ready is skipped."""
        test_pipeline = Pipeline(
            urn="test:pipeline:queued_deps_not_ready",
            name="Test Pipeline Queued Deps Not Ready",
            description="Pipeline for queued job with dependencies not ready",
            status=PipelineStatus.CREATED,
            correlation_id="test_queued_deps_not_ready",
        )
        session.add(test_pipeline)
        session.flush()

        # Create dependency job still running
        dependency_job = JobRun(
            job_type="dependency",
            job_function="dependency_function",
            status=JobStatus.RUNNING,
            pipeline_id=test_pipeline.id,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(dependency_job)
        session.flush()

        # Create stalled job
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.QUEUED,
            pipeline_id=test_pipeline.id,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=QUEUED_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.flush()

        # Create dependency relationship
        dependency = JobDependency(
            id=stalled_job.id,
            depends_on_job_id=dependency_job.id,
            dependency_type=DependencyType.SUCCESS_REQUIRED,
        )
        session.add(dependency)
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await cleanup_stalled_jobs(standalone_worker_context)

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Job should be in PENDING, waiting for dependencies
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.PENDING
        assert stalled_job.retry_count == 1

    async def test_cleanup_integration_stalled_running_pipeline_job_dependencies_failed(
        self, standalone_worker_context, session
    ):
        """Integration test: stalled pipeline RUNNING job with failed dependencies is skipped."""
        test_pipeline = Pipeline(
            urn="test:pipeline:running_deps_failed",
            name="Test Pipeline Running Deps Failed",
            description="Pipeline for running job with failed dependencies",
            status=PipelineStatus.CREATED,
            correlation_id="test_running_deps_failed",
        )
        session.add(test_pipeline)
        session.flush()

        # Create dependency job that failed
        dependency_job = JobRun(
            job_type="dependency",
            job_function="dependency_function",
            status=JobStatus.FAILED,
            pipeline_id=test_pipeline.id,
            max_retries=3,
            retry_count=3,
            job_params={},
        )
        session.add(dependency_job)
        session.flush()

        # Create stalled job - use recent created_at to avoid double cleanup
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.RUNNING,
            pipeline_id=test_pipeline.id,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=PENDING_TIMEOUT_MINUTES - 5),
            started_at=datetime.now(timezone.utc) - timedelta(minutes=RUNNING_TIMEOUT_MINUTES + 10),
            finished_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.flush()

        # Create dependency relationship
        dependency = JobDependency(
            id=stalled_job.id,
            depends_on_job_id=dependency_job.id,
            dependency_type=DependencyType.SUCCESS_REQUIRED,
        )
        session.add(dependency)
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await cleanup_stalled_jobs(standalone_worker_context)

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Job should be in PENDING, not enqueued
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.PENDING
        assert stalled_job.retry_count == 1

    async def test_cleanup_integration_stalled_pending_pipeline_job_dependencies_failed(
        self, standalone_worker_context, session
    ):
        """Integration test: stalled pipeline PENDING job with failed dependencies is skipped."""
        test_pipeline = Pipeline(
            urn="test:pipeline:pending_deps_failed",
            name="Test Pipeline Pending Deps Failed",
            description="Pipeline for pending job with failed dependencies",
            status=PipelineStatus.CREATED,
            correlation_id="test_pending_deps_failed",
        )
        session.add(test_pipeline)
        session.flush()

        # Create dependency job that failed
        dependency_job = JobRun(
            job_type="dependency",
            job_function="dependency_function",
            status=JobStatus.FAILED,
            pipeline_id=test_pipeline.id,
            max_retries=3,
            retry_count=3,
            job_params={},
        )
        session.add(dependency_job)
        session.flush()

        # Create stalled job
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.PENDING,
            pipeline_id=test_pipeline.id,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=PENDING_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.flush()

        # Create dependency relationship
        dependency = JobDependency(
            id=stalled_job.id,
            depends_on_job_id=dependency_job.id,
            dependency_type=DependencyType.SUCCESS_REQUIRED,
        )
        session.add(dependency)
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await cleanup_stalled_jobs(standalone_worker_context)

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Job should remain in PENDING, not enqueued
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.PENDING
        assert stalled_job.retry_count == 1

    async def test_cleanup_integration_stalled_running_pipeline_job_dependencies_not_ready(
        self, standalone_worker_context, session
    ):
        """Integration test: stalled pipeline RUNNING job with dependencies not ready is skipped."""
        test_pipeline = Pipeline(
            urn="test:pipeline:running_deps_not_ready",
            name="Test Pipeline Running Deps Not Ready",
            description="Pipeline for running job with dependencies not ready",
            status=PipelineStatus.CREATED,
            correlation_id="test_running_deps_not_ready",
        )
        session.add(test_pipeline)
        session.flush()

        # Create dependency job still running
        dependency_job = JobRun(
            job_type="dependency",
            job_function="dependency_function",
            status=JobStatus.RUNNING,
            pipeline_id=test_pipeline.id,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(dependency_job)
        session.flush()

        # Create stalled job - use recent created_at to avoid double cleanup
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.RUNNING,
            pipeline_id=test_pipeline.id,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=PENDING_TIMEOUT_MINUTES - 5),
            started_at=datetime.now(timezone.utc) - timedelta(minutes=RUNNING_TIMEOUT_MINUTES + 10),
            finished_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.flush()

        # Create dependency relationship
        dependency = JobDependency(
            id=stalled_job.id,
            depends_on_job_id=dependency_job.id,
            dependency_type=DependencyType.SUCCESS_REQUIRED,
        )
        session.add(dependency)
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await cleanup_stalled_jobs(standalone_worker_context)

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Job should be in PENDING, waiting for dependencies
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.PENDING
        assert stalled_job.retry_count == 1

    async def test_cleanup_integration_stalled_pending_pipeline_job_dependencies_not_ready(
        self, standalone_worker_context, session
    ):
        """Integration test: stalled pipeline PENDING job with dependencies not ready is skipped."""
        test_pipeline = Pipeline(
            urn="test:pipeline:pending_deps_not_ready",
            name="Test Pipeline Pending Deps Not Ready",
            description="Pipeline for pending job with dependencies not ready",
            status=PipelineStatus.CREATED,
            correlation_id="test_pending_deps_not_ready",
        )
        session.add(test_pipeline)
        session.flush()

        # Create dependency job still running
        dependency_job = JobRun(
            job_type="dependency",
            job_function="dependency_function",
            status=JobStatus.RUNNING,
            pipeline_id=test_pipeline.id,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(dependency_job)
        session.flush()

        # Create stalled job
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.PENDING,
            pipeline_id=test_pipeline.id,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=PENDING_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.flush()

        # Create dependency relationship
        dependency = JobDependency(
            id=stalled_job.id,
            depends_on_job_id=dependency_job.id,
            dependency_type=DependencyType.SUCCESS_REQUIRED,
        )
        session.add(dependency)
        session.commit()

        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await cleanup_stalled_jobs(standalone_worker_context)

        assert result["status"] == "ok"
        assert result["data"]["total_cleaned"] == 1

        # Job should remain in PENDING, waiting for dependencies
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.PENDING
        assert stalled_job.retry_count == 1


############################################################################################################################################
# ARQ Integration Tests
############################################################################################################################################


@pytest.mark.asyncio
@pytest.mark.integration
class TestCleanupStalledJobsArqIntegration:
    """Integration tests for cleanup_stalled_jobs using ARQ worker."""

    async def test_cleanup_arq_integration(self, arq_redis, arq_worker, standalone_worker_context, session):
        """Integration test: cleanup_stalled_jobs runs via ARQ worker."""
        # Create a stalled job
        stalled_job = JobRun(
            job_type="test_job",
            job_function="test_function",
            status=JobStatus.QUEUED,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=QUEUED_TIMEOUT_MINUTES + 5),
            started_at=None,
            max_retries=3,
            retry_count=0,
            job_params={},
        )
        session.add(stalled_job)
        session.commit()

        # Enqueue cleanup job via ARQ
        await arq_redis.enqueue_job("cleanup_stalled_jobs")

        # Run the worker (just cleanup_stalled_jobs, not the retried test_function)
        await arq_worker.async_run()
        # Don't call run_check() - the retried test_function doesn't exist and would fail

        # Verify the cleanup job succeeded
        cleanup_job = session.execute(
            select(JobRun).where(JobRun.job_function == "cleanup_stalled_jobs")
        ).scalar_one_or_none()

        assert cleanup_job is not None
        assert cleanup_job.status == JobStatus.SUCCEEDED
        assert cleanup_job.job_type == "cron_job"

        # Verify the stalled job was cleaned up
        session.refresh(stalled_job)
        assert stalled_job.status == JobStatus.QUEUED  # Jobs are enqueued after retry
        assert stalled_job.retry_count == 1

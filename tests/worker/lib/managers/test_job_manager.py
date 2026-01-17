# ruff: noqa: E402
"""
Comprehensive test suite for JobManager class.

Tests cover all aspects of job lifecycle management, pipeline coordination,
error handling, and database interactions.
"""

import pytest

pytest.importorskip("arq")

import re
from unittest.mock import Mock, PropertyMock, patch

from arq import ArqRedis
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.models.enums.job_pipeline import FailureCategory, JobStatus
from mavedb.models.job_run import JobRun
from mavedb.worker.lib.managers.constants import (
    CANCELLED_JOB_STATUSES,
    RETRYABLE_FAILURE_CATEGORIES,
    RETRYABLE_JOB_STATUSES,
    STARTABLE_JOB_STATUSES,
    TERMINAL_JOB_STATUSES,
)
from mavedb.worker.lib.managers.exceptions import (
    DatabaseConnectionError,
    JobStateError,
    JobTransitionError,
)
from mavedb.worker.lib.managers.job_manager import JobManager
from tests.helpers.transaction_spy import TransactionSpy

HANDLED_EXCEPTIONS_DURING_OBJECT_MANIPULATION = (
    AttributeError("Mock attribute error"),
    KeyError("Mock key error"),
    TypeError("Mock type error"),
    ValueError("Mock value error"),
)


@pytest.mark.integration
class TestJobManagerInitialization:
    """Test JobManager initialization and setup."""

    def test_init_with_valid_job(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test successful initialization with valid job ID."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        assert manager.db == session
        assert manager.job_id == sample_job_run.id
        assert manager.pipeline_id == sample_job_run.pipeline_id

    def test_init_with_no_pipeline(self, session, arq_redis, setup_worker_db, sample_independent_job_run):
        """Test initialization with job that has no pipeline."""
        manager = JobManager(session, arq_redis, sample_independent_job_run.id)

        assert manager.job_id == sample_independent_job_run.id
        assert manager.pipeline_id is None

    def test_init_with_invalid_job_id(self, session, arq_redis):
        """Test initialization failure with non-existent job ID."""
        job_id = 999  # Assuming this ID does not exist
        with pytest.raises(DatabaseConnectionError, match=f"Failed to fetch job {job_id}"):
            JobManager(session, arq_redis, job_id)


@pytest.mark.unit
class TestJobStartUnit:
    """Unit tests for job start lifecycle management."""

    @pytest.mark.parametrize(
        "invalid_status",
        [status for status in JobStatus._member_map_.values() if status not in STARTABLE_JOB_STATUSES],
    )
    def test_start_job_raises_job_transition_error_when_managed_job_has_unstartable_status(
        self, mock_job_manager, invalid_status, mock_job_run
    ):
        # Set initial job status to an invalid (unstartable) status.
        mock_job_run.status = invalid_status

        # Start job. Verify a JobTransitionError is raised due to invalid state in the mocked
        # job run. Spy on the transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with (
            pytest.raises(
                JobTransitionError,
                match=f"Cannot start job {mock_job_manager.job_id} from status {invalid_status}",
            ),
            TransactionSpy.spy(mock_job_manager.db),
        ):
            mock_job_manager.start_job()

        # Verify job state on the mocked object remains unchanged.
        assert mock_job_run.status == invalid_status
        assert mock_job_run.started_at is None
        assert mock_job_run.progress_message is None

    @pytest.mark.parametrize(
        "exception",
        HANDLED_EXCEPTIONS_DURING_OBJECT_MANIPULATION,
    )
    @pytest.mark.parametrize(
        "valid_status",
        [status for status in JobStatus._member_map_.values() if status in STARTABLE_JOB_STATUSES],
    )
    def test_start_job_raises_job_state_error_when_handled_error_is_raised_during_object_manipulation(
        self, mock_job_manager, exception, mock_job_run, valid_status
    ):
        """Test job start failure due to exception during job object manipulation."""
        # Set initial job status to a valid status. Job status must be startable for this test.
        mock_job_run.status = valid_status

        # Trigger: If any attribute access occurs on job, raise exception. If no access, return QUEUED.
        def get_or_error(*args):
            if args:
                raise exception
            return valid_status

        # Start job. Verify a JobStateError is raised by our trigger.
        # Spy on the transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with (
            TransactionSpy.spy(mock_job_manager.db),
            pytest.raises(JobStateError, match="Failed to update job start state"),
        ):
            type(mock_job_run).status = PropertyMock(side_effect=get_or_error)
            mock_job_manager.start_job()

        # Verify job state on the mocked object remains unchanged. Although it's theoretically
        # possible some job state is manipulated prior to an error being raised, our specific
        # trigger should prevent any changes from being made.
        assert mock_job_run.status == valid_status
        assert mock_job_run.started_at is None
        assert mock_job_run.progress_message is None

    @pytest.mark.parametrize(
        "valid_status",
        [status for status in JobStatus._member_map_.values() if status in STARTABLE_JOB_STATUSES],
    )
    def test_start_job_success(self, mock_job_manager, mock_job_run, valid_status):
        """Test successful job start."""
        # Set initial job status to a valid status. Job status must be startable for this test.
        mock_job_run.status = valid_status

        # Start job. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(mock_job_manager.db):
            mock_job_manager.start_job()

        # Verify job state was updated on our mock object with expected values.
        # These changes would normally be persisted by the caller after this method returns.
        assert mock_job_run.status == JobStatus.RUNNING
        assert mock_job_run.started_at is not None
        assert mock_job_run.progress_message == "Job began execution"


@pytest.mark.integration
class TestJobStartIntegration:
    """Integration tests for job start lifecycle management."""

    @pytest.mark.parametrize(
        "invalid_status",
        [status for status in JobStatus._member_map_.values() if status not in STARTABLE_JOB_STATUSES],
    )
    def test_job_exception_is_raised_when_job_has_invalid_status(
        self, session, arq_redis, setup_worker_db, sample_job_run, invalid_status
    ):
        """Test job start failure due to invalid job status."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Manually set job to invalid status and commit changes.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        job.status = invalid_status
        session.commit()

        # Start job. Verify a JobTransitionError is raised due to the previously set invalid state.
        # Spy on the transaction to ensure nothing is flushed/rolled back/committed prematurely.
        # Although the job might still set some attributes before the error is raised, the exception
        # indicates to the caller that the job was not started successfully and the transaction should be rolled back.
        with (
            TransactionSpy.spy(manager.db),
            pytest.raises(
                JobTransitionError,
                match=f"Cannot start job {sample_job_run.id} from status {invalid_status.value}",
            ),
        ):
            manager.start_job()

    @pytest.mark.parametrize(
        "valid_status",
        [status for status in JobStatus._member_map_.values() if status in STARTABLE_JOB_STATUSES],
    )
    def test_job_updated_successfully(self, session, arq_redis, setup_worker_db, sample_job_run, valid_status):
        """Test successful job start."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Manually set job to invalid status and commit changes.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        job.status = valid_status
        session.commit()

        # Start job. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            manager.start_job()

        # Commit pending changes made by start job.
        session.commit()

        # Verify job state was updated in transaction with expected values.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.RUNNING
        assert job.started_at is not None
        assert job.progress_message == "Job began execution"


@pytest.mark.unit
class TestJobCompletionUnit:
    """Unit tests for job completion lifecycle management."""

    @pytest.mark.parametrize(
        "invalid_status",
        [status for status in JobStatus._member_map_.values() if status not in TERMINAL_JOB_STATUSES],
    )
    def test_complete_job_raises_job_transition_error_when_managed_job_has_non_terminal_status(
        self, mock_job_manager, mock_job_run, invalid_status
    ):
        # Set initial job status to an invalid (non-terminal) status.
        mock_job_run.status = invalid_status

        # Complete job. Verify a JobTransitionError is raised due to invalid state in the mocked
        # job run. Spy on the transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with (
            pytest.raises(
                JobTransitionError,
                match=re.escape(
                    f"Cannot commplete job to status: {invalid_status}. Must complete to a terminal status: {TERMINAL_JOB_STATUSES}"
                ),
            ),
            TransactionSpy.spy(mock_job_manager.db),
        ):
            mock_job_manager.complete_job(status=invalid_status, result={})

        # Verify job state on the mocked object remains unchanged.
        assert mock_job_run.status == invalid_status
        assert mock_job_run.finished_at is None
        assert mock_job_run.metadata_ == {}
        assert mock_job_run.progress_message is None
        assert mock_job_run.error_message is None
        assert mock_job_run.error_traceback is None
        assert mock_job_run.failure_category is None

    @pytest.mark.parametrize(
        "exception",
        HANDLED_EXCEPTIONS_DURING_OBJECT_MANIPULATION,
    )
    @pytest.mark.parametrize(
        "valid_status",
        [status for status in JobStatus._member_map_.values() if status in TERMINAL_JOB_STATUSES],
    )
    def test_complete_job_raises_job_state_error_when_handled_error_is_raised_during_object_manipulation(
        self, mock_job_manager, mock_job_run, exception, valid_status
    ):
        """Test job completion failure due to exception during job object manipulation."""
        # Trigger: If any attribute setting on job status, raise exception. If only accessing, return whatever the mock
        # objects original status was (starting job status doesn't matter for this test).
        base_status = mock_job_run.status

        def get_or_error(*args):
            if args:
                raise exception
            return base_status

        # Complete job. Verify a JobStateError is raised by our trigger.
        # Spy on the transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with (
            pytest.raises(JobStateError, match="Failed to update job completion state"),
            TransactionSpy.spy(mock_job_manager.db),
        ):
            type(mock_job_run).status = PropertyMock(side_effect=get_or_error)
            mock_job_manager.complete_job(status=valid_status, result={})

        # Verify job state on the mocked object remains unchanged. Although it's theoretically
        # possible some job state is manipulated prior to an error being raised, our specific
        # trigger should prevent any changes from being made.
        assert mock_job_run.status == base_status
        assert mock_job_run.finished_at is None
        assert mock_job_run.metadata_ == {}
        assert mock_job_run.progress_message is None
        assert mock_job_run.error_message is None
        assert mock_job_run.error_traceback is None
        assert mock_job_run.failure_category is None

    def test_complete_job_sets_default_failure_category_when_job_failed(self, mock_job_manager, mock_job_run):
        """Test job completion sets default failure category when job failed without error."""

        # Complete job. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(mock_job_manager.db):
            mock_job_manager.complete_job(status=JobStatus.FAILED, result={})

        # Verify job state was updated on our mock object with expected values.
        assert mock_job_run.status == JobStatus.FAILED
        assert mock_job_run.finished_at is not None
        assert mock_job_run.metadata_ == {"result": {}}
        assert mock_job_run.progress_message == "Job failed"
        assert mock_job_run.error_message is None
        assert mock_job_run.error_traceback is None
        assert mock_job_run.failure_category == FailureCategory.UNKNOWN

    @pytest.mark.parametrize(
        "valid_status",
        [status for status in JobStatus._member_map_.values() if status in TERMINAL_JOB_STATUSES],
    )
    @pytest.mark.parametrize(
        "exception",
        [ValueError("Test error"), None],
    )
    def test_complete_job_success(self, mock_job_manager, valid_status, exception, mock_job_run):
        """Test successful job completion."""

        # Complete job. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(mock_job_manager.db):
            mock_job_manager.complete_job(status=valid_status, result={"output": "test"}, error=exception)

        # Verify job state was updated on our mock object with expected values.
        assert mock_job_run.status == valid_status
        assert mock_job_run.finished_at is not None
        assert mock_job_run.metadata_["result"] == {"output": "test"}
        assert mock_job_run.progress_message is not None

        # If an exception was provided, verify error fields are set appropriately.
        if exception:
            assert mock_job_run.error_message == str(exception)
            assert mock_job_run.error_traceback is not None
            assert mock_job_run.failure_category == FailureCategory.UNKNOWN

        else:
            assert mock_job_run.error_message is None
            assert mock_job_run.error_traceback is None

            # Proper handling of failure category only applies to FAILED status. See
            # test_complete_job_sets_default_failure_category_when_job_failed for that case.


@pytest.mark.integration
class TestJobCompletionIntegration:
    """Test job completion lifecycle management."""

    @pytest.mark.parametrize(
        "invalid_status",
        [status for status in JobStatus._member_map_.values() if status not in TERMINAL_JOB_STATUSES],
    )
    def test_job_exception_is_raised_when_job_has_invalid_status(
        self, session, arq_redis, setup_worker_db, sample_job_run, invalid_status
    ):
        """Test job completion failure due to invalid job status."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Complete job. Verify a JobTransitionError is raised due to the passed invalid state.
        # Spy on the transaction to ensure nothing is flushed/rolled back/committed prematurely.
        # Although the job might still set some attributes before the error is raised, the exception
        # indicates to the caller that the job was not completed successfully and the transaction should be rolled back.
        with (
            TransactionSpy.spy(manager.db),
            pytest.raises(
                JobTransitionError,
                match=re.escape(
                    f"Cannot commplete job to status: {invalid_status}. Must complete to a terminal status: {TERMINAL_JOB_STATUSES}"
                ),
            ),
        ):
            manager.complete_job(status=invalid_status, result={"output": "test"})

    @pytest.mark.parametrize(
        "valid_status",
        [status for status in JobStatus._member_map_.values() if status in TERMINAL_JOB_STATUSES],
    )
    def test_job_updated_successfully_without_error(
        self, session, arq_redis, setup_worker_db, sample_job_run, valid_status
    ):
        """Test successful job completion."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Complete job. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            manager.complete_job(status=valid_status, result={"output": "test"})

        # Commit pending changes made by start job.
        session.flush()

        # Verify job state was updated in transaction with expected values.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()

        assert job.status == valid_status
        assert job.finished_at is not None
        assert job.metadata_ == {"result": {"output": "test"}}
        assert job.error_message is None
        assert job.error_traceback is None

        # For cases where no error is provided, verify failure category is set appropriately based
        # on status. We automatically set UNKNOWN for FAILED status if no error is given.
        if valid_status == JobStatus.FAILED:
            assert job.failure_category == FailureCategory.UNKNOWN
        else:
            assert job.failure_category is None

    @pytest.mark.parametrize(
        "valid_status",
        [status for status in JobStatus._member_map_.values() if status in TERMINAL_JOB_STATUSES],
    )
    def test_job_updated_successfully_with_error(
        self, session, arq_redis, setup_worker_db, sample_job_run, valid_status
    ):
        """Test successful job completion."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Complete job. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            manager.complete_job(status=valid_status, result={"output": "test"}, error=ValueError("Test error"))

        # Commit pending changes made by start job.
        session.flush()

        # Verify job state was updated in transaction with expected values.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()

        assert job.status == valid_status
        assert job.finished_at is not None
        assert job.metadata_ == {"result": {"output": "test"}}
        assert job.error_message == "Test error"
        assert job.error_traceback is not None
        assert job.failure_category == FailureCategory.UNKNOWN


@pytest.mark.unit
class TestJobFailureUnit:
    """Unit tests for job failure lifecycle management."""

    def test_fail_job_success(self, mock_job_manager, mock_job_run):
        """Test that fail_job calls complete_job with status=JobStatus.FAILED."""

        # Fail job with a test exception. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        # This convenience expects an exception to be provided. To fail a job without an exception, callers should use complete_job directly.
        test_exception = Exception("Test exception")
        with (
            patch.object(mock_job_manager, "complete_job", wraps=mock_job_manager.complete_job) as mock_complete_job,
            TransactionSpy.spy(mock_job_manager.db),
        ):
            mock_job_manager.fail_job(error=test_exception, result={"output": "test"})

        # Verify this function is a thin wrapper around complete_job with expected parameters.
        mock_complete_job.assert_called_once_with(
            status=JobStatus.FAILED, result={"output": "test"}, error=test_exception
        )

        # Verify job state was updated on our mock object with expected values.
        assert mock_job_run.status == JobStatus.FAILED
        assert mock_job_run.finished_at is not None
        assert mock_job_run.metadata_ == {"result": {"output": "test"}}
        assert mock_job_run.progress_message == "Job failed"
        assert mock_job_run.error_message == str(test_exception)
        assert mock_job_run.error_traceback is not None
        assert mock_job_run.failure_category == FailureCategory.UNKNOWN


class TestJobFailureIntegration:
    """Test job failure lifecycle management."""

    def test_job_updated_successfully(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test successful job failure."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Fail job. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            manager.fail_job(result={"output": "test"}, error=ValueError("Test error"))

        # Commit pending changes made by fail job.
        session.flush()

        # Verify job state was updated in transaction with expected values.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()

        assert job.status == JobStatus.FAILED
        assert job.finished_at is not None
        assert job.metadata_ == {"result": {"output": "test"}}
        assert job.progress_message == "Job failed"
        assert job.error_message == "Test error"
        assert job.error_traceback is not None
        assert job.failure_category == FailureCategory.UNKNOWN


@pytest.mark.unit
class TestJobSuccessUnit:
    """Unit tests for job success lifecycle management."""

    def test_succeed_job_success(self, mock_job_manager, mock_job_run):
        """Test that succeed_job calls complete_job with status=JobStatus.SUCCEEDED."""

        # Succeed job. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with (
            patch.object(mock_job_manager, "complete_job", wraps=mock_job_manager.complete_job) as mock_complete_job,
            TransactionSpy.spy(mock_job_manager.db),
        ):
            mock_job_manager.succeed_job(result={"output": "test"})

        # Verify this function is a thin wrapper around complete_job with expected parameters.
        mock_complete_job.assert_called_once_with(status=JobStatus.SUCCEEDED, result={"output": "test"})

        # Verify job state was updated on our mock object with expected values.
        assert mock_job_run.status == JobStatus.SUCCEEDED
        assert mock_job_run.finished_at is not None
        assert mock_job_run.metadata_ == {"result": {"output": "test"}}
        assert mock_job_run.progress_message == "Job completed successfully"
        assert mock_job_run.error_message is None
        assert mock_job_run.error_traceback is None
        assert mock_job_run.failure_category is None


class TestJobSuccessIntegration:
    """Test job success lifecycle management."""

    def test_job_updated_successfully(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test successful job succeeding."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Complete job. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            manager.succeed_job(result={"output": "test"})

        # Commit pending changes made by start job.
        session.flush()

        # Verify job state was updated in transaction with expected values.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()

        assert job.status == JobStatus.SUCCEEDED
        assert job.finished_at is not None
        assert job.progress_message == "Job completed successfully"
        assert job.metadata_ == {"result": {"output": "test"}}
        assert job.error_message is None
        assert job.error_traceback is None
        assert job.failure_category is None


@pytest.mark.unit
class TestJobCancellationUnit:
    """Unit tests for job cancellation lifecycle management."""

    def test_cancel_job_success(self, mock_job_manager, mock_job_run):
        """Test that cancel_job calls complete_job with status=JobStatus.CANCELLED."""

        # Cancel job. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with (
            patch.object(mock_job_manager, "complete_job", wraps=mock_job_manager.complete_job) as mock_complete_job,
            TransactionSpy.spy(mock_job_manager.db),
        ):
            mock_job_manager.cancel_job(result={"error": "Job was cancelled"})

        # Verify this function is a thin wrapper around complete_job with expected parameters.
        mock_complete_job.assert_called_once_with(status=JobStatus.CANCELLED, result={"error": "Job was cancelled"})

        # Verify job state was updated on our mock object with expected values.
        assert mock_job_run.status == JobStatus.CANCELLED
        assert mock_job_run.finished_at is not None
        assert mock_job_run.metadata_ == {"result": {"error": "Job was cancelled"}}
        assert mock_job_run.progress_message == "Job cancelled"
        assert mock_job_run.error_message is None
        assert mock_job_run.error_traceback is None
        assert mock_job_run.failure_category is None


class TestJobCancellationIntegration:
    """Test job cancellation lifecycle management."""

    def test_job_updated_successfully(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test successful job cancellation."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Complete job. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            manager.cancel_job(result={"output": "test"})

        # Commit pending changes made by start job.
        session.flush()

        # Verify job state was updated in transaction with expected values.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()

        assert job.status == JobStatus.CANCELLED
        assert job.progress_message == "Job cancelled"
        assert job.finished_at is not None
        assert job.metadata_ == {"result": {"output": "test"}}
        assert job.error_message is None
        assert job.error_traceback is None
        assert job.failure_category is None


@pytest.mark.unit
class TestJobSkipUnit:
    """Unit tests for job skip lifecycle management."""

    def test_skip_job_success(self, mock_job_manager, mock_job_run):
        """Test that skip_job calls complete_job with status=JobStatus.SKIPPED."""

        # Skip job. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with (
            patch.object(mock_job_manager, "complete_job", wraps=mock_job_manager.complete_job) as mock_complete_job,
            TransactionSpy.spy(mock_job_manager.db),
        ):
            mock_job_manager.skip_job(result={"output": "test"})

        # Verify this function is a thin wrapper around complete_job with expected parameters.
        mock_complete_job.assert_called_once_with(status=JobStatus.SKIPPED, result={"output": "test"})

        # Verify job state was updated on our mock object with expected values.
        assert mock_job_run.status == JobStatus.SKIPPED
        assert mock_job_run.finished_at is not None
        assert mock_job_run.metadata_ == {"result": {"output": "test"}}
        assert mock_job_run.progress_message == "Job skipped"
        assert mock_job_run.error_message is None
        assert mock_job_run.error_traceback is None
        assert mock_job_run.failure_category is None


@pytest.mark.integration
class TestJobSkipIntegration:
    """Test job skip lifecycle management."""

    def test_job_updated_successfully(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test successful job skipping."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Skip job. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            manager.skip_job(result={"output": "test"})

        # Commit pending changes made by start job.
        session.flush()

        # Verify job state was updated in transaction with expected values.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()

        assert job.status == JobStatus.SKIPPED
        assert job.progress_message == "Job skipped"
        assert job.finished_at is not None
        assert job.metadata_ == {"result": {"output": "test"}}
        assert job.error_message is None
        assert job.error_traceback is None
        assert job.failure_category is None


@pytest.mark.unit
class TestPrepareRetryUnit:
    """Unit tests for job retry lifecycle management."""

    @pytest.mark.parametrize(
        "invalid_status",
        [status for status in JobStatus._member_map_.values() if status not in RETRYABLE_JOB_STATUSES],
    )
    def test_prepare_retry_raises_job_transition_error_when_managed_job_has_unretryable_status(
        self, mock_job_manager, invalid_status, mock_job_run
    ):
        # Set initial job status to an invalid (unretryable) status.
        mock_job_run.status = invalid_status

        # Preprare retry job. Verify a JobTransitionError is raised due to invalid state in the mocked
        # job run. Spy on the transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with (
            pytest.raises(
                JobTransitionError,
                match=re.escape(f"Cannot retry job {mock_job_manager.job_id} due to invalid state ({invalid_status})"),
            ),
            TransactionSpy.spy(mock_job_manager.db),
        ):
            mock_job_manager.prepare_retry()

        # Verify job state on the mocked object remains unchanged.
        assert mock_job_run.status == invalid_status
        assert mock_job_run.retry_count == 0
        assert mock_job_run.started_at is None
        assert mock_job_run.progress_message is None
        assert mock_job_run.error_message is None
        assert mock_job_run.error_traceback is None
        assert mock_job_run.failure_category is None
        assert mock_job_run.finished_at is None
        assert mock_job_run.metadata_ == {}

    @pytest.mark.parametrize(
        "exception",
        HANDLED_EXCEPTIONS_DURING_OBJECT_MANIPULATION,
    )
    def test_prepare_retry_raises_job_state_error_when_handled_error_is_raised_during_object_manipulation(
        self, mock_job_manager, exception, mock_job_run
    ):
        """Test job prepare retry failure due to exception during job object manipulation."""
        # Set initial job status to FAILED. Job status must be retryable for this test.
        initial_status = JobStatus.FAILED
        mock_job_run.status = initial_status

        # Trigger: If any attribute access occurs on job, raise exception. If no access, return FAILED.
        def get_or_error(*args):
            if args:
                raise exception
            return initial_status

        # Prepare retry. Verify a JobStateError is raised by our trigger.
        # Spy on the transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with (
            TransactionSpy.spy(mock_job_manager.db),
            pytest.raises(
                JobStateError,
                match="Failed to update job retry state",
            ),
        ):
            type(mock_job_run).status = PropertyMock(side_effect=get_or_error)
            mock_job_manager.prepare_retry()

        # Verify job state on the mocked object remains unchanged. Although it's theoretically
        # possible some job state is manipulated prior to an error being raised, our specific
        # trigger should prevent any changes from being made.
        assert mock_job_run.status == JobStatus.FAILED
        assert mock_job_run.retry_count == 0
        assert mock_job_run.started_at is None
        assert mock_job_run.progress_message is None
        assert mock_job_run.error_message is None
        assert mock_job_run.error_traceback is None
        assert mock_job_run.failure_category is None
        assert mock_job_run.finished_at is None
        assert mock_job_run.metadata_ == {}

    def test_prepare_retry_success(self, mock_job_manager, mock_job_run):
        """Test successful job prepare retry."""
        # Set initial job status to FAILED. Job status must be retryable for this test.
        mock_job_run.status = JobStatus.FAILED

        # Prepare retry. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        # Mock the flag_modified function: mock objects don't have _sa_instance_state attribute required by SQLAlchemy
        # funcs and it's easier to mock the functions that manipulate the state than to fully mock the state itself.
        with (
            patch("mavedb.worker.lib.managers.job_manager.flag_modified") as mock_flag_modified,
            TransactionSpy.spy(mock_job_manager.db),
        ):
            mock_job_manager.prepare_retry()

        # Verify flag_modified was called for metadata_ field.
        mock_flag_modified.assert_called_once_with(mock_job_run, "metadata_")

        # Verify job state was updated on our mock object with expected values.
        # These changes would normally be persisted by the caller after this method returns.
        assert mock_job_run.status == JobStatus.PENDING
        assert mock_job_run.retry_count == 1
        assert mock_job_run.progress_message == "Job retry prepared"
        assert mock_job_run.error_message is None
        assert mock_job_run.error_traceback is None
        assert mock_job_run.failure_category is None
        assert mock_job_run.finished_at is None
        assert mock_job_run.metadata_["retry_history"] is not None
        assert mock_job_run.started_at is None
        assert mock_job_run.metadata_.get("result") is None


@pytest.mark.integration
class TestPrepareRetryIntegration:
    """Test job retry lifecycle management."""

    @pytest.mark.parametrize(
        "job_status",
        [status for status in JobStatus._member_map_.values() if status not in RETRYABLE_JOB_STATUSES],
    )
    def test_prepare_retry_failed_due_to_invalid_status(
        self, session, arq_redis, setup_worker_db, sample_job_run, job_status
    ):
        """Test job retry failure due to invalid job status."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Update job to non-failed state
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        job.status = job_status
        session.commit()

        # Prepare retry job. Verify a JobTransitionError is raised due to the passed invalid state.
        # Spy on the transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with (
            TransactionSpy.spy(manager.db),
            pytest.raises(JobTransitionError, match=f"Cannot retry job {job.id} due to invalid state \({job.status}\)"),
        ):
            manager.prepare_retry()

    def test_prepare_retry_success(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test successful job retry."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Manually set job to FAILED status and commit changes.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        job.status = JobStatus.FAILED
        session.commit()

        # Prepare retry. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            manager.prepare_retry()

        # Commit pending changes made by start job.
        session.commit()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING
        assert job.retry_count == 1
        assert job.progress_message == "Job retry prepared"
        assert job.error_message is None
        assert job.error_traceback is None
        assert job.failure_category is None
        assert job.finished_at is None
        assert job.metadata_["retry_history"] is not None


@pytest.mark.unit
class TestPrepareQueueUnit:
    """Unit tests for job prepare for queue lifecycle management."""

    @pytest.mark.parametrize(
        "invalid_status",
        [status for status in JobStatus._member_map_.values() if status != JobStatus.PENDING],
    )
    def test_prepare_queue_raises_job_transition_error_when_managed_job_has_unretryable_status(
        self, mock_job_manager, invalid_status, mock_job_run
    ):
        """Test job prepare queue failure due to invalid job status."""
        # Set initial job status to an invalid (non-pending) status.
        mock_job_run.status = invalid_status

        # Prepare queue job. Verify a JobTransitionError is raised due to invalid state in the mocked
        # job run. Spy on the transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with (
            pytest.raises(
                JobTransitionError,
                match=re.escape(f"Cannot queue job {mock_job_manager.job_id} from status {invalid_status}"),
            ),
            TransactionSpy.spy(mock_job_manager.db),
        ):
            mock_job_manager.prepare_queue()

        # Verify job state on the mocked object remains unchanged.
        assert mock_job_run.status == invalid_status
        assert mock_job_run.progress_message is None

    @pytest.mark.parametrize(
        "exception",
        HANDLED_EXCEPTIONS_DURING_OBJECT_MANIPULATION,
    )
    def test_prepare_queue_raises_job_state_error_when_handled_error_is_raised_during_object_manipulation(
        self, mock_job_manager, exception, mock_job_run
    ):
        """Test job prepare queue failure due to exception during job object manipulation."""
        # Set initial job status to PENDING. Job status must be valid for this test.
        initial_status = JobStatus.PENDING
        mock_job_run.status = initial_status

        # Trigger: If any attribute access occurs on job, raise exception. If no access, return FAILED.
        def get_or_error(*args):
            if args:
                raise exception
            return initial_status

        # Prepare queue. Verify a JobStateError is raised by our trigger.
        # Spy on the transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with (
            TransactionSpy.spy(mock_job_manager.db),
            pytest.raises(
                JobStateError,
                match="Failed to update job queue state",
            ),
        ):
            type(mock_job_run).status = PropertyMock(side_effect=get_or_error)
            mock_job_manager.prepare_queue()

        # Verify job state on the mocked object remains unchanged. Although it's theoretically
        # possible some job state is manipulated prior to an error being raised, our specific
        # trigger should prevent any changes from being made.
        assert mock_job_run.status == JobStatus.PENDING
        assert mock_job_run.progress_message is None

    def test_prepare_queue_success(self, mock_job_manager, mock_job_run):
        """Test successful job prepare queue."""
        # Set initial job status to PENDING. Job status must be valid for this test.
        mock_job_run.status = JobStatus.PENDING

        # Prepare queue. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        # Mock the flag_modified function: mock objects don't have _sa_instance_state attribute required by SQLAlchemy
        # funcs and it's easier to mock the functions that manipulate the state than to fully mock the state itself.
        with (
            patch.object(mock_job_manager, "get_job", return_value=mock_job_run),
            TransactionSpy.spy(mock_job_manager.db),
        ):
            mock_job_manager.prepare_queue()

        # Verify job state was updated on our mock object with expected values.
        # These changes would normally be persisted by the caller after this method returns.
        assert mock_job_run.status == JobStatus.QUEUED
        assert mock_job_run.progress_message == "Job queued for execution"


@pytest.mark.integration
class TestPrepareQueue:
    """Test job prepare for queue lifecycle management."""

    @pytest.mark.parametrize(
        "job_status",
        [status for status in JobStatus._member_map_.values() if status != JobStatus.PENDING],
    )
    def test_prepare_queue_failed_due_to_invalid_status(
        self, session, arq_redis, setup_worker_db, sample_job_run, job_status
    ):
        """Test job prepare for queue failure due to invalid job status."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Update job to invalid state
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        job.status = job_status
        session.flush()

        # Prepare queue job. Verify a JobTransitionError is raised due to the passed invalid state.
        # Spy on the transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with (
            TransactionSpy.spy(manager.db),
            pytest.raises(
                JobTransitionError,
                match=f"Cannot queue job {job.id} from status {job.status}",
            ),
        ):
            manager.prepare_queue()

    def test_prepare_queue_success(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test successful job prepare for queue."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Sample run should be in PENDING state from fixture setup, but verify to be sure.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING, "Sample job run must be in PENDING state for this test."

        # Prepare queue. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            manager.prepare_queue()

        # Commit pending changes made by start job.
        session.flush()

        # Verify job state was updated in transaction with expected values.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED
        assert job.progress_message == "Job queued for execution"


@pytest.mark.unit
class TestResetJobUnit:
    """Unit tests for job reset lifecycle management."""

    @pytest.mark.parametrize(
        "exception",
        HANDLED_EXCEPTIONS_DURING_OBJECT_MANIPULATION,
    )
    def test_reset_job_raises_job_state_error_when_handled_error_is_raised_during_object_manipulation(
        self, mock_job_manager, exception, mock_job_run
    ):
        """Test job reset job failure due to exception during job object manipulation."""

        # Trigger: If any attribute setting occurs on job, raise exception. Otherwise return FAILED.
        # Set initial job status to FAILED. Job status is unimportant for this test (all statuses are resettable).
        initial_status = JobStatus.FAILED
        mock_job_run.status = initial_status

        def get_or_error(*args):
            if args:
                raise exception
            return initial_status

        # Prepare queue. Verify a JobStateError is raised by our trigger.
        # Spy on the transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with (
            TransactionSpy.spy(mock_job_manager.db),
            pytest.raises(
                JobStateError,
                match="Failed to reset job state",
            ),
        ):
            type(mock_job_run).status = PropertyMock(side_effect=get_or_error)
            mock_job_manager.reset_job()

        # Verify job state on the mocked object remains unchanged. Although it's theoretically
        # possible some job state is manipulated prior to an error being raised, our specific
        # trigger should prevent any changes from being made.
        assert mock_job_run.status == JobStatus.FAILED
        assert mock_job_run.started_at is None
        assert mock_job_run.finished_at is None
        assert mock_job_run.progress_current is None
        assert mock_job_run.progress_total is None
        assert mock_job_run.progress_message is None
        assert mock_job_run.error_message is None
        assert mock_job_run.error_traceback is None
        assert mock_job_run.failure_category is None
        assert mock_job_run.retry_count == 0
        assert mock_job_run.metadata_ == {}

    def test_reset_job_success(self, mock_job_manager, mock_job_run):
        """Test successful job reset."""
        # Set initial job status to provided status. All statuses are resettable, so the actual status is not important.
        mock_job_run.status = JobStatus.FAILED

        # Prepare queue. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(mock_job_manager.db):
            mock_job_manager.reset_job()

        # Verify job state was updated on our mock object with expected values.
        # These changes would normally be persisted by the caller after this method returns.
        assert mock_job_run.status == JobStatus.PENDING
        assert mock_job_run.started_at is None
        assert mock_job_run.finished_at is None
        assert mock_job_run.progress_current is None
        assert mock_job_run.progress_total is None
        assert mock_job_run.progress_message is None
        assert mock_job_run.error_message is None
        assert mock_job_run.error_traceback is None
        assert mock_job_run.failure_category is None
        assert mock_job_run.retry_count == 0
        assert mock_job_run.metadata_ == {}


@pytest.mark.integration
class TestResetJobIntegration:
    """Test job reset lifecycle management."""

    def test_reset_job_success(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test successful job reset."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Manually set job to a non-pending status and set various fields to non-default values.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        job.status = JobStatus.FAILED
        job.started_at = "2023-12-31T23:59:59Z"
        job.finished_at = "2024-01-01T00:00:00Z"
        job.progress_current = 50
        job.progress_total = 100
        job.progress_message = "Halfway done"
        job.error_message = "Test error message"
        job.error_traceback = "Test error traceback"
        job.failure_category = FailureCategory.UNKNOWN
        job.retry_count = 2
        job.metadata_ = {"result": {}, "retry_history": [{"attempt": 1}, {"attempt": 2}]}
        session.commit()

        # Reset job. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            manager.reset_job()

        # Commit pending changes made by reset job.
        session.commit()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING
        assert job.progress_current is None
        assert job.progress_total is None
        assert job.progress_message is None
        assert job.error_message is None
        assert job.error_traceback is None
        assert job.failure_category is None
        assert job.started_at is None
        assert job.finished_at is None
        assert job.retry_count == 0
        assert job.metadata_.get("retry_history") is None


@pytest.mark.unit
class TestJobProgressUpdateUnit:
    """Unit tests for job progress update lifecycle management."""

    @pytest.mark.parametrize(
        "exception",
        HANDLED_EXCEPTIONS_DURING_OBJECT_MANIPULATION,
    )
    def test_update_progress_raises_job_state_error_when_handled_error_is_raised_during_object_manipulation(
        self, mock_job_manager, exception, mock_job_run
    ):
        """Test job progress update failure due to exception during job object manipulation."""
        # Trigger: If any attribute setting occurs on job progress, raise exception. If only access, return initial progress.
        initial_progress_current = mock_job_run.progress_current

        def get_or_error(*args):
            if args:
                raise exception
            return initial_progress_current

        # Prepare queue. Verify a JobStateError is raised by our trigger.
        # Spy on the transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with (
            TransactionSpy.spy(mock_job_manager.db),
            pytest.raises(
                JobStateError,
                match="Failed to update job progress",
            ),
        ):
            type(mock_job_run).progress_current = PropertyMock(side_effect=get_or_error)
            mock_job_manager.update_progress(50, 100, "Halfway done")

        # Verify job state on the mocked object remains unchanged.
        assert mock_job_run.progress_current is None
        assert mock_job_run.progress_total is None
        assert mock_job_run.progress_message is None

    def test_update_progress_success(self, mock_job_manager, mock_job_run):
        """Test successful job progress update."""

        # Update progress. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(mock_job_manager.db):
            mock_job_manager.update_progress(50, 100, "Halfway done")

        # Verify job state was updated on our mock object with expected values.
        # These changes would normally be persisted by the caller after this method returns.
        assert mock_job_run.progress_current == 50
        assert mock_job_run.progress_total == 100
        assert mock_job_run.progress_message == "Halfway done"

    def test_update_progress_does_not_overwrite_old_message_when_no_new_message_is_provided(
        self, mock_job_manager, mock_job_run
    ):
        """Test successful job progress update without message."""

        # Set initial progress message to verify it is not overwritten.
        mock_job_run.progress_message = "Old message"

        # Update progress without message. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(mock_job_manager.db):
            mock_job_manager.update_progress(75, 200)

        # Verify job state was updated on our mock object with expected values.
        # These changes would normally be persisted by the caller after this method returns.
        assert mock_job_run.progress_current == 75
        assert mock_job_run.progress_total == 200
        assert mock_job_run.progress_message == "Old message"  # Message should remain unchanged from initial set.


@pytest.mark.integration
class TestJobProgressUpdateIntegration:
    """Test job progress update lifecycle management."""

    def test_update_progress_success(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test successful progress update."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Set initial progress to None to verify update.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        job.progress_current = None
        job.progress_total = None
        job.progress_message = None
        session.commit()

        # Update progress. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            manager.update_progress(50, 100, "Halfway done")

        # Commit pending changes made by update progress.
        session.commit()

        # Verify job state was updated in transaction with expected values.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.progress_current == 50
        assert job.progress_total == 100
        assert job.progress_message == "Halfway done"

    def test_update_progress_success_does_not_overwrite_old_message_when_no_new_message_is_provided(
        self, session, arq_redis, setup_worker_db, sample_job_run
    ):
        """Test successful progress update without message."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Set initial progress to None to verify update.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        job.progress_current = None
        job.progress_total = None
        job.progress_message = "Old message"
        session.commit()

        # Update progress without message. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            manager.update_progress(75, 200)

        # Commit pending changes made by update progress.
        session.flush()

        # Verify job state was updated in transaction with expected values.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.progress_current == 75
        assert job.progress_total == 200
        assert job.progress_message == "Old message"  # Message should remain unchanged from initial set.


@pytest.mark.unit
class TestJobProgressStatusUpdateUnit:
    """Unit tests for job progress status update lifecycle management."""

    @pytest.mark.parametrize(
        "exception",
        HANDLED_EXCEPTIONS_DURING_OBJECT_MANIPULATION,
    )
    def test_update_status_message_raises_job_state_error_when_handled_error_is_raised_during_object_manipulation(
        self, mock_job_manager, exception, mock_job_run
    ):
        """Test job status message update failure due to exception during job object manipulation."""
        # Trigger: If any attribute setting occurs on job progress message, raise exception. If only access, return initial message.
        initial_progress_message = mock_job_run.progress_message

        def get_or_error(*args):
            if args:
                raise exception
            return initial_progress_message

        # Prepare queue. Verify a JobStateError is raised by our trigger.
        # Spy on the transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with (
            TransactionSpy.spy(mock_job_manager.db),
            pytest.raises(
                JobStateError,
                match="Failed to update job status message",
            ),
        ):
            type(mock_job_run).progress_message = PropertyMock(side_effect=get_or_error)
            mock_job_manager.update_status_message("New status message")

        # Verify job state on the mocked object remains unchanged.
        assert mock_job_run.progress_message == initial_progress_message

    def test_update_status_message_success(self, mock_job_manager, mock_job_run):
        """Test successful job status message update."""

        # Update status message. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(mock_job_manager.db):
            mock_job_manager.update_status_message("New status message")

        # Verify job state was updated on our mock object with expected values.
        # These changes would normally be persisted by the caller after this method returns.
        assert mock_job_run.progress_message == "New status message"


@pytest.mark.integration
class TestJobProgressStatusUpdate:
    """Test job progress status update lifecycle management."""

    def test_update_status_message_success(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test successful status message update."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Set initial progress message to verify update.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        job.progress_message = "Old status message"
        session.commit()

        # Update status message. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            manager.update_status_message("New status message")

        # Commit pending changes made by update status message.
        session.commit()

        # Verify job state was updated in transaction with expected values.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.progress_message == "New status message"


@pytest.mark.unit
class TestJobProgressIncrementationUnit:
    """Unit tests for job progress incrementation lifecycle management."""

    @pytest.mark.parametrize(
        "exception",
        HANDLED_EXCEPTIONS_DURING_OBJECT_MANIPULATION,
    )
    def test_increment_progress_raises_job_state_error_when_handled_error_is_raised_during_object_manipulation(
        self, mock_job_manager, exception, mock_job_run
    ):
        """Test job progress incrementation failure due to exception during job object manipulation."""
        # Trigger: If any attribute access occurs on job progress, raise exception. If no access, return initial progress.
        initial_progress_current = mock_job_run.progress_current

        def get_or_error(*args):
            if args:
                raise exception
            return initial_progress_current

        # Prepare queue. Verify a JobStateError is raised by our trigger.
        # Spy on the transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with (
            TransactionSpy.spy(mock_job_manager.db),
            pytest.raises(
                JobStateError,
                match="Failed to increment job progress",
            ),
        ):
            type(mock_job_run).progress_current = PropertyMock(side_effect=get_or_error)
            mock_job_manager.increment_progress(10, "Incrementing progress")

        # Verify job state on the mocked object remains unchanged.
        assert mock_job_run.progress_current is None
        assert mock_job_run.progress_message is None

    def test_increment_progress_success(self, mock_job_manager, mock_job_run):
        """Test successful job progress incrementation."""

        # Increment progress. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(mock_job_manager.db):
            mock_job_manager.increment_progress(10, "Incrementing progress")

        # Verify job state was updated on our mock object with expected values.
        # These changes would normally be persisted by the caller after this method returns.
        assert mock_job_run.progress_current == 10
        assert mock_job_run.progress_message == "Incrementing progress"

    def test_increment_progress_success_old_message_is_not_overwritten_when_none_provided(
        self, mock_job_manager, mock_job_run
    ):
        """Test successful job progress incrementation without message."""

        # Set initial progress message to verify it is not overwritten.
        mock_job_run.progress_message = "Old message"

        # Increment progress without message. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(mock_job_manager.db):
            mock_job_manager.increment_progress(15)

        # Verify job state was updated on our mock object with expected values.
        # These changes would normally be persisted by the caller after this method returns.
        assert mock_job_run.progress_current == 15
        assert mock_job_run.progress_message == "Old message"  # Message should remain unchanged from initial set.


@pytest.mark.integration
class TestJobProgressIncrementationIntegration:
    """Test job progress incrementation lifecycle management."""

    @pytest.mark.parametrize(
        "msg",
        [None, "Incremented progress successfully"],
    )
    def test_increment_progress_success(self, session, arq_redis, setup_worker_db, sample_job_run, msg):
        """Test successful progress incrementation."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Set initial progress to 0 to verify incrementation.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        job.progress_current = 0
        job.progress_total = 100
        job.progress_message = "Test incrementation message"
        session.commit()

        # Increment progress. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            manager.increment_progress(10, msg)

        # Commit pending changes made by increment progress.
        session.commit()

        # Verify job state was updated in transaction with expected values.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.progress_current == 10
        assert job.progress_total == 100
        assert job.progress_message == (
            msg if msg else "Test incrementation message"
        )  # Message should remain unchanged if None

    def test_increment_progress_success_multiple_times(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test successful progress incrementation multiple times."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Set initial progress to 0 to verify incrementation.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        job.progress_current = 0
        job.progress_total = 100
        session.commit()

        # Increment progress multiple times. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            manager.increment_progress(20)
            manager.increment_progress(30)

        # Commit pending changes made by increment progress.
        session.commit()

        # Verify job state was updated in transaction with expected values.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.progress_current == 50
        assert job.progress_total == 100

    def test_increment_progress_success_exceeding_total(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test successful progress incrementation exceeding total."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Set initial progress to 0 to verify incrementation.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        job.progress_current = 0
        job.progress_total = 100
        session.commit()

        # Increment progress exceeding total. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            manager.increment_progress(150)

        # Commit pending changes made by increment progress.
        session.commit()

        # Verify job state was updated in transaction with expected values.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.progress_current == 150
        assert job.progress_total == 100


class TestJobProgressTotalUpdateUnit:
    """Unit tests for job progress total update lifecycle management."""

    @pytest.mark.parametrize(
        "exception",
        HANDLED_EXCEPTIONS_DURING_OBJECT_MANIPULATION,
    )
    def test_set_progress_total_raises_job_state_error_when_handled_error_is_raised_during_object_manipulation(
        self, mock_job_manager, exception, mock_job_run
    ):
        """Test job progress total update failure due to exception during job object manipulation."""
        # Trigger: If any attribute access occurs on job progress total, raise exception. If no access, return initial total.
        initial_progress_total = mock_job_run.progress_total

        def get_or_error(*args):
            if args:
                raise exception
            return initial_progress_total

        # Prepare queue. Verify a JobStateError is raised by our trigger.
        # Spy on the transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with (
            TransactionSpy.spy(mock_job_manager.db),
            pytest.raises(
                JobStateError,
                match="Failed to update job progress total state",
            ),
        ):
            type(mock_job_run).progress_total = PropertyMock(side_effect=get_or_error)
            mock_job_manager.set_progress_total(200)

        # Verify job state on the mocked object remains unchanged.
        assert mock_job_run.progress_total == initial_progress_total

    def test_set_progress_total_success(self, mock_job_manager, mock_job_run):
        """Test successful job progress total update."""

        # Set progress total. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(mock_job_manager.db):
            mock_job_manager.set_progress_total(200)

        # Verify job state was updated on our mock object with expected values.
        # These changes would normally be persisted by the caller after this method returns.
        assert mock_job_run.progress_total == 200

    def test_set_progress_total_does_not_overwrite_old_message_when_no_new_message_is_provided(
        self, mock_job_manager, mock_job_run
    ):
        """Test successful job progress total update without message."""

        # Set initial progress message to verify it is not overwritten.
        mock_job_run.progress_message = "Old message"

        # Set progress total without message. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(mock_job_manager.db):
            mock_job_manager.set_progress_total(300)

        # Verify job state was updated on our mock object with expected values.
        # These changes would normally be persisted by the caller after this method returns.
        assert mock_job_run.progress_total == 300
        assert mock_job_run.progress_message == "Old message"  # Message should remain unchanged from initial set.


@pytest.mark.integration
class TestJobProgressTotalUpdateIntegration:
    """Test job progress total update lifecycle management."""

    def test_set_progress_total_success(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test successful progress total update."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Set initial progress total and message to verify update.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        job.progress_total = 100
        job.progress_message = "Ready to start"
        session.commit()

        # Set progress total. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            manager.set_progress_total(200, message="Updated total progress")

        # Commit pending changes made by set progress total.
        session.commit()

        # Verify job state was updated in transaction with expected values.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.progress_total == 200
        assert job.progress_message == "Updated total progress"


@pytest.mark.unit
class TestJobIsCancelledUnit:
    """Unit tests for job is_cancelled lifecycle management."""

    @pytest.mark.parametrize(
        "status,expected_result",
        [(status, status in CANCELLED_JOB_STATUSES) for status in JobStatus._member_map_.values()],
    )
    def test_is_cancelled_success_not_cancelled(self, mock_job_manager, mock_job_run, status, expected_result):
        """Test successful is_cancelled check when not cancelled."""
        # Set initial job status to a non-cancelled status.
        mock_job_run.status = status

        # Check is_cancelled. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(mock_job_manager.db):
            result = mock_job_manager.is_cancelled()

        assert result == expected_result


@pytest.mark.integration
class TestJobIsCancelledIntegration:
    """Test job is_cancelled lifecycle management."""

    @pytest.mark.parametrize(
        "job_status",
        [status for status in JobStatus._member_map_.values() if status in CANCELLED_JOB_STATUSES],
    )
    def test_is_cancelled_success_cancelled(self, session, arq_redis, setup_worker_db, sample_job_run, job_status):
        """Test successful is_cancelled check when cancelled."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Mark the job as cancelled in the database
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        job.status = job_status
        session.commit()

        # Check is_cancelled. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            result = manager.is_cancelled()

        # Verify the job is marked as cancelled. This method requires no persistance.
        assert result is True

    @pytest.mark.parametrize(
        "job_status",
        [status for status in JobStatus._member_map_.values() if status not in CANCELLED_JOB_STATUSES],
    )
    def test_is_cancelled_success_not_cancelled(self, session, arq_redis, setup_worker_db, sample_job_run, job_status):
        """Test successful is_cancelled check when not cancelled."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Mark the job as not cancelled in the database
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        job.status = job_status
        session.commit()

        # Check is_cancelled. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            result = manager.is_cancelled()

        # Verify the job is not marked as cancelled. This method requires no persistance.
        assert result is False


@pytest.mark.unit
class TestJobShouldRetryUnit:
    """Unit tests for job should_retry lifecycle management."""

    @pytest.mark.parametrize(
        "exception",
        [
            pytest.param(
                exc,
                marks=pytest.mark.skip(
                    reason=(
                        "AttributeError is not propagated by mock objects: "
                        "Python's attribute lookup swallows AttributeError and mock returns a new mock instead. "
                        "See unittest.mock docs for details."
                    )
                )
                if isinstance(exc, AttributeError)
                else (),
                # ^ Only mark AttributeError for skip, others run as normal
            )
            for exc in HANDLED_EXCEPTIONS_DURING_OBJECT_MANIPULATION
        ],
    )
    def test_should_retry_raises_job_state_error_when_handled_error_is_raised_during_object_manipulation(
        self, mock_job_manager, exception, mock_job_run
    ):
        """
        Test should_retry check failure due to exception during job object manipulation.

        AttributeError is skipped in this test because Python's mock machinery swallows
        AttributeError raised by property getters and instead returns a new mock, so the
        exception is not propagated as expected. See unittest.mock documentation for details.
        ^^ or something like that... don't ask me to explain why.
        """

        # Trigger: If any attribute access occurs on job, raise exception.
        def get_or_error(*args):
            raise exception

        # Remove any instance attribute that could shadow the property
        if "status" in mock_job_run.__dict__:
            del mock_job_run.__dict__["status"]

        # In cases where we want to raise on attribute access, we need to override the entire property
        # or else AttributeError won't be raised due to some internal Mock nuances I don't understand.
        type(mock_job_run).status = property(get_or_error)

        # Prepare queue. Verify a JobStateError is raised by our trigger.
        # Spy on the transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with (
            TransactionSpy.spy(mock_job_manager.db),
            pytest.raises(
                JobStateError,
                match="Failed to check retry eligibility state",
            ),
        ):
            mock_job_manager.should_retry()

    @pytest.mark.parametrize(
        "status,expected_result",
        [
            (JobStatus.SUCCEEDED, False),
            (JobStatus.CANCELLED, False),
            (JobStatus.QUEUED, False),
            (JobStatus.RUNNING, False),
            (JobStatus.PENDING, False),
        ],
    )
    def test_should_retry_success_for_non_failed_statuses(
        self, mock_job_manager, mock_job_run, status, expected_result
    ):
        """Test successful should_retry check."""
        # Set initial job status to provided status.
        mock_job_run.status = status

        # Check should_retry. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(mock_job_manager.db):
            result = mock_job_manager.should_retry()

        # Verify the result matches expected.
        assert result == expected_result

    @pytest.mark.parametrize(
        "retry_count,max_retries,failure_category,expected_result",
        (
            [(0, 3, cat, True) for cat in RETRYABLE_FAILURE_CATEGORIES]  # Initial retry,
            + [(2, 3, RETRYABLE_FAILURE_CATEGORIES[0], True)]  # Within retry limit (barely)
            + [(3, 3, RETRYABLE_FAILURE_CATEGORIES[0], False)]  # Exceeded retries
            + [
                (1, 3, cat, False)
                for cat in FailureCategory._member_map_.values()
                if cat not in RETRYABLE_FAILURE_CATEGORIES
            ]  # Non-retryable failure categories
        ),
    )
    def test_should_retry_success_for_failed_status(
        self, mock_job_manager, mock_job_run, retry_count, max_retries, failure_category, expected_result
    ):
        """Test successful should_retry check for failed status."""
        # Set initial job status to FAILED with provided parameters.
        mock_job_run.status = JobStatus.FAILED
        mock_job_run.retry_count = retry_count
        mock_job_run.max_retries = max_retries
        mock_job_run.failure_category = failure_category

        # Check should_retry. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(mock_job_manager.db):
            result = mock_job_manager.should_retry()

        # Verify the result matches expected.
        assert result == expected_result


@pytest.mark.integration
class TestJobShouldRetryIntegration:
    """Test job should_retry lifecycle management."""

    @pytest.mark.parametrize(
        "job_status",
        [status for status in JobStatus._member_map_.values() if status != JobStatus.FAILED],
    )
    def test_should_retry_success_non_failed_jobs_should_not_retry(
        self, session, arq_redis, setup_worker_db, sample_job_run, job_status
    ):
        """Test successful should_retry check (only jobs in failed states may retry)."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Update job to non-failed state
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        job.status = job_status
        session.commit()

        # Check should_retry. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            result = manager.should_retry()

        # Verify the job should not retry. This method requires no persistance.
        assert result is False

    def test_should_retry_success_exceeded_retry_attempts_should_not_retry(
        self, session, arq_redis, setup_worker_db, sample_job_run
    ):
        """Test successful should_retry check with no retry attempts left."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Update job to failed state with no retries left
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        job.status = JobStatus.FAILED
        job.max_retries = 3
        job.retry_count = 3
        session.commit()

        # Check should_retry. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            result = manager.should_retry()

        # Verify the job should not retry. This method requires no persistance.
        assert result is False

    def test_should_retry_success_failure_category_is_not_retryable(
        self, session, arq_redis, setup_worker_db, sample_job_run
    ):
        """Test successful should_retry check with non-retryable failure category."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Update job to failed state with non-retryable failure category
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        job.status = JobStatus.FAILED
        job.max_retries = 3
        job.retry_count = 1
        job.failure_category = FailureCategory.UNKNOWN
        session.commit()

        # Check should_retry. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            result = manager.should_retry()

        # Verify the job should not retry. This method requires no persistance.
        assert result is False

    def test_should_retry_success(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test successful should_retry check with retryable failure category."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Update job to failed state with retryable failure category
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        job.status = JobStatus.FAILED
        job.max_retries = 3
        job.retry_count = 1
        job.failure_category = RETRYABLE_FAILURE_CATEGORIES[0]
        session.commit()

        # Check should_retry. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            result = manager.should_retry()

        # Verify the job should retry. This method requires no persistance.
        assert result is True


@pytest.mark.unit
class TestGetJobUnit:
    """Unit tests for job retrieval."""

    def test_get_job_wraps_database_connection_error_when_encounters_sqlalchemy_error(self, mock_job_run):
        """Test job retrieval failure during job fetch."""

        # Prepare mock JobManager with mocked DB session that will raise SQLAlchemyError on query.
        # We don't use the default fixture here since it usually wraps this function.
        mock_db = Mock(spec=Session)
        mock_redis = Mock(spec=ArqRedis)
        manager = object.__new__(JobManager)
        manager.db = mock_db
        manager.redis = mock_redis
        manager.job_id = mock_job_run.id

        with (
            TransactionSpy.mock_database_execution_failure(manager.db),
            pytest.raises(DatabaseConnectionError, match=f"Failed to fetch job {mock_job_run.id}"),
        ):
            manager.get_job()


@pytest.mark.integration
class TestGetJobIntegration:
    """Test job retrieval."""

    def test_get_job_success(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test successful job retrieval."""
        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Retrieve job. Spy on transaction to ensure nothing is flushed/rolled back/committed prematurely.
        with TransactionSpy.spy(manager.db):
            job = manager.get_job()

        # Verify the retrieved job matches expected.
        assert job.id == sample_job_run.id
        assert job.status == JobStatus.PENDING

    def test_get_job_raises_job_not_found_error_when_job_does_not_exist(self, session, arq_redis, setup_worker_db):
        """Test job retrieval failure when job does not exist."""
        with pytest.raises(DatabaseConnectionError, match="Failed to fetch job 9999"), TransactionSpy.spy(session):
            JobManager(session, arq_redis, job_id=9999)  # Non-existent job ID


@pytest.mark.integration
class TestJobManagerJob:
    """Test overall job lifecycle management."""

    def test_full_successful_job_lifecycle(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test full job lifecycle from start to completion."""
        # Pre-manager: Job is created in DB in Pending state. Verify initial state.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING, "Initial job status should be PENDING"

        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Prepare job to be enqueued
        with TransactionSpy.spy(manager.db):
            manager.prepare_queue()
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED, "Job status should be QUEUED after preparing queue"

        # Start job
        with TransactionSpy.spy(manager.db):
            manager.start_job()
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.RUNNING, "Job status should be RUNNING after starting"
        assert job.started_at is not None, "Job started_at should be set after starting"

        # Set initial progress
        with TransactionSpy.spy(manager.db):
            manager.update_progress(0, 100, "Job started")
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.progress_current == 0
        assert job.progress_total == 100
        assert job.progress_message == "Job started"

        # Update status message
        with TransactionSpy.spy(manager.db):
            manager.update_status_message("Began processing data")
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.progress_message == "Began processing data"

        # Set progress total
        with TransactionSpy.spy(manager.db):
            manager.set_progress_total(200, "Set total work units")
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.progress_total == 200
        assert job.progress_message == "Set total work units"

        # Increment progress
        with TransactionSpy.spy(manager.db):
            manager.increment_progress(100, "Halfway done")
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.progress_current == 100
        assert job.progress_message == "Halfway done"

        # Increment progress again
        with TransactionSpy.spy(manager.db):
            manager.increment_progress(100, "All done")
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.progress_current == 200
        assert job.progress_message == "All done"

        # Complete job
        with TransactionSpy.spy(manager.db):
            manager.succeed_job(result={"output": "success"})
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.SUCCEEDED
        assert job.finished_at is not None

        # Verify job is not cancelled and should not retry
        assert manager.is_cancelled() is False
        assert manager.should_retry() is False

        # Verify final job state
        final_job = manager.get_job()
        assert final_job.status == JobStatus.SUCCEEDED
        assert final_job.progress_current == 200
        assert final_job.progress_total == 200
        assert final_job.progress_message == "Job completed successfully"

    def test_full_cancelled_job_lifecycle(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test full job lifecycle for a cancelled job."""
        # Pre-manager: Job is created in DB in Pending state. Verify initial state.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING, "Initial job status should be PENDING"

        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Prepare job to be enqueued
        with TransactionSpy.spy(manager.db):
            manager.prepare_queue()
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED, "Job status should be QUEUED after preparing queue"

        # Start job
        with TransactionSpy.spy(manager.db):
            manager.start_job()
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.RUNNING

        # Cancel job
        with TransactionSpy.spy(manager.db):
            manager.cancel_job({"reason": "User requested cancellation"})
        session.flush()

        # Verify job is cancelled
        assert manager.is_cancelled() is True

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.CANCELLED
        assert job.finished_at is not None
        assert job.progress_message == "Job cancelled"

    def test_full_skipped_job_lifecycle(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test full job lifecycle for a skipped job."""
        # Pre-manager: Job is created in DB in Pending state. Verify initial state.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING, "Initial job status should be PENDING"

        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Skip job
        with TransactionSpy.spy(manager.db):
            manager.skip_job(result={"reason": "Precondition not met"})
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.SKIPPED
        assert job.finished_at is not None
        assert job.progress_message == "Job skipped"

    def test_full_failed_job_lifecycle(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test full job lifecycle for a failed job."""
        # Pre-manager: Job is created in DB in Pending state. Verify initial state.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING, "Initial job status should be PENDING"

        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Prepare job to be enqueued
        with TransactionSpy.spy(manager.db):
            manager.prepare_queue()
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED, "Job status should be QUEUED after preparing queue"

        # Start job
        with TransactionSpy.spy(manager.db):
            manager.start_job()
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.RUNNING

        # Fail job
        with TransactionSpy.spy(manager.db):
            manager.fail_job(
                error=Exception("An error occurred"),
                result={"details": "Traceback details here"},
            )
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.FAILED
        assert job.finished_at is not None
        assert job.error_message == "An error occurred"
        assert job.error_traceback is not None

    def test_full_retried_job_lifecycle(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test full job lifecycle for a retried job."""
        # Pre-manager: Job is created in DB in Pending state. Verify initial state.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING, "Initial job status should be PENDING"

        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Prepare job to be enqueued
        with TransactionSpy.spy(manager.db):
            manager.prepare_queue()
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED, "Job status should be QUEUED after preparing queue"

        # Start job
        with TransactionSpy.spy(manager.db):
            manager.start_job()
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.RUNNING

        # Fail job
        with TransactionSpy.spy(manager.db):
            manager.fail_job(
                error=Exception("Temporary error"),
                result={"details": "Traceback details here"},
            )
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.FAILED

        # TODO: Use some failure method added later to set failure category to retryable during the
        # call to fail_job above. For now, we manually set it here.
        job.failure_category = RETRYABLE_FAILURE_CATEGORIES[0]
        session.commit()

        # Should retry
        assert manager.should_retry() is True

        # Prepare retry
        with TransactionSpy.spy(manager.db):
            manager.prepare_retry()
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING
        assert job.retry_count == 1

    def test_full_reset_job_lifecycle(self, session, arq_redis, setup_worker_db, sample_job_run):
        """Test full job lifecycle for a reset job."""
        # Pre-manager: Job is created in DB in Pending state. Verify initial state.
        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING, "Initial job status should be PENDING"

        manager = JobManager(session, arq_redis, sample_job_run.id)

        # Prepare job to be enqueued
        with TransactionSpy.spy(manager.db):
            manager.prepare_queue()
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED, "Job status should be QUEUED after preparing queue"

        # Start job
        with TransactionSpy.spy(manager.db):
            manager.start_job()
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.RUNNING

        # Fail job
        with TransactionSpy.spy(manager.db):
            manager.fail_job(
                error=Exception("Some error"),
                result={"details": "Traceback details here"},
            )
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.FAILED

        # Retry job
        with TransactionSpy.spy(manager.db):
            manager.prepare_retry()
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING
        assert job.retry_count == 1

        # Queeue job again
        with TransactionSpy.spy(manager.db):
            manager.prepare_queue()
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.QUEUED, "Job status should be QUEUED after preparing queue"

        # Start job again
        with TransactionSpy.spy(manager.db):
            manager.start_job()
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.RUNNING

        # Fail job again
        with TransactionSpy.spy(manager.db):
            manager.fail_job(
                error=Exception("Another error"),
                result={"details": "Traceback details here"},
            )
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.FAILED
        assert job.retry_count == 1

        # Reset job
        with TransactionSpy.spy(manager.db):
            manager.reset_job()
        session.flush()

        job = session.execute(select(JobRun).where(JobRun.id == sample_job_run.id)).scalar_one()
        assert job.status == JobStatus.PENDING
        assert job.progress_current is None
        assert job.progress_total is None
        assert job.retry_count == 0

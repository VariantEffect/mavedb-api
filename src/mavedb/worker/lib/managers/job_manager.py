"""Job lifecycle management for individual job state transitions.

This module provides the JobManager class for managing individual job state transitions
with atomic operations and explicit error handling to ensure data consistency.
Pipeline coordination is handled separately by the PipelineManager.

Example usage:
    >>> from mavedb.worker.lib.job_manager import JobManager
    >>>
    >>> # Initialize with database and Redis connections
    >>> job_manager = JobManager(db_session, redis_client, job_id=123)
    >>>
    >>> # Start job execution
    >>> job_manager.start_job()
    >>>
    >>> # Update progress during execution
    >>> job_manager.update_progress(50, 100, "Processing variants...")
    >>>
    >>> # Complete job (pipeline coordination handled separately)
    >>> job_manager.complete_job(
    ...     status=JobStatus.SUCCEEDED,
    ...     result={"variants_processed": 1000}
    ... )

Error Handling:
    The JobManager uses specific exception types to distinguish between different
    failure modes, allowing callers to implement appropriate recovery strategies:

    - DatabaseConnectionError: Database connectivity issues
    - JobStateError: Critical state persistence failures
    - JobTransitionError: Invalid state transitions
"""

import logging
import traceback
from datetime import datetime
from typing import Any, Optional

from arq import ArqRedis
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from mavedb.lib.logging.context import format_raised_exception_info_as_dict
from mavedb.models.enums.job_pipeline import FailureCategory, JobStatus
from mavedb.models.job_run import JobRun
from mavedb.worker.lib.managers.base_manager import BaseManager
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
from mavedb.worker.lib.managers.types import JobResultData, RetryHistoryEntry

logger = logging.getLogger(__name__)


class JobManager(BaseManager):
    """Manages individual job lifecycle with atomic state transitions.

    The JobManager provides a high-level interface for managing individual job execution
    while ensuring database consistency. It handles job state transitions, progress updates,
    and retry logic. Pipeline coordination is handled separately by the PipelineManager.

    Key Features:
    - Atomic state transitions with rollback on failure
    - Explicit exception handling for different failure modes
    - Progress tracking and retry mechanisms
    - Automatic session cleanup on object manipulation failures
    - Focus on individual job lifecycle only

    Note:
        To avoid persisting inconsistent job state to the database, any failures
        during job manipulation (e.g., fetching job, updating fields) will result
        in a safe rollback of the current transaction. This ensures that partial
        updates do not corrupt job state. This manager DOES NOT COMMIT database
        changes, only flushes them. Commit responsibility lies with the caller.

    Usage Patterns:

        Basic job execution:
        >>> manager = JobManager(db, redis, job_id=123)
        >>> manager.start_job()
        >>> manager.update_progress(25, message="Starting validation")
        >>> manager.succeed_job(result={"count": 100})

        Progress tracking convenience:
        >>> manager.set_progress_total(1000, "Processing 1000 records")
        >>> for record in records:
        ...     process_record(record)
        ...     manager.increment_progress()  # Increment by 1
        ...     if manager.is_cancelled():
        ...         break

        Job failure handling:
        >>> try:
        ...     process_data()
        ... except ValidationError as e:
        ...     manager.fail_job(error=e, result={"partial_results": partial_data})

        Direct completion control:
        >>> manager.complete_job(status=JobStatus.SUCCEEDED, result=data)

        Error handling:
        >>> try:
        ...     manager.complete_job(status=JobStatus.SUCCEEDED, result=data)
        ... except JobStateError as e:
        ...     logger.critical(f"Critical state failure: {e}")
        ...     # Job completion failed - state not saved

        Job retry:
        >>> try:
        ...     manager.retry_job(reason="Transient network error")
        ... except JobTransitionError as e:
        ...     logger.error(f"Cannot retry job in current state: {e}")

    Exception Hierarchy:
    - DatabaseConnectionError: Cannot connect to database
    - JobStateError: Critical state persistence failures
    - JobTransitionError: Invalid state transitions (e.g., start already running job)

    Thread Safety:
        JobManager is not thread-safe. Each instance should be used by a single
        worker thread and should not be shared across concurrent operations.
    """

    context: dict[str, Any] = {}

    def __init__(self, db: Session, redis: ArqRedis, job_id: int):
        """Initialize JobManager for a specific job.

        Args:
            db: Active SQLAlchemy session for database operations. Session should
                be configured for the appropriate database and have proper
                transaction isolation.
            redis: ARQ Redis client for job queue operations. Must be connected
                   and ready for enqueue operations.
            job_id: Unique identifier of the job to manage. Must correspond to
                    an existing JobRun record in the database.

        Raises:
            DatabaseConnectionError: If the job cannot be fetched from database,
                indicating connectivity issues or invalid job_id.

        Example:
            >>> db_session = get_database_session()
            >>> redis_client = get_arq_redis_client()
            >>> manager = JobManager(db_session, redis_client, 12345)
            >>> # Manager is now ready to handle job 12345
        """
        super().__init__(db, redis)

        self.job_id = job_id
        job = self.get_job()
        self.pipeline_id = job.pipeline_id if job else None

        self.save_to_context(
            {"job_id": str(self.job_id), "pipeline_id": str(self.pipeline_id) if self.pipeline_id else None}
        )

    def save_to_context(self, ctx: dict) -> dict[str, Any]:
        for k, v in ctx.items():
            self.context[k] = v

        return self.context

    def logging_context(self) -> dict[str, Any]:
        return self.context

    def start_job(self) -> None:
        """Mark job as started and initialize execution tracking. This method does
        not flush or commit the database session; the caller is responsible for persisting changes.

        Transitions job from QUEUED or PENDING to RUNNING state, setting start
        timestamp and a default progress message. This method should be called
        once at the beginning of job execution.

        State Changes:
        - Sets status to JobStatus.RUNNING
        - Records started_at timestamp
        - Initializes progress to 0/100
        - Sets progress_message to "Job began execution"

        Raises:
            DatabaseConnectionError: Cannot fetch job from database
            JobStateError: Cannot save job start state to database
            JobTransitionError: Job not in valid state to start (must be QUEUED or PENDING)

        Example:
            >>> manager = JobManager(db, redis, 123)
            >>> manager.start_job()  # Job 123 now marked as RUNNING
            >>> # Proceed with job execution logic...
        """
        job_run = self.get_job()
        if job_run.status not in STARTABLE_JOB_STATUSES:
            self.save_to_context({"job_status": str(job_run.status)})
            logger.error(
                "Invalid job start attempt: status not in STARTABLE_JOB_STATUSES", extra=self.logging_context()
            )
            raise JobTransitionError(f"Cannot start job {self.job_id} from status {job_run.status}")

        try:
            job_run.status = JobStatus.RUNNING
            job_run.started_at = datetime.now()
            job_run.progress_message = "Job began execution"
        except (AttributeError, TypeError, KeyError, ValueError) as e:
            self.save_to_context(format_raised_exception_info_as_dict(e))
            logger.debug("Encountered an unexpected error while updating job start state", extra=self.logging_context())
            raise JobStateError(f"Failed to update job start state: {e}")

        self.save_to_context({"job_status": str(job_run.status)})
        logger.info("Job marked as started", extra=self.logging_context())

    def complete_job(self, status: JobStatus, result: JobResultData, error: Optional[Exception] = None) -> None:
        """Mark job as completed with the specified final status. This method does
        not flush or commit the database session; the caller is responsible for persisting changes.

        Transitions job to the passed terminal status (SUCCEEDED, FAILED, CANCELLED, SKIPPED),
        recording the finished_at timestamp, result data, and error details if applicable.

        Args:
            status: Final job status - must be a terminal status
                   (SUCCEEDED, FAILED, CANCELLED, SKIPPED)
            result: JobResultData to store in metadata. Should be JSON-serializable
                   dictionary containing any outputs, metrics, or artifacts produced.
            error: Exception that caused job failure, if applicable. Error details
                  will be logged and stored for debugging.

        State Changes:
        - Sets status to the specified terminal status
        - Sets finished_at timestamp
        - Stores result in job metadata
        - Records error details if provided and status is FAILED

        Raises:
            DatabaseConnectionError: Cannot fetch job or connect to database
            JobStateError: Cannot save job completion state - critical error
            JobTransitionError: Invalid terminal status provided

        Examples:
            Successful completion:
            >>> result_data = {"records_processed": 1500, "errors": 0}
            >>> manager.complete_job(
            ...     status=JobStatus.SUCCEEDED,
            ...     result=result_data
            ... )

            Failed completion with error:
            >>> try:
            ...     process_data()
            ... except ValidationError as e:
            ...     manager.complete_job(
            ...         status=JobStatus.FAILED,
            ...         result={"partial_results": data},
            ...         error=e
            ...     )

        Note:
            Job completion state is saved independently of any pipeline
            coordination. Use PipelineManager for coordinating dependent jobs.
        """
        # Validate terminal status
        if status not in TERMINAL_JOB_STATUSES:
            self.save_to_context({"job_status": str(status)})
            logger.error("Invalid job completion status: not in TERMINAL_JOB_STATUSES", extra=self.logging_context())
            raise JobTransitionError(
                f"Cannot commplete job to status: {status}. Must complete to a terminal status: {TERMINAL_JOB_STATUSES}"
            )

        job_run = self.get_job()
        try:
            job_run.status = status
            job_run.metadata_["result"] = {
                "status": result["status"],
                "data": result["data"],
                "exception_details": format_raised_exception_info_as_dict(result["exception"])  # type: ignore
                if result.get("exception")
                else None,
            }
            job_run.finished_at = datetime.now()

            if status == JobStatus.FAILED:
                job_run.failure_category = FailureCategory.UNKNOWN

            if error:
                job_run.error_message = str(error)
                job_run.error_traceback = traceback.format_exc()
                # TODO: Classify failure category based on error type
                job_run.failure_category = FailureCategory.UNKNOWN

                self.save_to_context({"failure_category": str(job_run.failure_category)})

        except (AttributeError, TypeError, KeyError, ValueError) as e:
            self.save_to_context(format_raised_exception_info_as_dict(e))
            logger.debug(
                "Encountered an unexpected error while updating job completion state", extra=self.logging_context()
            )
            raise JobStateError(f"Failed to update job completion state: {e}")

        self.save_to_context({"job_status": str(job_run.status)})
        logger.info("Job marked as completed", extra=self.logging_context())

    def fail_job(self, error: Exception, result: JobResultData) -> None:
        """Mark job as failed and record error details. This method does
        not flush or commit the database session; the caller is responsible for persisting changes.

        Convenience method for marking job execution as failed. This is equivalent
        to calling complete_job(status=JobStatus.FAILED, error=error, result=result) but
        provides clearer intent and a more focused API for failure scenarios.

        Args:
            error: Exception that caused job failure. Error details will be logged
                  and stored for debugging. Used to populate error message and traceback.
            result: Partial results to store in metadata. Should be
                   JSON-serializable dictionary containing any partial outputs,
                   metrics, or debugging information produced before failure.

        Raises:
            DatabaseConnectionError: Cannot fetch job or connect to database
            JobStateError: Cannot save job completion state - critical error

        Examples:
            Basic failure with exception:
            >>> try:
            ...     validate_data(input_data)
            ... except ValidationError as e:
            ...     manager.fail_job(error=e, result={})

            Failure with partial results:
            >>> try:
            ...     results = process_batch(records)
            ... except ProcessingError as e:
            ...     partial_results = {"processed": len(results), "failed_at": e.record_id}
            ...     manager.fail_job(error=e, result=partial_results)

        Note:
            This method is equivalent to complete_job(status=JobStatus.FAILED, error=error, result=result).
            Use this method when job failure is the primary outcome to make intent clearer.
        """
        self.complete_job(status=JobStatus.FAILED, result=result, error=error)

    def succeed_job(self, result: JobResultData) -> None:
        """Mark job as succeeded and record results. This method does
        not flush or commit the database session; the caller is responsible for persisting changes.

        Convenience method for marking job execution as successful. This is equivalent
        to calling complete_job(status=JobStatus.SUCCEEDED, result=result) but provides clearer
        intent and a more focused API for success scenarios.

        Args:
            result: Job result data to store in metadata. Should be JSON-serializable
                   dictionary containing any outputs, metrics, or artifacts produced.

        Raises:
            DatabaseConnectionError: Cannot fetch job or connect to database
            JobStateError: Cannot save job completion state - critical error

        Examples:
            Successful completion:
            >>> result_data = {"records_processed": 1500, "errors": 0, "duration": 45.2}
            >>> manager.succeed_job(result=result_data)

            Success with metrics:
            >>> metrics = {
            ...     "input_count": 10000,
            ...     "output_count": 9847,
            ...     "skipped": 153,
            ...     "processing_time": 120.5,
            ...     "memory_peak": "2.1GB"
            ... }
            >>> manager.succeed_job(result=metrics)

        Note:
            This method is equivalent to complete_job(status=JobStatus.SUCCEEDED, result=result).
            Use this method when job success is the primary outcome to make intent clearer.
        """
        self.complete_job(status=JobStatus.SUCCEEDED, result=result)

    def cancel_job(self, result: JobResultData) -> None:
        """Mark job as cancelled. This method does
        not flush or commit the database session; the caller is responsible for persisting changes.

        Convenience method for marking job execution as cancelled. This is equivalent
        to calling complete_job(status=JobStatus.CANCELLED, result=result) but provides
        clearer intent and a more focused API for cancellation scenarios.

        Args:
            reason: Human-readable reason for cancellation (e.g., "user_requested",
                   "pipeline_cancelled", "timeout"). Used for debugging and audit trails.
            result: Partial results to store in metadata. Should be JSON-serializable
                   dictionary containing any partial outputs or cancellation details.
                   If None, defaults to cancellation metadata.

        Raises:
            DatabaseConnectionError: Cannot fetch job or connect to database
            JobStateError: Cannot save job completion state - critical error

        Examples:
            Basic cancellation:
            >>> manager.cancel_job({"reason": "user_requested"})

        Note:
            This method is equivalent to complete_job(status=JobStatus.CANCELLED, result=result).
            Use this method when job cancellation is the primary outcome to make intent clearer.
        """
        self.complete_job(status=JobStatus.CANCELLED, result=result)

    def skip_job(self, result: JobResultData) -> None:
        """Mark job as skipped. This method does
        not flush or commit the database session; the caller is responsible for persisting changes.

        Convenience method for marking job as skipped (not executed). This is equivalent
        to calling complete_job(status=JobStatus.SKIPPED, result=result) but provides
        clearer intent and a more focused API for skip scenarios.

        Args:
            result: Skip details to store in metadata. Should be JSON-serializable
                   dictionary containing skip reason and context.
                   If None, defaults to skip metadata.

        Raises:
            DatabaseConnectionError: Cannot fetch job or connect to database
            JobStateError: Cannot save job completion state - critical error

        Examples:
            Basic skip:
            >>> manager.skip_job({"reason": "No work to perform"})

        Note:
            This method is equivalent to complete_job(status=JobStatus.SKIPPED, result=result).
            Use this method when job skipping is the primary outcome to make intent clearer.
        """
        self.complete_job(status=JobStatus.SKIPPED, result=result)

    def prepare_retry(self, reason: str = "retry_requested") -> None:
        """Prepare a failed job for retry by resetting state to PENDING. This method does
        not flush or commit the database session; the caller is responsible for persisting changes.

        Resets a failed job back to PENDING status so it can be re-enqueued
        by the pipeline coordination system. This is similar to job completion
        but transitions to PENDING instead of a terminal state.

        Args:
            reason: Human-readable reason for the retry (e.g., "transient_network_error",
                   "memory_limit_exceeded"). Used for debugging and audit trails.

        State Changes:
        - Increments retry_count
        - Resets status from FAILED, SKIPPED, CANCELLED to PENDING
        - Clears error_message, error_traceback, failure_category
        - Clears finished_at timestamp
        - Adds retry attempt to metadata history

        Raises:
            DatabaseConnectionError: Cannot fetch job from database
            JobTransitionError: Job not in FAILED state (cannot retry)
            JobStateError: Cannot save retry state changes

        Examples:
            Basic retry preparation:
            >>> try:
            ...     manager.prepare_retry("network_timeout")
            ... except JobTransitionError:
            ...     logger.error("Cannot retry job - not in failed state")

            Conditional retry with limits:
            >>> job = manager.get_job()
            >>> if job and job.retry_count < 3:
            ...     manager.prepare_retry(f"attempt_{job.retry_count + 1}")
            ...     # PipelineManager will handle enqueueing
            ... else:
            ...     logger.error("Max retries exceeded")

        Retry History:
            Each retry attempt is recorded in job metadata with:
            - retry_attempt: Sequential attempt number
            - timestamp: When retry was initiated
            - result: Previous execution results (for debugging)
            - reason: Provided retry reason

        Note:
            After calling this method, use PipelineManager.enqueue_ready_jobs()
            to actually enqueue the job for execution.
        """
        job_run = self.get_job()
        if job_run.status not in RETRYABLE_JOB_STATUSES:
            self.save_to_context({"job_status": str(job_run.status)})
            logger.error("Invalid job retry status: status not in RETRYABLE_JOB_STATUSES", extra=self.logging_context())
            raise JobTransitionError(f"Cannot retry job {self.job_id} due to invalid state ({job_run.status})")

        try:
            job_run.status = JobStatus.PENDING
            current_result: JobResultData = job_run.metadata_.get("result", {})
            job_run.retry_count = (job_run.retry_count or 0) + 1
            job_run.progress_message = "Job retry prepared"
            job_run.error_message = None
            job_run.error_traceback = None
            job_run.failure_category = None
            job_run.finished_at = None
            job_run.started_at = None

            # Add retry history - metadata manipulation (risky)
            retry_history: list[RetryHistoryEntry] = job_run.metadata_.setdefault("retry_history", [])
            retry_history.append(
                {
                    "attempt": job_run.retry_count,
                    "timestamp": datetime.now().isoformat(),
                    "result": current_result,
                    "reason": reason,
                }
            )
            job_run.metadata_.pop("result", None)  # Clear previous result
            flag_modified(job_run, "metadata_")

        except (AttributeError, TypeError, KeyError, ValueError) as e:
            self.save_to_context(format_raised_exception_info_as_dict(e))
            logger.debug("Encountered an unexpected error while updating job retry state", extra=self.logging_context())
            raise JobStateError(f"Failed to update job retry state: {e}")

        self.save_to_context({"job_status": str(job_run.status), "retry_attempt": job_run.retry_count})
        logger.info("Job successfully prepared for retry", extra=self.logging_context())

    def prepare_queue(self) -> None:
        """Prepare job for enqueueing by setting QUEUED status. This method does
        not flush or commit the database session; the caller is responsible for persisting changes.

        Transitions job from PENDING to QUEUED status before ARQ enqueueing.
        This ensures proper state tracking and validates the transition.

        Raises:
            JobTransitionError: Job not in PENDING state
            JobStateError: Cannot save state change
        """
        job_run = self.get_job()
        if job_run.status != JobStatus.PENDING:
            self.save_to_context({"job_status": str(job_run.status)})
            logger.error("Invalid job queue attempt: status not PENDING", extra=self.logging_context())
            raise JobTransitionError(f"Cannot queue job {self.job_id} from status {job_run.status}")

        try:
            job_run.status = JobStatus.QUEUED
            job_run.progress_message = "Job queued for execution"
        except (AttributeError, TypeError, KeyError, ValueError) as e:
            self.save_to_context(format_raised_exception_info_as_dict(e))
            logger.debug("Encountered an unexpected error while updating job queue state", extra=self.logging_context())
            raise JobStateError(f"Failed to update job queue state: {e}")

        self.save_to_context({"job_status": str(job_run.status)})
        logger.debug("Job successfully prepared for queueing", extra=self.logging_context())

    def reset_job(self) -> None:
        """Reset job to initial state for re-execution. This method does
        not flush or commit the database session; the caller is responsible for persisting changes.

        Resets all job state fields to their initial values, allowing the job
        to be re-executed from scratch. This is useful for testing or manual
        re-runs of jobs without retaining any prior execution history.

        State Changes:
        - Sets status to PENDING
        - Clears started_at and finished_at timestamps
        - Resets progress to 0/100 with default message
        - Clears error details and failure category
        - Resets retry_count to 0
        - Clears metadata

        Raises:
            DatabaseConnectionError: Cannot fetch job from database
            JobStateError: Cannot save reset state changes
        Examples:
            Basic job reset:
            >>> manager.reset_job()
            >>> # Job is now reset to initial state for re-execution
        """
        job_run = self.get_job()
        try:
            job_run.status = JobStatus.PENDING
            job_run.started_at = None
            job_run.finished_at = None
            job_run.progress_current = None
            job_run.progress_total = None
            job_run.progress_message = None
            job_run.error_message = None
            job_run.error_traceback = None
            job_run.failure_category = None
            job_run.retry_count = 0
            job_run.metadata_ = {}

        except (AttributeError, TypeError, KeyError, ValueError) as e:
            self.save_to_context(format_raised_exception_info_as_dict(e))
            logger.debug("Encountered an unexpected error while resetting job state", extra=self.logging_context())
            raise JobStateError(f"Failed to reset job state: {e}")

        self.save_to_context({"job_status": str(job_run.status), "retry_attempt": job_run.retry_count})
        logger.info("Job successfully reset to initial state", extra=self.logging_context())

    def update_progress(self, current: int, total: int = 100, message: Optional[str] = None) -> None:
        """Update job progress information during execution. This method does
        not flush or commit the database session; the caller is responsible for persisting changes.

        Provides real-time progress updates for long-running jobs. Progress updates
        are best-effort operations that won't interrupt job execution if they fail.
        This allows jobs to continue even if progress tracking has issues.

        Args:
            current: Current progress value (e.g., records processed so far)
            total: Total expected progress value (default: 100 for percentage)
            message: Optional human-readable progress description

        Examples:
            Percentage-based progress:
            >>> manager.update_progress(25, 100, "Validating input data")
            >>> manager.update_progress(50, 100, "Processing records")
            >>> manager.update_progress(100, 100, "Finalizing results")

            Count-based progress:
            >>> total_records = 50000
            >>> for i, record in enumerate(records):
            ...     process_record(record)
            ...     if i % 1000 == 0:  # Update every 1000 records
            ...         manager.update_progress(
            ...             current=i,
            ...             total=total_records,
            ...             message=f"Processed {i}/{total_records} records"
            ...         )

            Handling progress failures:
            >>> try:
            ...     manager.update_progress(75, message="Almost done")
            ... except DatabaseConnectionError:
            ...     logger.debug("Progress update failed, continuing job")
            ...     # Job continues normally

        Note:
            Progress updates are non-blocking and failure-tolerant. If a progress
            update fails, the job may choose to continue execution normally. Failed
            progress updates are logged at debug level.
        """
        job_run = self.get_job()
        try:
            job_run.progress_current = current
            job_run.progress_total = total
            if message:
                job_run.progress_message = message

        except (AttributeError, TypeError, KeyError, ValueError) as e:
            self.save_to_context(format_raised_exception_info_as_dict(e))
            logger.debug("Encountered an unexpected error while updating job progress", extra=self.logging_context())
            raise JobStateError(f"Failed to update job progress state: {e}")

        self.save_to_context(
            {"job_progress_current": current, "job_progress_total": total, "job_progress_message": message}
        )
        logger.debug("Updated progress successfully for job", extra=self.logging_context())

    def update_status_message(self, message: str) -> None:
        """Update job status message without changing progress. This method does
        not flush or commit the database session; the caller is responsible for persisting changes.

        Convenience method for updating the progress message while keeping
        current progress values unchanged. Useful for status updates during
        long-running operations.

        Args:
            message: Human-readable status message describing current activity

        Raises:
            DatabaseConnectionError: Cannot fetch job from database
            JobStateError: Cannot save status message update

        Example:
            >>> manager.update_status_message("Connecting to external API...")
            >>> # Do API work
            >>> manager.update_status_message("Processing API response...")
        """
        job_run = self.get_job()
        try:
            job_run.progress_message = message
        except (AttributeError, TypeError, KeyError, ValueError) as e:
            self.save_to_context(format_raised_exception_info_as_dict(e))
            logger.debug(
                "Encountered an unexpected error while updating job status message", extra=self.logging_context()
            )
            raise JobStateError(f"Failed to update job status message state: {e}")

        self.save_to_context({"job_progress_message": message})
        logger.debug("Updated status message successfully for job", extra=self.logging_context())

    def increment_progress(self, amount: int = 1, message: Optional[str] = None) -> None:
        """Increment job progress by a specified amount. This method does
        not flush or commit the database session; the caller is responsible for persisting changes.

        Convenience method for incrementing progress without needing to track
        the current progress value. Useful for batch processing where you want
        to increment by 1 for each item processed.

        Args:
            amount: Amount to increment progress by (default: 1)
            message: Optional message to update along with progress

        Raises:
            DatabaseConnectionError: Cannot fetch job from database
            JobStateError: Cannot save progress update

        Examples:
            >>> # Process items one by one
            >>> for item in items:
            ...     process_item(item)
            ...     manager.increment_progress()  # Increment by 1

            >>> # Process in batches
            >>> for batch in batches:
            ...     process_batch(batch)
            ...     manager.increment_progress(len(batch), f"Processed batch {i}")
        """
        job_run = self.get_job()
        try:
            current = job_run.progress_current or 0
            job_run.progress_current = current + amount
            if message:
                job_run.progress_message = message
        except (AttributeError, TypeError, KeyError, ValueError) as e:
            self.save_to_context(format_raised_exception_info_as_dict(e))
            logger.debug(
                "Encountered an unexpected error while incrementing job progress", extra=self.logging_context()
            )
            raise JobStateError(f"Failed to increment job progress state: {e}")

        self.save_to_context(
            {
                "job_progress_current": current,
                "job_progress_total": job_run.progress_total,
                "job_progress_message": message or "",
            }
        )
        logger.debug("Incremented progress successfully for job", extra=self.logging_context())

    def set_progress_total(self, total: int, message: Optional[str] = None) -> None:
        """Update the total progress value, useful when total becomes known during execution. This method does
        not flush or commit the database session; the caller is responsible for persisting changes.

        Convenience method for updating progress total when it's discovered during
        job execution (e.g., after counting records to process).

        Args:
            total: New total progress value
            message: Optional message to update along with total

        Raises:
            DatabaseConnectionError: Cannot fetch job from database
            JobStateError: Cannot save progress total update

        Example:
            >>> # Initially unknown total
            >>> manager.start_job()
            >>> records = load_all_records()  # Discovers actual count
            >>> manager.set_progress_total(len(records), f"Processing {len(records)} records")
        """
        job_run = self.get_job()
        try:
            job_run.progress_total = total
            if message:
                job_run.progress_message = message
        except (AttributeError, TypeError, KeyError, ValueError) as e:
            self.save_to_context(format_raised_exception_info_as_dict(e))
            logger.debug(
                "Encountered an unexpected error while updating job progress total", extra=self.logging_context()
            )
            raise JobStateError(f"Failed to update job progress total state: {e}")

        self.save_to_context({"job_progress_total": total, "job_progress_message": message})
        logger.debug("Updated progress total successfully for job", extra=self.logging_context())

    def is_cancelled(self) -> bool:
        """Check if job has been cancelled or should stop execution. This method does
        not flush or commit the database session; the caller is responsible for persisting changes.

        Convenience method for checking if the job should stop execution due to
        cancellation, pipeline failure, or other termination conditions. Jobs
        can use this for graceful shutdown.

        Returns:
            bool: True if job should stop execution, False if it can continue

        Raises:
            DatabaseConnectionError: Cannot fetch job status from database

        Example:
            >>> for item in large_dataset:
            ...     if manager.is_cancelled():
            ...         logger.info("Job cancelled, stopping gracefully")
            ...         break
            ...     process_item(item)
        """
        return self.get_job_status() in CANCELLED_JOB_STATUSES

    def should_retry(self) -> bool:
        """Check if job should be retried based on error type and retry count. This method does
        not flush or commit the database session; the caller is responsible for persisting changes.

        Convenience method that implements common retry logic. Checks current
        retry count against maximum and evaluates if the error type is retryable.

        Returns:
            bool: True if job should be retried, False otherwise

        Raises:
            DatabaseConnectionError: Cannot fetch job info from database

        Examples:
            >>> try:
            ...     result = do_work()
            ... except NetworkError as e:
            ...     manager.fail_job(e, result)
            ...     if manager.should_retry():
            ...         manager.retry_job()
            ...     else:
            ...         manager.fail_job(e, result)
        """
        job_run = self.get_job()
        try:
            self.save_to_context(
                {
                    "job_retry_count": job_run.retry_count,
                    "job_max_retries": job_run.max_retries,
                    "job_failure_category": str(job_run.failure_category) if job_run.failure_category else None,
                    "job_status": str(job_run.status),
                }
            )

            # Check if job is in FAILED state
            if job_run.status != JobStatus.FAILED:
                logger.debug("Job cannot be retried: not in FAILED state", extra=self.logging_context())
                return False

            # Check retry count
            current_retries = job_run.retry_count or 0
            if current_retries >= job_run.max_retries:
                logger.debug("Job cannot be retried: max retries reached", extra=self.logging_context())
                return False

            # Check if failure category is retryable
            if job_run.failure_category not in RETRYABLE_FAILURE_CATEGORIES:
                logger.debug("Job cannot be retried: failure category not retryable", extra=self.logging_context())
                return False

            logger.debug("Job is retryable", extra=self.logging_context())
            return True

        except (AttributeError, TypeError, KeyError, ValueError) as e:
            self.save_to_context(format_raised_exception_info_as_dict(e))
            logger.debug("Unexpected error checking retry eligibility", extra=self.logging_context())
            raise JobStateError(f"Failed to check retry eligibility state: {e}")

    def get_job_status(self) -> JobStatus:  # pragma: no cover
        """Get current job status for monitoring and debugging.

        Provides non-blocking access to job status without affecting job
        execution. Used by decorators and monitoring systems to check job state.

        Returns:
            JobStatus: Current job status (QUEUED, RUNNING, SUCCEEDED,
                                 FAILED, etc.).

        Raises:
            DatabaseConnectionError: Cannot connect to database, SQL query failed,
                                   or job not found (indicates data inconsistency)

        Examples:
            >>> status = manager.get_job_status()
            >>> if status == JobStatus.RUNNING:
            ...     logger.info("Job is currently executing")
        """
        return self.get_job().status

    def get_job(self) -> JobRun:
        """Get complete job information for monitoring and debugging.

        Retrieves full JobRun instance with all fields populated. Used by
        decorators and monitoring systems that need access to job metadata,
        progress, error details, or other comprehensive job information.

        Returns:
            JobRun: Complete job instance with all fields.

        Raises:
            DatabaseConnectionError: Cannot connect to database, SQL query failed,
                                   or job not found (indicates data inconsistency)

        Example:
            >>> job = manager.get_job()
            >>> if job:
            ...     logger.info(f"Job {job.urn} progress: {job.progress_current}/{job.progress_total}")
            ...     if job.error_message:
            ...         logger.error(f"Job error: {job.error_message}")
        """
        try:
            return self.db.execute(select(JobRun).where(JobRun.id == self.job_id)).scalar_one()
        except SQLAlchemyError as e:
            self.save_to_context(format_raised_exception_info_as_dict(e))
            logger.debug("Unexpected error fetching job info", extra=self.logging_context())
            raise DatabaseConnectionError(f"Failed to fetch job {self.job_id}: {e}")

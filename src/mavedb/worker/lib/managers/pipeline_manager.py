"""Pipeline coordination management for job dependencies and status.

This module provides the PipelineManager class for coordinating pipeline execution,
managing job dependencies, and updating pipeline status. The PipelineManager is
separated from individual job lifecycle management to provide clean separation of concerns.

Example usage:
    >>> from mavedb.worker.lib.pipeline_manager import PipelineManager
    >>>
    >>> # Initialize with database and Redis connections
    >>> pipeline_manager = PipelineManager(db_session, redis_client, pipeline_id=456)
    >>>
    >>> # Coordinate after a job completes
    >>> await pipeline_manager.coordinate_pipeline()
    >>>
    >>> # Update pipeline status
    >>> new_status = pipeline_manager.transition_pipeline_status()
    >>>
    >>> # Cancel remaining jobs when pipeline fails
    >>> cancelled_count = pipeline_manager.cancel_remaining_jobs(
    ...     reason="Dependency failed"
    ... )
    >>>
    >>> # Pause/unpause pipeline
    >>> was_paused = pipeline_manager.pause_pipeline("Maintenance")
    >>> was_unpaused = await pipeline_manager.unpause_pipeline("Complete")

Error Handling:
    The PipelineManager uses the same exception hierarchy as JobManager for consistency:

    - DatabaseConnectionError: Database connectivity issues
    - JobStateError: Critical state persistence failures
    - PipelineCoordinationError: Pipeline coordination failures
"""

import logging
from datetime import datetime, timedelta
from typing import Sequence

from arq import ArqRedis
from sqlalchemy import and_, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.job_dependency import JobDependency
from mavedb.models.job_run import JobRun
from mavedb.models.pipeline import Pipeline
from mavedb.worker.lib.managers import BaseManager, JobManager
from mavedb.worker.lib.managers.constants import (
    ACTIVE_JOB_STATUSES,
    CANCELLED_JOB_STATUSES,
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
from mavedb.worker.lib.managers.utils import (
    construct_bulk_cancellation_result,
    job_dependency_is_met,
    job_should_be_skipped_due_to_unfulfillable_dependency,
)

logger = logging.getLogger(__name__)


class PipelineManager(BaseManager):
    """Manages pipeline coordination and job dependencies with atomic operations.

    The PipelineManager provides a focused interface for coordinating pipeline execution
    without coupling to individual job lifecycle management. It handles dependency
    checking, status updates, and pipeline-wide operations like cancellation.

    Key Features:
    - Atomic pipeline status transitions with rollback on failure
    - Dependency-based job enqueueing with race condition prevention
    - Pipeline-wide cancellation with proper error handling
    - Separation from individual job lifecycle management
    - Consistent exception handling and logging

    Usage Patterns:

        Pipeline coordination after job completion:
        >>> manager = PipelineManager(db, redis, pipeline_id=123)
        >>> await manager.coordinate_pipeline()

        Manual pipeline operations:
        >>> # Update pipeline status based on current job states
        >>> new_status = manager.transition_pipeline_status()
        >>>
        >>> # Cancel remaining jobs
        >>> cancelled_count = manager.cancel_remaining_jobs(
        ...     reason="Manual cancellation"
        ... )
        >>>
        >>> # Pause pipeline execution
        >>> was_paused = manager.pause_pipeline(
        ...     reason="System maintenance"
        ... )
        >>>
        >>> # Resume pipeline execution
        >>> was_unpaused = await manager.unpause_pipeline(
        ...     reason="Maintenance complete"
        ... )

        Dependency management:
        >>> # Check if a job can be enqueued
        >>> can_run = manager.can_enqueue_job(job)
        >>>
        >>> # Enqueue all ready jobs (independent and dependent)
        >>> await manager.enqueue_ready_jobs()

        Pipeline monitoring:
        >>> # Get detailed progress statistics
        >>> progress = manager.get_pipeline_progress()
        >>> print(f"Pipeline {progress['completion_percentage']:.1f}% complete")
        >>>
        >>> # Get job counts by status
        >>> counts = manager.get_job_counts_by_status()
        >>> print(f"Failed jobs: {counts.get(JobStatus.FAILED, 0)}")

        Job retry and pipeline restart:
        >>> # Retry all failed jobs
        >>> retried_count = await manager.retry_failed_jobs()
        >>>
        >>> # Restart entire pipeline
        >>> restarted = await manager.restart_pipeline("Fixed issue")

    Thread Safety:
        PipelineManager is not thread-safe. Each instance should be used by a single
        worker thread and should not be shared across concurrent operations.
    """

    def __init__(self, db: Session, redis: ArqRedis, pipeline_id: int):
        """Initialize pipeline manager with database and Redis connections.

        Args:
            db: SQLAlchemy database session for job and pipeline queries
            redis: ARQ Redis client for job queue operations
            pipeline_id: ID of the pipeline this manager instance will coordinate

        Raises:
            DatabaseConnectionError: Cannot connect to database

        Example:
            >>> db_session = get_database_session()
            >>> redis_client = get_arq_redis_client()
            >>> manager = PipelineManager(db_session, redis_client, pipeline_id=456)
        """
        super().__init__(db, redis)
        self.pipeline_id = pipeline_id
        self.get_pipeline()  # Validate pipeline exists on init

    async def start_pipeline(self, coordinate: bool = True) -> None:
        """Start the pipeline

        Entry point to start pipeline execution. Sets pipeline status to RUNNING
        and enqueues independent jobs using coordinate pipeline if coordinate is True.

        Raises:
            DatabaseConnectionError: Cannot query or update pipeline
            PipelineStateError: Cannot update pipeline state
            PipelineCoordinationError: Failed to enqueue ready jobs

        Example:
            >>> # Start a new pipeline
            >>> await pipeline_manager.start_pipeline()
        """
        status = self.get_pipeline_status()

        if status != PipelineStatus.CREATED:
            logger.error(
                f"Pipeline {self.pipeline_id} is in a non-created state (current status: {status}) and may not be started"
            )
            raise PipelineTransitionError(f"Pipeline {self.pipeline_id} is in state {status} and may not be started")

        self.set_pipeline_status(PipelineStatus.RUNNING)
        self.db.flush()

        logger.info(f"Pipeline {self.pipeline_id} started successfully")

        # Allow controllable coordination logic. By default, we want to coordinate
        # immediately after starting to enqueue independent jobs. However, if a job
        # has already been enqueued and is beginning execution and starts the pipeline,
        # as a result of its job management decorator, we want to skip coordination here
        # so we do not double-enqueue jobs.
        if coordinate:
            await self.coordinate_pipeline()

    async def coordinate_pipeline(self) -> None:
        """Coordinate pipeline after a job completes.

        This is the main coordination entry point called after jobs complete.
        It updates pipeline status and enqueues ready jobs or cancels remaining jobs
        based on the completion result. The method operates on the entire pipeline
        state rather than tracking individual job completions.

        Raises:
            DatabaseConnectionError: Cannot query job or pipeline info
            PipelineStateError: Cannot update pipeline state
            PipelineCoordinationError: Failed to enqueue jobs or cancel remaining jobs
            JobStateError: Critical job state persistence failure
            JobTransitionError: Job cannot be transitioned from current state to new state


        Example:
            >>> # Called after successful job completion
            >>> await pipeline_manager.coordinate_pipeline()
        """
        new_status = self.transition_pipeline_status()
        self.db.flush()

        if new_status in CANCELLED_PIPELINE_STATUSES:
            self.cancel_remaining_jobs(reason="Pipeline failed or cancelled")

        # Only enqueue new jobs if pipeline is running
        if new_status in RUNNING_PIPELINE_STATUSES:
            await self.enqueue_ready_jobs()

            # After enqueuing jobs, re-evaluate pipeline status in case it changed.
            # We only expect the status to change if jobs with unsatisfiable dependencies were skipped.
            self.transition_pipeline_status()
            self.db.flush()

    def transition_pipeline_status(self) -> PipelineStatus:
        """Update pipeline status based on current job states.

        Analyzes the status distribution of all jobs in the pipeline to determine
        the appropriate pipeline status. Updates pipeline status and finished_at
        timestamp when the status changes to a terminal state.

        Returns:
            PipelineStatus: The current pipeline status after update. If unchanged, the
                            previous status is returned.

        Raises:
            DatabaseConnectionError: Cannot query job statuses or pipeline info
            JobStateError: Cannot update pipeline status or corrupted job data

        Status Logic:
        - FAILED: Any job has FAILED status
        - RUNNING: Any job is RUNNING or QUEUED
        - SUCCEEDED: All jobs are SUCCEEDED
        - PARTIAL: Mix of SUCCEEDED/SKIPPED/CANCELLED with no FAILED/RUNNING
        - CANCELLED: All remaining jobs are CANCELLED
        - No Change: If pipeline is PAUSED, CANCELLED, or has no jobs: status remains unchanged

        Example:
            >>> new_status = pipeline_manager.transition_pipeline_status()
            >>> print(f"Pipeline status is now {new_status}")
        """
        pipeline = self.get_pipeline()
        status_counts = self.get_job_counts_by_status()

        old_status = pipeline.status
        try:
            total_jobs = sum(status_counts.values())
            if old_status in TERMINAL_PIPELINE_STATUSES:
                logger.debug(f"Pipeline {self.pipeline_id} is in terminal status {old_status}; skipping update")
                return old_status  # No change from terminal state

            if old_status == PipelineStatus.PAUSED:
                logger.debug(f"Pipeline {self.pipeline_id} is paused; skipping status update")
                return old_status  # No change from paused state

            # The pipeline must not be in a terminal state (from above), but has no jobs. Consider it complete.
            if total_jobs == 0:
                logger.debug(f"No jobs found in pipeline {self.pipeline_id} - considering pipeline complete")

                self.set_pipeline_status(PipelineStatus.SUCCEEDED)
                return PipelineStatus.SUCCEEDED

        except (AttributeError, TypeError, KeyError, ValueError) as e:
            logger.debug(f"Invalid job status data for pipeline {self.pipeline_id}: {e}")
            raise PipelineStateError(f"Corrupted job status data for pipeline {self.pipeline_id}: {e}")

        # The pipeline is not in a terminal state and has jobs - determine new status
        try:
            if status_counts.get(JobStatus.FAILED, 0) > 0:
                new_status = PipelineStatus.FAILED
            elif status_counts.get(JobStatus.RUNNING, 0) > 0 or status_counts.get(JobStatus.QUEUED, 0) > 0:
                new_status = PipelineStatus.RUNNING

            # Pending jobs still exist, don't change the status.
            # These might be picked up soon, or they may be proactively
            # skipped later if dependencies cannot be met.
            #
            # Although there is a tension between having only pending
            # and succeeded jobs (which would suggest partial/succeeded),
            # we leave the status as-is until jobs are actually processed.
            #
            # *A pipeline with a terminal status must not have pending jobs*
            elif status_counts.get(JobStatus.PENDING, 0) > 0:
                new_status = old_status

            elif status_counts.get(JobStatus.SUCCEEDED, 0) > 0:
                succeeded_jobs = status_counts.get(JobStatus.SUCCEEDED, 0)
                skipped_jobs = status_counts.get(JobStatus.SKIPPED, 0)
                cancelled_jobs = status_counts.get(JobStatus.CANCELLED, 0)

                if succeeded_jobs == total_jobs:
                    new_status = PipelineStatus.SUCCEEDED
                    logger.debug(f"All jobs succeeded in pipeline {self.pipeline_id}")
                elif (succeeded_jobs + skipped_jobs + cancelled_jobs) == total_jobs:
                    new_status = PipelineStatus.PARTIAL
                    logger.debug(f"Pipeline {self.pipeline_id} completed partially: {status_counts}")
                else:
                    new_status = PipelineStatus.PARTIAL
                    logger.warning(f"Inconsistent job counts detected for pipeline {self.pipeline_id}: {status_counts}")
                    # TODO: Notification hooks
            else:
                new_status = PipelineStatus.CANCELLED

            if pipeline.status != new_status:
                self.set_pipeline_status(new_status)

        except (AttributeError, TypeError, KeyError, ValueError) as e:
            logger.debug(f"Object manipulation failed updating pipeline status for {self.pipeline_id}: {e}")
            raise PipelineStateError(f"Failed to update pipeline status for {self.pipeline_id}: {e}")

        if new_status != old_status:
            logger.info(f"Pipeline {self.pipeline_id} status successfully updated to {new_status} from {old_status}")
        else:
            logger.debug(f"No status change for pipeline {self.pipeline_id} (remains {old_status})")

        return new_status

    async def enqueue_ready_jobs(self) -> None:
        """Find and enqueue all jobs that are ready to run.

        Identifies pending jobs in the pipeline (including retries) whose dependencies
        are satisfied, updates their status to QUEUED, and enqueues them in ARQ.
        This handles both independent jobs and jobs with dependencies, as well as
        jobs that have been prepared for retry.

        Does not enqueue jobs if the pipeline is paused.

        Raises:
            DatabaseConnectionError: Cannot query pending jobs or job dependencies
            JobStateError: Cannot update job state to QUEUED (critical failure)
            PipelineCoordinationError: One or more jobs failed to enqueue in ARQ

        Process:
        1. Ensure pipeline is running (skip enqueues if not)
        2. Query all PENDING jobs in pipeline (includes retries)
        3. Check dependency requirements for each job
        4. For jobs ready to run: flush status change and enqueue in ARQ

        Note:
            - This method handles both independent and dependent jobs uniformly -
            any job in PENDING status that meets its dependency requirements
            (including jobs with no dependencies) will be enqueued, unless the
            pipeline is paused.

        Examples:
            Basic usage:
            >>> # Enqueue all ready jobs in the pipeline
            >>> await pipeline_manager.enqueue_ready_jobs()

            Handling coordination errors:
            >>> try:
            ...     await pipeline_manager.enqueue_ready_jobs()
            ... except PipelineCoordinationError as e:
            ...     logger.error(f"Failed to enqueue some jobs: {e}")
            ...     # Optionally cancel pipeline or take other recovery actions
        """
        current_status = self.get_pipeline_status()
        if current_status not in RUNNING_PIPELINE_STATUSES:
            logger.error(f"Pipeline {self.pipeline_id} is not running - skipping job enqueue")
            raise PipelineStateError(
                f"Pipeline {self.pipeline_id} is in status {current_status} and cannot enqueue jobs"
            )

        jobs_to_queue: list[JobRun] = []
        for job in self.get_pending_jobs():
            job_manager = JobManager(self.db, self.redis, job.id)

            # Attempt to enqueue the job if dependencies are met
            if self.can_enqueue_job(job):
                job_manager.prepare_queue()
                jobs_to_queue.append(job)
                continue

            should_skip, reason = self.should_skip_job_due_to_dependencies(job)
            if should_skip:
                job_manager.skip_job(
                    {
                        "status": "skipped",
                        "exception": None,
                        "data": {"result": reason, "timestamp": datetime.now().isoformat()},
                    }
                )
                logger.info(f"Skipped job {job.urn} due to unreachable dependencies: {reason}")
                continue

        # Ensure enqueued jobs can view the status change and pipelines
        # can view skipped jobs by flushing transactions.
        self.db.flush()

        if not jobs_to_queue:
            logger.debug(f"No ready jobs to enqueue in pipeline {self.pipeline_id}")
            return

        successfully_enqueued = []
        for job in jobs_to_queue:
            await self._enqueue_in_arq(job, is_retry=False)
            successfully_enqueued.append(job.urn)
            logger.info(f"Successfully enqueued job {job.urn}")

        logger.info(f"Successfully enqueued {len(successfully_enqueued)} jobs: {successfully_enqueued}.")

    def cancel_remaining_jobs(self, reason: str = "Pipeline cancelled") -> None:
        """Cancel all remaining jobs in the pipeline when the pipeline fails.

        Finds all active pipeline jobs and marks them as SKIPPED or CANCELLED
        to prevent further execution when the pipeline has failed. Records the
        cancellation reason and timestamp for audit purposes.

        Args:
            reason: Human-readable reason for cancellation

        Raises:
            DatabaseConnectionError: Cannot query jobs to cancel
            PipelineCoordinationError: Failed to cancel one or more jobs
        """
        remaining_jobs = self.get_active_jobs()
        if not remaining_jobs:
            logger.debug(f"No jobs to cancel in pipeline {self.pipeline_id}")
        else:
            bulk_cancellation_result = construct_bulk_cancellation_result(reason)

            for job in remaining_jobs:
                job_manager = JobManager(self.db, self.redis, job.id)

                # Skip PENDING jobs, cancel RUNNING/QUEUED jobs
                if job_manager.get_job_status() == JobStatus.PENDING:
                    job_manager.skip_job(result=bulk_cancellation_result)
                    logger.debug(f"Skipped job {job.urn}: {reason}")
                else:
                    job_manager.cancel_job(result=bulk_cancellation_result)
                    logger.debug(f"Cancelled job {job.urn}: {reason}")

        logger.info(f"Cancelled all remaining jobs in pipeline {self.pipeline_id}")

    async def cancel_pipeline(self, reason: str = "Pipeline cancelled") -> None:
        """Cancel the entire pipeline and all remaining jobs.

        Sets the pipeline status to CANCELLED and cancels all PENDING and QUEUED
        jobs in the pipeline. Records the cancellation reason for audit purposes.

        Args:
            reason: Human-readable reason for pipeline cancellation

        Raises:
            DatabaseConnectionError: Cannot query or update pipeline/jobs
            PipelineCoordinationError: Failed to cancel pipeline or jobs

        Example:
            >>> # Cancel a running pipeline due to external event
            >>> await pipeline_manager.cancel_pipeline(
            ...     reason="User requested cancellation"
            ... )
        """
        current_status = self.get_pipeline_status()

        if current_status in TERMINAL_PIPELINE_STATUSES:
            logger.error(f"Pipeline {self.pipeline_id} is already in terminal status {current_status}")
            raise PipelineTransitionError(
                f"Pipeline {self.pipeline_id} is in terminal state {current_status} and may not be cancelled"
            )

        self.set_pipeline_status(PipelineStatus.CANCELLED)
        self.db.flush()
        logger.info(f"Pipeline {self.pipeline_id} cancelled: {reason}")

        await self.coordinate_pipeline()

    async def pause_pipeline(self, reason: str = "Pipeline paused") -> None:
        """Pause the pipeline to stop further job execution.

        Sets the pipeline status to PAUSED, preventing new jobs from being enqueued
        while allowing currently running jobs to complete. This provides a way to
        temporarily halt pipeline execution without cancelling remaining jobs.

        Args:
            reason: Human-readable reason for pausing the pipeline

        Raises:
            DatabaseConnectionError: Cannot query or update pipeline
            JobStateError: Cannot update pipeline state
            PipelineTransitionError: Pipeline cannot be paused due to current state

        Example:
            >>> # Pause pipeline for maintenance
            >>> was_paused = manager.pause_pipeline(
            ...     reason="System maintenance"
            ... )
        """
        current_status = self.get_pipeline_status()

        if current_status in TERMINAL_PIPELINE_STATUSES:
            logger.error(f"Pipeline {self.pipeline_id} cannot be paused (current status: {current_status})")
            raise PipelineTransitionError(
                f"Pipeline {self.pipeline_id} is in terminal state {current_status} and may not be paused"
            )

        if current_status == PipelineStatus.PAUSED:
            logger.error(f"Pipeline {self.pipeline_id} is already paused")
            raise PipelineTransitionError(f"Pipeline {self.pipeline_id} is already paused")

        self.set_pipeline_status(PipelineStatus.PAUSED)
        self.db.flush()

        logger.info(f"Pipeline {self.pipeline_id} paused (was {current_status}): {reason}")
        await self.coordinate_pipeline()

    async def unpause_pipeline(self, reason: str = "Pipeline unpaused") -> None:
        """Unpause the pipeline and resume job execution.

        Sets the pipeline status from PAUSED back to RUNNING and enqueues any
        jobs that are ready to run. This resumes normal pipeline execution
        after a pause.

        Args:
            reason: Human-readable reason for unpausing the pipeline

        Raises:
            DatabaseConnectionError: Cannot query or update pipeline
            PipelineStateError: Cannot update pipeline state
            PipelineCoordinationError: Failed to enqueue ready jobs after unpause

        Example:
            >>> # Resume pipeline after maintenance
            >>> was_unpaused = await manager.unpause_pipeline(
            ...     reason="Maintenance complete"
            ... )
        """
        current_status = self.get_pipeline_status()

        if current_status != PipelineStatus.PAUSED:
            logger.error(
                f"Pipeline {self.pipeline_id} is not paused (current status: {current_status}) and may not be unpaused"
            )
            raise PipelineTransitionError(
                f"Pipeline {self.pipeline_id} is not paused (current status: {current_status}) and may not be unpaused"
            )

        self.set_pipeline_status(PipelineStatus.RUNNING)
        self.db.flush()

        logger.info(f"Pipeline {self.pipeline_id} unpaused (was {current_status}): {reason}")
        await self.coordinate_pipeline()

    async def restart_pipeline(self) -> None:
        """Restart the entire pipeline from the beginning.

        Resets ALL jobs in the pipeline to PENDING status, resets pipeline state to RUNNING, and re-enqueues
        independent jobs. This is useful for recovering from pipeline-wide issues.

        Raises:
            PipelineCoordinationError: If restart operations fail
            DatabaseConnectionError: If database operations fail

        Example:
            >>> success = await manager.restart_pipeline("Fixed configuration issue")
            >>> print(f"Pipeline restart: {'successful' if success else 'failed'}")
        """
        all_jobs = self.get_all_jobs()
        if not all_jobs:
            logger.debug(f"No jobs found for pipeline {self.pipeline_id} restart")
            return

        # Reset all jobs to PENDING status
        for job in all_jobs:
            job_manager = JobManager(self.db, self.redis, job.id)
            job_manager.reset_job()

        # Reset pipeline status to created
        self.set_pipeline_status(PipelineStatus.CREATED)
        self.db.flush()

        logger.info(f"Pipeline {self.pipeline_id} reset for restart successfully")
        await self.start_pipeline()

    def can_enqueue_job(self, job: JobRun) -> bool:
        """Check if a job can be enqueued based on dependency requirements.

        Validates that all job dependencies are satisfied according to their
        dependency types before allowing enqueue. Prevents premature execution
        of jobs that depend on incomplete predecessors.

        Args:
            job: JobRun instance to check dependencies for

        Returns:
            bool: True if all dependencies are satisfied and job can be enqueued,
                  False if dependencies are still pending

        Raises:
            DatabaseConnectionError: Cannot query job dependencies
            JobStateError: Corrupted dependency data detected

        Dependency Types:
            - SUCCESS_REQUIRED: Dependent job must have SUCCEEDED status
            - COMPLETION_REQUIRED: Dependent job must be SUCCEEDED or FAILED
        """
        for dependency, dependent_job in self.get_dependencies_for_job(job):
            try:
                if not job_dependency_is_met(
                    dependency_type=dependency.dependency_type,
                    dependent_job_status=dependent_job.status,
                ):
                    logger.debug(f"Job {job.urn} cannot be enqueued; dependency on job {dependent_job.urn} not met")
                    return False

            except (AttributeError, KeyError, TypeError, ValueError) as e:
                logger.debug(f"Invalid dependency data detected for job {job.id}: {e}")
                raise PipelineStateError(f"Corrupted dependency data during enqueue check for job {job.id}: {e}")

        logger.debug(f"All dependencies satisfied for job {job.urn}; ready to enqueue")
        return True

    def should_skip_job_due_to_dependencies(self, job: JobRun) -> tuple[bool, str]:
        """Check if a job's dependencies are unsatisfiable and the job should be skipped.

        Validates whether a job's dependencies can still be met based on the
        current status of dependent jobs. This helps identify jobs that should
        be skipped because their dependencies are in terminal non-success states.

        Args:
            job: JobRun instance to check dependencies for

        Returns:
            tuple[bool, str]: (True, reason) if dependencies cannot be met and job
                              should be skipped, (False, "") if dependencies may
                              still be satisfied

        Raises:
            DatabaseConnectionError: Cannot query job dependencies
            PipelineStateError: Critical state persistence failure

        Notes:
            - A job is considered unreachable if any of its dependencies that
              require SUCCESS have FAILED, SKIPPED, or CANCELLED status.
            - A job is considered unreachable if any of its dependencies that
              require COMPLETION have SKIPPED or CANCELLED status.

        Examples:
            Basic usage:
            >>> should_skip, reason = manager.should_skip_job_due_to_dependencies(job)
            >>> if should_skip:
            ...     print(f"Job should be skipped: {reason}")
            >>> else:
            ...     print("Job dependencies may still be satisfied")
        """
        for dependency, dep_job in self.get_dependencies_for_job(job):
            try:
                should_skip, reason = job_should_be_skipped_due_to_unfulfillable_dependency(
                    dependency_type=dependency.dependency_type,
                    dependent_job_status=dep_job.status,
                )

                if should_skip:
                    logger.debug(f"Job {job.urn} should be skipped due to dependency on job {dep_job.urn}: {reason}")
                    # guaranteed to be str if should_skip is True
                    return True, reason  # type: ignore

            except (AttributeError, KeyError, TypeError, ValueError) as e:
                logger.debug(f"Invalid dependency data detected for job {job.id}: {e}")
                raise PipelineStateError(f"Corrupted dependency data during skip check for job {job.id}: {e}")

        logger.debug(f"Job {job.urn} dependencies may still be satisfied; not skipping")
        return False, ""

    async def retry_failed_jobs(self) -> None:
        """Retry all failed jobs in the pipeline.

        Resets failed jobs to PENDING status and re-enqueues them for execution.
        Only affects jobs with FAILED status; other jobs remain unchanged.

        Raises:
            PipelineCoordinationError: If job retry fails
            DatabaseConnectionError: If database operations fail

        Example:
            >>> await manager.retry_failed_jobs()
            >>> print("Successfully retried failed jobs")
        """
        failed_jobs = self.get_failed_jobs()
        if not failed_jobs:
            logger.debug(f"No failed jobs found for pipeline {self.pipeline_id}")
            return

        for job in failed_jobs:
            job_manager = JobManager(self.db, self.redis, job.id)
            job_manager.prepare_retry()

        # Ensure the pipeline status is set to running so jobs are picked up
        self.set_pipeline_status(PipelineStatus.RUNNING)
        self.db.flush()

        await self.coordinate_pipeline()

    async def retry_unsuccessful_jobs(self) -> None:
        """Retry all unsuccessful jobs in the pipeline.

        Resets unsuccessful jobs (CANCELLED, SKIPPED, FAILED) to PENDING status
        and re-enqueues them for execution. This is useful for recovering from
        partial failures or interruptions.

        Raises:
            PipelineCoordinationError: If job retry fails
            DatabaseConnectionError: If database operations fail

        Example:
            >>> await manager.retry_unsuccessful_jobs()
            >>> print("Successfully retried unsuccessful jobs")
        """
        unsuccessful_jobs = self.get_unsuccessful_jobs()
        if not unsuccessful_jobs:
            logger.debug(f"No unsuccessful jobs found for pipeline {self.pipeline_id}")
            return

        for job in unsuccessful_jobs:
            job_manager = JobManager(self.db, self.redis, job.id)
            job_manager.prepare_retry()

        # Ensure the pipeline status is set to running so jobs are picked up
        self.set_pipeline_status(PipelineStatus.RUNNING)
        self.db.flush()

        await self.coordinate_pipeline()

    async def retry_pipeline(self) -> None:
        """Retry all unsuccessful jobs in the pipeline.

        Convenience method to retry all jobs that did not complete successfully,
        including CANCELLED, SKIPPED, and FAILED jobs. Resets their status to PENDING
        and re-enqueues them for execution.

        This is equivalent to calling `retry_unsuccessful_jobs` but provides a clearer
        semantic for pipeline-level retries.
        """
        await self.retry_unsuccessful_jobs()

    def get_jobs_by_status(self, status: list[JobStatus]) -> Sequence[JobRun]:
        """Get all jobs in the pipeline with a specific status.

        Args:
            status: JobStatus to filter jobs by

        Returns:
            Sequence[JobRun]: List of jobs with the specified status ordered by creation time

        Raises:
            DatabaseConnectionError: Cannot query job information

        Example:
            >>> running_jobs = manager.get_jobs_by_status([JobStatus.RUNNING])
            >>> print(f"Found {len(running_jobs)} running jobs")
        """
        try:
            return (
                self.db.execute(
                    select(JobRun)
                    .where(and_(JobRun.pipeline_id == self.pipeline_id, JobRun.status.in_(status)))
                    .order_by(JobRun.created_at)
                )
                .scalars()
                .all()
            )
        except SQLAlchemyError as e:
            logger.debug(
                f"Database query failed getting jobs with status {status} for pipeline {self.pipeline_id}: {e}"
            )
            raise DatabaseConnectionError(f"Failed to get jobs with status {status}: {e}")

    def get_pending_jobs(self) -> Sequence[JobRun]:
        """Get all PENDING jobs in the pipeline.

        Convenience method for fetching all pending jobs. This is equivalent
        to calling get_jobs_by_status([JobStatus.PENDING]) but provides
        clearer intent and a more focused API.

        Returns:
            Sequence[JobRun]: List of pending jobs ordered by creation time

        Raises:
            DatabaseConnectionError: Cannot query job information

        Example:
            >>> pending_jobs = manager.get_pending_jobs()
            >>> print(f"Found {len(pending_jobs)} pending jobs")
        """
        return self.get_jobs_by_status([JobStatus.PENDING])

    def get_running_jobs(self) -> Sequence[JobRun]:
        """Get all RUNNING jobs in the pipeline.

        Convenience method for fetching all running jobs. This is equivalent
        to calling get_jobs_by_status([JobStatus.RUNNING]) but provides
        clearer intent and a more focused API.

        Returns:
            Sequence[JobRun]: List of running jobs ordered by creation time

        Raises:
            DatabaseConnectionError: Cannot query job information

        Example:
            >>> running_jobs = manager.get_running_jobs()
            >>> print(f"Found {len(running_jobs)} running jobs")
        """
        return self.get_jobs_by_status([JobStatus.RUNNING])

    def get_active_jobs(self) -> Sequence[JobRun]:
        """Get all active jobs in the pipeline.

        Convenience method for fetching all active jobs. This is equivalent
        to calling get_jobs_by_status(ACTIVE_JOB_STATUSES) but provides
        clearer intent and a more focused API.

        Returns:
            Sequence[JobRun]: List of remaining jobs ordered by creation time

        Raises:
            DatabaseConnectionError: Cannot query job information

        Example:
            >>> active_jobs = manager.get_active_jobs()
            >>> print(f"Found {len(active_jobs)} active jobs")
        """
        return self.get_jobs_by_status(ACTIVE_JOB_STATUSES)

    def get_failed_jobs(self) -> Sequence[JobRun]:
        """Get all failed jobs in the pipeline.

        Convenience method for fetching all failed jobs. This is equivalent
        to calling get_jobs_by_status([JobStatus.FAILED]) but provides
        clearer intent and a more focused API.

        Returns:
            Sequence[JobRun]: List of failed jobs ordered by creation time

        Raises:
            DatabaseConnectionError: Cannot query job information

        Example:
            >>> failed_jobs = manager.get_failed_jobs()
            >>> print(f"Found {len(failed_jobs)} failed jobs for potential retry")
        """
        return self.get_jobs_by_status([JobStatus.FAILED])

    def get_unsuccessful_jobs(self) -> Sequence[JobRun]:
        """Get all unsuccessful jobs in the pipeline.

        Convenience method for fetching all unsuccessful (but terminated) jobs. This is equivalent
        to calling get_jobs_by_status([JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.SKIPPED])
        but provides clearer intent and a more focused API.

        Returns:
            Sequence[JobRun]: List of unsuccessful jobs ordered by creation time

        Raises:
            DatabaseConnectionError: Cannot query job information

        Example:
            >>> unsuccessful_jobs = manager.get_unsuccessful_jobs()
            >>> print(f"Found {len(unsuccessful_jobs)} unsuccessful jobs")
        """
        return self.get_jobs_by_status(CANCELLED_JOB_STATUSES)

    def get_all_jobs(self) -> Sequence[JobRun]:
        """Get all jobs in the pipeline regardless of status.

        Returns:
            Sequence[JobRun]: List of all jobs in pipeline ordered by creation time

        Raises:
            DatabaseConnectionError: Cannot query job information

        Examples:
            >>> all_jobs = manager.get_all_jobs()
            >>> print(f"Total jobs in pipeline: {len(all_jobs)}")
        """
        try:
            return (
                self.db.execute(
                    select(JobRun).where(JobRun.pipeline_id == self.pipeline_id).order_by(JobRun.created_at)
                )
                .scalars()
                .all()
            )
        except SQLAlchemyError as e:
            logger.debug(f"Database query failed getting all jobs for pipeline {self.pipeline_id}: {e}")
            raise DatabaseConnectionError(f"Failed to get all jobs: {e}")

    def get_dependencies_for_job(self, job: JobRun) -> Sequence[tuple[JobDependency, JobRun]]:
        """Get all dependencies for a specific job.

        Args:
            job: JobRun instance to fetch dependencies for

        Returns:
            Sequence[Row[tuple[JobDependency, JobRun]]]: List of dependencies with associated JobRun instances

        Raises:
            DatabaseConnectionError: Cannot query job dependencies

        Examples:
            >>> dependencies = manager.get_dependencies_for_job(job)
            >>> for dependency, dep_job in dependencies:
            ...     print(f"Job {job.urn} depends on job {dep_job.urn} with dependency type {dependency.dependency_type}")
        """
        try:
            # Although the returned type wraps tuples in a row, the contents are still accessible as tuples.
            # This allows unpacking as shown in the example, and we can ignore the type checker warning so
            # callers can have access to the simpler interface.
            return self.db.execute(
                select(JobDependency, JobRun)
                .join(JobRun, JobDependency.depends_on_job_id == JobRun.id)
                .where(JobDependency.id == job.id)
            ).all()  # type: ignore
        except SQLAlchemyError as e:
            logger.debug(f"SQL query failed for dependencies of job {job.id}: {e}")
            raise DatabaseConnectionError(f"Failed to get job dependencies for job {job.id}: {e}")

    def get_pipeline(self) -> Pipeline:
        """Get the Pipeline instance for this manager.

        Returns:
            Pipeline: The Pipeline instance associated with this manager

        Raises:
            DatabaseConnectionError: Cannot query pipeline information

        Examples:
            >>> pipeline = manager.get_pipeline()
            >>> print(f"Pipeline ID: {pipeline.id}, Status: {pipeline.status}")
        """

        try:
            return self.db.execute(select(Pipeline).where(Pipeline.id == self.pipeline_id)).scalar_one()
        except SQLAlchemyError as e:
            logger.debug(f"Database query failed getting pipeline {self.pipeline_id}: {e}")
            raise DatabaseConnectionError(f"Failed to get pipeline {self.pipeline_id}: {e}")

    def get_job_counts_by_status(self) -> dict[JobStatus, int]:
        """Get count of jobs by status for monitoring.

        Returns a simple dictionary mapping job statuses to their counts,
        useful for dashboard displays and monitoring systems.

        Returns:
            dict[JobStatus, int]: Dictionary mapping JobStatus to count

        Raises:
            DatabaseConnectionError: Cannot query job information

        Example:
            >>> counts = manager.get_job_counts_by_status()
            >>> print(f"Failed jobs: {counts.get(JobStatus.FAILED, 0)}")
        """
        try:
            job_counts = self.db.execute(
                select(JobRun.status, func.count(JobRun.id))
                .where(JobRun.pipeline_id == self.pipeline_id)
                .group_by(JobRun.status)
            ).all()
        except SQLAlchemyError as e:
            logger.debug(f"Database query failed getting job counts for pipeline {self.pipeline_id}: {e}")
            raise DatabaseConnectionError(f"Failed to get job counts for pipeline {self.pipeline_id}: {e}")

        return {status: count for status, count in job_counts}

    def get_pipeline_progress(self) -> dict:
        """Get detailed pipeline progress statistics.

        Provides comprehensive pipeline progress information including job counts,
        completion percentage, duration, and estimated completion time.

        Returns:
            dict: Pipeline progress statistics with the following keys:
                - total_jobs: Total number of jobs in pipeline
                - completed_jobs: Number of jobs in terminal states
                - successful_jobs: Number of successfully completed jobs
                - failed_jobs: Number of failed jobs
                - running_jobs: Number of currently running jobs
                - pending_jobs: Number of jobs waiting to run
                - completion_percentage: Percentage of jobs completed (0-100)
                - duration: Time pipeline has been running (in seconds)
                - status_counts: Dictionary of job counts by status

        Raises:
            DatabaseConnectionError: Cannot query pipeline or job information

        Example:
            >>> progress = manager.get_pipeline_progress()
            >>> print(f"Pipeline {progress['completion_percentage']:.1f}% complete")
        """
        status_counts = self.get_job_counts_by_status()
        pipeline = self.get_pipeline()

        try:
            total_jobs = sum(status_counts.values())

            if total_jobs == 0:
                return {
                    "total_jobs": 0,
                    "completed_jobs": 0,
                    "successful_jobs": 0,
                    "failed_jobs": 0,
                    "running_jobs": 0,
                    "pending_jobs": 0,
                    "completion_percentage": 100.0,
                    "duration": 0,
                    "status_counts": {},
                }

            # Calculate progress metrics
            successful_jobs = status_counts.get(JobStatus.SUCCEEDED, 0)
            failed_jobs = status_counts.get(JobStatus.FAILED, 0)
            running_jobs = status_counts.get(JobStatus.RUNNING, 0) + status_counts.get(JobStatus.QUEUED, 0)
            pending_jobs = status_counts.get(JobStatus.PENDING, 0)
            skipped_jobs = status_counts.get(JobStatus.SKIPPED, 0)
            cancelled_jobs = status_counts.get(JobStatus.CANCELLED, 0)

            completed_jobs = successful_jobs + failed_jobs + skipped_jobs + cancelled_jobs
            completion_percentage = (completed_jobs / total_jobs) * 100 if total_jobs > 0 else 0

            # Calculate duration
            duration = 0
            if pipeline.created_at:
                end_time = pipeline.finished_at or datetime.now()
                duration = int((end_time - pipeline.created_at).total_seconds())

        except (AttributeError, TypeError, KeyError, ValueError) as e:
            logger.debug(f"Invalid data detected calculating progress for pipeline {self.pipeline_id}: {e}")
            raise PipelineStateError(f"Corrupted data during progress calculation for pipeline {self.pipeline_id}: {e}")

        return {
            "total_jobs": total_jobs,
            "completed_jobs": completed_jobs,
            "successful_jobs": successful_jobs,
            "failed_jobs": failed_jobs,
            "running_jobs": running_jobs,
            "pending_jobs": pending_jobs,
            "completion_percentage": completion_percentage,
            "duration": duration,
            "status_counts": status_counts,
        }

    def get_pipeline_status(self) -> PipelineStatus:
        """Get the current status of the pipeline.

        Returns:
            PipelineStatus: Current status of the pipeline

        Raises:
            DatabaseConnectionError: Cannot query pipeline information

        Example:
            >>> status = manager.get_pipeline_status()
            >>> print(f"Pipeline status: {status}")
        """
        return self.get_pipeline().status

    def set_pipeline_status(self, new_status: PipelineStatus) -> None:
        """Set the status of the pipeline.

        Args:
            new_status: PipelineStatus enum value to set the pipeline to

        Raises:
            DatabaseConnectionError: Cannot query or update pipeline information
            PipelineStateError: Cannot update pipeline status

        Example:
            >>> manager.set_pipeline_status(PipelineStatus.PAUSED)
            >>> print("Pipeline paused")

        Note:
            This method does not perform any validation on the status transition,
            nor does it attempt to coordinate the pipeline after the status change
            or flush the change to the database.
        """
        pipeline = self.get_pipeline()
        try:
            pipeline.status = new_status

            # Ensure finished_at is set/cleared appropriately
            if new_status in TERMINAL_PIPELINE_STATUSES:
                pipeline.finished_at = datetime.now()
            else:
                pipeline.finished_at = None

            # Ensure started_at is set/cleared appropriately
            if new_status == PipelineStatus.CREATED:
                pipeline.started_at = None
            elif new_status == PipelineStatus.RUNNING and pipeline.started_at is None:
                pipeline.started_at = datetime.now()

        except (AttributeError, TypeError, KeyError, ValueError) as e:
            logger.debug(f"Object manipulation failed setting status for pipeline {self.pipeline_id}: {e}")
            raise PipelineStateError(f"Failed to set pipeline status for {self.pipeline_id}: {e}")

        logger.info(f"Pipeline {self.pipeline_id} status set to {new_status}")

    async def _enqueue_in_arq(self, job: JobRun, is_retry: bool) -> None:
        """Enqueue a job in ARQ with proper error handling and retry delay.

        Args:
            job: JobRun instance to enqueue
            is_retry: Whether this is a retry attempt

        Raises:
            PipelineCoordinationError: If ARQ enqueuing fails
        """
        try:
            defer_by = timedelta(seconds=job.retry_delay_seconds if is_retry and job.retry_delay_seconds else 0)
            arq_success = await self.redis.enqueue_job(job.job_function, job.id, _defer_by=defer_by, _job_id=job.urn)
        except Exception as e:
            logger.debug(f"ARQ enqueue operation failed for job {job.urn}: {e}")
            raise PipelineCoordinationError(f"Failed to enqueue job in ARQ: {e}")

        if arq_success:
            logger.info(f"{'Retried' if is_retry else 'Enqueued'} job {job.urn} in ARQ")
        else:
            logger.info(f"Job {job.urn} has already been enqueued in ARQ")

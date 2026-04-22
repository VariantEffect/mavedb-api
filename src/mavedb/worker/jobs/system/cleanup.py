"""Periodic cleanup job for detecting and handling stalled/zombie jobs.

This module provides a janitor job that runs periodically to find jobs that have
been stuck in intermediate states (QUEUED, RUNNING, PENDING) beyond reasonable
timeouts and handles them appropriately.

Jobs can get stuck due to:
- Worker crashes during execution
- Race conditions during enqueue (process crash between state change and ARQ enqueue)
- Network issues preventing state updates
- Database deadlocks or transaction failures

The cleanup job acts as a safety net to ensure jobs don't remain in limbo forever.
"""

import logging
from datetime import datetime, timedelta, timezone

from arq import ArqRedis
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.slack import send_slack_error
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.enums.job_pipeline import FailureCategory, JobStatus
from mavedb.models.job_run import JobRun
from mavedb.worker.lib.decorators.job_guarantee import with_guaranteed_job_run_record
from mavedb.worker.lib.decorators.job_management import with_job_management
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.managers.pipeline_manager import PipelineManager

logger = logging.getLogger(__name__)

# Timeout thresholds for detecting stalled jobs (in minutes).
# RUNNING_TIMEOUT_MINUTES must stay below ArqWorkerSettings.job_timeout (currently 2 hours)
# to avoid marking legitimately running jobs as stalled.
QUEUED_TIMEOUT_MINUTES = 10  # QUEUED jobs should start within 10 min
RUNNING_TIMEOUT_MINUTES = 90  # RUNNING jobs should complete within 90 min (30 min buffer under ARQ timeout)
PENDING_TIMEOUT_MINUTES = 30  # PENDING jobs in pipelines should be enqueued within 30 minutes


async def _handle_stalled_job_retry(
    job: JobRun,
    manager: JobManager,
    redis: ArqRedis,
    stall_reason: str,
    db: Session,
) -> bool:
    """Handle retry and enqueue for a stalled job.

    For pipeline jobs, the dependency state determines the recovery path before
    any retry bookkeeping occurs:

    - Unfulfillable dependency (terminal failure/cancel): skip directly without
      consuming retry budget — the job can never run regardless of retries.
    - Dependency not yet met (still running/pending): fail+retry back to PENDING
      so the pipeline manager will enqueue it once the dependency completes.
    - Dependency satisfied (or standalone job): fail+retry+enqueue via ARQ.

    Args:
        job: The stalled job to handle
        manager: JobManager for this job
        redis: ARQ Redis connection
        stall_reason: Human-readable reason for stalling
        db: Database session

    Returns:
        True if job was successfully handled, False if permanently failed
    """
    # For pipeline jobs, decide the recovery path upfront based on dependency state.
    # This keeps the three outcomes — skip, wait, enqueue — distinct and avoids
    # consuming the retry budget for jobs that can never run.
    if job.pipeline_id is not None:
        pipeline_manager = PipelineManager(db, redis, job.pipeline_id)

        should_skip, skip_reason = pipeline_manager.should_skip_job_due_to_dependencies(job)
        if should_skip:
            # Dependency is permanently unsatisfiable — skip directly without fail/retry.
            logger.info(
                f"Skipping stalled pipeline job {job.urn} due to unsatisfiable dependencies: {skip_reason}",
                extra=manager.logging_context(),
            )
            manager.skip_job(
                result=JobExecutionOutcome.skipped(
                    data={"reason": skip_reason, "timestamp": datetime.now().isoformat()}
                )
            )
            return True

        if not pipeline_manager.can_enqueue_job(job):
            # Dependencies exist but aren't terminal yet — retry back to PENDING and let
            # the pipeline manager enqueue the job when the dependency completes.
            logger.info(
                f"Stalled pipeline job {job.urn} dependencies not yet met - leaving in PENDING for pipeline manager",
                extra=manager.logging_context(),
            )
            manager.fail_job(
                result=JobExecutionOutcome.failed(
                    reason=stall_reason, data={"reason": stall_reason}, failure_category=FailureCategory.TIMEOUT
                ),
            )
            job.failure_category = FailureCategory.TIMEOUT
            db.flush()

            if not manager.should_retry():
                job.failure_category = FailureCategory.SYSTEM_ERROR
                db.flush()
                logger.warning(
                    f"Stalled job {job.urn} cannot be retried (max retries reached)", extra=manager.logging_context()
                )
                return False

            await manager.prepare_retry(reason=stall_reason)
            db.flush()
            return True

    # Standalone job or pipeline job whose dependencies are satisfied — fail, retry, and enqueue.
    manager.fail_job(
        result=JobExecutionOutcome.failed(
            reason=stall_reason, data={"reason": stall_reason}, failure_category=FailureCategory.TIMEOUT
        ),
    )
    job.failure_category = FailureCategory.TIMEOUT
    db.flush()

    if not manager.should_retry():
        job.failure_category = FailureCategory.SYSTEM_ERROR
        db.flush()
        logger.warning(
            f"Stalled job {job.urn} cannot be retried (max retries reached)", extra=manager.logging_context()
        )
        return False

    await manager.prepare_retry(reason=stall_reason)
    db.flush()

    try:
        manager.prepare_queue()  # Transition to QUEUED
        db.flush()
        result = await redis.enqueue_job(job.job_function, job.id, _job_id=job.urn)

        if result is None:
            raise RuntimeError(
                f"Failed to enqueue job {job.urn} when retrying stalled job - Redis did not return a job ID"
            )

        logger.info(f"Successfully retried and enqueued stalled job {job.urn}", extra=manager.logging_context())
        return True

    except Exception as e:
        logger.error(f"Failed to enqueue stalled job {job.urn}: {e}", extra=manager.logging_context())
        error_msg = f"Failed to enqueue after stall recovery: {e}"
        manager.fail_job(
            result=JobExecutionOutcome.failed(
                reason=error_msg, data={"reason": error_msg}, failure_category=FailureCategory.SYSTEM_ERROR
            ),
        )
        job.failure_category = FailureCategory.SYSTEM_ERROR  # Enqueue failures during cleanup are not retryable
        return False


@with_guaranteed_job_run_record("cron_job")
@with_job_management
async def cleanup_stalled_jobs(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    """Detect and handle jobs that have stalled in intermediate states.

    This job runs periodically (every 15 minutes) to find jobs that have been
    stuck in QUEUED, RUNNING, or PENDING states beyond reasonable timeouts
    and handles them appropriately.

    Stalled job detection criteria:
    - QUEUED: Created > 10 minutes ago but never started (stuck between prepare_queue and ARQ pickup)
    - RUNNING: Started > 60 minutes ago but not finished (worker likely crashed)
    - PENDING: Created > 30 minutes ago in a pipeline (coordination failure)

    Actions taken:
    - If job has retries remaining: Mark PENDING for retry (will be re-enqueued by pipeline)
    - If max retries reached: Mark FAILED with SYSTEM_ERROR category

    Args:
        ctx: ARQ worker context containing database session and redis connection
        job_id: ID of the current job run
        job_manager: JobManager instance for managing the current job run

    Returns:
        JobExecutionOutcome with counts of cleaned up jobs by state

    Example:
        Job stalled in QUEUED (crash during enqueue):
        - Job marked QUEUED but process crashed before ARQ enqueue
        - After 10 minutes, janitor detects and retries (or fails if max retries reached)

        Job stalled in RUNNING (worker crash):
        - Worker started job, marked it RUNNING, then crashed
        - After 60 minutes (longer than ARQ timeout), janitor detects and retries
    """
    # Setup initial context and progress
    job_manager.save_to_context(
        {
            "application": "mavedb-worker",
            "function": "cleanup_stalled_jobs",
            "resource": "stalled_jobs",
            "correlation_id": None,
            "thresholds": {
                "queued_timeout_minutes": QUEUED_TIMEOUT_MINUTES,
                "running_timeout_minutes": RUNNING_TIMEOUT_MINUTES,
                "pending_timeout_minutes": PENDING_TIMEOUT_MINUTES,
            },
        }
    )
    job_manager.update_progress(0, 100, "Starting cleanup of stalled jobs.")
    logger.debug(msg="Began cleanup of stalled jobs.", extra=job_manager.logging_context())

    # To properly handle retries and state transitions, we need the Redis connection to enqueue retry jobs
    assert job_manager.redis is not None, "Redis connection is required for cleanup_stalled_jobs"

    now = datetime.now(timezone.utc)
    cleaned_jobs: dict[str, list[str]] = {
        "queued": [],
        "running": [],
        "pending": [],
    }

    # Find QUEUED jobs that have been waiting too long
    # These likely got stuck during enqueue (state marked QUEUED but never reached ARQ)
    queued_threshold = now - timedelta(minutes=QUEUED_TIMEOUT_MINUTES)
    queued_jobs = job_manager.db.scalars(
        select(JobRun).where(
            JobRun.status == JobStatus.QUEUED,
            JobRun.started_at.is_(None),  # Never started
            JobRun.created_at < queued_threshold,  # Created long ago
        )
    ).all()

    job_manager.save_to_context({"stalled_queued_jobs_count": len(queued_jobs)})
    job_manager.update_progress(10, 100, f"Found {len(queued_jobs)} stalled QUEUED jobs to evaluate.")
    logger.debug("Cleaning stalled QUEUED jobs.", extra=job_manager.logging_context())

    for job in queued_jobs:
        manager = JobManager(job_manager.db, job_manager.redis, job.id)
        elapsed_minutes = (now - job.created_at).total_seconds() / 60

        logger.warning(
            f"Detected stalled QUEUED job {job.urn} "
            f"(created {job.created_at}, queued for {elapsed_minutes:.1f} minutes)",
            extra=manager.logging_context(),
        )

        # Use unified retry handler
        stall_reason = f"Job stalled in QUEUED state for {elapsed_minutes:.1f} minutes"
        await _handle_stalled_job_retry(job, manager, job_manager.redis, stall_reason, job_manager.db)

        manager.db.commit()
        cleaned_jobs["queued"].append(job.urn)

    job_manager.save_to_context({"cleaned_queued_jobs": queued_jobs})
    logger.debug("Completed cleaning stalled QUEUED jobs.", extra=job_manager.logging_context())

    # Find RUNNING jobs that have been running too long OR have missing started_at
    # These likely indicate worker crashes (worker died mid-execution) or data inconsistencies
    running_threshold = now - timedelta(minutes=RUNNING_TIMEOUT_MINUTES)
    running_jobs = job_manager.db.scalars(
        select(JobRun).where(
            JobRun.status == JobStatus.RUNNING,
            (JobRun.started_at < running_threshold)
            | (JobRun.started_at.is_(None)),  # Started long ago or missing timestamp
            JobRun.finished_at.is_(None),  # Not finished
        )
    ).all()

    job_manager.save_to_context({"stalled_running_jobs_count": len(running_jobs)})
    job_manager.update_progress(50, 100, f"Found {len(running_jobs)} stalled RUNNING jobs to evaluate.")
    logger.debug("Cleaning stalled RUNNING jobs.", extra=job_manager.logging_context())

    for job in running_jobs:
        manager = JobManager(job_manager.db, job_manager.redis, job.id)
        if not job.started_at:
            logger.error(
                f"RUNNING job {job.urn} has no started_at timestamp, cannot evaluate for stalling",
                extra=manager.logging_context(),
            )
            send_slack_error(
                f"Error in cleanup_stalled_jobs: RUNNING job {job.urn} has no started_at timestamp, cannot evaluate for stalling"
            )
            continue

        elapsed_minutes = (now - job.started_at).total_seconds() / 60

        logger.warning(
            f"Detected stalled RUNNING job {job.urn} "
            f"(started {job.started_at}, running for {elapsed_minutes:.1f} minutes)",
            extra=manager.logging_context(),
        )

        # Use unified retry handler
        stall_reason = f"Job stalled in RUNNING state for {elapsed_minutes:.1f} minutes (likely worker crash)"
        await _handle_stalled_job_retry(job, manager, job_manager.redis, stall_reason, job_manager.db)

        manager.db.commit()
        cleaned_jobs["running"].append(job.urn)

    job_manager.save_to_context({"cleaned_running_jobs": running_jobs})
    logger.debug("Completed cleaning stalled RUNNING jobs.", extra=job_manager.logging_context())

    # Find PENDING jobs that have been pending too long and should have moved on.
    # For pipeline jobs, treat them as stalled when they are either ready to run
    # now or permanently blocked by terminal dependency outcomes. Jobs waiting on
    # non-terminal dependencies are still in a legitimate waiting state.
    pending_threshold = now - timedelta(minutes=PENDING_TIMEOUT_MINUTES)
    pending_jobs = job_manager.db.scalars(
        select(JobRun).where(
            JobRun.status == JobStatus.PENDING,
            JobRun.created_at < pending_threshold,  # Created long ago
        )
    ).all()

    stalled_pending_jobs: list[JobRun] = []
    for job in pending_jobs:
        if job.pipeline_id is None:
            stalled_pending_jobs.append(job)
            continue

        pipeline_manager = PipelineManager(job_manager.db, job_manager.redis, job.pipeline_id)
        should_skip, _ = pipeline_manager.should_skip_job_due_to_dependencies(job)
        if pipeline_manager.can_enqueue_job(job) or should_skip:
            stalled_pending_jobs.append(job)

    job_manager.save_to_context({"stalled_pending_jobs_count": len(stalled_pending_jobs)})
    job_manager.update_progress(80, 100, f"Found {len(stalled_pending_jobs)} stalled PENDING jobs to evaluate.")
    logger.debug("Cleaning stalled PENDING jobs.", extra=job_manager.logging_context())

    for job in stalled_pending_jobs:
        manager = JobManager(job_manager.db, job_manager.redis, job.id)
        elapsed_minutes = (now - job.created_at).total_seconds() / 60

        logger.warning(
            f"Detected stalled PENDING job {job.urn} "
            f"(created {job.created_at}, pending for {elapsed_minutes:.1f} minutes)",
            extra=manager.logging_context(),
        )

        # Use unified retry handler
        stall_reason = f"Job stalled in PENDING state for {elapsed_minutes:.1f} minutes"
        await _handle_stalled_job_retry(job, manager, job_manager.redis, stall_reason, job_manager.db)

        manager.db.commit()
        cleaned_jobs["pending"].append(job.urn)

    job_manager.save_to_context({"cleaned_pending_jobs": stalled_pending_jobs})
    logger.debug("Completed cleaning stalled PENDING jobs.", extra=job_manager.logging_context())

    total_cleaned = sum(len(jobs) for jobs in cleaned_jobs.values())

    if total_cleaned > 0:
        logger.info(
            f"Cleanup complete: {total_cleaned} stalled jobs handled - "
            f"{len(cleaned_jobs['queued'])} queued, "
            f"{len(cleaned_jobs['running'])} running, "
            f"{len(cleaned_jobs['pending'])} pending",
            extra=job_manager.logging_context(),
        )
    else:
        logger.debug("Cleanup complete: No stalled jobs found", extra=job_manager.logging_context())

    job_manager.update_progress(100, 100, f"Cleanup complete: {total_cleaned} stalled jobs handled.")
    return JobExecutionOutcome.succeeded(
        data={
            "total_cleaned": total_cleaned,
            "queued_jobs": cleaned_jobs["queued"],
            "running_jobs": cleaned_jobs["running"],
            "pending_jobs": cleaned_jobs["pending"],
            "timestamp": now.isoformat(),
            "thresholds": {
                "queued_timeout_minutes": QUEUED_TIMEOUT_MINUTES,
                "running_timeout_minutes": RUNNING_TIMEOUT_MINUTES,
                "pending_timeout_minutes": PENDING_TIMEOUT_MINUTES,
            },
        }
    )

"""
Managed Job Decorator - Unified decorator for complete job lifecycle management.

Provides automatic job lifecycle tracking with support for async functions.
Includes JobManager injection for advanced operations and robust error handling.
"""

import asyncio
import functools
import inspect
import logging
from typing import Any, Awaitable, Callable, TypeVar, cast

from arq import ArqRedis
from sqlalchemy.orm import Session

from mavedb.lib.slack import send_slack_error, send_slack_job_error, send_slack_job_failure
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.enums.job_pipeline import FailureCategory, JobStatus
from mavedb.worker.lib.decorators.utils import ensure_ctx, ensure_job_id, ensure_session_ctx, is_test_mode
from mavedb.worker.lib.managers import JobManager
from mavedb.worker.lib.managers.constants import TERMINAL_JOB_STATUSES
from mavedb.worker.lib.managers.exceptions import JobManagerInitializationError
from mavedb.worker.lib.managers.utils import classify_exception

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def with_job_management(func: F) -> F:
    """
    Decorator that adds automatic job lifecycle management to ARQ worker functions.

    Features:
    - Job start/completion tracking with error handling
    - JobManager injection for advanced operations
    - Robust error handling with guaranteed state persistence

    The decorator injects a 'job_manager' parameter into the function that provides
    access to progress updates and the underlying JobManager.

    Args:
        func: The async function to decorate

    Returns:
        Decorated async function with lifecycle management
    """
    if not inspect.iscoroutinefunction(func):  # pragma: no cover
        raise ValueError("with_job_management decorator can only be applied to async functions")

    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        with ensure_session_ctx(ctx=ensure_ctx(args)):
            # No-op in test mode
            if is_test_mode():
                return await func(*args, **kwargs)

            return await _execute_managed_job(func, args, kwargs)

    return cast(F, async_wrapper)


async def _execute_managed_job(func: Callable[..., Awaitable[JobExecutionOutcome]], args: tuple, kwargs: dict) -> Any:
    """Execute a managed ARQ job with full lifecycle tracking."""
    try:
        ctx = ensure_ctx(args)
        db_session: Session = ctx["db"]
        job_id = ensure_job_id(args)

        if "redis" not in ctx:
            raise ValueError("Redis connection not found in job context")
        redis_pool: ArqRedis = ctx["redis"]
    except Exception as e:
        logger.critical(f"Failed to initialize job management context: {e}")
        send_slack_error(e)
        raise

    arq_job_id: str | None = ctx.get("job_id")
    result: JobExecutionOutcome | None = None
    job_manager: JobManager | None = None

    try:
        # Initialize JobManager
        job_manager = JobManager(db_session, redis_pool, job_id)

        # Inject the job manager into kwargs for access within the function
        kwargs["job_manager"] = job_manager

        # Check if the job was cancelled before ARQ picked it up. This race
        # occurs when a sibling job fails, the coordinator cancels remaining
        # QUEUED jobs in the DB, but those jobs are already in the Redis queue
        # waiting for ARQ to start them.
        current_status = job_manager.get_job_status()
        if current_status in TERMINAL_JOB_STATUSES:
            logger.info(f"Job {job_id} already in terminal state {current_status}; skipping execution")
            return JobExecutionOutcome.skipped(data={"reason": f"Job already in terminal state: {current_status}"})

        # Mark job as started and persist state
        job_manager.start_job()
        db_session.commit()

        # Execute the async function
        result = await func(*args, **kwargs)

        # Refresh job state after function execution
        job = job_manager.get_job()

        if result.status == JobStatus.FAILED:
            job_manager.fail_job(result=result)
            if not job_manager.should_retry():
                send_slack_job_failure(
                    job_urn=job.urn,
                    job_function=job.job_function,
                    reason=result.error or "",
                    failure_category=str(result.failure_category or ""),
                    retry_count=job.retry_count,
                    max_retries=job.max_retries,
                    will_retry=False,
                )

        elif result.status == JobStatus.ERRORED:
            job_manager.error_job(result=result)
            if not job_manager.should_retry():
                send_slack_job_error(
                    job_urn=job.urn,
                    job_function=job.job_function,
                    err=result.exception or Exception(result.error or "Unknown error"),
                    failure_category=str(result.failure_category or ""),
                    retry_count=job.retry_count,
                    max_retries=job.max_retries,
                    will_retry=False,
                )

        elif result.status == JobStatus.SKIPPED:
            job_manager.skip_job(result=result)
        else:
            job_manager.succeed_job(result=result)
        db_session.commit()

        if job_manager.should_retry():
            await job_manager.prepare_retry(reason="Job did not complete successfully")
            db_session.commit()

        return result

    # The coroutine is being cancelled — either ARQ hit job_timeout, or cleanup_stalled_jobs
    # aborted this attempt (Job.abort()) as part of stall recovery. CancelledError is a
    # BaseException, so the `except Exception` handler below never sees it; without this branch
    # the JobRun row would be orphaned as RUNNING forever while the coroutine dies.
    #
    # We mark the row terminally (FAILED / TIMEOUT) so DB state matches reality, then RE-RAISE.
    # Re-raising is essential: it lets ARQ record the cancellation as the job's result, which is
    # what makes a concurrent Job.abort() confirm the job actually died.
    except asyncio.CancelledError:
        logger.warning(f"Job {job_id} cancelled (timeout or stall-recovery abort); marking FAILED before re-raising")
        try:
            db_session.rollback()
            if job_manager is not None:
                job_manager.fail_job(
                    result=JobExecutionOutcome.failed(
                        reason="Job cancelled due to timeout, stall-recovery, or internal ARQ abort",
                        data={"reason": "cancelled"},
                        failure_category=FailureCategory.TIMEOUT,
                    )
                )
                db_session.commit()
                job = job_manager.get_job()
                send_slack_job_failure(
                    job_urn=job.urn,
                    job_function=job.job_function,
                    reason="Job cancelled due to timeout.",
                    failure_category=str(FailureCategory.TIMEOUT),
                    retry_count=job.retry_count,
                    max_retries=job.max_retries,
                    will_retry=False,
                )

        except Exception as inner_e:
            logger.critical(f"Failed to mark cancelled job {job_id} as failed: {inner_e}")
            send_slack_error(inner_e)
        raise

    except Exception as e:
        # Prioritize salvaging lifecycle state
        will_retry = False
        try:
            db_session.rollback()

            # Build errored result — this is an unhandled exception
            result = JobExecutionOutcome.errored(exception=e, failure_category=classify_exception(e))

            if job_manager is None:
                logger.critical(f"JobManager not initialized; cannot mark job as errored for job_id={job_id}")
                raise JobManagerInitializationError("JobManager failed to initialize for error handling") from e

            # Mark job as errored
            job_manager.error_job(result=result)
            db_session.commit()

            if job_manager.should_retry():
                will_retry = True

                # Prepare job for retry and persist state
                await job_manager.prepare_retry(reason=str(e))
                db_session.commit()

                # Short circuit raising the exception. We indicate to the caller
                # we did encounter a terminal failure and coordination should proceed.
                return result

        except Exception as inner_e:
            logger.critical(f"Failed to mark job {job_id} as errored: {inner_e}")
            send_slack_error(inner_e)

            # Re-raise the outer exception immediately to prevent duplicate notifications
        finally:
            logger.error(f"Job {job_id} failed: {e}")
            # Only alert when the job is permanently terminal — if it will retry,
            # the next attempt may succeed and no human action is required.
            if not will_retry:
                try:
                    if job_manager is None:
                        logger.critical(f"JobManager not initialized; cannot mark job as errored for job_id={job_id}")
                        raise JobManagerInitializationError("JobManager failed to initialize for error handling") from e

                    job = job_manager.get_job()
                    send_slack_job_error(
                        job_urn=job.urn,
                        job_function=job.job_function,
                        err=e,
                        failure_category=str(classify_exception(e)),
                        retry_count=job.retry_count,
                        max_retries=job.max_retries,
                        will_retry=False,
                    )
                except Exception:
                    send_slack_error(e)

            # Swallow the exception after alerting so ARQ can finish the job cleanly and log results.
            # We don't mind that we lose ARQs built in job marking, since we perform our own job
            # lifecycle management via with_job_management.
            return result

    finally:
        # Flush the job manager's accumulated context into ctx["state"] so that
        # log_job (after_job_end hook) can emit it as the canonical worker job log.
        if job_manager is not None and arq_job_id and isinstance(ctx.get("state"), dict):
            ctx["state"][arq_job_id] = job_manager.context

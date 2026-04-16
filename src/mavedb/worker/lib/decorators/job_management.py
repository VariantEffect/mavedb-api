"""
Managed Job Decorator - Unified decorator for complete job lifecycle management.

Provides automatic job lifecycle tracking with support for async functions.
Includes JobManager injection for advanced operations and robust error handling.
"""

import functools
import inspect
import logging
from typing import Any, Awaitable, Callable, TypeVar, cast

from arq import ArqRedis
from sqlalchemy.orm import Session

from mavedb.lib.slack import send_slack_error
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.enums.job_pipeline import JobStatus
from mavedb.worker.lib.decorators.utils import ensure_ctx, ensure_job_id, ensure_session_ctx, is_test_mode
from mavedb.worker.lib.managers import JobManager

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

    try:
        # Initialize JobManager
        job_manager = JobManager(db_session, redis_pool, job_id)

        # Inject the job manager into kwargs for access within the function
        kwargs["job_manager"] = job_manager

        # Mark job as started and persist state
        job_manager.start_job()
        db_session.commit()

        # Execute the async function
        result = await func(*args, **kwargs)

        # Move job to final state based on result status
        if result.status == JobStatus.FAILED:
            job_manager.fail_job(result=result)
            if result.error:
                send_slack_error(result.error)

        elif result.status == JobStatus.ERRORED:
            job_manager.error_job(result=result)
            send_slack_error(result.exception or result.error)

        elif result.status == JobStatus.SKIPPED:
            job_manager.skip_job(result=result)
        else:
            job_manager.succeed_job(result=result)
        db_session.commit()

        # If the job is not marked as succeeded, check if we should retry
        if job_manager.get_job_status() != JobStatus.SUCCEEDED and job_manager.should_retry():
            job_manager.prepare_retry(reason="Job did not complete successfully")
            db_session.commit()

        return result

    except Exception as e:
        # Prioritize salvaging lifecycle state
        try:
            db_session.rollback()

            # Build errored result — this is an unhandled exception
            result = JobExecutionOutcome.errored(exception=e)

            # Mark job as errored
            job_manager.error_job(result=result)
            db_session.commit()

            if job_manager.should_retry():
                # Prepare job for retry and persist state
                job_manager.prepare_retry(reason=str(e))
                db_session.commit()

                # Short circuit raising the exception. We indicate to the caller
                # we did encounter a terminal failure and coordination should proceed.
                return result

        except Exception as inner_e:
            logger.critical(f"Failed to mark job {job_id} as errored: {inner_e}")

            # Notify separately about inner failure, which affects job persistence
            send_slack_error(inner_e)

            # Re-raise the outer exception immediately to prevent duplicate notifications
        finally:
            logger.error(f"Job {job_id} failed: {e}")

            # Notify about the original exception
            send_slack_error(e)

            # Swallow the exception after alerting so ARQ can finish the job cleanly and log results.
            # We don't mind that we lose ARQs built in job marking, since we perform our own job
            # lifecycle management via with_job_management.
            return result

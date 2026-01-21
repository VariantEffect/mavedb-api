"""
Managed Job Decorator - Unified decorator for complete job lifecycle management.

Provides automatic job lifecycle tracking with support for both sync and async functions.
Includes JobManager injection for advanced operations and robust error handling.
"""

import functools
import inspect
import logging
from typing import Any, Awaitable, Callable, TypeVar, cast

from arq import ArqRedis
from sqlalchemy.orm import Session

from mavedb.worker.lib.decorators.utils import is_test_mode
from mavedb.worker.lib.managers import JobManager
from mavedb.worker.lib.managers.types import JobResultData

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

    Example:
    ```
        @with_job_management
        async def my_job_function(ctx, param1, param2, job_manager: JobManager):
            job_manager.update_progress(10, message="Starting work")

            # Access JobManager for advanced operations
            job_info = job_manager.get_job_info()

            # Do work...
            job_manager.update_progress(50, message="Halfway done")

            # More work...
            job_manager.update_progress(100, message="Complete")

            return {"result": "success"}
    ```

    Args:
        func: The async function to decorate

    Returns:
        Decorated async function with lifecycle management
    """
    if not inspect.iscoroutinefunction(func):  # pragma: no cover
        raise ValueError("with_job_management decorator can only be applied to async functions")

    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        # No-op in test mode
        if is_test_mode():
            return await func(*args, **kwargs)

        return await _execute_managed_job(func, args, kwargs)

    return cast(F, async_wrapper)


async def _execute_managed_job(func: Callable[..., Awaitable[JobResultData]], args: tuple, kwargs: dict) -> Any:
    """
    Execute a managed ARQ job with full lifecycle tracking.

    This function handles the complete job lifecycle including:
    - JobManager initialization from context
    - Job start tracking
    - ProgressTracker injection
    - Async function execution
    - Job completion tracking
    - Error handling and cleanup

    Args:
        func: Async function to execute
        args: Function arguments
        kwargs: Function keyword arguments

    Returns:
        Function result

    Raises:
        Exception: Re-raises any exception after proper job failure tracking
    """
    # Extract context (implicit first argument by ARQ convention)
    if not args:
        raise ValueError("Managed job functions must receive context as first argument")
    ctx = args[0]

    # Get database session and job ID from context
    if "db" not in ctx:
        raise ValueError("DB session not found in job context")
    if "redis" not in ctx:
        raise ValueError("Redis connection not found in job context")

    # Extract job_id (second argument by MaveDB convention)
    if not args or len(args) < 2 or not isinstance(args[1], int):
        raise ValueError("Job ID not found in pipeline context")
    job_id = args[1]

    db_session: Session = ctx["db"]
    redis_pool: ArqRedis = ctx["redis"]

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

        # Mark job as succeeded and persist state
        job_manager.succeed_job(result=result)
        db_session.commit()

        return result

    except Exception as e:
        # Prioritize salvaging lifecycle state
        try:
            db_session.rollback()

            # Build failure result data
            result = {
                "status": "failed",
                "data": {},
                "exception_details": {
                    "type": type(e).__name__,
                    "message": str(e),
                    "traceback": None,  # Could be populated with actual traceback if needed
                },
            }

            # Mark job as failed
            job_manager.fail_job(result=result, error=e)
            db_session.commit()

            # TODO: Decide on retry logic based on exception type and result.
            if job_manager.should_retry():
                # Prepare job for retry and persist state
                job_manager.prepare_retry(reason=str(e))
                db_session.commit()

                result["status"] = "retried"

                # short circuit raising the exception. We indicate to the caller
                # we did encounter a terminal failure and coordination should proceed.
                return result

        except Exception as inner_e:
            logger.critical(f"Failed to mark job {job_id} as failed: {inner_e}")

            # TODO: Notification hooks

            # Re-raise the outer exception immediately to prevent duplicate notifications
        finally:
            logger.error(f"Job {job_id} failed: {e}")

            # TODO: Notification hooks

            # Swallow the exception after alerting so ARQ can finish the job cleanly and log results.
            # We don't mind that we lose ARQs built in job marking, since we perform our own job
            # lifecycle management via with_job_management.
            return result


# Export decorator at module level for easy import
__all__ = ["with_job_management"]

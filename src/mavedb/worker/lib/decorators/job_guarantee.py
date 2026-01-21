"""
Job Guarantee Decorator - Ensures a JobRun record is persisted before job execution.

This decorator guarantees that a corresponding JobRun record is created and tracked for the decorated
function in the database before execution begins. It is designed to be stacked before managed job
decorators (such as with_job_management) to provide a consistent audit trail and robust error handling
for all job entrypoints, including cron-triggered jobs.

Features:
- Persists JobRun with job_type, function name, and parameters
- Integrates cleanly with managed job and pipeline decorators

Example:
    @with_guaranteed_job_run_record("cron_job")
    @with_job_management
    async def my_cron_job(ctx, ...):
        ...
"""

import functools
from typing import Any, Awaitable, Callable, TypeVar

from sqlalchemy.orm import Session

from mavedb import __version__
from mavedb.models.enums.job_pipeline import JobStatus
from mavedb.models.job_run import JobRun
from mavedb.worker.lib.decorators.utils import is_test_mode
from mavedb.worker.lib.managers.types import JobResultData

F = TypeVar("F", bound=Callable[..., Awaitable[Any]])


def with_guaranteed_job_run_record(job_type: str) -> Callable[[F], F]:
    """
    Async decorator to ensure a JobRun record is created and persisted before executing the job function.
    Should be applied before the managed job decorator.

    Args:
        job_type (str): The type/category of the job (e.g., "cron_job", "data_processing").

    Returns:
        Decorated async function with job run persistence guarantee.

    Example:
    ```
        @with_guaranteed_job_run_record("cron_job")
        @with_job_management
        async def my_cron_job(ctx, ...):
            ...
    ```
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            # No-op in test mode
            if is_test_mode():
                return await func(*args, **kwargs)

            # The job id must be passed as the second argument to the wrapped function.
            job = _create_job_run(job_type, func, args, kwargs)
            args = list(args)
            args.insert(1, job.id)
            args = tuple(args)

            return await func(*args, **kwargs)

        return async_wrapper  # type: ignore

    return decorator


def _create_job_run(job_type: str, func: Callable[..., Awaitable[JobResultData]], args: tuple, kwargs: dict) -> None:
    """
    Creates and persists a JobRun record for a function before job execution.
    """
    # Extract context (implicit first argument by ARQ convention)
    if not args:
        raise ValueError("Managed job functions must receive context as first argument")
    ctx = args[0]

    # Get database session from context
    if "db" not in ctx:
        raise ValueError("DB session not found in job context")

    db: Session = ctx["db"]

    job_run = JobRun(
        job_type=job_type,
        job_function=func.__name__,
        status=JobStatus.PENDING,
        mavedb_version=__version__,
    )
    db.add(job_run)
    db.commit()

    return job_run

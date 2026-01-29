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
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.models.enums.job_pipeline import PipelineStatus
from mavedb.models.job_run import JobRun
from mavedb.worker.lib.decorators import with_job_management
from mavedb.worker.lib.decorators.utils import ensure_ctx, ensure_job_id, ensure_session_ctx, is_test_mode
from mavedb.worker.lib.managers import PipelineManager
from mavedb.worker.lib.managers.types import JobResultData

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def with_pipeline_management(func: F) -> F:
    """
    Decorator that adds automatic pipeline lifecycle management to ARQ worker functions. Practically,
    this means calling `PipelineManager.coordinate_pipeline()` after the decorated function completes.

    This decorator performs no pipeline coordination prior to function execution; it only
    coordinates the pipeline after the function has run (whether successfully or with failure).
    As a result, this decorator is best suited for jobs that represent discrete steps within a pipeline.
    Pipelines are expected to be pre-defined and associated with jobs prior to execution and should be transitioned
    to a running state by other means (e.g. a dedicated pipeline starter job). Attempting to start pipelines
    within this decorator is not supported, and doing so may lead to unexpected behavior.

    Because pipeline management depends on job management, this decorator is built on top of the
    `with_job_management` decorator.

    This decorator may be added to jobs which may or may not belong to a pipeline. If the job does not
    belong to a pipeline, the decorator will simply skip pipeline coordination steps. Although pipeline
    membership is optional, the decorator still will always enforce job lifecycle management via
    `with_job_management`.

    Features:
    - Pipeline lifecycle tracking
    - Job lifecycle tracking via with_job_management
    - Robust error handling, logging, and TODO(alerting) on failures

    Example:
        @with_pipeline_management
        async def my_job_function(ctx, param1, param2):
            ... job logic ...

        On decorator exit, pipeline coordination is attempted.

    Args:
        func: The async function to decorate

    Returns:
        Decorated async function with lifecycle management
    """
    if not inspect.iscoroutinefunction(func):  # pragma: no cover
        raise ValueError("with_pipeline_management decorator can only be applied to async functions")

    # Wrap the function with job management. It isn't as simple as stacking decorators
    # as we can only call job management after setting up pipeline management.

    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        with ensure_session_ctx(ctx=ensure_ctx(args)):
            # No-op in test mode
            if is_test_mode():
                return await func(*args, **kwargs)

            return await _execute_managed_pipeline(func, args, kwargs)

    return cast(F, async_wrapper)


async def _execute_managed_pipeline(func: Callable[..., Awaitable[JobResultData]], args: tuple, kwargs: dict) -> Any:
    """
    Execute the managed pipeline function with lifecycle management.

    Args:
        func: The async function to execute.
        args: Positional arguments for the function.
        kwargs: Keyword arguments for the function.

    Returns:
        Any: The result of the function execution.

    Raises:
        Exception: Propagates any exception raised during function execution.
    """
    ctx = ensure_ctx(args)
    job_id = ensure_job_id(args)
    db_session: Session = ctx["db"]

    if "redis" not in ctx:
        raise ValueError("Redis connection not found in pipeline context")
    redis_pool: ArqRedis = ctx["redis"]

    pipeline_manager = None
    pipeline_id = None
    try:
        # Attempt to load the pipeline ID from the job.
        # - If pipeline_id is not None, initialize PipelineManager
        # - If None, skip pipeline coordination. We do not enforce every job to belong to a pipeline.
        # - If error occurs, handle below
        pipeline_id = db_session.execute(select(JobRun.pipeline_id).where(JobRun.id == job_id)).scalar_one()
        if pipeline_id:
            pipeline_manager = PipelineManager(db=db_session, redis=redis_pool, pipeline_id=pipeline_id)

        logger.info(f"Pipeline ID for job {job_id} is {pipeline_id}. Coordinating pipeline.")

        # If the pipeline is still in the created state, start it now. From this context,
        # we do not wish to coordinate the pipeline. Doing so would result in the current
        # job being re-queued before it has been marked as running, leading to potential state
        # inconsistencies.
        if pipeline_manager and pipeline_manager.get_pipeline_status() == PipelineStatus.CREATED:
            await pipeline_manager.start_pipeline(coordinate=False)
            db_session.commit()

            logger.info(f"Pipeline {pipeline_id} associated with job {job_id} started successfully")

        # Wrap the function with job management, then execute. This ensures both:
        # - Job lifecycle management is nested within pipeline management
        # - Exceptions from the job management layer are caught here for pipeline coordination
        job_managed_func = with_job_management(func)
        result = await job_managed_func(*args, **kwargs)

        # Attempt to coordinate pipeline next steps after successful job execution
        if pipeline_manager:
            await pipeline_manager.coordinate_pipeline()

            # Commit any changes made during pipeline coordination
            db_session.commit()

            logger.info(f"Pipeline {pipeline_id} associated with job {job_id} coordinated successfully")
        else:
            logger.info(f"No pipeline associated with job {job_id}; skipping coordination")

        return result

    except Exception as e:
        try:
            # Rollback any uncommitted changes
            db_session.rollback()

            # Attempt one final coordination to clean up any stubborn pipeline state
            if pipeline_manager:
                await pipeline_manager.coordinate_pipeline()

                # Commit any changes made during final coordination
                db_session.commit()

        except Exception as inner_e:
            logger.critical(
                f"Unable to perform cleanup coordination on pipeline {pipeline_id} associated with job {job_id} after error: {inner_e}"
            )

            # No further work here. We can rely on the notification hooks below to alert on the original failure
            # and should allow result generation to proceed as normal so the job can be logged.
        finally:
            logger.error(f"Pipeline {pipeline_id} associated with job {job_id} failed to coordinate: {e}")

            # Build job result data for failure
            result = {"status": "failed", "data": {}, "exception": e}

            # TODO: Notification hooks

            # Swallow the exception after alerting so ARQ can finish the job cleanly and log results.
            # We don't mind that we lose ARQs built in job marking, since we perform our own job
            # lifecycle management via with_job_management.
            return result

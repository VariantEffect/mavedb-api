"""Database materialized view refresh jobs.

This module contains jobs for refreshing materialized views used throughout
the MaveDB application. Materialized views provide optimized, pre-computed
data for complex queries and are refreshed periodically to maintain
data consistency and performance.
"""

import logging

from mavedb.db.view import refresh_all_mat_views
from mavedb.models.published_variant import PublishedVariantsMV
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.job_guarantee import with_guaranteed_job_run_record
from mavedb.worker.lib.decorators.job_management import with_job_management
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.managers.types import JobResultData

logger = logging.getLogger(__name__)


# TODO#405: Refresh materialized views within an executor.
@with_guaranteed_job_run_record("cron_job")
@with_job_management
async def refresh_materialized_views(ctx: dict, job_id: int, job_manager: JobManager) -> JobResultData:
    """Refresh all materialized views in the database.

    This job refreshes all materialized views to ensure that they are up-to-date
    with the latest data. It is typically run as a scheduled cron job and meant
    to be invoked indirectly via a job queue system.

    Args:
        ctx (dict): The job context dictionary.
        job_id (int): The ID of the job run.
        job_manager (JobManager): Manager for job lifecycle and DB operations.

    Side Effects:
        - Refreshes all materialized views in the database.

    Returns:
        dict: Result indicating success and any exception details
    """
    # Setup initial context and progress
    job_manager.save_to_context(
        {
            "application": "mavedb-worker",
            "function": "refresh_materialized_views",
            "resource": "all_materialized_views",
            "correlation_id": None,
        }
    )
    job_manager.update_progress(0, 100, "Starting refresh of all materialized views.")
    logger.debug(msg="Began refresh of all materialized views.", extra=job_manager.logging_context())

    # Do refresh
    refresh_all_mat_views(job_manager.db)
    job_manager.db.flush()

    # Finalize job state
    job_manager.update_progress(100, 100, "Completed refresh of all materialized views.")
    logger.debug(msg="Done refreshing materialized views.", extra=job_manager.logging_context())

    return {"status": "ok", "data": {}, "exception_details": None}


@with_pipeline_management
async def refresh_published_variants_view(ctx: dict, job_id: int, job_manager: JobManager) -> JobResultData:
    """Refresh the published variants materialized view.

    This job refreshes the PublishedVariantsMV materialized view to ensure that it
    is up-to-date with the latest data. It is meant to be invoked as part of a job queue system.

    Args:
        ctx (dict): The job context dictionary.
        job_id (int): The ID of the job run.
        job_manager (JobManager): Manager for job lifecycle and DB operations.

    Side Effects:
        - Refreshes the PublishedVariantsMV materialized view in the database.

    Returns:
        dict: Result indicating success and any exception details
    """
    # Get the job definition we are working on
    job = job_manager.get_job()

    _job_required_params = ["correlation_id"]
    validate_job_params(_job_required_params, job)

    # Fetch required resources based on param inputs. Safely ignore mypy warnings here, as they were checked above.
    correlation_id = job.job_params["correlation_id"]  # type: ignore

    # Setup initial context and progress
    job_manager.save_to_context(
        {
            "application": "mavedb-worker",
            "function": "refresh_published_variants_view",
            "resource": "published_variants_materialized_view",
            "correlation_id": correlation_id,
        }
    )
    job_manager.update_progress(0, 100, "Starting refresh of published variants materialized view.")
    logger.info(msg="Started refresh of published variants materialized view", extra=job_manager.logging_context())

    # Do refresh
    PublishedVariantsMV.refresh(job_manager.db)
    job_manager.db.flush()

    # Finalize job state
    job_manager.update_progress(100, 100, "Completed refresh of published variants materialized view.")
    logger.debug(msg="Done refreshing published variants materialized view.", extra=job_manager.logging_context())

    return {"status": "ok", "data": {}, "exception_details": None}

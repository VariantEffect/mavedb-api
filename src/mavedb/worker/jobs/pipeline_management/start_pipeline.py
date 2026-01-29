import logging

from mavedb.lib.exceptions import PipelineNotFoundError
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.managers.pipeline_manager import PipelineManager
from mavedb.worker.lib.managers.types import JobResultData

logger = logging.getLogger(__name__)


@with_pipeline_management
async def start_pipeline(ctx: dict, job_id: int, job_manager: JobManager) -> JobResultData:
    """Start the pipeline associated with the given job.

    This job initializes and starts the pipeline execution process.
    It sets up the necessary pipeline management context and triggers
    the pipeline coordination.

    NOTE: This function requires a dedicated 'start_pipeline' job run record
    in the database. This job run must be created prior to invoking this function
    and should be associated with the pipeline to be started.

    Args:
        ctx (dict): The job context dictionary.
        job_id (int): The ID of the job run.
        job_manager (JobManager): Manager for job lifecycle and DB operations.

    Side Effects:
        - Initializes and starts the pipeline execution.

    Returns:
        dict: Result indicating success and any exception details
    """
    # Setup initial context and progress
    job_manager.save_to_context(
        {
            "application": "mavedb-worker",
            "function": "start_pipeline",
            "resource": f"pipeline_for_job_{job_id}",
            "correlation_id": None,
        }
    )
    job_manager.update_progress(0, 100, "Coordinating pipeline for the first time.")
    logger.debug(msg="Coordinating pipeline for the first time.", extra=job_manager.logging_context())

    if not job_manager.pipeline_id:
        return {
            "status": "exception",
            "data": {},
            "exception": PipelineNotFoundError("No pipeline associated with this job."),
        }

    # Initialize PipelineManager and coordinate pipeline. The pipeline manager decorator
    # will have started the pipeline for us already, but doesn't coordinate on start automatically.
    redis = job_manager.redis or ctx["redis"]
    pipeline_manager = PipelineManager(job_manager.db, redis, job_manager.pipeline_id)
    await pipeline_manager.coordinate_pipeline()

    # Finalize job state
    job_manager.db.flush()
    job_manager.update_progress(100, 100, "Initial pipeline coordination complete.")
    logger.debug(msg="Done starting pipeline.", extra=job_manager.logging_context())

    return {"status": "ok", "data": {}, "exception": None}

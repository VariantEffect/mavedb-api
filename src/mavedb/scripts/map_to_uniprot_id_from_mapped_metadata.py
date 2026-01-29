import asyncio
import datetime
import logging

import asyncclick as click  # using asyncclick to allow async commands

from mavedb.db.session import SessionLocal
from mavedb.lib.workflow.job_factory import JobFactory
from mavedb.models.enums.job_pipeline import JobStatus
from mavedb.models.score_set import ScoreSet
from mavedb.worker.jobs.external_services.uniprot import (
    poll_uniprot_mapping_jobs_for_score_set,
    submit_uniprot_mapping_jobs_for_score_set,
)
from mavedb.worker.jobs.registry import STANDALONE_JOB_DEFINITIONS
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.managers.types import JobResultData
from mavedb.worker.settings.lifecycle import standalone_ctx

logger = logging.getLogger(__name__)


@click.command()
@click.argument("score_set_urn", type=str, required=True)
@click.option("--polling-interval", type=int, default=30, help="Polling interval in seconds for checking job status.")
@click.option("--polling-attempts", type=int, default=5, help="Number of tries to poll for job completion.")
@click.option(
    "--refresh",
    is_flag=True,
    default=False,
    help="Refresh the existing mapped identifier, if one exists.",
)
async def main(
    score_set_urn: str,
    polling_interval: int,
    polling_attempts: int,
    refresh: bool = False,
) -> None:
    db = SessionLocal()

    if score_set_urn:
        score_set = db.query(ScoreSet).filter(ScoreSet.urn == score_set_urn).one()

    score_set_id = score_set.id
    if not refresh and any(tg.uniprot_id_from_mapped_metadata for tg in score_set.target_genes):
        logger.info(f"Score set {score_set_urn} already has mapped UniProt IDs. Use --refresh to re-map.")
        return

    # Unique correlation ID for this batch run
    correlation_id = f"populate_mapped_variants_{datetime.datetime.now().isoformat()}"

    # Job definitions
    submission_def = STANDALONE_JOB_DEFINITIONS[submit_uniprot_mapping_jobs_for_score_set]
    polling_def = STANDALONE_JOB_DEFINITIONS[poll_uniprot_mapping_jobs_for_score_set]
    job_factory = JobFactory(db)

    # Use a standalone context for job execution outside of ARQ worker.
    ctx = standalone_ctx()
    ctx["db"] = db

    submission_run = job_factory.create_job_run(
        job_def=submission_def,
        pipeline_id=None,
        correlation_id=correlation_id,
        pipeline_params={
            "score_set_id": score_set_id,
            "correlation_id": correlation_id,
        },
    )
    db.add(submission_run)
    db.flush()

    polling_run = job_factory.create_job_run(
        job_def=polling_def,
        pipeline_id=None,
        correlation_id=correlation_id,
        pipeline_params={
            "score_set_id": score_set_id,
            "correlation_id": correlation_id,
            "mapping_jobs": {},  # Will be filled in by the submission job
        },
    )
    db.add(polling_run)
    db.flush()

    # Dependencies are still valid outside of pipeline contexts, but we must invoke
    # dependent jobs manually.
    polling_dependency = job_factory.create_job_dependency(
        parent_job_run_id=submission_run.id, child_job_run_id=polling_run.id
    )
    db.add(polling_dependency)
    db.flush()

    logger.info(
        f"Submitted UniProt ID mapping submission job run ID {submission_run.id} for score set URN {score_set_urn}."
    )

    # Despite accepting a third argument for the job manager and MyPy expecting it, this
    # argument will be injected automatically by the decorator. We only need to pass
    # the ctx and job_run.id here for the decorator to generate the job manager.
    await submit_uniprot_mapping_jobs_for_score_set(ctx, submission_run.id)  # type: ignore[call-arg]

    job_manager = JobManager(db, None, submission_run.id)
    for i in range(polling_attempts):
        logger.info(
            f"Submitted UniProt ID mapping polling job run ID {polling_run.id} for score set URN {score_set_urn}, attempt {i + 1}."
        )

        # Despite accepting a third argument for the job manager and MyPy expecting it, this
        # argument will be injected automatically by the decorator. We only need to pass
        # the ctx and job_run.id here for the decorator to generate the job manager.
        polling_result: JobResultData = await poll_uniprot_mapping_jobs_for_score_set(ctx, polling_run.id)  # type: ignore[call-arg]
        db.refresh(polling_run)

        if polling_run.status == JobStatus.SUCCEEDED:
            logger.info(f"Polling job for score set URN {score_set_urn} succeeded on attempt {i + 1}.")
            break

        logger.info(
            f"Polling job for score set URN {score_set_urn} failed on attempt {i + 1} with error: {polling_result.get('exception')}"
        )
        db.refresh(polling_run)
        job_manager.prepare_retry(f"Polling job failed. Attempting retry in {polling_interval} seconds.")
        await asyncio.sleep(polling_interval)

    logger.info(f"Completed UniProt ID mapping for score set URN {score_set_urn}. Polling result : {polling_result}")


if __name__ == "__main__":
    main()

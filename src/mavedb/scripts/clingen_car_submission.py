import datetime
import logging
from typing import Sequence

import asyncclick as click
from sqlalchemy import select

from mavedb.db.session import SessionLocal
from mavedb.lib.workflow.job_factory import JobFactory
from mavedb.models.score_set import ScoreSet
from mavedb.worker.jobs.external_services.clingen import submit_score_set_mappings_to_car
from mavedb.worker.jobs.registry import STANDALONE_JOB_DEFINITIONS
from mavedb.worker.settings.lifecycle import standalone_ctx

logger = logging.getLogger(__name__)


@click.command()
@click.argument("urns", nargs=-1)
@click.option("--all", help="Submit variants for every score set in MaveDB.", is_flag=True)
async def main(urns: Sequence[str], all: bool) -> None:
    """
    Submit data to ClinGen Allele Registry for mapped variant CAID generation for the given URNs.
    """
    db = SessionLocal()

    if urns and all:
        logger.error("Cannot provide both URNs and --all option.")
        return

    if all:
        score_set_ids = db.scalars(select(ScoreSet.id)).all()
        logger.info(f"Command invoked with --all. Routine will submit CAR data for {len(score_set_ids)} score sets.")
    else:
        score_set_ids = db.scalars(select(ScoreSet.id).where(ScoreSet.urn.in_(urns))).all()
        logger.info(f"Submitting CAR data for the provided score sets ({len(score_set_ids)}).")

    # Unique correlation ID for this batch run
    correlation_id = f"populate_mapped_variants_{datetime.datetime.now().isoformat()}"

    # Job definition for CAR submission
    job_def = STANDALONE_JOB_DEFINITIONS[submit_score_set_mappings_to_car]
    job_factory = JobFactory(db)

    # Use a standalone context for job execution outside of ARQ worker.
    ctx = standalone_ctx()
    ctx["db"] = db

    for score_set_id in score_set_ids:
        logger.info(f"Submitting CAR data for score set ID {score_set_id}...")

        job_run = job_factory.create_job_run(
            job_def=job_def,
            pipeline_id=None,
            correlation_id=correlation_id,
            pipeline_params={
                "score_set_id": score_set_id,
                "correlation_id": correlation_id,
            },
        )
        db.add(job_run)
        db.flush()
        logger.info(f"Submitted job run ID {job_run.id} for score set ID {score_set_id}.")

        # Despite accepting a third argument for the job manager and MyPy expecting it, this
        # argument will be injected automatically by the decorator. We only need to pass
        # the ctx and job_run.id here for the decorator to generate the job manager.
        await submit_score_set_mappings_to_car(ctx, job_run.id)  # type: ignore


if __name__ == "__main__":
    main()

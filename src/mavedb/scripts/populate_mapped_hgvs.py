import datetime
import logging
from typing import Sequence

import asyncclick as click
from sqlalchemy import select

from mavedb.db.session import SessionLocal
from mavedb.lib.workflow.job_factory import JobFactory
from mavedb.models.score_set import ScoreSet
from mavedb.worker.jobs.external_services.hgvs import populate_hgvs_for_score_set
from mavedb.worker.jobs.registry import STANDALONE_JOB_DEFINITIONS
from mavedb.worker.settings.lifecycle import standalone_ctx

logger = logging.getLogger(__name__)


@click.command()
@click.argument("urns", nargs=-1)
@click.option(
    "--all", "all_score_sets", is_flag=True, help="Populate mapped HGVS for every score set in MaveDB.", default=False
)
async def main(urns: Sequence[str], all_score_sets: bool) -> None:
    """
    Populate mapped variants with standardized HGVS nomenclature from ClinGen for one or more score sets.
    """
    db = SessionLocal()

    if urns and all_score_sets:
        logger.error("Cannot provide both URNs and --all option.")
        return

    if all_score_sets:
        logger.info("Processing all score sets in the database.")
        score_sets = db.scalars(select(ScoreSet)).all()
    else:
        logger.info(f"Processing score sets with URNs: {urns}")
        score_sets = db.scalars(select(ScoreSet).where(ScoreSet.urn.in_(urns))).all()

    # Unique correlation ID for this batch run
    correlation_id = f"populate_mapped_hgvs_{datetime.datetime.now().isoformat()}"

    # Job definition for HGVS population
    job_def = STANDALONE_JOB_DEFINITIONS[populate_hgvs_for_score_set]
    job_factory = JobFactory(db)

    # Use a standalone context for job execution outside of ARQ worker.
    ctx = standalone_ctx()
    ctx["db"] = db

    for score_set in score_sets:
        logger.info(f"Populating mapped HGVS for score set ID {score_set.id} (URN: {score_set.urn})...")

        job_run = job_factory.create_job_run(
            job_def=job_def,
            pipeline_id=None,
            correlation_id=correlation_id,
            pipeline_params={
                "score_set_id": score_set.id,
                "correlation_id": correlation_id,
            },
        )
        db.add(job_run)
        db.flush()
        logger.info(f"Submitted job run ID {job_run.id} for score set ID {score_set.id}.")

        # Despite accepting a third argument for the job manager and MyPy expecting it, this
        # argument will be injected automatically by the decorator. We only need to pass
        # the ctx and job_run.id here for the decorator to generate the job manager.
        await populate_hgvs_for_score_set(ctx, job_run.id)  # type: ignore


if __name__ == "__main__":
    main()

import datetime
import logging

import asyncclick as click

from mavedb.db.session import SessionLocal
from mavedb.lib.workflow.job_factory import JobFactory
from mavedb.models.score_set import ScoreSet
from mavedb.worker.jobs.external_services.gnomad import link_gnomad_variants
from mavedb.worker.jobs.registry import STANDALONE_JOB_DEFINITIONS
from mavedb.worker.settings.lifecycle import standalone_ctx

logger = logging.getLogger(__name__)


@click.command()
@click.argument("urns", nargs=-1)
@click.option("--all", "all_score_sets", is_flag=True, help="Process all score sets in the database.", default=False)
async def main(urns: list[str], all_score_sets: bool) -> None:
    """
    Query AWS Athena for gnomAD variants matching mapped variant CAIDs for one or more score sets.
    """
    db = SessionLocal()

    if all_score_sets:
        logger.info("Processing all score sets in the database.")
        score_sets = db.query(ScoreSet).all()
    else:
        logger.info(f"Processing score sets with URNs: {urns}")
        score_sets = db.query(ScoreSet).filter(ScoreSet.urn.in_(urns)).all()

    # Unique correlation ID for this batch run
    correlation_id = f"populate_mapped_variants_{datetime.datetime.now().isoformat()}"

    # Job definition for gnomAD linking
    job_def = STANDALONE_JOB_DEFINITIONS[link_gnomad_variants]
    job_factory = JobFactory(db)

    # Use a standalone context for job execution outside of ARQ worker.
    ctx = standalone_ctx()
    ctx["db"] = db

    for score_set in score_sets:
        logger.info(f"Linking gnomAD variants for score set ID {score_set.id} (URN: {score_set.urn})...")

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
        await link_gnomad_variants(ctx, job_run.id)  # type: ignore


if __name__ == "__main__":
    main()

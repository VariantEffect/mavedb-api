import datetime
import logging
from typing import Sequence

import asyncclick as click
from sqlalchemy import select

from mavedb.db.session import SessionLocal
from mavedb.lib.workflow.job_factory import JobFactory
from mavedb.models.score_set import ScoreSet
from mavedb.worker.jobs.external_services.clinvar import refresh_clinvar_controls
from mavedb.worker.jobs.registry import STANDALONE_JOB_DEFINITIONS
from mavedb.worker.settings.lifecycle import standalone_ctx

logger = logging.getLogger(__name__)


@click.command()
@click.argument("urns", nargs=-1)
@click.option("--all", help="Refresh ClinVar variant data for all score sets.", is_flag=True)
@click.option("--month", type=int, help="Month of the ClinVar data release to use (1-12).", required=True)
@click.option("--year", type=int, help="Year of the ClinVar data release to use (e.g., 2024).", required=True)
async def main(urns: Sequence[str], all: bool, month: int, year: int) -> None:
    """
    Refresh ClinVar variant data for mapped variants in the given score sets.
    """
    db = SessionLocal()

    if urns and all:
        logger.error("Cannot provide both URNs and --all option.")
        return

    if all:
        score_set_ids = db.scalars(select(ScoreSet.id)).all()
        logger.info(
            f"Command invoked with --all. Routine will refresh ClinVar variant data for {len(score_set_ids)} score sets."
        )
    else:
        score_set_ids = db.scalars(select(ScoreSet.id).where(ScoreSet.urn.in_(urns))).all()
        logger.info(f"Refreshing ClinVar variant data for the provided score sets ({len(score_set_ids)}).")

    # Unique correlation ID for this batch run
    correlation_id = f"populate_mapped_variants_{datetime.datetime.now().isoformat()}"

    # Job definition for ClinVar controls refresh
    job_def = STANDALONE_JOB_DEFINITIONS[refresh_clinvar_controls]
    job_factory = JobFactory(db)

    # Use a standalone context for job execution outside of ARQ worker.
    ctx = standalone_ctx()
    ctx["db"] = db

    for score_set_id in score_set_ids:
        logger.info(f"Refreshing ClinVar variant data for score set ID {score_set_id}...")

        job_run = job_factory.create_job_run(
            job_def=job_def,
            pipeline_id=None,
            correlation_id=correlation_id,
            pipeline_params={
                "score_set_id": score_set_id,
                "correlation_id": correlation_id,
                "month": month,
                "year": year,
            },
        )
        db.add(job_run)
        db.flush()
        logger.info(f"Submitted job run ID {job_run.id} for score set ID {score_set_id}.")

        # Despite accepting a third argument for the job manager and MyPy expecting it, this
        # argument will be injected automatically by the decorator. We only need to pass
        # the ctx and job_run.id here for the decorator to generate the job manager.
        await refresh_clinvar_controls(ctx, job_run.id)  # type: ignore


if __name__ == "__main__":
    main()

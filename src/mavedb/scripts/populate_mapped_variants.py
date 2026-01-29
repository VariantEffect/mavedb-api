import datetime
import logging
from typing import Optional, Sequence

import asyncclick as click  # using asyncclick to allow async commands
from sqlalchemy import select

from mavedb.db.session import SessionLocal
from mavedb.lib.workflow.job_factory import JobFactory
from mavedb.models.score_set import ScoreSet
from mavedb.scripts.environment import script_environment
from mavedb.worker.jobs import STANDALONE_JOB_DEFINITIONS, map_variants_for_score_set
from mavedb.worker.settings.lifecycle import standalone_ctx

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@script_environment.command()
@click.argument("urns", nargs=-1)
@click.option("--all", help="Populate mapped variants for every score set in MaveDB.", is_flag=True)
@click.option("--as-user-id", type=int, help="User ID to attribute as the updater of the mapped variants.")
async def populate_mapped_variant_data(urns: Sequence[Optional[str]], all: bool, as_user_id: Optional[int]):
    score_set_ids: Sequence[Optional[int]]
    db = SessionLocal()

    if all:
        score_set_ids = db.scalars(select(ScoreSet.id)).all()
        logger.info(
            f"Command invoked with --all. Routine will populate mapped variant data for {len(score_set_ids)} score sets."
        )
    else:
        score_set_ids = db.scalars(select(ScoreSet.id).where(ScoreSet.urn.in_(urns))).all()
        logger.info(f"Populating mapped variant data for the provided score sets ({len(score_set_ids)}).")

    # Unique correlation ID for this batch run
    correlation_id = f"populate_mapped_variants_{datetime.datetime.now().isoformat()}"

    # Job definition for mapping variants
    job_def = STANDALONE_JOB_DEFINITIONS[map_variants_for_score_set]
    job_factory = JobFactory(db)

    # Use a standalone context for job execution outside of ARQ worker.
    ctx = standalone_ctx()
    ctx["db"] = db

    for score_set_id in score_set_ids:
        logger.info(f"Populating mapped variant data for score set ID {score_set_id}...")

        job_run = job_factory.create_job_run(
            job_def=job_def,
            pipeline_id=None,
            correlation_id=correlation_id,
            pipeline_params={
                "score_set_id": score_set_id,
                "updater_id": as_user_id
                if as_user_id is not None
                else 1,  # Use provided user ID or default to System user
                "correlation_id": correlation_id,
            },
        )
        db.add(job_run)
        db.flush()
        logger.info(f"Submitted job run ID {job_run.id} for score set ID {score_set_id}.")

        await map_variants_for_score_set(ctx, job_run.id)


if __name__ == "__main__":
    populate_mapped_variant_data()

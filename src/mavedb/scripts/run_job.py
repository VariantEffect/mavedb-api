"""Run a standalone worker job locally or enqueue it via ARQ.

By default, jobs execute in-process using a standalone worker context (no
Redis/worker required). Use --enqueue to submit to the ARQ worker instead.

Usage:
    # Run locally
    poetry run python -m mavedb.scripts.run_job link_gnomad_variants \
        --score-set-urn urn:mavedb:00000001-a-1

    # Enqueue to ARQ worker
    poetry run python -m mavedb.scripts.run_job link_gnomad_variants \
        --score-set-urn urn:mavedb:00000001-a-1 --enqueue

    # List available jobs
    poetry run python -m mavedb.scripts.run_job --list

    # Run job with extra params
    poetry run python -m mavedb.scripts.run_job refresh_clinvar_controls \
        --score-set-urn urn:mavedb:00000001-a-1 --param year=2024 --param month=1
"""

import datetime
import logging
import sys
from typing import Callable

import asyncclick as click
from arq import create_pool
from sqlalchemy import select

from mavedb.db.session import SessionLocal
from mavedb.lib.types.workflow import JobDefinition
from mavedb.lib.workflow.job_factory import JobFactory
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.worker.jobs.registry import STANDALONE_JOB_DEFINITIONS
from mavedb.worker.lib.managers.utils import arq_job_id
from mavedb.worker.settings import RedisWorkerSettings
from mavedb.worker.settings.lifecycle import standalone_ctx

logger = logging.getLogger(__name__)


def _build_job_lookup() -> dict[str, tuple[Callable, JobDefinition]]:
    """Build a mapping from job function name → (callable, job_definition)."""
    return {job_def["function"]: (func, job_def) for func, job_def in STANDALONE_JOB_DEFINITIONS.items()}


def _print_available_jobs() -> None:
    click.echo("Available standalone jobs:\n")
    lookup = _build_job_lookup()
    for name, (_, job_def) in sorted(lookup.items()):
        required_params = [k for k, v in job_def["params"].items() if v is None]
        # correlation_id is auto-generated
        display_params = [p for p in required_params if p != "correlation_id"]
        click.echo(f"  {name}")
        click.echo(f"    Type: {job_def['type']}")
        if display_params:
            click.echo(f"    Required params: {', '.join(display_params)}")
        click.echo()


def _coerce_param_value(value: str) -> int | str:
    """Attempt to coerce a string param value to int if it looks numeric."""
    try:
        return int(value)
    except ValueError:
        return value


@click.command()
@click.argument("job_name", required=False)
@click.option("--list", "list_jobs", is_flag=True, help="List available jobs and exit.")
@click.option("--enqueue", is_flag=True, help="Enqueue to ARQ worker instead of running locally.")
@click.option("--score-set-urn", "score_set_urn", help="URN of the score set to process.")
@click.option("--all", "all_score_sets", is_flag=True, help="Run the job for every score set.")
@click.option("--updater-id", "updater_id", type=int, help="ID of the user (required by some jobs).")
@click.option(
    "--param",
    "extra_params",
    multiple=True,
    help="Additional key=value param (repeatable). e.g. --param year=2024",
)
async def main(
    job_name: str | None,
    list_jobs: bool,
    enqueue: bool,
    score_set_urn: str | None,
    all_score_sets: bool,
    updater_id: int | None,
    extra_params: tuple[str, ...],
) -> None:
    """Run a standalone worker job.

    JOB_NAME is the function name of the job to run (e.g. link_gnomad_variants).
    Use --list to see available jobs.
    """
    if list_jobs or not job_name:
        _print_available_jobs()
        return

    lookup = _build_job_lookup()
    if job_name not in lookup:
        click.echo(f"Unknown job: {job_name}", err=True)
        click.echo(f"Available: {', '.join(sorted(lookup.keys()))}", err=True)
        sys.exit(1)

    job_func, job_def = lookup[job_name]

    # Parse extra params
    parsed_extra: dict[str, int | str] = {}
    for param_str in extra_params:
        if "=" not in param_str:
            click.echo(f"Invalid --param format (expected key=value): {param_str}", err=True)
            sys.exit(1)
        key, value = param_str.split("=", 1)
        parsed_extra[key] = _coerce_param_value(value)

    # Determine which params this job needs
    required_params = {k for k, v in job_def["params"].items() if v is None}
    needs_score_set = "score_set_id" in required_params
    needs_updater = "updater_id" in required_params

    db = SessionLocal()

    # Resolve score sets if needed
    score_set_ids: list[int] = []
    if needs_score_set:
        if score_set_urn and all_score_sets:
            click.echo("Cannot provide both --score-set-urn and --all.", err=True)
            sys.exit(1)
        if not score_set_urn and not all_score_sets:
            click.echo("--score-set-urn or --all is required for this job.", err=True)
            sys.exit(1)

        if all_score_sets:
            score_set_ids = [id_ for id_ in db.scalars(select(ScoreSet.id)).all() if id_ is not None]
            click.echo(f"Processing all {len(score_set_ids)} score sets.")
        else:
            # Support comma-separated URNs
            urns = [u.strip() for u in score_set_urn.split(",")]  # type: ignore[union-attr]
            score_sets = db.scalars(select(ScoreSet).where(ScoreSet.urn.in_(urns))).all()
            missing = set(urns) - {ss.urn for ss in score_sets}
            if missing:
                click.echo(f"Score sets not found: {', '.join(missing)}", err=True)
                sys.exit(1)
            score_set_ids = [ss.id for ss in score_sets if ss.id is not None]

    # Resolve user if needed
    if needs_updater:
        if not updater_id:
            click.echo("--updater-id is required for this job.", err=True)
            sys.exit(1)
        user = db.scalars(select(User).where(User.id == updater_id)).one_or_none()
        if not user:
            click.echo(f"User not found: {updater_id}", err=True)
            sys.exit(1)
        updater_id = user.id

    correlation_id = f"{job_name}_{datetime.datetime.now().isoformat()}"
    redis = await create_pool(RedisWorkerSettings)
    job_factory = JobFactory(db)

    if enqueue:
        await _enqueue_jobs(
            db,
            redis,
            job_factory,
            job_def,
            job_name,
            score_set_ids,
            updater_id,
            correlation_id,
            parsed_extra,
            needs_score_set,
        )
    else:
        await _run_locally(
            db,
            redis,
            job_factory,
            job_func,
            job_def,
            score_set_ids,
            updater_id,
            correlation_id,
            parsed_extra,
            needs_score_set,
        )

    db.close()


async def _enqueue_jobs(
    db, redis, job_factory, job_def, job_name, score_set_ids, updater_id, correlation_id, extra_params, needs_score_set
) -> None:
    """Create JobRun records and enqueue them in ARQ."""

    try:
        items = score_set_ids if needs_score_set else [None]
        for score_set_id in items:
            pipeline_params = {"correlation_id": correlation_id, **extra_params}
            if score_set_id is not None:
                pipeline_params["score_set_id"] = score_set_id
            if updater_id is not None:
                pipeline_params["updater_id"] = updater_id

            job_run = job_factory.create_job_run(
                job_def=job_def,
                pipeline_id=None,
                correlation_id=correlation_id,
                pipeline_params=pipeline_params,
            )
            db.flush()

            arq_id = arq_job_id(job_run)
            job = await redis.enqueue_job(job_run.job_function, job_run.id, _job_id=arq_id)
            if job:
                click.echo(f"Enqueued {job_name} (job_run={job_run.id}, arq_id={arq_id})")
            else:
                click.echo(f"Job already enqueued (job_run={job_run.id})", err=True)

        db.commit()
    finally:
        await redis.aclose()


async def _run_locally(
    db, redis, job_factory, job_func, job_def, score_set_ids, updater_id, correlation_id, extra_params, needs_score_set
) -> None:
    """Execute jobs in-process using a standalone worker context."""
    ctx = standalone_ctx()
    ctx["db"] = db
    ctx["redis"] = redis

    items = score_set_ids if needs_score_set else [None]
    for score_set_id in items:
        pipeline_params = {"correlation_id": correlation_id, **extra_params}
        if score_set_id is not None:
            pipeline_params["score_set_id"] = score_set_id
        if updater_id is not None:
            pipeline_params["updater_id"] = updater_id

        job_run = job_factory.create_job_run(
            job_def=job_def,
            pipeline_id=None,
            correlation_id=correlation_id,
            pipeline_params=pipeline_params,
        )
        db.commit()

        resource = f"score_set_{score_set_id}" if score_set_id else "standalone"
        click.echo(f"Running {job_def['function']} for {resource} (job_run={job_run.id})...")

        # The job_manager argument is injected by the with_pipeline_management decorator;
        # we only pass ctx and job_run.id.
        await job_func(ctx, job_run.id)  # type: ignore[call-arg]

        click.echo(f"  Completed job_run={job_run.id}")

    await redis.aclose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()

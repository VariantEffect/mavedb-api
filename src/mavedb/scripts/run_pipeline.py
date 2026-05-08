"""Run a named pipeline end-to-end via ARQ.

Creates the Pipeline and all associated JobRun/JobDependency records via
PipelineFactory, then enqueues the start_pipeline entrypoint in ARQ.
Requires a running Redis instance and worker.

Usage:
    poetry run python -m mavedb.scripts.run_pipeline annotate_score_set \
        --score-set-urn urn:mavedb:00000001-a-1 --updater-id 1

    poetry run python -m mavedb.scripts.run_pipeline --list
"""

import datetime
import logging
import sys

import asyncclick as click
from arq import create_pool
from sqlalchemy import select

from mavedb.db.session import SessionLocal
from mavedb.lib.workflow.definitions import PIPELINE_DEFINITIONS
from mavedb.lib.workflow.pipeline_factory import PipelineFactory
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.worker.lib.managers.utils import arq_job_id
from mavedb.worker.settings import RedisWorkerSettings

logger = logging.getLogger(__name__)


def _print_available_pipelines() -> None:
    click.echo("Available pipelines:\n")
    for name, definition in PIPELINE_DEFINITIONS.items():
        click.echo(f"  {name}")
        click.echo(f"    {definition['description']}")

        # Collect unique required params (those with None values) across all jobs
        required_params: set[str] = set()
        for job_def in definition["job_definitions"]:
            for param, value in job_def["params"].items():
                if value is None:
                    required_params.add(param)

        # correlation_id is auto-generated, not user-supplied
        required_params.discard("correlation_id")
        if required_params:
            click.echo(f"    Required params: {', '.join(sorted(required_params))}")

        job_keys = [j["key"] for j in definition["job_definitions"]]
        click.echo(f"    Jobs ({len(job_keys)}): {', '.join(job_keys)}")
        click.echo()


@click.command()
@click.argument("pipeline_name", required=False)
@click.option("--list", "list_pipelines", is_flag=True, help="List available pipelines and exit.")
@click.option("--score-set-urn", "score_set_urn", help="URN of the score set to process.")
@click.option("--updater-id", "updater_id", type=int, help="ID of the user to attribute pipeline actions to.")
@click.option(
    "--extra-param",
    "extra_params",
    multiple=True,
    type=(str, str),
    help="Additional key=value params for the pipeline (repeatable).",
)
async def main(
    pipeline_name: str | None,
    list_pipelines: bool,
    score_set_urn: str | None,
    updater_id: int | None,
    extra_params: tuple[tuple[str, str], ...],
) -> None:
    """Run a named pipeline via ARQ.

    PIPELINE_NAME is the name of the pipeline to run (e.g. annotate_score_set).
    Use --list to see available pipelines.
    """
    if list_pipelines or not pipeline_name:
        _print_available_pipelines()
        return

    if pipeline_name not in PIPELINE_DEFINITIONS:
        click.echo(f"Unknown pipeline: {pipeline_name}", err=True)
        click.echo(f"Available: {', '.join(PIPELINE_DEFINITIONS.keys())}", err=True)
        sys.exit(1)

    if not score_set_urn:
        click.echo("--score-set-urn is required.", err=True)
        sys.exit(1)

    db = SessionLocal()
    score_set = db.scalars(select(ScoreSet).where(ScoreSet.urn == score_set_urn)).one_or_none()
    if not score_set:
        click.echo(f"Score set not found: {score_set_urn}", err=True)
        sys.exit(1)

    resolved_updater_id = updater_id or score_set.modified_by_id or score_set.created_by_id
    if resolved_updater_id is None:
        click.echo(
            "--updater-id is required (score set has no existing modifier or creator to fall back to).", err=True
        )
        sys.exit(1)

    user = db.scalars(select(User).where(User.id == resolved_updater_id)).one_or_none()
    if not user:
        click.echo(f"User not found: {resolved_updater_id}", err=True)
        sys.exit(1)

    correlation_id = f"{pipeline_name}_{score_set.urn}_{user.id}_{datetime.datetime.now().isoformat()}"
    pipeline_params: dict = {
        "correlation_id": correlation_id,
        "score_set_id": score_set.id,
        "updater_id": user.id,
    }
    for key, value in extra_params:
        pipeline_params[key] = value

    try:
        pipeline_factory = PipelineFactory(session=db)
        pipeline, pipeline_entrypoint = pipeline_factory.create_pipeline(
            pipeline_name=pipeline_name,
            creating_user=user,
            pipeline_params=pipeline_params,
        )
    except (KeyError, ValueError) as e:
        click.echo(f"Failed to create pipeline: {e}", err=True)
        sys.exit(1)

    click.echo(f"Created pipeline '{pipeline_name}' (id={pipeline.id}, correlation_id={correlation_id})")

    # Connect to Redis and enqueue
    redis = await create_pool(RedisWorkerSettings)
    try:
        job = await redis.enqueue_job(
            pipeline_entrypoint.job_function,
            pipeline_entrypoint.id,
            _job_id=arq_job_id(pipeline_entrypoint),
        )
        if job:
            click.echo(f"Enqueued start_pipeline job: {job.job_id}. Pipeline will execute asynchronously.")
        else:
            click.echo("Job was already enqueued (duplicate).", err=True)
    finally:
        await redis.aclose()
        db.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()

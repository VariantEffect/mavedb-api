"""Operator-facing CLI for inspecting pipeline state.

Usage:
    # List all pipelines
    poetry run python -m mavedb.scripts.pipelines list-pipelines

    # Filter by status
    poetry run python -m mavedb.scripts.pipelines list-pipelines --status running

    # Show a single pipeline with progress statistics
    poetry run python -m mavedb.scripts.pipelines show-pipeline urn:mavedb-pipeline:<uuid>
"""

import json
import logging
from datetime import datetime
from typing import Optional

import asyncclick as click
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.models.enums.job_pipeline import PipelineStatus
from mavedb.models.pipeline import Pipeline
from mavedb.scripts.environment import script_environment, with_database_session
from mavedb.worker.lib.managers.pipeline_manager import PipelineManager

logger = logging.getLogger(__name__)


def _format_dt(dt: Optional[datetime]) -> str:
    return dt.isoformat() if dt else "-"


@script_environment.command(name="list-pipelines")
@with_database_session
@click.option(
    "--status",
    type=click.Choice([s.value for s in PipelineStatus]),
    default=None,
    help="Filter by pipeline status.",
)
@click.option("--name", default=None, help="Filter by pipeline name (exact match).")
@click.option("--correlation-id", default=None, help="Filter by correlation id.")
@click.option("--created-by-user-id", type=int, default=None, help="Filter by creating user id.")
@click.option("--limit", type=int, default=50, show_default=True, help="Maximum rows to return.")
@click.option("--json", "as_json", is_flag=True, help="Emit results as JSON.")
def list_pipelines(
    db: Session,
    status: Optional[str],
    name: Optional[str],
    correlation_id: Optional[str],
    created_by_user_id: Optional[int],
    limit: int,
    as_json: bool,
) -> None:
    """List pipelines with optional filters."""
    query = select(Pipeline)
    if status:
        query = query.where(Pipeline.status == status)
    if name:
        query = query.where(Pipeline.name == name)
    if correlation_id:
        query = query.where(Pipeline.correlation_id == correlation_id)
    if created_by_user_id is not None:
        query = query.where(Pipeline.created_by_user_id == created_by_user_id)

    query = query.order_by(Pipeline.created_at.desc()).limit(limit)
    pipelines = db.scalars(query).all()

    if as_json:
        rows = [
            {
                "id": p.id,
                "urn": p.urn,
                "name": p.name,
                "status": p.status,
                "correlation_id": p.correlation_id,
                "created_at": _format_dt(p.created_at),
                "started_at": _format_dt(p.started_at),
                "finished_at": _format_dt(p.finished_at),
                "created_by_user_id": p.created_by_user_id,
            }
            for p in pipelines
        ]
        click.echo(json.dumps(rows, indent=2))
        return

    if not pipelines:
        click.echo("No pipelines match the given filters.")
        return

    click.echo(f"{'ID':>6}  {'STATUS':<12} {'NAME':<32} {'CREATED':<26} URN")
    for p in pipelines:
        click.echo(
            f"{p.id:>6}  {str(p.status):<12} {p.name[:32]:<32} " f"{_format_dt(p.created_at):<26} {p.urn or '-'}"
        )


@script_environment.command(name="show-pipeline")
@with_database_session
@click.argument("urn")
@click.option("--json", "as_json", is_flag=True, help="Emit full result as JSON.")
def show_pipeline(db: Session, urn: str, as_json: bool) -> None:
    """Show a single pipeline with progress statistics."""
    pipeline = db.scalars(select(Pipeline).where(Pipeline.urn == urn)).one_or_none()
    if pipeline is None:
        click.echo(f"Pipeline not found: {urn}", err=True)
        raise SystemExit(1)

    # PipelineManager requires a redis client only for coordination; read-only progress
    # aggregation does not dispatch jobs, so a None redis client is safe here if somewhat hacky.
    manager = PipelineManager(db=db, redis=None, pipeline_id=pipeline.id)  # type: ignore[arg-type]
    progress = manager.get_pipeline_progress()

    payload = {
        "id": pipeline.id,
        "urn": pipeline.urn,
        "name": pipeline.name,
        "description": pipeline.description,
        "status": pipeline.status,
        "correlation_id": pipeline.correlation_id,
        "created_at": _format_dt(pipeline.created_at),
        "started_at": _format_dt(pipeline.started_at),
        "finished_at": _format_dt(pipeline.finished_at),
        "created_by_user_id": pipeline.created_by_user_id,
        "mavedb_version": pipeline.mavedb_version,
        "metadata": pipeline.metadata_,
        "progress": progress,
    }

    if as_json:
        click.echo(json.dumps(payload, indent=2, default=str))
        return

    click.echo(f"Pipeline: {pipeline.urn} (id={pipeline.id})")
    click.echo(f"  Name:           {pipeline.name}")
    click.echo(f"  Status:         {pipeline.status}")
    click.echo(f"  Correlation:    {pipeline.correlation_id or '-'}")
    click.echo(f"  Created:        {_format_dt(pipeline.created_at)}")
    click.echo(f"  Started:        {_format_dt(pipeline.started_at)}")
    click.echo(f"  Finished:       {_format_dt(pipeline.finished_at)}")
    click.echo(f"  Created by uid: {pipeline.created_by_user_id}")
    click.echo("  Progress:")
    click.echo(f"    Total jobs:        {progress['total_jobs']}")
    click.echo(f"    Completed:         {progress['completed_jobs']}")
    click.echo(f"    Successful:        {progress['successful_jobs']}")
    click.echo(f"    Failed:            {progress['failed_jobs']}")
    click.echo(f"    Running:           {progress['running_jobs']}")
    click.echo(f"    Pending:           {progress['pending_jobs']}")
    click.echo(f"    Completion pct:    {progress['completion_percentage']:.1f}%")
    click.echo(f"    Duration (s):      {progress['duration']}")
    if progress["status_counts"]:
        click.echo("    Status counts:")
        for status_key, count in sorted(progress["status_counts"].items()):
            click.echo(f"      {status_key}: {count}")


if __name__ == "__main__":
    script_environment()

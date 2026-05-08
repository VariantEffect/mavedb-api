"""Operator-facing CLI for inspecting job run state.

Usage:
    # List all recent job runs
    poetry run python -m mavedb.scripts.job_runs list-job-runs

    # Filter by status and job type
    poetry run python -m mavedb.scripts.job_runs list-job-runs --status failed --job-type variant_mapping

    # Show a single job run with full error details
    poetry run python -m mavedb.scripts.job_runs show-job-run urn:mavedb-job:<uuid>
"""

import json
import logging
from datetime import datetime
from typing import Optional

import asyncclick as click
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.models.enums.job_pipeline import JobStatus
from mavedb.models.job_run import JobRun
from mavedb.scripts.environment import script_environment, with_database_session

logger = logging.getLogger(__name__)


def _format_dt(dt: Optional[datetime]) -> str:
    return dt.isoformat() if dt else "-"


@script_environment.command(name="list-job-runs")
@with_database_session
@click.option(
    "--status",
    type=click.Choice([s.value for s in JobStatus]),
    default=None,
    help="Filter by job run status.",
)
@click.option("--job-type", default=None, help="Filter by job type.")
@click.option("--job-function", default=None, help="Filter by job function name.")
@click.option("--correlation-id", default=None, help="Filter by correlation id.")
@click.option("--pipeline-id", type=int, default=None, help="Filter by parent pipeline id.")
@click.option("--limit", type=int, default=50, show_default=True, help="Maximum rows to return.")
@click.option("--json", "as_json", is_flag=True, help="Emit results as JSON.")
def list_job_runs(
    db: Session,
    status: Optional[str],
    job_type: Optional[str],
    job_function: Optional[str],
    correlation_id: Optional[str],
    pipeline_id: Optional[int],
    limit: int,
    as_json: bool,
) -> None:
    """List job runs with optional filters."""
    query = select(JobRun)
    if status:
        query = query.where(JobRun.status == status)
    if job_type:
        query = query.where(JobRun.job_type == job_type)
    if job_function:
        query = query.where(JobRun.job_function == job_function)
    if correlation_id:
        query = query.where(JobRun.correlation_id == correlation_id)
    if pipeline_id is not None:
        query = query.where(JobRun.pipeline_id == pipeline_id)

    query = query.order_by(JobRun.created_at.desc()).limit(limit)
    job_runs = db.scalars(query).all()

    if as_json:
        rows = [
            {
                "id": j.id,
                "urn": j.urn,
                "status": j.status,
                "job_type": j.job_type,
                "job_function": j.job_function,
                "correlation_id": j.correlation_id,
                "pipeline_id": j.pipeline_id,
                "retry_count": j.retry_count,
                "failure_category": j.failure_category,
                "created_at": _format_dt(j.created_at),
                "started_at": _format_dt(j.started_at),
                "finished_at": _format_dt(j.finished_at),
            }
            for j in job_runs
        ]
        click.echo(json.dumps(rows, indent=2))
        return

    if not job_runs:
        click.echo("No job runs match the given filters.")
        return

    click.echo(f"{'ID':>6}  {'STATUS':<10} {'TYPE':<24} {'FUNCTION':<36} " f"{'RETRIES':<8} {'CREATED':<26} URN")
    for j in job_runs:
        click.echo(
            f"{j.id:>6}  {str(j.status):<10} {j.job_type[:24]:<24} "
            f"{j.job_function[:36]:<36} {j.retry_count:<8} "
            f"{_format_dt(j.created_at):<26} {j.urn or '-'}"
        )


@script_environment.command(name="show-job-run")
@with_database_session
@click.argument("urn")
@click.option("--json", "as_json", is_flag=True, help="Emit full result as JSON.")
@click.option("--no-traceback", is_flag=True, help="Omit the error traceback from the output.")
def show_job_run(db: Session, urn: str, as_json: bool, no_traceback: bool) -> None:
    """Show a single job run including error details."""
    job_run = db.scalars(select(JobRun).where(JobRun.urn == urn)).one_or_none()
    if job_run is None:
        click.echo(f"Job run not found: {urn}", err=True)
        raise SystemExit(1)

    payload = {
        "id": job_run.id,
        "urn": job_run.urn,
        "status": job_run.status,
        "job_type": job_run.job_type,
        "job_function": job_run.job_function,
        "job_params": job_run.job_params,
        "correlation_id": job_run.correlation_id,
        "pipeline_id": job_run.pipeline_id,
        "max_retries": job_run.max_retries,
        "retry_count": job_run.retry_count,
        "retry_delay_seconds": job_run.retry_delay_seconds,
        "scheduled_at": _format_dt(job_run.scheduled_at),
        "started_at": _format_dt(job_run.started_at),
        "finished_at": _format_dt(job_run.finished_at),
        "created_at": _format_dt(job_run.created_at),
        "progress_current": job_run.progress_current,
        "progress_total": job_run.progress_total,
        "progress_message": job_run.progress_message,
        "failure_category": job_run.failure_category,
        "error_message": job_run.error_message,
        "mavedb_version": job_run.mavedb_version,
        "metadata": job_run.metadata_,
    }
    if not no_traceback:
        payload["error_traceback"] = job_run.error_traceback

    if as_json:
        click.echo(json.dumps(payload, indent=2, default=str))
        return

    click.echo(f"Job Run: {job_run.urn} (id={job_run.id})")
    click.echo(f"  Status:         {job_run.status}")
    click.echo(f"  Type:           {job_run.job_type}")
    click.echo(f"  Function:       {job_run.job_function}")
    click.echo(f"  Pipeline id:    {job_run.pipeline_id}")
    click.echo(f"  Correlation:    {job_run.correlation_id or '-'}")
    click.echo(f"  Retries:        {job_run.retry_count}/{job_run.max_retries}")
    click.echo(f"  Scheduled:      {_format_dt(job_run.scheduled_at)}")
    click.echo(f"  Started:        {_format_dt(job_run.started_at)}")
    click.echo(f"  Finished:       {_format_dt(job_run.finished_at)}")
    if job_run.progress_total is not None:
        click.echo(f"  Progress:       {job_run.progress_current or 0}/{job_run.progress_total}")
    if job_run.progress_message:
        click.echo(f"  Progress msg:   {job_run.progress_message}")
    if job_run.failure_category:
        click.echo(f"  Failure cat:    {job_run.failure_category}")
    if job_run.error_message:
        click.echo(f"  Error message:  {job_run.error_message}")
    if job_run.error_traceback and not no_traceback:
        click.echo("  Error traceback:")
        for line in job_run.error_traceback.splitlines():
            click.echo(f"    {line}")


if __name__ == "__main__":
    script_environment()

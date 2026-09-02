"""Operator-facing CLI for tracking which score sets need a pipeline run.

Command
-------
list-score-sets
    Produces a table of all score sets with their most recent pipeline run and
    whether they need to be re-processed since a given deployment cutoff date.

Usage:

    # Tracking list — show all published score sets and last pipeline run
    poetry run python -m mavedb.scripts.pipeline_tracking list-score-sets

    # Filter to only those whose last run is before (or missing since) a deployment date
    poetry run python -m mavedb.scripts.pipeline_tracking list-score-sets \\
        --needs-rerun-since 2026-05-01

    # Include private score sets
    poetry run python -m mavedb.scripts.pipeline_tracking list-score-sets --include-private

    # JSON output for piping / spreadsheet import
    poetry run python -m mavedb.scripts.pipeline_tracking list-score-sets --json
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

import asyncclick as click
from sqlalchemy import Integer, cast, select
from sqlalchemy.orm import Session

from mavedb.models.job_run import JobRun
from mavedb.models.pipeline import Pipeline
from mavedb.models.score_set import ScoreSet
from mavedb.scripts.environment import script_environment, with_database_session

logger = logging.getLogger(__name__)


def _format_dt(dt: Optional[datetime]) -> str:
    return dt.isoformat() if dt else "-"


def _needs_rerun(last_pipeline_finished: Optional[datetime], cutoff: Optional[datetime]) -> bool:
    """Return True if the score set needs a re-run relative to *cutoff*."""
    if cutoff is None:
        return False
    if last_pipeline_finished is None:
        return True

    # Normalise both to UTC-aware for comparison
    aware_cutoff = cutoff if cutoff.tzinfo else cutoff.replace(tzinfo=timezone.utc)
    aware_finished = (
        last_pipeline_finished if last_pipeline_finished.tzinfo else last_pipeline_finished.replace(tzinfo=timezone.utc)
    )
    return aware_finished < aware_cutoff


def _last_pipeline_subquery(db: Session, score_set_id: int) -> Optional[Pipeline]:
    """Return the most recently finished (or created) pipeline for *score_set_id*."""
    # score_set_id is stored as an integer in job_params JSONB
    job_run_sq = (
        select(JobRun.pipeline_id)
        .where(cast(JobRun.job_params["score_set_id"].astext, Integer) == score_set_id)
        .where(JobRun.pipeline_id.is_not(None))
        .distinct()
        .subquery()
    )

    return db.scalars(
        select(Pipeline).where(Pipeline.id.in_(select(job_run_sq))).order_by(Pipeline.created_at.desc()).limit(1)
    ).one_or_none()


def _build_score_set_rows(
    db: Session,
    score_sets: list[ScoreSet],
    cutoff: Optional[datetime],
) -> list[dict]:
    rows = []
    for ss in score_sets:
        last_pipeline = _last_pipeline_subquery(db, ss.id)
        last_pipeline_name = last_pipeline.name if last_pipeline else None
        last_pipeline_status = str(last_pipeline.status) if last_pipeline else None
        last_finished = last_pipeline.finished_at if last_pipeline else None
        last_created = last_pipeline.created_at if last_pipeline else None

        rows.append(
            {
                "score_set_urn": ss.urn or "(no urn)",
                "processing_state": str(ss.processing_state) if ss.processing_state else "-",
                "mapping_state": str(ss.mapping_state) if ss.mapping_state else "-",
                "num_variants": ss.num_variants,
                "private": ss.private,
                "last_pipeline_name": last_pipeline_name or "-",
                "last_pipeline_status": last_pipeline_status or "-",
                "last_pipeline_created_at": _format_dt(last_created),
                "last_pipeline_finished_at": _format_dt(last_finished),
                "needs_rerun": _needs_rerun(last_finished, cutoff),
            }
        )

    return rows


@script_environment.command(name="list-score-sets")
@with_database_session
@click.option(
    "--needs-rerun-since",
    "needs_rerun_since",
    default=None,
    help=(
        "ISO date/datetime of a deployment cutoff (e.g. 2026-05-01 or 2026-05-01T12:00:00). "
        "Score sets whose last pipeline finished before this timestamp (or have never run) are "
        "flagged as needing a re-run."
    ),
)
@click.option("--include-private", is_flag=True, default=False, help="Include private (unpublished) score sets.")
@click.option("--needs-rerun-only", is_flag=True, default=False, help="Only show score sets that need a re-run.")
@click.option("--limit", type=int, default=None, help="Cap the number of rows returned (applied after all filtering).")
@click.option("--json", "as_json", is_flag=True, help="Emit results as JSON.")
def list_score_sets(
    db: Session,
    needs_rerun_since: Optional[str],
    include_private: bool,
    needs_rerun_only: bool,
    limit: Optional[int],
    as_json: bool,
) -> None:
    """List all score sets with their last pipeline run and re-run status."""
    cutoff: Optional[datetime] = None
    if needs_rerun_since:
        try:
            cutoff = datetime.fromisoformat(needs_rerun_since)
        except ValueError:
            click.echo(
                f"Invalid --needs-rerun-since value: {needs_rerun_since!r}. Use ISO format e.g. 2026-05-01.", err=True
            )
            raise SystemExit(1)

    query = select(ScoreSet).where(ScoreSet.urn.is_not(None))
    if not include_private:
        query = query.where(ScoreSet.private == False)  # noqa: E712
    query = query.order_by(ScoreSet.urn)

    score_sets = db.scalars(query).all()
    rows = _build_score_set_rows(db, list(score_sets), cutoff)

    if needs_rerun_only:
        rows = [r for r in rows if r["needs_rerun"]]

    if limit is not None:
        rows = rows[:limit]

    if as_json:
        click.echo(json.dumps(rows, indent=2, default=str))
        return

    if not rows:
        click.echo("No score sets match the given filters.")
        return

    needs_rerun_count = sum(1 for r in rows if r["needs_rerun"])
    click.echo(f"Total: {len(rows)} score set(s)" + (f", {needs_rerun_count} need re-run" if cutoff else ""))
    if cutoff:
        click.echo(f"Cutoff: {cutoff.isoformat()}")
    click.echo()

    col_w = {"urn": 28, "ps": 28, "ms": 26, "nv": 8, "pipe": 28, "pipe_status": 12, "finished": 12}
    header = (
        f"{'URN':<{col_w['urn']}}  {'PROC_STATE':<{col_w['ps']}}  {'MAP_STATE':<{col_w['ms']}}  "
        f"{'VARIANTS':>{col_w['nv']}}  {'LAST_PIPELINE':<{col_w['pipe']}}  "
        f"{'PIPE_STATUS':<{col_w['pipe_status']}}  {'LAST_FINISHED':<{col_w['finished']}}"
        + ("  RERUN?" if cutoff else "")
    )
    click.echo(header)
    click.echo("-" * len(header))

    for r in rows:
        rerun_flag = ("YES" if r["needs_rerun"] else "no") if cutoff else ""
        click.echo(
            f"{r['score_set_urn']:<{col_w['urn']}}  "
            f"{r['processing_state']:<{col_w['ps']}}  "
            f"{r['mapping_state']:<{col_w['ms']}}  "
            f"{r['num_variants']:>{col_w['nv']}}  "
            f"{r['last_pipeline_name']:<{col_w['pipe']}}  "
            f"{r['last_pipeline_status']:<{col_w['pipe_status']}}  "
            f"{r['last_pipeline_finished_at']:<{col_w['finished']}}" + (f"  {rerun_flag}" if cutoff else "")
        )


if __name__ == "__main__":
    script_environment()

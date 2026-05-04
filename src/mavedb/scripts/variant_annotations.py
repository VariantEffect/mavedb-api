"""Operator-facing CLI for inspecting variant annotation state.

Usage:
    # Summarize annotation status counts for all variants in a score set
    poetry run python -m mavedb.scripts.variant_annotations show-score-set urn:mavedb:00000001-a-1
    poetry run python -m mavedb.scripts.variant_annotations show-score-set urn:mavedb:00000001-a-1 --json

For per-variant detail, query the v_variant_annotations view directly:
    SELECT * FROM v_variant_annotations WHERE score_set_urn = 'urn:mavedb:00000001-a-1';
    SELECT * FROM v_variant_annotations WHERE variant_urn = 'urn:mavedb:00000001-a-1#42';
    SELECT * FROM v_variant_annotations WHERE annotation_status = 'failed';
"""

import json
import logging
from typing import Optional

import asyncclick as click
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.variant_annotation_status import VariantAnnotationStatus
from mavedb.scripts.environment import script_environment, with_database_session

logger = logging.getLogger(__name__)


def _get_score_set(db: Session, urn: str) -> Optional[ScoreSet]:
    return db.scalars(select(ScoreSet).where(ScoreSet.urn == urn)).one_or_none()


@script_environment.command(name="show-score-set")
@with_database_session
@click.argument("score_set_urn")
@click.option("--json", "as_json", is_flag=True, help="Emit result as JSON.")
def show_score_set(db: Session, score_set_urn: str, as_json: bool) -> None:
    """Summarize annotation status counts for all variants in a score set."""
    score_set = _get_score_set(db, score_set_urn)
    if score_set is None:
        click.echo(f"Score set not found: {score_set_urn}", err=True)
        raise SystemExit(1)

    # Count current annotation statuses grouped by annotation_type and status
    rows = db.execute(
        select(
            VariantAnnotationStatus.annotation_type,
            VariantAnnotationStatus.status,
            func.count().label("count"),
        )
        .join(Variant, Variant.id == VariantAnnotationStatus.variant_id)
        .where(
            Variant.score_set_id == score_set.id,
            VariantAnnotationStatus.current == True,  # noqa: E712
        )
        .group_by(VariantAnnotationStatus.annotation_type, VariantAnnotationStatus.status)
        .order_by(VariantAnnotationStatus.annotation_type, VariantAnnotationStatus.status)
    ).all()

    # Total variant count for the score set
    total_variants = db.scalar(select(func.count()).where(Variant.score_set_id == score_set.id)) or 0

    if as_json:
        click.echo(
            json.dumps(
                {
                    "score_set_urn": score_set_urn,
                    "total_variants": total_variants,
                    "annotation_summary": [
                        {"annotation_type": r.annotation_type, "status": r.status, "count": r.count} for r in rows
                    ],
                },
                indent=2,
            )
        )
        return

    click.echo(f"Score set: {score_set_urn}  ({total_variants} variants)")
    click.echo(f"\n{'ANNOTATION TYPE':<32} {'STATUS':<10} COUNT")
    for r in rows:
        click.echo(f"{r.annotation_type:<32} {str(r.status):<10} {r.count}")

    if not rows:
        click.echo("No annotation status records found.")


if __name__ == "__main__":
    script_environment()

"""Operator-facing CLI for inspecting a score set's current annotation status.

The score set is the entry point (pipelines run per score set), but annotation status is a per-allele
fact: this resolves the score set's *current* alleles (and variants) through the live mapping links and
reports each subject's true current status — including statuses produced by another score set's run
that landed on a shared allele. It does **not** filter by which run wrote the event.

Usage:
    poetry run python -m mavedb.scripts.variant_annotations show-score-set urn:mavedb:00000001-a-1
    poetry run python -m mavedb.scripts.variant_annotations show-score-set urn:mavedb:00000001-a-1 --json

For per-subject detail, query the v_current_annotation_events view directly:
    SELECT * FROM v_current_annotation_events WHERE allele_id = <id>;
    SELECT * FROM v_current_annotation_events WHERE disposition = 'failed';
"""

import json
import logging
from typing import Optional

import asyncclick as click
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from mavedb.lib.clingen.alleles import get_alleles_for_score_set
from mavedb.models.annotation_event_view import CurrentAnnotationEventView
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.scripts.environment import script_environment, with_database_session

logger = logging.getLogger(__name__)


def _get_score_set(db: Session, urn: str) -> Optional[ScoreSet]:
    return db.scalars(select(ScoreSet).where(ScoreSet.urn == urn)).one_or_none()


def current_annotation_summary(db: Session, score_set: ScoreSet) -> list[dict]:
    """Current annotation disposition counts for a score set, by annotation type.

    Resolves the score set's subjects — its current alleles (via the live mapping links) for
    allele-subject types, and its variants for variant-subject types (mapping/RT/LDH) — then counts the
    current status of each in ``v_current_annotation_events``. Allele-subject counts are per allele
    (shared alleles counted once), reflecting the true current status regardless of which run produced
    it; variant-subject counts are per variant.
    """
    allele_ids = {row.allele_id for row in get_alleles_for_score_set(db, score_set.id)}
    variant_ids = set(db.scalars(select(Variant.id).where(Variant.score_set_id == score_set.id)).all())

    if not allele_ids and not variant_ids:
        return []

    rows = db.execute(
        select(
            CurrentAnnotationEventView.annotation_type,
            CurrentAnnotationEventView.disposition,
            func.count().label("count"),
        )
        .where(
            or_(
                CurrentAnnotationEventView.allele_id.in_(allele_ids),
                CurrentAnnotationEventView.variant_id.in_(variant_ids),
            )
        )
        .group_by(CurrentAnnotationEventView.annotation_type, CurrentAnnotationEventView.disposition)
        .order_by(CurrentAnnotationEventView.annotation_type, CurrentAnnotationEventView.disposition)
    ).all()

    return [{"annotation_type": r.annotation_type, "disposition": r.disposition, "count": r.count} for r in rows]


@script_environment.command(name="show-score-set")
@with_database_session
@click.argument("score_set_urn")
@click.option("--json", "as_json", is_flag=True, help="Emit result as JSON.")
def show_score_set(db: Session, score_set_urn: str, as_json: bool) -> None:
    """Summarize the current annotation status of a score set's alleles and variants."""
    score_set = _get_score_set(db, score_set_urn)
    if score_set is None:
        click.echo(f"Score set not found: {score_set_urn}", err=True)
        raise SystemExit(1)

    summary = current_annotation_summary(db, score_set)
    total_variants = db.scalar(select(func.count()).where(Variant.score_set_id == score_set.id)) or 0

    if as_json:
        click.echo(
            json.dumps(
                {
                    "score_set_urn": score_set_urn,
                    "total_variants": total_variants,
                    "annotation_summary": summary,
                },
                indent=2,
            )
        )
        return

    click.echo(f"Score set: {score_set_urn}  ({total_variants} variants)")
    click.echo(f"\n{'ANNOTATION TYPE':<32} {'DISPOSITION':<16} COUNT")
    for r in summary:
        click.echo(f"{r['annotation_type']:<32} {str(r['disposition']):<16} {r['count']}")

    if not summary:
        click.echo("No current annotation events found for this score set.")


if __name__ == "__main__":
    script_environment()

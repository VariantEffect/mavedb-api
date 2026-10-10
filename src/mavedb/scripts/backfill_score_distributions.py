"""Fill ``ScoreSet.score_distribution`` for score sets whose variants predate the column.

New uploads compute the summary in the variant creation job; this one-off covers existing score sets.
Score sets are processed one at a time so only one score set's variant data is in memory at once. With
``--commit``, each score set commits as it finishes, so a run against a serving database never holds row locks on
more than one score set at a time and an interrupted run keeps its progress.

Usage:

    python -m mavedb.scripts.backfill_score_distributions --dry-run
    python -m mavedb.scripts.backfill_score_distributions --commit
    python -m mavedb.scripts.backfill_score_distributions --commit --force
"""

import logging

import asyncclick as click
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.score_distribution import summarize_variant_data
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.scripts.environment import DatabaseSessionAction, script_environment, with_database_session

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

VARIANT_FETCH_BATCH_SIZE = 10_000


def backfill(db: Session, force: bool = False, commit_each: bool = False) -> tuple[int, int]:
    """Summarize each score set with variants; returns ``(filled, without_numeric_scores)``.

    ``commit_each`` commits after every score set instead of leaving the transaction to the caller.
    """
    query = select(ScoreSet.id).where(ScoreSet.num_variants > 0).order_by(ScoreSet.id)
    if not force:
        query = query.where(ScoreSet.score_distribution.is_(None))
    score_set_ids = db.scalars(query).all()
    logger.info(f"Summarizing scores for {len(score_set_ids)} score sets.")

    filled = 0
    empty = 0
    for index, score_set_id in enumerate(score_set_ids, start=1):
        variant_data = db.scalars(
            select(Variant.data)
            .where(Variant.score_set_id == score_set_id)
            .execution_options(yield_per=VARIANT_FETCH_BATCH_SIZE)
        )
        summary = summarize_variant_data(variant_data)

        score_set = db.get_one(ScoreSet, score_set_id)
        score_set.score_distribution = summary
        if commit_each:
            db.commit()
        else:
            db.flush()
        db.expunge(score_set)

        if summary is None:
            empty += 1
        else:
            filled += 1

        if index % 100 == 0:
            logger.info(f"Processed {index} of {len(score_set_ids)} score sets.")

    return filled, empty


@script_environment.command()
@with_database_session(pass_action=True)
@click.option("--force", is_flag=True, help="Recompute summaries that already exist.")
def backfill_score_distributions(db: Session, action: DatabaseSessionAction, force: bool) -> None:
    """Compute the score distribution summary for every score set with variants."""
    filled, empty = backfill(db, force, commit_each=action is DatabaseSessionAction.COMMIT)
    logger.info(f"Filled {filled} score distributions; {empty} score sets had no numeric scores.")


if __name__ == "__main__":
    backfill_score_distributions()

"""
Score-set scoping shared by the audit scripts.

An unscoped audit covers the whole database, so on an environment where only some score sets have been
through the current pipeline, findings from stale score sets mix with real ones. These options restrict an
audit to the score sets that matter. Given together, they intersect.
"""

import datetime
from typing import Callable, Optional, Sequence

import asyncclick as click
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from mavedb.models.collection import Collection
from mavedb.models.collection_score_set_association import CollectionScoreSetAssociation
from mavedb.models.score_set import ScoreSet

# A score set every one of whose variants has an RT event on or after :since. RT writes an event for every
# variant it processes, not_applicable and failed included, so a variant RT has not reached since then keeps
# the score set out.
TRANSLATED_SINCE_SQL = """
    SELECT score_set_id
    FROM (
        SELECT e.score_set_id, e.variant_id, max(e.created_at) AS translated_at
        FROM annotation_event e
        WHERE e.annotation_type = 'cross_level_translation'
        GROUP BY 1, 2
    ) latest
    GROUP BY score_set_id
    HAVING min(translated_at) >= :since
"""


def scope_options(command: Callable) -> Callable:
    """Add ``--score-set``, ``--collection`` and ``--translated-since`` to an audit command."""
    command = click.option(
        "--translated-since",
        type=click.DateTime(formats=["%Y-%m-%d"]),
        default=None,
        help="Only score sets whose every variant was reverse translated on or after this date (UTC).",
    )(command)
    command = click.option("--collection", default=None, help="Only score sets in this collection (URN).")(command)
    command = click.option("--score-set", "urns", multiple=True, help="Only this score set (URN). Repeatable.")(command)
    return command


def resolve_scope(
    db: Session,
    urns: Sequence[str],
    collection: Optional[str],
    translated_since: Optional[datetime.datetime],
) -> Optional[list[int]]:
    """The score-set ids the scope options select, or ``None`` when none were given."""
    if not urns and collection is None and translated_since is None:
        return None

    query = select(ScoreSet.id)

    if urns:
        found = set(db.scalars(select(ScoreSet.urn).where(ScoreSet.urn.in_(urns))))
        if missing := sorted(set(urns) - found):
            raise click.BadParameter(f"No score set with URN: {', '.join(missing)}", param_hint="--score-set")
        query = query.where(ScoreSet.urn.in_(urns))

    if collection is not None:
        if db.scalar(select(Collection.id).where(Collection.urn == collection)) is None:
            raise click.BadParameter(f"No collection with URN: {collection}", param_hint="--collection")
        query = (
            query.join(CollectionScoreSetAssociation, CollectionScoreSetAssociation.score_set_id == ScoreSet.id)
            .join(Collection, Collection.id == CollectionScoreSetAssociation.collection_id)
            .where(Collection.urn == collection)
        )

    if translated_since is not None:
        since = translated_since.replace(tzinfo=datetime.timezone.utc)
        translated = [row[0] for row in db.execute(text(TRANSLATED_SINCE_SQL), {"since": since})]
        query = query.where(ScoreSet.id.in_(translated))

    score_set_ids = list(db.scalars(query))
    if not score_set_ids:
        raise click.UsageError("No score set matches the given scope.")

    return score_set_ids

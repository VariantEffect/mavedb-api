"""Write the measured projection group of every live mapping record, score set by score set.

New mappings get their groups from the reverse translation job. This covers records that have not been
through it since projection groups existed, notably those the substrate backfill reshaped from legacy
mappings, which carry no groups. Only coordinates are needed (cdot and UTA), not the mapper, so it is fast
enough to run straight after a release. Re-running is safe: unchanged groups are left alone.

Rewriting a changed group detaches the alleles reverse translation added to it, the measured variant's
other-level partner and its protein consequence, along with their ClinVar, gnomAD and VEP annotations.
Each score set's log line counts them as ``detached_rt_links``; re-run reverse translation on every score
set with a non-zero count. ``--missing-only`` only writes groups that do not exist yet, so it never
detaches anything.

With ``--commit``, each score set commits as it finishes, so an interrupted run keeps its progress and a run
against a serving database holds row locks on one score set at a time. ``--shard`` splits the score sets
between concurrent processes.

Usage:
    python -m mavedb.scripts.project_measured_variants --dry-run --score-set urn:mavedb:00000663-a-1
    python -m mavedb.scripts.project_measured_variants --commit --shard 0/4
    python -m mavedb.scripts.project_measured_variants --commit --missing-only
"""

import logging
import time
from collections import Counter
from typing import Optional

import asyncclick as click
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.data_providers.services import cdot_rest
from mavedb.models.score_set import ScoreSet
from mavedb.scripts.environment import DatabaseSessionAction, script_environment, with_database_session
from mavedb.worker.jobs.variant_processing.reverse_translation import cdna_transcript_resolver
from mavedb.worker.lib.projection import project_measured_variants
from mavedb.worker.lib.translation_ports import WorkerCoordinateTranslator

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _parse_shard(shard: Optional[str]) -> Optional[tuple[int, int]]:
    if shard is None:
        return None
    index, _, count = shard.partition("/")
    if not (index.isdigit() and count.isdigit()) or not 0 <= int(index) < int(count):
        raise click.BadParameter("expected INDEX/COUNT, e.g. 0/4", param_hint="--shard")
    return int(index), int(count)


@script_environment.command()
@with_database_session(pass_action=True)
@click.option("--score-set", "urns", multiple=True, help="Only this score set (URN). Repeatable.")
@click.option(
    "--shard", default=None, help="Process only every COUNT-th score set by size, starting at INDEX, as INDEX/COUNT."
)
@click.option(
    "--missing-only",
    is_flag=True,
    help="Only records with no measured projection group yet. Never detaches reverse translation alleles.",
)
def project_measured_variants_command(
    db: Session, action: DatabaseSessionAction, urns: tuple[str, ...], shard: Optional[str], missing_only: bool
) -> None:
    """Write measured projection groups for every score set with variants."""
    shard_spec = _parse_shard(shard)
    # Largest first, dealt round-robin, so shards finish together rather than one holding the big score sets.
    query = (
        select(ScoreSet.id, ScoreSet.urn)
        .where(ScoreSet.num_variants > 0)
        .order_by(ScoreSet.num_variants.desc(), ScoreSet.id)
    )
    if urns:
        query = query.where(ScoreSet.urn.in_(urns))
    score_sets = [
        (id_, urn)
        for position, (id_, urn) in enumerate(db.execute(query).tuples())
        if shard_spec is None or position % shard_spec[1] == shard_spec[0]
    ]
    logger.info(f"Projecting measured variants for {len(score_sets)} score sets.")

    coordinates = WorkerCoordinateTranslator(cdot_rest())
    totals: Counter[str] = Counter()
    started = time.monotonic()
    for index, (score_set_id, urn) in enumerate(score_sets, start=1):
        counts = project_measured_variants(
            db,
            score_set_id,
            coordinates=coordinates,
            resolve_transcript=cdna_transcript_resolver(db, score_set_id, coordinates),
            missing_only=missing_only,
        )
        if action is DatabaseSessionAction.COMMIT:
            db.commit()
        else:
            db.flush()
        db.expunge_all()
        totals.update(counts)
        logger.info(f"[{index}/{len(score_sets)}] {urn}: {dict(counts)}")

    elapsed = time.monotonic() - started
    logger.info(f"Done in {elapsed:.0f}s: {dict(totals)}")


if __name__ == "__main__":
    project_measured_variants_command()

"""Drive a score-set pipeline (mapping or annotation enrichment) over many score sets at once.

Two selection modes:

* ``--select unmapped`` (default) → runs ``map_annotate_score_set`` over score sets that still need
  mapping. "Needs mapping" = a variant lacking a **live** ``MappingRecord`` dated on/after
  ``--remap-before`` (default 2026-05-08, significant mapping-capability upgrade). This catches
  never-mapped, partially-mapped, and *stale-but-complete* score sets — including reshape-backfilled
  ones, whose records carry the old ``mapped_date`` and so need a real re-map under the upgraded
  pipeline.
* ``--select unenriched`` → runs ``annotate_score_set`` over mapped score sets that have not yet had
  the reverse-translation fan-out (no derived, non-authoritative live allele link) **and** have a
  transcript reverse translation can use. The transcript gate excludes score sets whose every
  variant would skip as ``transcript_unresolved``, which would otherwise be re-selected forever and
  waste worker cycles. This is the post-backfill enrichment half.

**Throughput — gene-grouped ordering.** Score sets whose variants resolve to the same CAIDs/PAIDs
cache the expensive ClinGen CAR resolution (24h TTL) and reuse already-deduplicated alleles. So by
default the run is ordered by a normalized gene key — ``coalesce(mapped_hgnc_name,
target_accession.gene, target_gene.name)``, first whitespace token uppercased (``JAG1 Exon 1-7`` →
``JAG1``) — clustering same-gene score sets consecutively so the second-onward sets in a gene group
hit a warm cache and existing alleles. (The resolved ``mapped_hgnc_name`` is null pre-mapping, so the
unmapped mode falls back to the accession gene / submitter name.) ``--no-group-by-gene`` sorts by id.

Enrichment is non-blocking; both pipelines hit external services, so ``--delay-seconds`` paces starts
and ``--limit`` bounds a batch. Idempotent: re-runs skip score sets already in the target end state
(``--force`` overrides).

Usage::

    poetry run python -m mavedb.scripts.run_score_set_pipelines --dry-run
    poetry run python -m mavedb.scripts.run_score_set_pipelines --limit 50 --delay-seconds 3
    poetry run python -m mavedb.scripts.run_score_set_pipelines --select unenriched --limit 50
    poetry run python -m mavedb.scripts.run_score_set_pipelines --remap-before 2026-05-08
"""

from __future__ import annotations

import asyncio
import datetime
import logging
from typing import Optional

import asyncclick as click
from arq import create_pool
from sqlalchemy import and_, func, not_, or_, select
from sqlalchemy.orm import Session, selectinload

from mavedb.db.session import SessionLocal
from mavedb.lib.workflow.kickoff import enqueue_pipeline_for_score_set
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.enums.sequence_level import SequenceLevel
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_gene_mapping import TargetGeneMapping
from mavedb.models.user import User
from mavedb.models.variant import Variant
from mavedb.worker.settings import RedisWorkerSettings

logger = logging.getLogger(__name__)

# The mapping-capability upgrade: score sets last mapped before this need a re-map, even if their
# mapping_state is "complete".
DEFAULT_REMAP_BEFORE = datetime.date(2026, 5, 8)

# Default pipeline per selection mode.
PIPELINE_FOR_SELECT = {
    "unmapped": "map_annotate_score_set",
    "unenriched": "annotate_score_set",
}


def _normalize_gene(raw: str) -> str:
    """Gene key for cache-locality ordering: first whitespace token, uppercased.

    ``"JAG1 Exon 1-7"`` → ``"JAG1"``, ``" brca1 "`` → ``"BRCA1"``. Gene symbols are one token, so the
    first word is the gene; upper-casing folds submitter case differences together.
    """
    tokens = raw.strip().split()
    return tokens[0].upper().strip(".,;:") if tokens else ""


def _grouping_key(score_set: ScoreSet) -> str:
    """Normalized gene key for a score set — the min across its target genes for determinism.

    Prefers the resolved gene (``mapped_hgnc_name``, populated only post-mapping), then the accession's
    gene, then the submitter target name.
    """
    keys = []
    for target_gene in score_set.target_genes:
        accession_gene = target_gene.target_accession.gene if target_gene.target_accession else None
        raw = target_gene.mapped_hgnc_name or accession_gene or target_gene.name
        if raw:
            keys.append(_normalize_gene(raw))
    return min(keys) if keys else ""


def _needs_mapping(remap_before: datetime.date):
    """Predicate: the score set has a variant lacking a live MappingRecord dated on/after the cutoff."""
    fresh_record = select(MappingRecord.id).where(
        MappingRecord.variant_id == Variant.id,
        MappingRecord.valid_to.is_(None),
        MappingRecord.mapped_date >= remap_before,
    )
    variant_needing_mapping = select(Variant.id).where(Variant.score_set_id == ScoreSet.id, ~fresh_record.exists())
    return variant_needing_mapping.exists()


def _mapped_without_enrichment():
    """Predicate: the score set is mapped (a live MappingRecord) but has no reverse-translation-derived
    (non-authoritative) live allele link yet."""
    has_live_record = (
        select(MappingRecord.id)
        .join(Variant, Variant.id == MappingRecord.variant_id)
        .where(Variant.score_set_id == ScoreSet.id, MappingRecord.valid_to.is_(None))
    )
    has_derived_link = (
        select(MappingRecordAllele.id)
        .join(MappingRecord, MappingRecord.id == MappingRecordAllele.mapping_record_id)
        .join(Variant, Variant.id == MappingRecord.variant_id)
        .where(
            Variant.score_set_id == ScoreSet.id,
            MappingRecord.valid_to.is_(None),
            MappingRecordAllele.valid_to.is_(None),
            MappingRecordAllele.is_authoritative.is_(False),
        )
    )
    return and_(has_live_record.exists(), not_(has_derived_link.exists()))


def _has_usable_transcript():
    """Predicate: the score set has a transcript that reverse translation can use.

    Mirrors the two transcript sources in
    ``worker/jobs/variant_processing/reverse_translation.py``: a cdna ``TargetGeneMapping``'s
    ``reference_accession`` (the mapper-supplied coding transcript), and a RefSeq protein
    (``NP_``/``XP_``) ``hgvs_assay_level`` on a live ``MappingRecord`` (resolved ``NP_``→``NM_`` via
    UTA). A score set with neither would have every variant skip as ``transcript_unresolved``, so
    the unenriched selection excludes it rather than re-queue it forever."""
    has_cdna_transcript = (
        select(TargetGeneMapping.id)
        .join(TargetGene, TargetGene.id == TargetGeneMapping.target_gene_id)
        .where(
            TargetGene.score_set_id == ScoreSet.id,
            TargetGeneMapping.alignment_level == SequenceLevel.cdna,
            TargetGeneMapping.reference_accession.isnot(None),
        )
    )
    # `_` is a LIKE wildcard, so escape it to match the literal underscore in the RefSeq prefix.
    has_refseq_protein_record = (
        select(MappingRecord.id)
        .join(Variant, Variant.id == MappingRecord.variant_id)
        .where(
            Variant.score_set_id == ScoreSet.id,
            MappingRecord.valid_to.is_(None),
            or_(
                MappingRecord.hgvs_assay_level.like(r"NP\_%", escape="\\"),
                MappingRecord.hgvs_assay_level.like(r"XP\_%", escape="\\"),
            ),
        )
    )
    return or_(has_cdna_transcript.exists(), has_refseq_protein_record.exists())


def _needs_enrichment():
    """Predicate: the score set is mapped-but-not-enriched AND has a usable transcript, so
    ``annotate_score_set``'s reverse-translation step has something to translate against."""
    return and_(_mapped_without_enrichment(), _has_usable_transcript())


def count_unenrichable_without_transcript(db: Session, *, only_published: bool = True) -> int:
    """Count mapped-but-not-enriched score sets that ``--select unenriched`` skips for lack of a
    usable transcript, so the operator sees they were intentionally dropped (not lost)."""
    query = (
        select(func.count())
        .select_from(ScoreSet)
        .where(
            ScoreSet.processing_state == ProcessingState.success,
            _mapped_without_enrichment(),
            not_(_has_usable_transcript()),
        )
    )
    if only_published:
        query = query.where(ScoreSet.private.is_(False), ScoreSet.published_date.isnot(None))
    return db.scalar(query) or 0


def select_score_sets(
    db: Session,
    *,
    phase: str = "unmapped",
    remap_before: datetime.date = DEFAULT_REMAP_BEFORE,
    only_published: bool = True,
    force: bool = False,
    score_set_urn: Optional[str] = None,
    group_by_gene: bool = True,
    limit: Optional[int] = None,
) -> list[ScoreSet]:
    """Return score sets to run, gene-grouped by default.

    ``phase`` picks the eligibility predicate (``"unmapped"`` / ``"unenriched"``); ``force`` bypasses
    it (every processed score set qualifies). Only variant-processed score sets are considered (the
    pipelines assume variants exist), published unless ``only_published=False``. Ordering: by the
    normalized gene key so same-gene sets cluster (``group_by_gene``), else by id; ``limit`` applies
    after ordering so whole gene groups are taken first.
    """
    query = (
        select(ScoreSet)
        .options(selectinload(ScoreSet.target_genes).selectinload(TargetGene.target_accession))
        .where(ScoreSet.processing_state == ProcessingState.success)
    )
    if only_published:
        query = query.where(ScoreSet.private.is_(False), ScoreSet.published_date.isnot(None))
    if score_set_urn is not None:
        query = query.where(ScoreSet.urn == score_set_urn)
    if not force:
        query = query.where(_needs_mapping(remap_before) if phase == "unmapped" else _needs_enrichment())

    score_sets = list(db.scalars(query).unique().all())
    score_sets.sort(key=(lambda ss: (_grouping_key(ss), ss.id)) if group_by_gene else (lambda ss: ss.id))
    return score_sets[:limit] if limit is not None else score_sets


def _resolve_user(db: Session, score_set: ScoreSet, updater_id: Optional[int]) -> Optional[User]:
    """The user to attribute the pipeline to: explicit override, else the score set's modifier/creator."""
    resolved_id = updater_id or score_set.modified_by_id or score_set.created_by_id
    if resolved_id is None:
        return None
    return db.scalars(select(User).where(User.id == resolved_id)).one_or_none()


@click.command()
@click.option(
    "--select",
    "phase",
    type=click.Choice(["unmapped", "unenriched"]),
    default="unmapped",
    show_default=True,
    help="Which score sets to run: those needing mapping, or those mapped-but-not-enriched.",
)
@click.option("--pipeline", "pipeline_name", default=None, help="Override the per-mode default pipeline.")
@click.option(
    "--remap-before",
    "remap_before",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=DEFAULT_REMAP_BEFORE.isoformat(),
    show_default=True,
    help="Re-map score sets last mapped before this date (unmapped mode only).",
)
@click.option("--score-set-urn", "score_set_urn", default=None, help="Run only this score set.")
@click.option("--updater-id", "updater_id", type=int, default=None, help="User to attribute pipelines to.")
@click.option("--limit", type=int, default=None, help="Max score sets to enqueue this run.")
@click.option(
    "--delay-seconds", "delay_seconds", type=float, default=0.0, show_default=True, help="Pause between starts."
)
@click.option("--no-group-by-gene", "group_by_gene", flag_value=False, default=True, help="Sort by id instead of gene.")
@click.option("--force", is_flag=True, help="Ignore the eligibility check (include already-done score sets).")
@click.option("--include-unpublished", is_flag=True, help="Also include private/unpublished score sets.")
@click.option("--dry-run", is_flag=True, help="List the selected score sets without enqueuing anything.")
async def main(
    phase: str,
    pipeline_name: Optional[str],
    remap_before: datetime.datetime,
    score_set_urn: Optional[str],
    updater_id: Optional[int],
    limit: Optional[int],
    delay_seconds: float,
    group_by_gene: bool,
    force: bool,
    include_unpublished: bool,
    dry_run: bool,
) -> None:
    """Run a score-set pipeline over many score sets, gene-grouped for cache reuse."""
    pipeline = pipeline_name or PIPELINE_FOR_SELECT[phase]
    db = SessionLocal()
    try:
        score_sets = select_score_sets(
            db,
            phase=phase,
            remap_before=remap_before.date(),
            only_published=not include_unpublished,
            force=force,
            score_set_urn=score_set_urn,
            group_by_gene=group_by_gene,
            limit=limit,
        )
        click.echo(f"{len(score_sets)} score set(s) selected ({phase}) for pipeline '{pipeline}'.")

        if phase == "unenriched" and not force:
            skipped = count_unenrichable_without_transcript(db, only_published=not include_unpublished)
            if skipped:
                click.echo(f"  ({skipped} mapped score set(s) skipped: no usable transcript for reverse translation.)")

        if dry_run:
            for score_set in score_sets:
                click.echo(f"  {_grouping_key(score_set):<12} {score_set.urn}")
            click.echo("Dry run — nothing enqueued.")
            return

        if not score_sets:
            return

        redis = await create_pool(RedisWorkerSettings)
        enqueued = 0
        try:
            for index, score_set in enumerate(score_sets):
                user = _resolve_user(db, score_set, updater_id)
                if user is None:
                    click.echo(f"  SKIP {score_set.urn}: no updater found (pass --updater-id).", err=True)
                    continue

                try:
                    pipeline_run, job = await enqueue_pipeline_for_score_set(
                        db, redis, pipeline_name=pipeline, score_set=score_set, user=user
                    )
                except (KeyError, ValueError) as exc:
                    click.echo(f"  FAIL {score_set.urn}: {exc}", err=True)
                    continue

                enqueued += 1
                disposition = "queued" if job else "duplicate"
                click.echo(
                    f"  [{enqueued}/{len(score_sets)}] {score_set.urn} → pipeline {pipeline_run.id} ({disposition})"
                )

                if delay_seconds and index < len(score_sets) - 1:
                    await asyncio.sleep(delay_seconds)

        finally:
            await redis.aclose()

        click.echo(f"Enqueued {enqueued} pipeline(s).")

    finally:
        db.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()

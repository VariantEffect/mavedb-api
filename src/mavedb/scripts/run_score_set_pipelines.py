"""Bulk-drive map + annotate pipelines across a cohort of score sets.

Unlike run_pipeline.py (exactly one score set per invocation), this script selects a
cohort of score sets, orders it to exploit ClinGen's 24h Allele Registry cache, bounds
concurrency against a campaign-wide in-flight window, skips work already done, and
reports per-score-set outcomes.

This is a windowed top-up driver, not a long-lived babysitter: each invocation refills
the in-flight window up to --concurrency in gene order, prints campaign status, and
exits. Re-invoke it (by hand, cron, or /loop) to keep driving progress; the heavy
pipeline work happens entirely in the worker, with the Pipeline/JobRun tables as
durable state.

Usage:
    # Preview what would be enqueued, without enqueuing anything.
    poetry run python -m mavedb.scripts.run_score_set_pipelines map_annotate_score_set \\
        --collection-urn urn:mavedb:collection-0000001 --published-only --dry-run

    # Drive up to 4 concurrent pipelines for every published human score set.
    poetry run python -m mavedb.scripts.run_score_set_pipelines map_annotate_score_set \\
        --taxonomy-id 9606 --published-only --concurrency 4

    # Get every score set a CAID first (fast), before annotating.
    poetry run python -m mavedb.scripts.run_score_set_pipelines map_annotate_score_set \\
        --phase caid --collection-urn urn:mavedb:collection-0000001

    poetry run python -m mavedb.scripts.run_score_set_pipelines --list
"""

import datetime
import logging
import sys
from typing import Literal, Optional, Sequence

import asyncclick as click
from arq import create_pool
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from mavedb.db.session import SessionLocal
from mavedb.lib.types.workflow import JobDefinition, PipelineDefinition
from mavedb.lib.workflow.definitions import PIPELINE_DEFINITIONS
from mavedb.lib.workflow.pipeline_factory import PipelineFactory
from mavedb.models.collection import Collection
from mavedb.models.collection_score_set_association import CollectionScoreSetAssociation
from mavedb.models.enums.job_pipeline import PipelineStatus
from mavedb.models.job_run import JobRun
from mavedb.models.pipeline import Pipeline
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_sequence import TargetSequence
from mavedb.models.taxonomy import Taxonomy
from mavedb.models.user import User
from mavedb.scripts.run_pipeline import _print_available_pipelines
from mavedb.worker.lib.managers.utils import arq_job_id
from mavedb.worker.settings import RedisWorkerSettings

logger = logging.getLogger(__name__)

# This script owns its own terminal/in-flight classification rather than importing
# mavedb.worker.lib.managers.constants' TERMINAL_PIPELINE_STATUSES/CANCELLABLE_PIPELINE_STATUSES:
# those lists are defined for the worker's cancellability semantics, and while they
# currently happen to partition PipelineStatus the same way we need, coupling to them
# would mean a worker-motivated change could silently change this script's throttling
# behavior. classify_status asserts exhaustiveness so any future 8th status is caught
# loudly rather than defaulting.
_TERMINAL_STATUSES = frozenset(
    {
        PipelineStatus.SUCCEEDED,
        PipelineStatus.FAILED,
        PipelineStatus.PARTIAL,
        PipelineStatus.CANCELLED,
    }
)
_IN_FLIGHT_STATUSES = frozenset({PipelineStatus.CREATED, PipelineStatus.RUNNING, PipelineStatus.PAUSED})

PRESET_JOB_KEYS: dict[str, frozenset[str]] = {
    "caid": frozenset({"submit_score_set_mappings_to_car"}),
    "fast-annotate": frozenset(
        {
            "link_gnomad_variants",
            "refresh_clinvar_controls",
            "populate_hgvs_for_score_set",
            "populate_variant_translations_for_score_set",
            "submit_uniprot_mapping_jobs_for_score_set",
            "poll_uniprot_mapping_jobs_for_score_set",
        }
    ),
    "vep": frozenset({"populate_vep_for_score_set"}),
}

EnqueueDecision = Literal["enqueue", "skip_current", "skip_in_flight", "skip_cap"]


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------


def classify_status(status: PipelineStatus) -> Literal["terminal", "in_flight"]:
    """Classify a PipelineStatus as terminal or in-flight.

    Raises ValueError on an unrecognized status rather than silently defaulting,
    so an unhandled future PipelineStatus member is caught immediately.
    """
    if status in _TERMINAL_STATUSES:
        return "terminal"
    if status in _IN_FLIGHT_STATUSES:
        return "in_flight"
    raise ValueError(f"Unrecognized PipelineStatus: {status!r}")


def is_failure(status: PipelineStatus) -> bool:
    """CANCELLED is a terminal, intentional outcome, not a failure."""
    return status in (PipelineStatus.FAILED, PipelineStatus.PARTIAL)


def is_current(
    status: PipelineStatus, finished_at: Optional[datetime.datetime], current_since: Optional[datetime.date]
) -> bool:
    """Whether a pipeline counts as "already done" for skip-if-current purposes.

    current_since=None disables skip-if-current entirely (locked decision: operators
    must be explicit about what "done" means for a given campaign).
    """
    if current_since is None:
        return False
    if status != PipelineStatus.SUCCEEDED or finished_at is None:
        return False
    # finished_at may come back from the DB normalized to the server/session timezone rather
    # than UTC; comparing .date() directly can shift the day near midnight. Normalize to UTC first.
    return finished_at.astimezone(datetime.timezone.utc).date() >= current_since


def normalize_gene(name: str) -> str:
    return name.strip().casefold()


def grouping_key(normalized_gene_names: Sequence[str]) -> str:
    return min(normalized_gene_names) if normalized_gene_names else ""


def order_cohort(items: list[tuple[ScoreSet, list[str]]]) -> list[tuple[ScoreSet, list[str]]]:
    """Stable sort by (grouping_key(genes), urn) to cluster gene-adjacent score sets
    together, exploiting ClinGen's 24h cache for shared variants/alleles."""
    return sorted(items, key=lambda item: (grouping_key(item[1]), item[0].urn or ""))


def effective_pipeline_name(pipeline_name: str, phase: Optional[str]) -> str:
    """The name tracked everywhere downstream: cohort-query joins, the in-flight
    window, skip-if-current, and (for phase runs) the name half of create_pipeline's
    custom_pipeline tuple."""
    return f"{pipeline_name}:{phase}" if phase else pipeline_name


def resolve_job_subset(job_definitions: list[JobDefinition], leaf_keys: frozenset[str]) -> list[JobDefinition]:
    """Compute the transitive-dependency closure of leaf_keys within job_definitions.

    Raises ValueError naming any leaf_keys not present in this base pipeline (e.g.
    --phase vep against publish_score_set, which has no populate_vep_for_score_set job).
    Returns the subsequence of job_definitions whose key is in the closure, preserving
    the base pipeline's original order so JobRun creation order stays deterministic.
    """
    by_key = {job_def["key"]: job_def for job_def in job_definitions}

    missing = leaf_keys - by_key.keys()
    if missing:
        raise ValueError(f"Job key(s) not present in this pipeline: {', '.join(sorted(missing))}")

    needed: set[str] = set()
    worklist = list(leaf_keys)
    while worklist:
        key = worklist.pop()
        if key in needed:
            continue
        needed.add(key)
        for dep_key, _dependency_type in by_key[key]["dependencies"]:
            worklist.append(dep_key)

    return [job_def for job_def in job_definitions if job_def["key"] in needed]


def build_custom_pipeline_def(
    base_def: PipelineDefinition, phase: str, subset_jobs: list[JobDefinition]
) -> PipelineDefinition:
    return {"description": f"{base_def['description']} (phase: {phase})", "job_definitions": subset_jobs}


def plan_enqueue(
    ordered_cohort: list[tuple[ScoreSet, list[str]]],
    *,
    in_flight_score_set_ids: set[int],
    current_score_set_ids: set[int],
    slots: int,
    limit: Optional[int],
) -> list[tuple[ScoreSet, str, EnqueueDecision]]:
    """Single source of truth for both --dry-run output and the real enqueue loop.

    Skip-current/skip-in-flight decisions never consume a slot; only "enqueue" does.
    """
    plan: list[tuple[ScoreSet, str, EnqueueDecision]] = []
    remaining_slots = slots
    enqueued_count = 0

    for score_set, genes in ordered_cohort:
        key = grouping_key(genes)

        if score_set.id in current_score_set_ids:
            plan.append((score_set, key, "skip_current"))
            continue

        if score_set.id in in_flight_score_set_ids:
            plan.append((score_set, key, "skip_in_flight"))
            continue

        if remaining_slots <= 0:
            plan.append((score_set, key, "skip_cap"))
            continue

        if limit is not None and enqueued_count >= limit:
            plan.append((score_set, key, "skip_cap"))
            continue

        plan.append((score_set, key, "enqueue"))
        remaining_slots -= 1
        enqueued_count += 1

    return plan


# ---------------------------------------------------------------------------
# DB-backed functions
# ---------------------------------------------------------------------------


def resolve_cohort(
    db: Session,
    *,
    explicit_urns: Optional[list[str]],
    collection_urn: Optional[str],
    published_only: bool,
    taxonomy_id: Optional[int],
    organism: Optional[str],
) -> list[ScoreSet]:
    """Resolve the cohort of score sets targeted by this invocation.

    All filters AND together, including explicit URNs (they narrow, not bypass).
    Callers must refuse to run (see main()) when every filter is empty, rather than
    silently operating over every score set in MaveDB.
    """
    query = select(ScoreSet).options(
        selectinload(ScoreSet.target_genes)
        .selectinload(TargetGene.target_sequence)
        .selectinload(TargetSequence.taxonomy)
    )

    if explicit_urns is not None:
        query = query.where(ScoreSet.urn.in_(explicit_urns))

    if collection_urn:
        query = (
            query.join(CollectionScoreSetAssociation, CollectionScoreSetAssociation.score_set_id == ScoreSet.id)
            .join(Collection, Collection.id == CollectionScoreSetAssociation.collection_id)
            .where(Collection.urn == collection_urn)
        )

    if published_only:
        query = query.where(ScoreSet.published_date.isnot(None))

    needs_distinct = False
    if taxonomy_id is not None or organism:
        query = (
            query.join(TargetGene, TargetGene.score_set_id == ScoreSet.id)
            .join(TargetSequence, TargetSequence.id == TargetGene.target_sequence_id)
            .join(Taxonomy, Taxonomy.id == TargetSequence.taxonomy_id)
        )
        if taxonomy_id is not None:
            query = query.where(Taxonomy.code == taxonomy_id)
        if organism:
            query = query.where(Taxonomy.organism_name == organism)
        needs_distinct = True

    if needs_distinct:
        query = query.distinct()

    return list(db.scalars(query).all())


def build_cohort_items(score_sets: list[ScoreSet]) -> list[tuple[ScoreSet, list[str]]]:
    return [(ss, [normalize_gene(tg.name) for tg in ss.target_genes]) for ss in score_sets]  # type: ignore[arg-type]


def _pipeline_score_set_query(*, tracked_name: str, statuses: Optional[Sequence[PipelineStatus]]):
    query = (
        select(Pipeline, JobRun.job_params["score_set_id"].astext)
        .join(JobRun, JobRun.pipeline_id == Pipeline.id)
        .where(Pipeline.name == tracked_name)
        # Every Pipeline has a start_pipeline JobRun with job_params={}, so
        # job_params["score_set_id"] is SQL NULL for that row. Without this filter,
        # dedup-by-pipeline.id in callers can nondeterministically pick that NULL row
        # over the real one, since row order across a JOIN with no ORDER BY isn't guaranteed.
        .where(JobRun.job_params["score_set_id"].astext.isnot(None))
    )
    if statuses is not None:
        query = query.where(Pipeline.status.in_(statuses))
    return query


def in_flight_pipelines(db: Session, *, tracked_name: str) -> list[tuple[Pipeline, Optional[int]]]:
    """Campaign-wide (not scoped to the resolved cohort): this is the real global
    ClinGen throttle. Dedupes by Pipeline.id since a pipeline may have several
    annotation-phase JobRuns that each carry score_set_id."""
    query = _pipeline_score_set_query(tracked_name=tracked_name, statuses=list(_IN_FLIGHT_STATUSES))
    seen: dict[int, tuple[Pipeline, Optional[int]]] = {}
    for pipeline, score_set_id in db.execute(query).all():
        if pipeline.id not in seen:
            seen[pipeline.id] = (pipeline, int(score_set_id) if score_set_id is not None else None)
    return list(seen.values())


def pipelines_by_score_set(
    db: Session,
    *,
    tracked_name: str,
    score_set_ids: list[int],
    statuses: Optional[Sequence[PipelineStatus]] = None,
) -> dict[int, list[Pipeline]]:
    """Cohort-scoped join, optional status filter.

    Filters on JobRun.job_params["score_set_id"].astext.in_(<str ids>) — .astext
    returns text, so ids are cast to str on the Python side (exact-string equality
    means id 1 would never otherwise match id 12).
    """
    if not score_set_ids:
        return {}

    query = _pipeline_score_set_query(tracked_name=tracked_name, statuses=statuses).where(
        JobRun.job_params["score_set_id"].astext.in_([str(i) for i in score_set_ids])
    )

    result: dict[int, list[Pipeline]] = {}
    seen_pairs: set[tuple[int, int]] = set()
    for pipeline, score_set_id in db.execute(query).all():
        if score_set_id is None:
            continue
        ss_id = int(score_set_id)
        pair = (pipeline.id, ss_id)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        result.setdefault(ss_id, []).append(pipeline)

    return result


def representative_error(db: Session, pipeline_id: int) -> Optional[str]:
    """Latest JobRun.error_message for a FAILED job under this pipeline."""
    job_run = db.scalars(
        select(JobRun)
        .where(JobRun.pipeline_id == pipeline_id, JobRun.error_message.isnot(None))
        .order_by(JobRun.created_at.desc())
        .limit(1)
    ).one_or_none()
    return job_run.error_message if job_run else None


def resolve_updater(
    db: Session, score_set: ScoreSet, updater_id_override: Optional[int], user_cache: dict[int, User]
) -> Optional[User]:
    """Same fallback chain as run_pipeline.py, memoized to avoid refetching the same
    user across a large cohort."""
    resolved_id = updater_id_override or score_set.modified_by_id or score_set.created_by_id
    if resolved_id is None:
        return None
    if resolved_id not in user_cache:
        user = db.scalars(select(User).where(User.id == resolved_id)).one_or_none()
        if user is None:
            return None
        user_cache[resolved_id] = user
    return user_cache[resolved_id]


class EnqueueOutcome:
    def __init__(self, ok: bool, message: str):
        self.ok = ok
        self.message = message


async def enqueue_pipeline(
    db: Session,
    redis,
    *,
    pipeline_name: Optional[str],
    custom_pipeline: Optional[tuple[str, PipelineDefinition]],
    score_set: ScoreSet,
    user: User,
    extra_params: tuple[tuple[str, str], ...],
) -> EnqueueOutcome:
    """run_pipeline.py's enqueue body, generalized to accept either a base pipeline
    name or a resolved (name, job-subset) pair. A redis.enqueue_job failure triggers
    discard_pipeline so orphaned Pipeline/JobRun rows don't accumulate across a large
    loop of enqueues."""
    tracked_name = custom_pipeline[0] if custom_pipeline else pipeline_name
    assert tracked_name is not None

    correlation_id = f"{tracked_name}_{score_set.urn}_{user.id}_{datetime.datetime.now().isoformat()}"
    pipeline_params: dict = {
        "correlation_id": correlation_id,
        "score_set_id": score_set.id,
        "updater_id": user.id,
    }
    for key, value in extra_params:
        pipeline_params[key] = value

    pipeline_factory = PipelineFactory(session=db)
    pipeline: Optional[Pipeline] = None
    try:
        pipeline, pipeline_entrypoint = pipeline_factory.create_pipeline(
            pipeline_name=pipeline_name,
            creating_user=user,
            pipeline_params=pipeline_params,
            custom_pipeline=custom_pipeline,
        )
    except (KeyError, ValueError) as e:
        return EnqueueOutcome(ok=False, message=f"Failed to create pipeline: {e}")

    if custom_pipeline is not None:
        # job_params only receives keys a job already declares (see JobFactory.create_job_run),
        # so a job_keys entry in pipeline_params would silently vanish. Pipeline.metadata_ is
        # where per-pipeline (not per-job) audit data belongs.
        pipeline.metadata_["job_keys"] = [job_def["key"] for job_def in custom_pipeline[1]["job_definitions"]]
        db.add(pipeline)
        db.commit()

    try:
        job = await redis.enqueue_job(
            pipeline_entrypoint.job_function,
            pipeline_entrypoint.id,
            _job_id=arq_job_id(pipeline_entrypoint),
        )
    except Exception as e:
        pipeline_factory.discard_pipeline(pipeline)
        return EnqueueOutcome(ok=False, message=f"Failed to enqueue: {e}")

    if job is None:
        return EnqueueOutcome(ok=True, message=f"Job was already enqueued (duplicate); pipeline id={pipeline.id}")

    return EnqueueOutcome(ok=True, message=f"Enqueued pipeline id={pipeline.id}, job={job.job_id}")


def _format_age(now: datetime.datetime, created_at: Optional[datetime.datetime]) -> str:
    if created_at is None:
        return "-"
    delta = now - created_at.replace(tzinfo=created_at.tzinfo or datetime.timezone.utc)
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    return f"{hours}h{minutes}m"


def render_report(
    db: Session,
    *,
    tracked_name: str,
    ordered_cohort: list[tuple[ScoreSet, list[str]]],
    in_flight_rows: list[tuple[Pipeline, Optional[int]]],
    current_since: Optional[datetime.date],
) -> tuple[str, list[str]]:
    """Per-score-set outcome table plus the in-flight detail table.

    Returns (report_text, failed_urns) — failed_urns feeds --failure-out and exit code 2.
    """
    lines: list[str] = []
    failed_urns: list[str] = []

    score_set_ids: list[int] = [ss.id for ss, _ in ordered_cohort]  # type: ignore[misc]
    latest_by_score_set = pipelines_by_score_set(db, tracked_name=tracked_name, score_set_ids=score_set_ids)

    lines.append(f"Cohort report for '{tracked_name}' ({len(ordered_cohort)} score sets):")
    lines.append(f"{'URN':<40} {'STATUS':<12} ERROR")
    for score_set, _genes in ordered_cohort:
        pipelines = latest_by_score_set.get(score_set.id, [])  # type: ignore[arg-type]
        if not pipelines:
            lines.append(f"{score_set.urn:<40} {'no run':<12}")
            continue

        latest = max(pipelines, key=lambda p: p.created_at)
        error = ""
        if is_failure(latest.status):
            failed_urns.append(score_set.urn)  # type: ignore[arg-type]
            error = representative_error(db, latest.id) or ""
        lines.append(f"{score_set.urn:<40} {str(latest.status):<12} {error}")

    lines.append("")
    lines.append(f"In-flight ('{tracked_name}'), {len(in_flight_rows)} pipeline(s):")
    if in_flight_rows:
        now = datetime.datetime.now(datetime.timezone.utc)
        lines.append(f"{'URN':<40} {'STATUS':<12} AGE")
        score_set_by_id = {ss.id: ss for ss, _ in ordered_cohort}
        for pipeline, score_set_id in in_flight_rows:
            urn = (
                score_set_by_id[score_set_id].urn
                if score_set_id in score_set_by_id
                else f"(score_set_id={score_set_id})"
            )
            lines.append(f"{urn:<40} {str(pipeline.status):<12} {_format_age(now, pipeline.created_at)}")
    else:
        lines.append("  (none)")

    return "\n".join(lines), failed_urns


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.command()
@click.argument("pipeline_name", required=False)
@click.option("--list", "list_pipelines", is_flag=True, help="List available pipelines and exit.")
@click.option(
    "--phase",
    type=click.Choice(list(PRESET_JOB_KEYS.keys())),
    default=None,
    help="Restrict this run to the named job subset (+ transitive deps) within pipeline_name's graph.",
)
@click.option("--collection-urn", default=None, help="Only score sets in this collection.")
@click.option("--published-only", is_flag=True, help="Only score sets with a published_date.")
@click.option(
    "--taxonomy-id",
    type=int,
    default=None,
    help="Only score sets with a sequence-based target in this taxonomy (Taxonomy.code).",
)
@click.option(
    "--organism",
    default=None,
    help="Only score sets with a sequence-based target for this organism (Taxonomy.organism_name).",
)
@click.option("--score-set-urn", "score_set_urns", multiple=True, help="Restrict to these URNs (repeatable).")
@click.option(
    "--urns-file",
    type=click.Path(exists=True, dir_okay=False, readable=True),
    default=None,
    help="File of URNs, one per line, to restrict to (blank lines skipped).",
)
@click.option(
    "--current-since",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Skip score sets whose latest pipeline SUCCEEDED on/after this date. Omit to disable skip-if-current.",
)
@click.option(
    "--concurrency",
    type=int,
    default=4,
    show_default=True,
    help="Campaign-wide cap on in-flight pipelines of this tracked name.",
)
@click.option("--limit", type=int, default=None, help="Additional cap on this invocation's enqueue count.")
@click.option("--dry-run", is_flag=True, help="Print the planned decision per cohort entry; enqueue nothing.")
@click.option(
    "--failure-out",
    type=click.Path(dir_okay=False, writable=True),
    default=None,
    help="Write one URN per line for every cohort score set whose latest pipeline is FAILED/PARTIAL.",
)
@click.option("--updater-id", type=int, default=None, help="ID of the user to attribute pipeline actions to.")
@click.option(
    "--extra-param",
    "extra_params",
    multiple=True,
    type=(str, str),
    help="Additional key=value params for the pipeline (repeatable).",
)
async def main(
    pipeline_name: Optional[str],
    list_pipelines: bool,
    phase: Optional[str],
    collection_urn: Optional[str],
    published_only: bool,
    taxonomy_id: Optional[int],
    organism: Optional[str],
    score_set_urns: tuple[str, ...],
    urns_file: Optional[str],
    current_since: Optional[datetime.datetime],
    concurrency: int,
    limit: Optional[int],
    dry_run: bool,
    failure_out: Optional[str],
    updater_id: Optional[int],
    extra_params: tuple[tuple[str, str], ...],
) -> None:
    """Bulk-drive PIPELINE_NAME across a cohort of score sets. Use --list to see available pipelines."""
    if list_pipelines or not pipeline_name:
        _print_available_pipelines()
        return

    if pipeline_name not in PIPELINE_DEFINITIONS:
        click.echo(f"Unknown pipeline: {pipeline_name}", err=True)
        click.echo(f"Available: {', '.join(PIPELINE_DEFINITIONS.keys())}", err=True)
        sys.exit(1)

    explicit_urns: Optional[list[str]] = None
    if score_set_urns or urns_file:
        urns = list(score_set_urns)
        if urns_file:
            with open(urns_file) as f:
                urns.extend(line.strip() for line in f if line.strip())
        explicit_urns = urns

    if not (explicit_urns or collection_urn or published_only or taxonomy_id is not None or organism):
        click.echo(
            "Refusing to run with no cohort filter (--collection-urn, --score-set-urn, --urns-file, "
            "--published-only, --taxonomy-id, --organism). Operating over every score set in MaveDB "
            "is almost certainly not what you want.",
            err=True,
        )
        sys.exit(1)

    custom_pipeline: Optional[tuple[str, PipelineDefinition]] = None
    effective_name = effective_pipeline_name(pipeline_name, phase)
    run_pipeline_name: Optional[str] = pipeline_name
    if phase:
        base_def = PIPELINE_DEFINITIONS[pipeline_name]
        try:
            subset_jobs = resolve_job_subset(base_def["job_definitions"], PRESET_JOB_KEYS[phase])
        except ValueError as e:
            click.echo(f"Failed to resolve --phase {phase}: {e}", err=True)
            sys.exit(1)
        custom_pipeline = (effective_name, build_custom_pipeline_def(base_def, phase, subset_jobs))
        run_pipeline_name = None

    db = SessionLocal()

    score_sets = resolve_cohort(
        db,
        explicit_urns=explicit_urns,
        collection_urn=collection_urn,
        published_only=published_only,
        taxonomy_id=taxonomy_id,
        organism=organism,
    )

    if explicit_urns is not None:
        missing = set(explicit_urns) - {ss.urn for ss in score_sets}
        for urn in sorted(missing):
            click.echo(f"Requested URN not found (or excluded by other filters): {urn}", err=True)

    ordered_cohort = order_cohort(build_cohort_items(score_sets))
    current_since_date = current_since.date() if current_since else None

    current_score_set_ids: set[int] = set()
    if current_since_date is not None:
        succeeded = pipelines_by_score_set(
            db,
            tracked_name=effective_name,
            score_set_ids=[ss.id for ss, _ in ordered_cohort],  # type: ignore[misc]
            statuses=[PipelineStatus.SUCCEEDED],
        )
        for ss_id, pipelines in succeeded.items():
            if any(is_current(p.status, p.finished_at, current_since_date) for p in pipelines):
                current_score_set_ids.add(ss_id)

    in_flight_rows = in_flight_pipelines(db, tracked_name=effective_name)
    in_flight_score_set_ids = {ss_id for _p, ss_id in in_flight_rows if ss_id is not None}

    slots = max(0, concurrency - len(in_flight_rows))
    plan = plan_enqueue(
        ordered_cohort,
        in_flight_score_set_ids=in_flight_score_set_ids,
        current_score_set_ids=current_score_set_ids,
        slots=slots,
        limit=limit,
    )

    click.echo(f"Tracked pipeline name: {effective_name}")
    click.echo(
        f"Cohort size: {len(ordered_cohort)}; in-flight: {len(in_flight_rows)}; concurrency: {concurrency}; slots available: {slots}"
    )

    if dry_run:
        for score_set, key, decision in plan:
            click.echo(f"  [{decision:<14}] {score_set.urn}  (gene={key or '-'})")
    elif any(decision == "enqueue" for _ss, _key, decision in plan):
        user_cache: dict[int, User] = {}
        redis = await create_pool(RedisWorkerSettings)
        try:
            for score_set, _key, decision in plan:
                if decision != "enqueue":
                    continue
                user = resolve_updater(db, score_set, updater_id, user_cache)
                if user is None:
                    click.echo(f"  [skip: no updater] {score_set.urn}", err=True)
                    continue

                outcome = await enqueue_pipeline(
                    db,
                    redis,
                    pipeline_name=run_pipeline_name,
                    custom_pipeline=custom_pipeline,
                    score_set=score_set,
                    user=user,
                    extra_params=extra_params,
                )

                prefix = "  [enqueued]" if outcome.ok else "  [failed]  "
                click.echo(f"{prefix} {score_set.urn}: {outcome.message}", err=not outcome.ok)
        finally:
            await redis.aclose()

    report_text, failed_urns = render_report(
        db,
        tracked_name=effective_name,
        ordered_cohort=ordered_cohort,
        in_flight_rows=in_flight_rows,
        current_since=current_since_date,
    )
    click.echo("")
    click.echo(report_text)

    if failure_out:
        with open(failure_out, "w") as f:
            for urn in failed_urns:
                f.write(f"{urn}\n")

    db.close()

    if failed_urns:
        sys.exit(2)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()

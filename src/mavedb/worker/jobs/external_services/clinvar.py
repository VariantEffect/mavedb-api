"""ClinVar integration jobs for variant annotation.

Links deduplicated alleles to ClinVar clinical-control data across every archival ClinVar
release. Each release is a distinct, versioned ``ClinvarControl`` entity, and an allele
accumulates one live ``ClinvarAlleleLink`` per release it appears in (multi-live, unlike
gnomAD/VEP which hold one live result per allele).

Status is recorded in two steps, so a fact that doesn't depend on the release is stored once:
``clinvar_allele_id`` (one per allele: does ClinGen link its CAID to a ClinVar AlleleID?) and
``clinvar_control`` (one per allele per release, only for alleles with an AlleleID: is it in that
release?).

Both ClinGen API calls and ClinVar TSV data fetches are automatically cached using
aiocache with Redis backend:
- ClinGen API calls: 24-hour TTL
- ClinVar TSV files: 90-day TTL (archival data doesn't change)
"""

import logging
from collections import Counter, defaultdict
from collections.abc import Iterable
from datetime import datetime
from typing import Optional

import requests
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from mavedb.lib.annotation_status_manager import STATEMENT_CHUNK_SIZE, AnnotationStatusManager
from mavedb.lib.clingen.allele_registry import get_associated_clinvar_allele_id
from mavedb.lib.clingen.alleles import get_alleles_for_score_set, group_alleles_for_annotation
from mavedb.lib.clinvar.utils import fetch_clinvar_variant_data
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.lib.utils import batched
from mavedb.models.clinical_control import ClinvarControl
from mavedb.models.clinvar_allele_link import ClinvarAlleleLink
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.enums.event_reason import EventReason
from mavedb.models.enums.job_pipeline import FailureCategory
from mavedb.models.enums.sequence_level import SequenceLevel
from mavedb.models.score_set import ScoreSet
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager

logger = logging.getLogger(__name__)

# Alleles between progress checkpoints while looking up AlleleIDs. A first run can make thousands of uncached
# ClinGen lookups; without checkpoints the stall reaper cancels it at 30 min.
CLINVAR_PROGRESS_EVERY_ALLELES = 500

LOOKUP_PROGRESS_SHARE = 50
"""Percent of the job's progress bar the AlleleID lookups take; evaluating the releases takes the rest. Lookups are
the slow part of a first run."""
# ClinVar archived data starts from February 2015, then January of each
# subsequent year. This list is used to generate the date range for refreshing.
CLINVAR_START_YEAR = 2015
CLINVAR_START_MONTH = 2


def _generate_clinvar_versions() -> list[tuple[int, int]]:
    """Generate all ClinVar version (year, month) pairs from Feb 2015 to current Jan.

    Returns a list of (year, month) tuples representing each ClinVar archival
    snapshot that should be processed.
    """
    current_year = datetime.now().year
    first_version = (CLINVAR_START_YEAR, CLINVAR_START_MONTH)
    archival_versions = [(year, 1) for year in range(CLINVAR_START_YEAR + 1, current_year + 1)]
    return [first_version, *archival_versions]


async def _lookup_clinvar_allele_ids(caids: Iterable[str]) -> dict[str, str | requests.exceptions.RequestException]:
    """Resolve each CAID's ClinVar AlleleID through ClinGen, keeping a failed lookup's exception as its result."""
    clinvar_allele_ids: dict[str, str | requests.exceptions.RequestException] = {}
    for caid in caids:
        try:
            clinvar_allele_ids[caid] = await get_associated_clinvar_allele_id(caid)
        except requests.exceptions.RequestException as exc:
            clinvar_allele_ids[caid] = exc

    return clinvar_allele_ids


def _report_allele_id(
    annotation_manager: AnnotationStatusManager,
    allele_id: int,
    caid: str,
    disposition: Disposition,
    reason: EventReason,
    *,
    clinvar_allele_id: str | None = None,
    error_message: str | None = None,
) -> None:
    """Report an allele's ``clinvar_allele_id`` status: whether ClinGen links its CAID to a ClinVar AlleleID."""
    metadata = {"clingen_allele_id": caid}
    if clinvar_allele_id:
        metadata["clinvar_allele_id"] = clinvar_allele_id
    if error_message:
        metadata["error_message"] = error_message

    annotation_manager.report(
        AnnotationType.CLINVAR_ALLELE_ID, allele_id=allele_id, disposition=disposition, reason=reason, metadata=metadata
    )


def _report_release(
    annotation_manager: AnnotationStatusManager,
    allele_id: int,
    caid: str,
    clinvar_allele_id: str,
    release: str,
    disposition: Disposition,
    reason: EventReason,
    *,
    error_message: str | None = None,
) -> None:
    """Report an allele's ``clinvar_control`` status for ``release``: whether its AlleleID is in that release."""
    metadata = {"clingen_allele_id": caid, "clinvar_allele_id": clinvar_allele_id}
    if error_message:
        metadata["error_message"] = error_message

    annotation_manager.report(
        AnnotationType.CLINVAR_CONTROL,
        allele_id=allele_id,
        disposition=disposition,
        reason=reason,
        source_version=release,
        metadata=metadata,
    )


@with_pipeline_management
async def refresh_clinvar_controls(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    """Link deduplicated alleles to ClinVar clinical-control data across all archival versions.

    First resolves each allele's ClinGen Allele ID (CAID) to a ClinVar AlleleID, once per allele. A known
    AlleleID is stable, so only alleles without one (never looked up, absent last time, or failing) are looked
    up again. Then, for every ClinVar archival snapshot (Feb 2015, then Jan of each subsequent year through
    the current year), evaluates each allele with an AlleleID that has no outcome for that release yet:
    upserts the versioned :class:`ClinvarControl` and establishes a live :class:`ClinvarAlleleLink`. Archival
    releases don't change, so an evaluated release is never evaluated again. An allele that gains an AlleleID
    is evaluated against every release on the same run. Individual version fetch failures are logged and
    skipped — the job continues with the remaining versions.

    Job Parameters:
        - score_set_id (int): The ID of the ScoreSet whose alleles to process.
        - correlation_id (str): Correlation ID for tracing requests across services.
        - force (bool, optional): Look up every AlleleID and evaluate every release again. The link write
          still get-or-creates (no duplicate live link), so a forced re-run of unchanged data writes no new
          links.

    Side Effects:
        - Creates ClinvarControl and ClinvarAlleleLink rows.

    Returns:
        JobExecutionOutcome: outcome with version and per-link counts.
    """
    job = job_manager.get_job()

    _job_required_params = ["score_set_id", "correlation_id"]
    validate_job_params(_job_required_params, job)

    score_set = job_manager.db.scalars(select(ScoreSet).where(ScoreSet.id == job.job_params["score_set_id"])).one()  # type: ignore
    correlation_id = job.job_params["correlation_id"]  # type: ignore
    force = bool(job.job_params.get("force", False))  # type: ignore[union-attr]

    versions = _generate_clinvar_versions()

    job_manager.save_to_context(
        {
            "application": "mavedb-worker",
            "function": "refresh_clinvar_controls",
            "resource": score_set.urn,
            "correlation_id": correlation_id,
            "versions": versions,
            "total_versions": len(versions),
            "force": force,
        }
    )
    job_manager.update_progress(0, 100, f"Starting ClinVar refresh across {len(versions)} versions.")
    logger.info(f"Starting ClinVar refresh across {len(versions)} versions", extra=job_manager.logging_context())

    # One work-unit per allele (payload = CAID; alleles without one are dropped). Covers the score
    # set's alleles — authoritative and RT-derived — since the genomic allele ClinVar keys
    # on is often the RT-derived one. Events are allele-keyed, so every allele is recorded per release
    # (no fan-out).
    allele_rows = get_alleles_for_score_set(job_manager.db, score_set.id)
    allele_data = group_alleles_for_annotation(allele_rows, payload=lambda row: row.clingen_allele_id)
    allele_levels = {row.allele_id: row.level for row in allele_rows}
    job_manager.save_to_context({"num_alleles_with_caids": len(allele_data)})

    # Link counts accumulate across all versions (an allele may link in every release it appears in).
    annotation_counts: Counter[str] = Counter(
        {
            "created_link_count": 0,
            "preexisting_link_count": 0,
            "skipped_link_count": 0,
            "failed_link_count": 0,
            "already_evaluated_count": 0,
        }
    )

    if not allele_data:
        logger.warning(
            msg="No current alleles with CAIDs were found for this score set. Skipping ClinVar refresh.",
            extra=job_manager.logging_context(),
        )
        job_manager.db.flush()
        return JobExecutionOutcome.succeeded(
            data={"versions_completed": 0, "versions_total": len(versions), **dict(annotation_counts)}
        )

    annotation_manager = AnnotationStatusManager(job_manager.db, job_run_id=job_manager.job_id)

    known_allele_ids, corrected = await _resolve_allele_ids(
        job_manager, annotation_manager, allele_data, allele_levels, force=force, counts=annotation_counts
    )

    evaluated = defaultdict(set) if force else _evaluated_releases(annotation_manager, set(known_allele_ids))

    # An allele whose AlleleID changed was linked through the old one, so every release is evaluated again against the
    # new one, which replaces or withdraws each old link.
    for allele_ids in evaluated.values():
        allele_ids -= corrected

    versions_completed = 0
    for version_index, (year, month) in enumerate(versions):
        clinvar_version = f"{month:02d}_{year}"
        job_manager.append_to_context("clinvar_versions", clinvar_version)
        job_manager.update_progress(
            LOOKUP_PROGRESS_SHARE + int(version_index / len(versions) * (100 - LOOKUP_PROGRESS_SHARE)),
            100,
            f"Processing ClinVar version {clinvar_version} ({version_index + 1}/{len(versions)}).",
        )

        pending = [allele_id for allele_id in known_allele_ids if allele_id not in evaluated[clinvar_version]]
        annotation_counts["already_evaluated_count"] += len(known_allele_ids) - len(pending)
        if not pending:
            versions_completed += 1
            continue

        logger.info(f"Processing ClinVar version {clinvar_version}", extra=job_manager.logging_context())
        try:
            tsv_data = await fetch_clinvar_variant_data(month, year)
        except Exception:
            logger.error(
                f"Failed to fetch/parse ClinVar TSV for version {clinvar_version}, skipping.",
                extra=job_manager.logging_context(),
                exc_info=True,
            )
            continue

        _evaluate_release(
            job_manager,
            annotation_manager,
            clinvar_version,
            tsv_data,
            pending,
            allele_data,
            known_allele_ids,
            corrected,
            annotation_counts,
        )
        versions_completed += 1

    logger.info(
        f"ClinVar refresh complete: {versions_completed}/{len(versions)} versions, "
        f"{annotation_counts['created_link_count']} new links, "
        f"{annotation_counts['preexisting_link_count']} preexisting.",
        extra=job_manager.logging_context(),
    )

    if annotation_counts["failed_link_count"] > 0 and not known_allele_ids:
        error_message = f"All {annotation_counts['failed_link_count']} ClinVar lookups failed for score set {score_set.urn}. Possible ClinGen API outage."
        logger.error(error_message, extra=job_manager.logging_context())
        job_manager.db.flush()
        return JobExecutionOutcome.failed(
            reason=error_message,
            data={
                "versions_completed": versions_completed,
                "versions_total": len(versions),
                **dict(annotation_counts),
            },
            failure_category=FailureCategory.DEPENDENCY_FAILURE,
        )

    job_manager.db.flush()
    return JobExecutionOutcome.succeeded(
        data={"versions_completed": versions_completed, "versions_total": len(versions), **dict(annotation_counts)}
    )


async def _resolve_allele_ids(
    job_manager: JobManager,
    annotation_manager: AnnotationStatusManager,
    allele_data: dict[int, str],
    allele_levels: dict[int, Optional[str]],
    *,
    force: bool,
    counts: Counter[str],
) -> tuple[dict[int, str], set[int]]:
    """Step 1: each allele's ClinVar AlleleID, which doesn't depend on the release. Returns the alleles that have one,
    and those to evaluate against every release again: their AlleleID differs from the stored one, or they hold live
    links through another AlleleID.

    A known AlleleID is stable, so only alleles without one (never linked through one) are looked up, unless
    ``force``. Lookups run in chunks, each committed by a progress checkpoint before the next one
    awaits ClinGen.
    """
    stored = _stored_clinvar_allele_ids(annotation_manager, set(allele_data))
    linked = _linked_clinvar_allele_ids(job_manager, set(allele_data))
    known = {} if force else {allele_id: stored.get(allele_id) or min(ids) for allele_id, ids in linked.items()}
    to_look_up: list[tuple[int, str]] = []
    for allele_id, caid in allele_data.items():
        # ClinVar is a nucleotide-level database, so protein-level alleles never apply.
        if allele_levels.get(allele_id) == SequenceLevel.protein.value:
            counts["skipped_link_count"] += 1
            _report_allele_id(
                annotation_manager,
                allele_id,
                caid,
                Disposition.NOT_APPLICABLE,
                EventReason.PROTEIN_LEVEL_ALLELE,
                error_message="Protein-level alleles are not linked to ClinVar; see the nucleotide encodings.",
            )
        # A cis-block (multi-variant) CAID structurally cannot key ClinVar — a terminal gap, not a statement about
        # ClinVar's contents.
        elif "," in caid:
            counts["skipped_link_count"] += 1
            _report_allele_id(
                annotation_manager,
                allele_id,
                caid,
                Disposition.NOT_APPLICABLE,
                EventReason.MULTI_VARIANT_CAID,
                error_message="Multi-variant ClinGen allele IDs cannot be associated with ClinVar data.",
            )
        elif allele_id in known:
            _report_allele_id(
                annotation_manager,
                allele_id,
                caid,
                Disposition.PRESENT,
                EventReason.PREEXISTING,
                clinvar_allele_id=known[allele_id],
            )
        else:
            to_look_up.append((allele_id, caid))

    for chunk_start in range(0, len(to_look_up), CLINVAR_PROGRESS_EVERY_ALLELES):
        if chunk_start:
            job_manager.update_progress(
                int(chunk_start / len(to_look_up) * LOOKUP_PROGRESS_SHARE),
                100,
                f"Looking up ClinVar allele IDs: {chunk_start}/{len(to_look_up)} alleles.",
            )

        # The checkpoint above commits the previous chunk's statuses, releasing the status lock before this await.
        # A sibling job waiting on that lock would otherwise freeze the event loop, and this job would never run
        # again to release it.
        chunk = to_look_up[chunk_start : chunk_start + CLINVAR_PROGRESS_EVERY_ALLELES]
        lookups = await _lookup_clinvar_allele_ids(caid for _, caid in chunk)
        for allele_id, caid in chunk:
            clinvar_allele_id = lookups[caid]
            if isinstance(clinvar_allele_id, requests.exceptions.RequestException):
                counts["failed_link_count"] += 1
                _report_allele_id(
                    annotation_manager,
                    allele_id,
                    caid,
                    Disposition.FAILED,
                    EventReason.API_ERROR,
                    error_message=f"Failed to retrieve ClinVar allele ID from ClinGen API: {clinvar_allele_id}",
                )
                logger.error(
                    f"Failed to retrieve ClinVar allele ID from ClinGen API for ClinGen allele ID {caid}.",
                    extra=job_manager.logging_context(),
                    exc_info=clinvar_allele_id,
                )
            # ClinGen has no ClinVar AlleleID for this CAID — the allele is not a ClinVar control: an informative
            # negative about the source, not a pipeline gap. Looked up again on the next run, since a new ClinVar
            # submission can link it.
            elif not clinvar_allele_id:
                counts["skipped_link_count"] += 1
                _report_allele_id(
                    annotation_manager,
                    allele_id,
                    caid,
                    Disposition.ABSENT,
                    EventReason.NO_RECORD,
                    error_message="No ClinVar allele ID found for ClinGen allele ID.",
                )
            else:
                known[allele_id] = clinvar_allele_id
                # Metadata alone isn't a change, so a different AlleleID needs a different reason to be stored.
                replaced = stored.get(allele_id) not in (None, clinvar_allele_id)
                if replaced:
                    logger.warning(
                        f"ClinGen allele ID {caid} now resolves to ClinVar allele {clinvar_allele_id}, not "
                        f"{stored[allele_id]}. Evaluating allele {allele_id} against every release again.",
                        extra=job_manager.logging_context(),
                    )
                _report_allele_id(
                    annotation_manager,
                    allele_id,
                    caid,
                    Disposition.PRESENT,
                    EventReason.CORRECTED if replaced else EventReason.CREATED,
                    clinvar_allele_id=clinvar_allele_id,
                )
        annotation_manager.flush()

    annotation_manager.flush()
    # The stored AlleleID, not the links, decides a correction: an AlleleID that no release contains leaves no links,
    # and every release's status was evaluated through it.
    corrected = {
        allele_id
        for allele_id, clinvar_allele_id in known.items()
        if stored.get(allele_id, clinvar_allele_id) != clinvar_allele_id
        or linked.get(allele_id, {clinvar_allele_id}) != {clinvar_allele_id}
    }
    return known, corrected


def _evaluate_release(
    job_manager: JobManager,
    annotation_manager: AnnotationStatusManager,
    release: str,
    tsv_data: dict,
    pending: list[int],
    allele_data: dict[int, str],
    known_allele_ids: dict[int, str],
    corrected: set[int],
    counts: Counter[str],
) -> None:
    """Step 2, for one release: link each pending allele whose AlleleID is in the release's data, and report
    whether it is. A ``corrected`` allele's link through its old AlleleID is replaced by the new link, or withdrawn
    when the new AlleleID isn't in the release. The next release's progress checkpoint commits these, before its data
    fetch awaits."""
    for allele_id in pending:
        caid, clinvar_allele_id = allele_data[allele_id], known_allele_ids[allele_id]
        # The allele has a ClinVar AlleleID but it is absent from this release's snapshot — a genuine, version-scoped
        # negative (ClinVar queried, nothing for this release).
        if clinvar_allele_id not in tsv_data:
            counts["skipped_link_count"] += 1
            if allele_id in corrected:
                release_controls = select(ClinvarControl.id).where(ClinvarControl.db_version == release)
                job_manager.db.execute(
                    ClinvarAlleleLink.retire_live_where(
                        ClinvarAlleleLink.allele_id == allele_id,
                        ClinvarAlleleLink.clinvar_control_id.in_(release_controls),
                    )
                )
            _report_release(
                annotation_manager,
                allele_id,
                caid,
                clinvar_allele_id,
                release,
                Disposition.ABSENT,
                EventReason.NO_RECORD,
                error_message="No ClinVar data found for ClinVar allele ID.",
            )
            continue

        reason = _link_clinvar_control(job_manager, allele_id, clinvar_allele_id, release, tsv_data[clinvar_allele_id])
        counts["preexisting_link_count" if reason is EventReason.PREEXISTING else "created_link_count"] += 1
        _report_release(annotation_manager, allele_id, caid, clinvar_allele_id, release, Disposition.PRESENT, reason)

    annotation_manager.flush()
    job_manager.db.flush()
    logger.info(f"Completed ClinVar version {release} for {len(pending)} alleles.", extra=job_manager.logging_context())


def _stored_clinvar_allele_ids(annotation_manager: AnnotationStatusManager, allele_ids: set[int]) -> dict[int, str]:
    """The ClinVar AlleleID each allele's ``clinvar_allele_id`` status last found, for alleles that have one."""
    return {
        row.allele_id: row.status_metadata["clinvar_allele_id"]
        for row in annotation_manager.current_statuses(AnnotationType.CLINVAR_ALLELE_ID, allele_ids)
        if row.disposition == Disposition.PRESENT.value and (row.status_metadata or {}).get("clinvar_allele_id")
    }


def _linked_clinvar_allele_ids(job_manager: JobManager, allele_ids: set[int]) -> dict[int, set[str]]:
    """The AlleleIDs of the controls each allele has live links to. More than one means a correction left a stale
    link, for instance when a release's data couldn't be fetched.

    An allele with an AlleleID that no release contains has no link, so it's looked up again each run.
    """
    known: defaultdict[int, set[str]] = defaultdict(set)
    for batch in batched(sorted(allele_ids), STATEMENT_CHUNK_SIZE):
        rows = job_manager.db.execute(
            select(ClinvarAlleleLink.allele_id, ClinvarControl.db_identifier)
            .join(ClinvarControl, ClinvarControl.id == ClinvarAlleleLink.clinvar_control_id)
            .where(ClinvarAlleleLink.allele_id.in_(batch), ClinvarAlleleLink.current)
            .distinct()
        )
        for allele_id, db_identifier in rows:
            known[allele_id].add(db_identifier)
    return dict(known)


def _evaluated_releases(
    annotation_manager: AnnotationStatusManager, allele_ids: set[int]
) -> defaultdict[str, set[int]]:
    """For each release, the alleles already evaluated against it: a ``clinvar_control`` outcome and no live
    failure."""
    evaluated: defaultdict[str, set[int]] = defaultdict(set)
    for row in annotation_manager.current_statuses(AnnotationType.CLINVAR_CONTROL, allele_ids):
        if row.disposition is not None and row.failing_since is None:
            evaluated[row.release].add(row.allele_id)
    return evaluated


def _link_clinvar_control(
    job_manager: JobManager, allele_id: int, clinvar_allele_id: str, clinvar_version: str, variant_data: dict
) -> EventReason:
    """Upsert the release's control for ``clinvar_allele_id`` and give the allele a live link to it. Returns
    ``CREATED``, ``PREEXISTING`` or ``SUPERSEDED``."""
    # Atomic upsert — avoids a check-then-act race when two refresh_clinvar_controls jobs run concurrently for
    # different score sets and encounter the same (db_name, db_identifier, db_version) tuple. ON CONFLICT DO UPDATE
    # returns exactly one row.
    upsert_stmt = (
        pg_insert(ClinvarControl)
        .values(
            db_identifier=str(clinvar_allele_id),
            db_version=clinvar_version,
            db_name="ClinVar",
            gene_symbol=variant_data.get("GeneSymbol"),
            clinical_significance=variant_data.get("ClinicalSignificance"),
            clinical_review_status=variant_data.get("ReviewStatus"),
            clinvar_variation_id=variant_data.get("VariationID"),
        )
        .on_conflict_do_update(
            constraint="uq_clinvar_controls_db_name_identifier_version",
            set_={
                "gene_symbol": variant_data.get("GeneSymbol"),
                "clinical_significance": variant_data.get("ClinicalSignificance"),
                "clinical_review_status": variant_data.get("ReviewStatus"),
                "clinvar_variation_id": variant_data.get("VariationID"),
            },
        )
        .returning(ClinvarControl)
    )
    clinvar_control = job_manager.db.scalars(upsert_stmt).one()

    # At most one live link per (allele, release). Normally a release is immutable, so the allele's live link for
    # this version is either a reconfirm of this same control (no-op) or absent (insert) — multi-live accumulates
    # only across *different* releases, never supersedes. Defensive guard: if the allele already holds a live link
    # to a *different* control of this same version, the release re-resolved under us (re-ingestion / upstream
    # correction — should never happen for archival data). Supersede newest-wins with a shared timestamp and log.
    live_link = job_manager.db.scalar(
        select(ClinvarAlleleLink)
        .join(ClinvarControl, ClinvarControl.id == ClinvarAlleleLink.clinvar_control_id)
        .where(
            ClinvarAlleleLink.allele_id == allele_id,
            ClinvarAlleleLink.current,
            ClinvarControl.db_version == clinvar_version,
        )
    )
    if live_link is None:
        job_manager.db.add(ClinvarAlleleLink(allele_id=allele_id, clinvar_control_id=clinvar_control.id))
        return EventReason.CREATED
    if live_link.clinvar_control_id == clinvar_control.id:
        return EventReason.PREEXISTING

    live_link.supersede_with(
        job_manager.db, ClinvarAlleleLink(allele_id=allele_id, clinvar_control_id=clinvar_control.id)
    )
    logger.warning(
        msg=(
            f"Allele {allele_id} held a live ClinVar link to control {live_link.clinvar_control_id} for version "
            f"{clinvar_version}, but re-resolved to control {clinvar_control.id} (ClinVar allele "
            f"{clinvar_allele_id}). Superseding newest-wins; archival ClinVar data should be immutable — "
            "investigate the upstream re-resolution."
        ),
        extra=job_manager.logging_context(),
    )
    return EventReason.SUPERSEDED

"""ClinVar integration jobs for variant annotation.

Links deduplicated alleles to ClinVar clinical-control data across every archival ClinVar
release. Each release is a distinct, versioned ``ClinvarControl`` entity, and an allele
accumulates one live ``ClinvarAlleleLink`` per release it appears in (multi-live, unlike
gnomAD/VEP which hold one live result per allele).

Both ClinGen API calls and ClinVar TSV data fetches are automatically cached using
aiocache with Redis backend:
- ClinGen API calls: 24-hour TTL
- ClinVar TSV files: 90-day TTL (archival data doesn't change)
"""

import logging
from datetime import datetime

import requests
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from mavedb.lib.clingen.allele_registry import get_associated_clinvar_allele_id
from mavedb.lib.clingen.alleles import get_alleles_for_score_set, group_alleles_for_annotation
from mavedb.lib.clinvar.utils import fetch_clinvar_variant_data
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.clinical_control import ClinvarControl
from mavedb.models.clinvar_allele_link import ClinvarAlleleLink
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationFailureCategory, AnnotationStatus, FailureCategory
from mavedb.models.score_set import ScoreSet
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager

logger = logging.getLogger(__name__)

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
    versions = [(CLINVAR_START_YEAR, CLINVAR_START_MONTH)]
    for year in range(CLINVAR_START_YEAR + 1, current_year + 1):
        versions.append((year, 1))
    return versions


def _annotate_clinvar(
    annotation_manager: AnnotationStatusManager,
    variant_ids: list[int],
    version: str,
    status: AnnotationStatus,
    *,
    failure_category: AnnotationFailureCategory | None = None,
    error_message: str | None = None,
    metadata: dict | None = None,
) -> None:
    """Fan a CLINVAR_CONTROL annotation out to every variant served by an allele.

    AAS migration seam: the single choke point for ClinVar's per-variant VAS writes. At migration it
    becomes an allele-keyed event writer; the per-variant fan-out goes away, and the variant
    association narrows to provenance. See docs/design/allele-annotation-status.md.

    Unlike gnomAD/VEP, ClinVar writes a status row per ClinVar release, so retirement is
    version-scoped (``replace_all_versions=False``) — each release keeps its own current row.
    """
    annotation_data: dict = {"annotation_metadata": metadata or {}}
    if error_message is not None:
        annotation_data["error_message"] = error_message

    for variant_id in variant_ids:
        annotation_manager.add_annotation(
            variant_id=variant_id,
            annotation_type=AnnotationType.CLINVAR_CONTROL,
            version=version,
            status=status,
            failure_category=failure_category,
            annotation_data=annotation_data,
            current=True,
            replace_all_versions=False,
        )


@with_pipeline_management
async def refresh_clinvar_controls(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    """Link deduplicated alleles to ClinVar clinical-control data across all archival versions.

    Iterates over every ClinVar archival snapshot (Feb 2015, then Jan of each subsequent year through
    the current year). For each version it resolves each allele's ClinGen Allele ID (CAID) to a ClinVar
    allele id, upserts the versioned :class:`ClinvarControl`, and establishes a live
    :class:`ClinvarAlleleLink`. An allele accumulates one live link per release (multi-live). Individual
    version fetch failures are logged and skipped — the job continues with the remaining versions.

    Job Parameters:
        - score_set_id (int): The ID of the ScoreSet whose alleles to process.
        - correlation_id (str): Correlation ID for tracing requests across services.
        - force (bool, optional): Bypass the per-version skip and re-resolve every allele. The link
          write still get-or-creates (no duplicate live link), so a forced re-run of unchanged data
          writes no new links.

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

    # One work-unit per allele (payload = CAID; alleles without one are dropped). Covers ALL the score
    # set's alleles — authoritative and RT-derived — since the genomic allele ClinVar keys on is often
    # the RT-derived one; VAS still fans only to authoritative_variant_ids (the bandaid seam).
    allele_data = group_alleles_for_annotation(
        get_alleles_for_score_set(job_manager.db, score_set.id),
        payload=lambda row: row.clingen_allele_id,
    )
    job_manager.save_to_context({"num_alleles_with_caids": len(allele_data)})

    if not allele_data:
        logger.warning(
            msg="No current alleles with CAIDs were found for this score set. Skipping ClinVar refresh.",
            extra=job_manager.logging_context(),
        )
        job_manager.db.flush()
        return JobExecutionOutcome.succeeded(
            data={
                "versions_completed": 0,
                "versions_total": len(versions),
                "created_link_count": 0,
                "preexisting_link_count": 0,
            }
        )

    all_allele_ids = set(allele_data.keys())

    def alleles_linked_at_version(clinvar_version: str) -> set[int]:
        """Allele ids (within the work set) holding a live ClinvarAlleleLink to a control of this version."""
        return set(
            job_manager.db.scalars(
                select(ClinvarAlleleLink.allele_id)
                .join(ClinvarControl, ClinvarControl.id == ClinvarAlleleLink.clinvar_control_id)
                .where(ClinvarAlleleLink.allele_id.in_(all_allele_ids))
                .where(ClinvarAlleleLink.current)
                .where(ClinvarControl.db_version == clinvar_version)
            ).all()
        )

    total_failed = 0
    created_link_count = 0
    preexisting_link_count = 0
    versions_completed = 0

    for version_index, (year, month) in enumerate(versions):
        clinvar_version = f"{month:02d}_{year}"
        job_manager.save_to_context({"current_version": clinvar_version, "version_index": version_index})

        version_progress = int((version_index / len(versions)) * 100)
        job_manager.update_progress(
            version_progress,
            100,
            f"Processing ClinVar version {clinvar_version} ({version_index + 1}/{len(versions)}).",
        )
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

        # Cost: skip alleles already linked at this version (an archival release cannot change). force
        # bypasses the skip but the link write still get-or-creates, so a forced no-op writes nothing.
        already_linked = set() if force else alleles_linked_at_version(clinvar_version)

        annotation_manager = AnnotationStatusManager(job_manager.db, job_run_id=job_manager.job_id)
        for allele_id, entry in allele_data.items():
            caid = entry.payload

            if allele_id in already_linked:
                preexisting_link_count += 1
                _annotate_clinvar(
                    annotation_manager,
                    entry.authoritative_variant_ids,
                    clinvar_version,
                    AnnotationStatus.SUCCESS,
                    metadata={"clingen_allele_id": caid, "action": "preexisting"},
                )
                continue

            if "," in caid:
                _annotate_clinvar(
                    annotation_manager,
                    entry.authoritative_variant_ids,
                    clinvar_version,
                    AnnotationStatus.SKIPPED,
                    failure_category=AnnotationFailureCategory.UNSUPPORTED_IDENTIFIER,
                    error_message="Multi-variant ClinGen allele IDs cannot be associated with ClinVar data.",
                    metadata={"clingen_allele_id": caid},
                )
                continue

            try:
                clinvar_allele_id = await get_associated_clinvar_allele_id(caid)
            except requests.exceptions.RequestException as exc:
                total_failed += 1
                _annotate_clinvar(
                    annotation_manager,
                    entry.authoritative_variant_ids,
                    clinvar_version,
                    AnnotationStatus.FAILED,
                    failure_category=AnnotationFailureCategory.EXTERNAL_API_ERROR,
                    error_message=f"Failed to retrieve ClinVar allele ID from ClinGen API: {str(exc)}",
                    metadata={"clingen_allele_id": caid},
                )
                logger.error(
                    f"Failed to retrieve ClinVar allele ID from ClinGen API for ClinGen allele ID {caid}.",
                    extra=job_manager.logging_context(),
                    exc_info=exc,
                )
                continue

            if not clinvar_allele_id:
                _annotate_clinvar(
                    annotation_manager,
                    entry.authoritative_variant_ids,
                    clinvar_version,
                    AnnotationStatus.SKIPPED,
                    failure_category=AnnotationFailureCategory.NO_LINKED_ALLELE,
                    error_message="No ClinVar allele ID found for ClinGen allele ID.",
                    metadata={"clingen_allele_id": caid},
                )
                continue

            if clinvar_allele_id not in tsv_data:
                _annotate_clinvar(
                    annotation_manager,
                    entry.authoritative_variant_ids,
                    clinvar_version,
                    AnnotationStatus.SKIPPED,
                    failure_category=AnnotationFailureCategory.EXTERNAL_REFERENCE_NOT_FOUND,
                    error_message="No ClinVar data found for ClinVar allele ID.",
                    metadata={"clingen_allele_id": caid},
                )
                continue

            variant_data = tsv_data[clinvar_allele_id]

            # Atomic upsert — avoids a check-then-act race when two refresh_clinvar_controls jobs run
            # concurrently for different score sets and encounter the same
            # (db_name, db_identifier, db_version) tuple. ON CONFLICT DO UPDATE returns exactly one row.
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

            # At most one live link per (allele, release). Normally a release is immutable, so the
            # allele's live link for this version is either a reconfirm of this same control (no-op) or
            # absent (insert) — multi-live accumulates only across *different* releases, never supersedes.
            # Defensive guard: if the allele already holds a live link to a *different* control of this
            # same version, the release re-resolved under us (re-ingestion / upstream correction — should
            # never happen for archival data). Supersede newest-wins with a shared timestamp and log.
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
                created_link_count += 1
                action = "created"
            elif live_link.clinvar_control_id == clinvar_control.id:
                preexisting_link_count += 1
                action = "preexisting"
            else:
                at = job_manager.db.scalar(select(func.now()))
                assert at is not None  # SELECT now() always returns a timestamp
                live_link.retire(at=at)
                job_manager.db.add(
                    ClinvarAlleleLink(allele_id=allele_id, clinvar_control_id=clinvar_control.id, valid_from=at)
                )
                created_link_count += 1
                action = "superseded"
                logger.warning(
                    msg=(
                        f"Allele {allele_id} held a live ClinVar link to control "
                        f"{live_link.clinvar_control_id} for version {clinvar_version}, but re-resolved to "
                        f"control {clinvar_control.id} (ClinVar allele {clinvar_allele_id}). Superseding "
                        "newest-wins; archival ClinVar data should be immutable — investigate the upstream "
                        "re-resolution."
                    ),
                    extra=job_manager.logging_context(),
                )

            _annotate_clinvar(
                annotation_manager,
                entry.authoritative_variant_ids,
                clinvar_version,
                AnnotationStatus.SUCCESS,
                metadata={"clingen_allele_id": caid, "clinvar_allele_id": clinvar_allele_id, "action": action},
            )

        annotation_manager.flush()
        job_manager.db.flush()
        versions_completed += 1
        logger.info(
            f"Completed ClinVar version {clinvar_version} for {len(allele_data)} alleles.",
            extra=job_manager.logging_context(),
        )

    logger.info(
        f"ClinVar refresh complete: {versions_completed}/{len(versions)} versions, "
        f"{created_link_count} new links, {preexisting_link_count} preexisting.",
        extra=job_manager.logging_context(),
    )

    if total_failed > 0 and created_link_count == 0 and preexisting_link_count == 0:
        error_message = (
            f"All {total_failed} ClinVar lookups failed for score set {score_set.urn}. Possible ClinGen API outage."
        )
        logger.error(error_message, extra=job_manager.logging_context())
        job_manager.db.flush()
        return JobExecutionOutcome.failed(
            reason=error_message,
            data={
                "versions_completed": versions_completed,
                "versions_total": len(versions),
                "created_link_count": 0,
                "preexisting_link_count": 0,
            },
            failure_category=FailureCategory.DEPENDENCY_FAILURE,
        )

    job_manager.db.flush()
    return JobExecutionOutcome.succeeded(
        data={
            "versions_completed": versions_completed,
            "versions_total": len(versions),
            "created_link_count": created_link_count,
            "preexisting_link_count": preexisting_link_count,
        }
    )

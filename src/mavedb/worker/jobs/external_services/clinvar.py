"""ClinVar integration jobs for variant annotation

This module contains job definitions and utility functions for integrating ClinVar
variant data into MaveDB. It includes functions to fetch and parse ClinVar variant
summary data, and update MaveDB records with the latest ClinVar annotations.

Both ClinGen API calls and ClinVar TSV data fetches are automatically cached using
aiocache with Redis backend:
- ClinGen API calls: 24-hour TTL
- ClinVar TSV files: 90-day TTL (archival data doesn't change)

This significantly reduces redundant network requests when refreshing ClinVar
controls across multiple months/years.
"""

import logging
from datetime import datetime

import requests
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from mavedb.lib.clingen.allele_registry import get_associated_clinvar_allele_id
from mavedb.lib.clinvar.utils import fetch_clinvar_variant_data
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.clinical_control import ClinicalControl
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationFailureCategory, AnnotationStatus, FailureCategory
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager

logger = logging.getLogger(__name__)

# ClinVar archived data starts from February 2015, then January of each
# subsequent year. This list is used to generate the date range for refreshing.
CLINVAR_START_YEAR = 2015
CLINVAR_START_MONTH = 2


def generate_clinvar_versions() -> list[tuple[int, int]]:
    """Generate all ClinVar version (year, month) pairs from Feb 2015 to current Jan.

    Returns a list of (year, month) tuples representing each ClinVar archival
    snapshot that should be processed.
    """
    current_year = datetime.now().year
    versions = [(CLINVAR_START_YEAR, CLINVAR_START_MONTH)]
    for year in range(CLINVAR_START_YEAR + 1, current_year + 1):
        versions.append((year, 1))
    return versions


@with_pipeline_management
async def refresh_clinvar_controls(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    """Refresh ClinVar clinical control data across all archival versions.

    Iterates over every ClinVar archival snapshot (Feb 2015, then Jan of each
    subsequent year through the current year), fetching TSV data and updating
    clinical control records for all mapped variants in the score set. Individual
    version failures are logged and skipped — the job continues processing
    remaining versions.
    """
    job = job_manager.get_job()

    _job_required_params = ["score_set_id", "correlation_id"]
    validate_job_params(_job_required_params, job)

    score_set = job_manager.db.scalars(select(ScoreSet).where(ScoreSet.id == job.job_params["score_set_id"])).one()  # type: ignore
    correlation_id = job.job_params["correlation_id"]  # type: ignore

    versions = generate_clinvar_versions()

    job_manager.save_to_context(
        {
            "application": "mavedb-worker",
            "function": "refresh_clinvar_controls",
            "resource": score_set.urn,
            "correlation_id": correlation_id,
            "versions": versions,
            "total_versions": len(versions),
        }
    )
    job_manager.update_progress(0, 100, f"Starting ClinVar refresh across {len(versions)} versions.")
    logger.info(f"Starting ClinVar refresh across {len(versions)} versions", extra=job_manager.logging_context())

    variants_to_refresh = job_manager.db.scalars(
        select(MappedVariant)
        .join(Variant)
        .where(
            Variant.score_set_id == score_set.id,
            MappedVariant.current.is_(True),
        )
    ).all()
    total_variants_to_refresh = len(variants_to_refresh)
    job_manager.save_to_context({"total_variants_to_refresh": total_variants_to_refresh})

    total_refreshed = 0
    total_failed = 0
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

        annotation_manager = AnnotationStatusManager(job_manager.db, job_run_id=job_manager.job_id)
        for mapped_variant in variants_to_refresh:
            clingen_id = mapped_variant.clingen_allele_id

            if clingen_id is None:
                annotation_manager.add_annotation(
                    variant_id=mapped_variant.variant_id,  # type: ignore
                    annotation_type=AnnotationType.CLINVAR_CONTROL,
                    version=clinvar_version,
                    status=AnnotationStatus.SKIPPED,
                    failure_category=AnnotationFailureCategory.MISSING_IDENTIFIER,
                    annotation_data={
                        "error_message": "Mapped variant does not have an associated ClinGen allele ID.",
                    },
                    current=True,
                    replace_all_versions=False,
                )
                continue

            if "," in clingen_id:
                annotation_manager.add_annotation(
                    variant_id=mapped_variant.variant_id,  # type: ignore
                    annotation_type=AnnotationType.CLINVAR_CONTROL,
                    version=clinvar_version,
                    status=AnnotationStatus.SKIPPED,
                    failure_category=AnnotationFailureCategory.UNSUPPORTED_IDENTIFIER,
                    annotation_data={
                        "error_message": "Multi-variant ClinGen allele IDs cannot be associated with ClinVar data.",
                    },
                    current=True,
                    replace_all_versions=False,
                )
                continue

            try:
                clinvar_allele_id = await get_associated_clinvar_allele_id(clingen_id)  # type: ignore
            except requests.exceptions.RequestException as exc:
                annotation_manager.add_annotation(
                    variant_id=mapped_variant.variant_id,  # type: ignore
                    annotation_type=AnnotationType.CLINVAR_CONTROL,
                    version=clinvar_version,
                    status=AnnotationStatus.FAILED,
                    failure_category=AnnotationFailureCategory.EXTERNAL_API_ERROR,
                    annotation_data={
                        "error_message": f"Failed to retrieve ClinVar allele ID from ClinGen API: {str(exc)}",
                    },
                    current=True,
                    replace_all_versions=False,
                )
                logger.error(
                    f"Failed to retrieve ClinVar allele ID from ClinGen API for ClinGen allele ID {clingen_id}.",
                    extra=job_manager.logging_context(),
                    exc_info=exc,
                )
                total_failed += 1
                continue

            if not clinvar_allele_id:
                annotation_manager.add_annotation(
                    variant_id=mapped_variant.variant_id,  # type: ignore
                    annotation_type=AnnotationType.CLINVAR_CONTROL,
                    version=clinvar_version,
                    status=AnnotationStatus.SKIPPED,
                    failure_category=AnnotationFailureCategory.NO_LINKED_ALLELE,
                    annotation_data={
                        "error_message": "No ClinVar allele ID found for ClinGen allele ID.",
                    },
                    current=True,
                    replace_all_versions=False,
                )
                continue

            if clinvar_allele_id not in tsv_data:
                annotation_manager.add_annotation(
                    variant_id=mapped_variant.variant_id,  # type: ignore
                    annotation_type=AnnotationType.CLINVAR_CONTROL,
                    version=clinvar_version,
                    status=AnnotationStatus.SKIPPED,
                    failure_category=AnnotationFailureCategory.EXTERNAL_REFERENCE_NOT_FOUND,
                    annotation_data={
                        "error_message": "No ClinVar data found for ClinVar allele ID.",
                    },
                    current=True,
                    replace_all_versions=False,
                )
                continue

            variant_data = tsv_data[clinvar_allele_id]
            identifier = str(clinvar_allele_id)

            # Atomic upsert — avoids a check-then-act race when two
            # refresh_clinvar_controls jobs run concurrently for different
            # score sets and encounter the same (db_name, db_identifier,
            # db_version) tuple. ON CONFLICT DO UPDATE is guaranteed to
            # return exactly one row regardless of concurrent inserts.
            upsert_stmt = (
                pg_insert(ClinicalControl)
                .values(
                    db_identifier=identifier,
                    db_version=clinvar_version,
                    db_name="ClinVar",
                    gene_symbol=variant_data.get("GeneSymbol"),
                    clinical_significance=variant_data.get("ClinicalSignificance"),
                    clinical_review_status=variant_data.get("ReviewStatus"),
                )
                .on_conflict_do_update(
                    constraint="uq_clinical_controls_db_name_identifier_version",
                    set_={
                        "gene_symbol": variant_data.get("GeneSymbol"),
                        "clinical_significance": variant_data.get("ClinicalSignificance"),
                        "clinical_review_status": variant_data.get("ReviewStatus"),
                    },
                )
                .returning(ClinicalControl)
            )
            clinvar_variant = job_manager.db.scalars(upsert_stmt).one()

            job_manager.db.add(clinvar_variant)
            job_manager.db.flush()

            if clinvar_variant not in mapped_variant.clinical_controls:
                mapped_variant.clinical_controls.append(clinvar_variant)
                job_manager.db.add(mapped_variant)

            annotation_manager.add_annotation(
                variant_id=mapped_variant.variant_id,  # type: ignore
                annotation_type=AnnotationType.CLINVAR_CONTROL,
                version=clinvar_version,
                status=AnnotationStatus.SUCCESS,
                annotation_data={
                    "annotation_metadata": {
                        "clinvar_allele_id": clinvar_allele_id,
                    },
                },
                current=True,
                replace_all_versions=False,
            )

            total_refreshed += 1

        annotation_manager.flush()
        versions_completed += 1
        logger.info(
            f"Completed ClinVar version {clinvar_version} for {total_variants_to_refresh} variants.",
            extra=job_manager.logging_context(),
        )

    logger.info(
        f"ClinVar refresh complete: {versions_completed}/{len(versions)} versions, "
        f"{total_refreshed} variant-version annotations.",
        extra=job_manager.logging_context(),
    )

    if total_failed > 0 and total_refreshed == 0:
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
                "variant_annotations": 0,
            },
            failure_category=FailureCategory.DEPENDENCY_FAILURE,
        )

    job_manager.db.flush()
    return JobExecutionOutcome.succeeded(
        data={
            "versions_completed": versions_completed,
            "versions_total": len(versions),
            "variant_annotations": total_refreshed,
        }
    )

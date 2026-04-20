"""ClinGen allele HGVS population jobs for mapped variant annotation.

This module populates mapped variants with HGVS representations (genomic, coding,
protein) by querying the ClinGen Allele Registry. It uses ClinGen allele IDs
(CAIDs) already associated with mapped variants to look up standardized HGVS
nomenclature at different levels (hgvs_g, hgvs_c, hgvs_p), plus the assay-level
HGVS derived from post-mapped VRS data.
"""

import logging
from typing import Optional

import requests
from sqlalchemy import select

from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from mavedb.lib.clingen.allele_registry import (
    extract_hgvs_from_ca_allele_data,
    extract_hgvs_from_pa_allele_data,
    get_clingen_allele_data,
)
from mavedb.lib.target_genes import get_target_coding_info
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationFailureCategory, AnnotationStatus
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager

logger = logging.getLogger(__name__)


@with_pipeline_management
async def populate_hgvs_for_score_set(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    """Populate mapped variants with HGVS representations for a score set.

    Queries the ClinGen Allele Registry using existing ClinGen allele IDs to populate
    standardized HGVS nomenclature (genomic, coding, protein) on mapped variants.
    Also extracts the assay-level HGVS from post-mapped VRS data.

    Required job_params in the JobRun:
        - score_set_id (int): ID of the ScoreSet to process
        - correlation_id (str): Correlation ID for tracking

    Args:
        ctx: Worker context containing DB and Redis connections.
        job_id: The ID of the job run.
        job_manager: Manager for job lifecycle and DB operations.

    Side Effects:
        - Updates MappedVariant records with hgvs_assay_level, hgvs_g, hgvs_c, hgvs_p.
        - Creates AnnotationStatus records for each processed variant.

    Returns:
        JobExecutionOutcome indicating success, failure, or skip.
    """
    job = job_manager.get_job()

    _job_required_params = ["score_set_id", "correlation_id"]
    validate_job_params(_job_required_params, job)

    # Fetch required resources based on param inputs. Safely ignore mypy warnings here, as they were checked above.
    score_set = job_manager.db.scalars(select(ScoreSet).where(ScoreSet.id == job.job_params["score_set_id"])).one()  # type: ignore
    correlation_id = job.job_params["correlation_id"]  # type: ignore

    # Setup initial context and progress
    job_manager.save_to_context(
        {
            "application": "mavedb-worker",
            "function": "populate_hgvs_for_score_set",
            "resource": score_set.urn,
            "correlation_id": correlation_id,
        }
    )
    job_manager.update_progress(0, 100, "Starting mapped HGVS population.")
    logger.info(msg="Started mapped HGVS population", extra=job_manager.logging_context())

    # Determine target info; multi-target score sets are not yet supported
    try:
        target_is_coding, transcript_accession = get_target_coding_info(score_set)
    except NotImplementedError:
        job_manager.update_progress(100, 100, "Multi-target score sets are not yet supported. Skipping.")
        logger.warning(
            msg="Multi-target score sets not supported for HGVS population. Skipping.",
            extra=job_manager.logging_context(),
        )
        return JobExecutionOutcome.skipped(data={"reason": "Multi-target score sets not supported"})

    job_manager.save_to_context({"target_is_coding": target_is_coding, "transcript_accession": transcript_accession})
    logger.info(
        msg=f"Target info resolved: coding={target_is_coding}, transcript={transcript_accession}",
        extra=job_manager.logging_context(),
    )

    # Fetch current mapped variants for the score set
    variant_rows = job_manager.db.execute(
        select(Variant.id, MappedVariant)
        .join(Variant)
        .join(ScoreSet)
        .where(ScoreSet.id == score_set.id)
        .where(MappedVariant.current.is_(True))
    ).all()

    total_variants = len(variant_rows)
    job_manager.save_to_context({"total_variants": total_variants})

    if not variant_rows:
        job_manager.update_progress(100, 100, "No current mapped variants found. Nothing to do.")
        logger.warning(
            msg="No current mapped variants found for this score set. Skipping HGVS population.",
            extra=job_manager.logging_context(),
        )
        return JobExecutionOutcome.succeeded(data={"populated_count": 0, "skipped_count": 0, "failed_count": 0})

    job_manager.update_progress(5, 100, f"Processing {total_variants} mapped variants for HGVS population.")

    annotation_manager = AnnotationStatusManager(job_manager.db)
    populated_count = 0
    skipped_count = 0
    failed_count = 0

    for index, (variant_id, mapped_variant) in enumerate(variant_rows):
        # Periodic progress updates
        if total_variants > 0 and index % max(total_variants // 20, 1) == 0:
            progress = 5 + int((index / total_variants) * 90)
            job_manager.update_progress(progress, 100, f"Processing HGVS for variant {index + 1}/{total_variants}.")

        hgvs_g: Optional[str] = None
        hgvs_c: Optional[str] = None
        hgvs_p: Optional[str] = None

        clingen_id = mapped_variant.clingen_allele_id

        job_manager.save_to_context(
            {
                "mapped_variant_id": mapped_variant.id,
                "clingen_allele_id": clingen_id,
                "progress_index": index,
            }
        )

        if not clingen_id:
            annotation_manager.add_annotation(
                variant_id=variant_id,
                annotation_type=AnnotationType.MAPPED_HGVS,
                version=None,
                status=AnnotationStatus.SKIPPED,
                failure_category=AnnotationFailureCategory.MISSING_IDENTIFIER,
                annotation_data={
                    "job_run_id": job_manager.job_id,
                    "error_message": "No ClinGen allele ID available for ClinGen HGVS lookup.",
                },
                current=True,
            )
            logger.debug(
                "Skipping variant %s: no ClinGen allele ID.",
                variant_id,
                extra=job_manager.logging_context(),
            )
            skipped_count += 1
            continue

        # Skip multi-variant allele IDs (comma-separated)
        if "," in clingen_id:
            annotation_manager.add_annotation(
                variant_id=variant_id,
                annotation_type=AnnotationType.MAPPED_HGVS,
                version=None,
                status=AnnotationStatus.SKIPPED,
                failure_category=AnnotationFailureCategory.UNSUPPORTED_IDENTIFIER,
                annotation_data={
                    "job_run_id": job_manager.job_id,
                    "error_message": "Multi-variant ClinGen allele IDs not supported for HGVS lookup.",
                },
                current=True,
            )
            logger.debug(
                "Skipping variant %s: multi-variant ClinGen allele ID.",
                variant_id,
                extra=job_manager.logging_context(),
            )
            skipped_count += 1
            continue

        # Query ClinGen API for allele data
        try:
            allele_data = await get_clingen_allele_data(clingen_id)
        except requests.exceptions.RequestException as exc:
            annotation_manager.add_annotation(
                variant_id=variant_id,
                annotation_type=AnnotationType.MAPPED_HGVS,
                version=None,
                status=AnnotationStatus.FAILED,
                failure_category=AnnotationFailureCategory.EXTERNAL_API_ERROR,
                annotation_data={
                    "job_run_id": job_manager.job_id,
                    "error_message": f"Failed to fetch ClinGen allele data: {str(exc)}",
                },
                current=True,
            )
            logger.error(
                "ClinGen API request failed for allele %s.",
                clingen_id,
                extra=job_manager.logging_context(),
                exc_info=exc,
            )
            failed_count += 1
            continue

        if allele_data is None:
            annotation_manager.add_annotation(
                variant_id=variant_id,
                annotation_type=AnnotationType.MAPPED_HGVS,
                version=None,
                status=AnnotationStatus.SKIPPED,
                failure_category=AnnotationFailureCategory.EXTERNAL_REFERENCE_NOT_FOUND,
                annotation_data={
                    "job_run_id": job_manager.job_id,
                    "error_message": f"ClinGen allele {clingen_id} not found in the registry.",
                },
                current=True,
            )
            logger.debug(
                "ClinGen allele %s not found in registry. Skipping variant %s.",
                clingen_id,
                variant_id,
                extra=job_manager.logging_context(),
            )
            skipped_count += 1
            continue

        # Extract HGVS based on allele type
        if clingen_id.startswith("CA"):
            hgvs_g, hgvs_c, hgvs_p = extract_hgvs_from_ca_allele_data(
                allele_data, target_is_coding, transcript_accession
            )
        elif clingen_id.startswith("PA"):
            hgvs_g, hgvs_c, hgvs_p = extract_hgvs_from_pa_allele_data(allele_data)

        # Update mapped variant
        mapped_variant.hgvs_g = hgvs_g
        mapped_variant.hgvs_c = hgvs_c
        mapped_variant.hgvs_p = hgvs_p
        job_manager.db.add(mapped_variant)

        annotation_manager.add_annotation(
            variant_id=variant_id,
            annotation_type=AnnotationType.MAPPED_HGVS,
            version=None,
            status=AnnotationStatus.SUCCESS,
            annotation_data={
                "job_run_id": job_manager.job_id,
                "annotation_metadata": {
                    "hgvs_g": hgvs_g,
                    "hgvs_c": hgvs_c,
                    "hgvs_p": hgvs_p,
                },
            },
            current=True,
        )
        populated_count += 1

    annotation_manager.flush()
    job_manager.db.flush()

    job_manager.save_to_context(
        {
            "populated_count": populated_count,
            "skipped_count": skipped_count,
            "failed_count": failed_count,
        }
    )
    job_manager.update_progress(100, 100, "Completed mapped HGVS population.")
    logger.info(
        msg=f"Completed mapped HGVS population: {populated_count} populated, {skipped_count} skipped, {failed_count} failed.",
        extra=job_manager.logging_context(),
    )

    return JobExecutionOutcome.succeeded(
        data={
            "populated_count": populated_count,
            "skipped_count": skipped_count,
            "failed_count": failed_count,
        }
    )

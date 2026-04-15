"""HGVS mapping jobs for variant nomenclature standardization.

This module handles the submission and processing of variant nomenclature mapping
using the Ensembl Variant Recoder and VEP APIs to populate HGVS expressions for
mapped variants. This enables standardized variant representation across genomic,
transcript, and protein coordinate systems.

The processing is asynchronous, requiring batch submission of variant coordinates
to external APIs for nomenclature conversion and validation.
"""

import logging

from sqlalchemy import select
from sqlalchemy.orm.attributes import flag_modified

from mavedb.lib.exceptions import HGVSProcessingError
from mavedb.lib.hgvs import populate_mapped_hgvs_for_variants
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.managers.types import JobResultData

logger = logging.getLogger(__name__)


@with_pipeline_management
async def submit_hgvs_mapping_jobs_for_score_set(ctx: dict, job_id: int, job_manager: JobManager) -> JobResultData:
    """Populate HGVS nomenclature for all mapped variants in a ScoreSet.

    This function retrieves all mapped variants for a given ScoreSet and populates
    their HGVS expressions (genomic, transcript, and protein nomenclature) using
    the Ensembl Variant Recoder and VEP APIs. HGVS nomenclature is essential for
    standardized variant representation and downstream analyses.

    Job Parameters:
        - score_set_id (int): The ID of the ScoreSet containing mapped variants.
        - correlation_id (str): Correlation ID for tracing requests across services.

    Args:
        ctx (dict): The job context dictionary.
        job_id (int): The ID of the job being executed.
        job_manager (JobManager): Manager for job lifecycle and DB operations.

    Side Effects:
        - Fetches all mapped variants for the score set.
        - Submits variant coordinates to Ensembl APIs for HGVS conversion.
        - Updates mapped variants with post_mapped HGVS expressions.
        - Persists changes to the database.
        - Logs progress and any errors encountered.

    Raises:
        - HGVSProcessingError: If HGVS mapping fails for a variant.

    Returns:
        dict: Result indicating success and any exception details
    """
    # Get the job definition we are working on
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
            "function": "submit_hgvs_mapping_jobs_for_score_set",
            "resource": score_set.urn,
            "correlation_id": correlation_id,
        }
    )
    job_manager.update_progress(0, 100, "Starting HGVS nomenclature mapping.")
    logger.info(msg="Started HGVS nomenclature mapping", extra=job_manager.logging_context())

    # Preset processed variants metadata so it persists even if no variants are processed
    job.metadata_["variants_processed"] = 0
    job.metadata_["variants_with_hgvs"] = 0
    job.metadata_["variants_without_hgvs"] = 0
    job_manager.db.flush()

    # Fetch all mapped variants for the score set
    mapped_variants = job_manager.db.scalars(
        select(MappedVariant)
        .join(Variant)
        .where(
            Variant.score_set_id == job.job_params["score_set_id"],
            MappedVariant.current.is_(True),
        )
    ).all()

    if not mapped_variants:
        job_manager.update_progress(100, 100, "No mapped variants found. Skipped HGVS nomenclature mapping.")
        logger.warning(
            msg=f"No mapped variants found for score set {score_set.urn}. Skipped HGVS mapping.",
            extra=job_manager.logging_context(),
        )
        return {"status": "ok", "data": {}, "exception": None}

    job_manager.save_to_context({"total_variants_to_process": len(mapped_variants)})
    logger.info(
        msg=f"Found {len(mapped_variants)} mapped variants for HGVS mapping",
        extra=job_manager.logging_context(),
    )

    # Process variants and populate HGVS nomenclature
    variants_processed = 0
    variants_with_hgvs = 0
    variants_without_hgvs = 0

    for idx, mapped_variant in enumerate(mapped_variants):
        try:
            logger.debug(
                msg=f"Processing variant {idx + 1}/{len(mapped_variants)} (ID: {mapped_variant.id})",
                extra=job_manager.logging_context(),
            )

            # Populate HGVS nomenclature for this variant
            hgvs_populated = populate_mapped_hgvs_for_variants(job_manager.db, score_set, [mapped_variant])

            if hgvs_populated:
                variants_with_hgvs += 1
                logger.debug(
                    msg=f"Successfully populated HGVS for variant {mapped_variant.id}",
                    extra=job_manager.logging_context(),
                )
            else:
                variants_without_hgvs += 1
                logger.warning(
                    msg=f"Could not populate HGVS for variant {mapped_variant.id}",
                    extra=job_manager.logging_context(),
                )

            variants_processed += 1
            job_manager.db.flush()

            # Update progress
            progress_pct = int((idx + 1) / len(mapped_variants) * 100)
            job_manager.update_progress(
                progress_pct,
                100,
                f"Processed {variants_processed}/{len(mapped_variants)} variants",
            )

            job_manager.save_to_context(
                {
                    "variants_processed_so_far": variants_processed,
                    "variants_with_hgvs_so_far": variants_with_hgvs,
                }
            )

        except HGVSProcessingError as e:
            logger.error(
                msg=f"HGVS processing error for variant {mapped_variant.id}: {str(e)}",
                extra=job_manager.logging_context(),
            )
            return {
                "status": "failed",
                "data": {
                    "variants_processed": variants_processed,
                    "variants_with_hgvs": variants_with_hgvs,
                },
                "exception": e,
            }
        except Exception as e:
            logger.error(
                msg=f"Unexpected error processing variant {mapped_variant.id}: {str(e)}",
                extra=job_manager.logging_context(),
            )
            return {
                "status": "failed",
                "data": {
                    "variants_processed": variants_processed,
                    "variants_with_hgvs": variants_with_hgvs,
                },
                "exception": HGVSProcessingError(f"Unexpected error processing variant {mapped_variant.id}: {str(e)}"),
            }

    # Update metadata with final counts
    job.metadata_["variants_processed"] = variants_processed
    job.metadata_["variants_with_hgvs"] = variants_with_hgvs
    job.metadata_["variants_without_hgvs"] = variants_without_hgvs
    flag_modified(job, "metadata_")
    job_manager.db.flush()

    job_manager.update_progress(
        100,
        100,
        f"Completed HGVS nomenclature mapping for {variants_with_hgvs}/{variants_processed} variants.",
    )
    logger.info(
        msg=f"Completed HGVS mapping: {variants_with_hgvs} variants with HGVS, {variants_without_hgvs} without",
        extra=job_manager.logging_context(),
    )

    return {
        "status": "ok",
        "data": {
            "variants_processed": variants_processed,
            "variants_with_hgvs": variants_with_hgvs,
            "variants_without_hgvs": variants_without_hgvs,
        },
        "exception": None,
    }

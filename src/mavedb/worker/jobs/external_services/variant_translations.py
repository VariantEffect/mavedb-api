"""Variant translation jobs for ClinGen allele registry mapping.

This module handles the submission and processing of variant translation requests
using the ClinGen Allele Registry API to populate VariantTranslation records.
This enables mapping between different variant identifier systems (CA, PA, transcript variants)
and enriches variants with cross-referenced allele information.

The processing is asynchronous, requiring queries to the ClinGen API to resolve
canonical PA IDs and matching registered transcript CA IDs.
"""

import logging

from sqlalchemy import select
from sqlalchemy.orm.attributes import flag_modified

from mavedb.lib.exceptions import VariantTranslationProcessingError
from mavedb.lib.variant_translations import populate_variant_translations_for_variant
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.managers.types import JobResultData

logger = logging.getLogger(__name__)


@with_pipeline_management
async def populate_variant_translations_for_score_set(ctx: dict, job_id: int, job_manager: JobManager) -> JobResultData:
    """Populate variant translations for all mapped variants in a ScoreSet.

    This function retrieves all mapped variants with ClinGen allele IDs for a given
    ScoreSet and queries the ClinGen Allele Registry API to resolve canonical PA IDs
    and matching registered transcript CA IDs. These mappings are stored as VariantTranslation
    records for cross-reference and enrichment purposes.

    Job Parameters:
        - score_set_id (int): The ID of the ScoreSet containing mapped variants.
        - correlation_id (str): Correlation ID for tracing requests across services.

    Args:
        ctx (dict): The job context dictionary.
        job_id (int): The ID of the job being executed.
        job_manager (JobManager): Manager for job lifecycle and DB operations.

    Side Effects:
        - Fetches all mapped variants with ClinGen allele IDs.
        - Queries ClinGen Allele Registry API for canonical and transcript variant mappings.
        - Creates VariantTranslation records for variant mappings.
        - Persists changes to the database.
        - Logs progress and any errors encountered.

    Raises:
        - VariantTranslationProcessingError: If variant translation processing fails.

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
            "function": "populate_variant_translations_for_score_set",
            "resource": score_set.urn,
            "correlation_id": correlation_id,
        }
    )
    job_manager.update_progress(0, 100, "Starting variant translation population from ClinGen Allele Registry.")
    logger.info(msg="Started variant translation population", extra=job_manager.logging_context())

    # Preset processed variants metadata so it persists even if no variants are processed
    job.metadata_["clingen_allele_ids_processed"] = 0
    job.metadata_["variant_translations_created"] = 0
    job.metadata_["variant_translations_skipped"] = 0
    job.metadata_["allele_ids_with_errors"] = 0
    job_manager.db.flush()

    # Fetch all unique ClinGen allele IDs for mapped variants in this score set
    clingen_allele_ids = job_manager.db.scalars(
        select(MappedVariant.clingen_allele_id)
        .join(Variant)
        .where(
            Variant.score_set_id == job.job_params["score_set_id"],
            MappedVariant.current.is_(True),
            MappedVariant.clingen_allele_id.isnot(None),
        )
    ).all()

    if not clingen_allele_ids:
        job_manager.update_progress(100, 100, "No ClinGen allele IDs found. Skipped variant translation population.")
        logger.warning(
            msg=f"No ClinGen allele IDs found for score set {score_set.urn}. Skipped variant translation population.",
            extra=job_manager.logging_context(),
        )
        return {"status": "ok", "data": {}, "exception": None}

    job_manager.save_to_context({"total_clingen_allele_ids": len(clingen_allele_ids)})
    logger.info(
        msg=f"Found {len(clingen_allele_ids)} ClinGen allele IDs for variant translation",
        extra=job_manager.logging_context(),
    )

    # Expand multi-variants (comma-separated allele IDs)
    expanded_allele_ids = []
    for allele_id in clingen_allele_ids:
        if not allele_id:
            continue
        if "," in allele_id:
            expanded_allele_ids.extend([aid.strip() for aid in allele_id.split(",")])
        else:
            expanded_allele_ids.append(allele_id)

    # Remove duplicates while preserving order
    unique_allele_ids = list(dict.fromkeys(expanded_allele_ids))
    job_manager.save_to_context({"total_unique_expanded_allele_ids": len(unique_allele_ids)})

    # Process each ClinGen allele ID
    allele_ids_processed = 0
    variant_translations_created = 0
    variant_translations_skipped = 0
    allele_ids_with_errors = 0

    for idx, allele_id in enumerate(unique_allele_ids):
        try:
            logger.debug(
                msg=f"Processing allele ID {idx + 1}/{len(unique_allele_ids)}: {allele_id}",
                extra=job_manager.logging_context(),
            )

            # Validate allele ID format
            if not allele_id.startswith(("CA", "PA")):
                logger.warning(
                    msg=f"Invalid ClinGen allele ID format: {allele_id}",
                    extra=job_manager.logging_context(),
                )
                allele_ids_with_errors += 1
                continue

            # Process variant translations for this allele ID
            created_count = await populate_variant_translations_for_variant(job_manager.db, allele_id)

            variant_translations_created += created_count
            if created_count == 0:
                variant_translations_skipped += 1
                logger.debug(
                    msg=f"No new variant translations created for {allele_id}",
                    extra=job_manager.logging_context(),
                )

            allele_ids_processed += 1
            job_manager.db.flush()

            # Update progress
            progress_pct = int((idx + 1) / len(unique_allele_ids) * 100)
            job_manager.update_progress(
                progress_pct,
                100,
                f"Processed {allele_ids_processed}/{len(unique_allele_ids)} allele IDs ({variant_translations_created} translations created)",
            )

            job_manager.save_to_context(
                {
                    "allele_ids_processed_so_far": allele_ids_processed,
                    "variant_translations_created_so_far": variant_translations_created,
                }
            )

        except VariantTranslationProcessingError as e:
            logger.error(
                msg=f"Variant translation processing error for allele ID {allele_id}: {str(e)}",
                extra=job_manager.logging_context(),
            )
            allele_ids_with_errors += 1
            continue
        except Exception as e:
            logger.error(
                msg=f"Unexpected error processing allele ID {allele_id}: {str(e)}",
                extra=job_manager.logging_context(),
            )
            allele_ids_with_errors += 1
            continue

    # Update metadata with final counts
    job.metadata_["clingen_allele_ids_processed"] = allele_ids_processed
    job.metadata_["variant_translations_created"] = variant_translations_created
    job.metadata_["variant_translations_skipped"] = variant_translations_skipped
    job.metadata_["allele_ids_with_errors"] = allele_ids_with_errors
    flag_modified(job, "metadata_")
    job_manager.db.flush()

    job_manager.update_progress(
        100,
        100,
        f"Completed variant translation population: {variant_translations_created} translations created from {allele_ids_processed} allele IDs.",
    )
    logger.info(
        msg=f"Completed variant translation population: {variant_translations_created} created, {variant_translations_skipped} skipped, {allele_ids_with_errors} errors",
        extra=job_manager.logging_context(),
    )

    return {
        "status": "ok",
        "data": {
            "allele_ids_processed": allele_ids_processed,
            "variant_translations_created": variant_translations_created,
            "variant_translations_skipped": variant_translations_skipped,
            "allele_ids_with_errors": allele_ids_with_errors,
        },
        "exception": None,
    }

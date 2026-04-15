"""VEP functional consequence jobs for variant effect prediction.

This module handles the submission and processing of variant effect predictions
using the Ensembl VEP API.

The processing is asynchronous, requiring batch submission of HGVS strings
to the VEP API with fallback to Variant Recoder for unmapped variants.
"""

import logging
from datetime import date

from sqlalchemy import select

from mavedb.lib.exceptions import VEPProcessingError
from mavedb.lib.utils import batched
from mavedb.lib.variants import get_hgvs_from_post_mapped
from mavedb.lib.vep import get_functional_consequence
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.managers.types import JobExecutionOutcome

logger = logging.getLogger(__name__)


# TODO add annotation with manager
# e.g. annotation_manager = AnnotationStatusManager(job_manager.db), annotation_manager.add_annotation()
# see clinvar.py in this folder


@with_pipeline_management
async def populate_vep_for_score_set(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    """Populate VEP functional consequence predictions for all mapped variants in a ScoreSet.

    This function retrieves all mapped variants with post_mapped HGVS expressions for a given
    ScoreSet and submits them to the Ensembl VEP API in batches of 200. It handles fallback
    to the Variant Recoder API for variants that cannot be processed by VEP directly.

    Job Parameters:
        - score_set_id (int): The ID of the ScoreSet containing mapped variants.
        - correlation_id (str): Correlation ID for tracing requests across services.

    Args:
        ctx (dict): The job context dictionary.
        job_id (int): The ID of the job being executed.
        job_manager (JobManager): Manager for job lifecycle and DB operations.

    Side Effects:
        - Fetches all mapped variants with post_mapped HGVS expressions.
        - Submits batches of HGVS strings to VEP API.
        - Updates mapped variants with functional consequence predictions and access dates.
        - Persists changes to the database.
        - Logs progress and any errors encountered.

    Raises:
        - VEPProcessingError: If VEP API processing fails for a batch.

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
            "function": "populate_vep_for_score_set",
            "resource": score_set.urn,
            "correlation_id": correlation_id,
        }
    )
    job_manager.update_progress(0, 100, "Starting VEP population.")
    logger.info(msg="Started VEP population", extra=job_manager.logging_context())

    # TODO use update progress function throughout. not necessarily here

    # Fetch all mapped variants with post_mapped VRS objects
    mapped_variants = job_manager.db.scalars(
        select(MappedVariant)
        .join(Variant)
        .where(
            Variant.score_set_id == score_set.id,
            MappedVariant.current.is_(True),
            MappedVariant.post_mapped.isnot(None),
        )
    ).all()

    if not mapped_variants:
        job_manager.update_progress(100, 100, "No mapped variants found. Skipped VEP population.")
        logger.warning(
            msg=f"No mapped variants found for score set {score_set.urn}. Skipped VEP population.",
            extra=job_manager.logging_context(),
        )
        return JobExecutionOutcome.succeeded(
            data={"variants_processed": 0, "variants_with_consequences": 0, "variants_without_consequences": 0}
        )

    job_manager.save_to_context({"total_variants_to_process": len(mapped_variants)})
    logger.info(
        msg=f"Found {len(mapped_variants)} mapped variants for VEP processing",
        extra=job_manager.logging_context(),
    )

    # Extract HGVS strings and build batches of 200
    hgvs_and_variant_id_pairs: list[tuple[str, int]] = []

    for mapped_variant in mapped_variants:
        hgvs_string = get_hgvs_from_post_mapped(mapped_variant)  # type: ignore
        if not hgvs_string:
            logger.warning(
                msg=f"No HGVS string could be extracted from post_mapped for variant {mapped_variant.id}.",
                extra=job_manager.logging_context(),
            )
            continue

        hgvs_and_variant_id_pairs.append((hgvs_string, mapped_variant.id))

    batches = batched(hgvs_and_variant_id_pairs, 200)

    job_manager.save_to_context({"total_batches": len(batches)})
    logger.info(
        msg=f"Prepared {len(batches)} batches for VEP processing",
        extra=job_manager.logging_context(),
    )

    # Process each batch
    variants_processed = 0
    variants_with_consequences = 0
    variants_without_consequences = 0

    # Setup annotation manager
    # annotation_manager = AnnotationStatusManager(job_manager.db)

    for batch_idx, batch in enumerate(batches):
        try:
            logger.info(
                msg=f"Processing batch {batch_idx + 1}/{len(batches)} with {len(batch['hgvs_strings'])} variants",
                extra=job_manager.logging_context(),
            )

            # Get functional consequences from VEP
            consequences = get_functional_consequence(batch["hgvs_strings"])
            logger.debug(
                msg=f"Received consequences for {len(consequences)} variants in batch {batch_idx + 1}",
                extra=job_manager.logging_context(),
            )

            # Update mapped variants with consequences
            for hgvs, variant_id in zip(batch["hgvs_strings"], batch["variant_ids"]):
                mapped_variant = next(
                    (mv for mv in mapped_variants if mv.id == variant_id),
                    None,
                )
                if not mapped_variant:
                    logger.warning(
                        msg=f"Could not find mapped variant with ID {variant_id}",
                        extra=job_manager.logging_context(),
                    )
                    continue

                consequence = consequences.get(hgvs)
                if consequence:
                    mapped_variant.vep_functional_consequence = consequence
                    mapped_variant.vep_access_date = date.today()
                    job_manager.db.add(mapped_variant)
                    variants_with_consequences += 1
                    logger.debug(
                        msg=f"Set consequence '{consequence}' for variant {variant_id} (HGVS: {hgvs})",
                        extra=job_manager.logging_context(),
                    )
                else:
                    variants_without_consequences += 1
                    logger.warning(
                        msg=f"Could not retrieve functional consequence for HGVS {hgvs}",
                        extra=job_manager.logging_context(),
                    )

                variants_processed += 1

            job_manager.db.flush()

            # TODO handle vep and variant recoder batches separately
            # process all vep batch by batch
            # then process all recoder batch by batch, with separate progress tracking for each
            # then do last vep processing from recoder results, with separate progress tracking for that as well
            # progress equals ~33% * number of batches processed for each of the 3 steps

            # Update progress
            progress_pct = int((batch_idx + 1) / len(batches) * 100)
            job_manager.update_progress(
                progress_pct,
                100,
                f"Processed batch {batch_idx + 1}/{len(batches)} ({variants_processed}/{len(mapped_variants)} variants)",
            )

            job_manager.save_to_context(
                {
                    "processed_batches": batch_idx + 1,
                    "variants_processed_so_far": variants_processed,
                    "variants_with_consequences_so_far": variants_with_consequences,
                }
            )

        except VEPProcessingError as e:
            logger.error(
                msg=f"VEP processing error for batch {batch_idx + 1}: {str(e)}",
                extra=job_manager.logging_context(),
            )
            return {
                "status": "failed",
                "data": {
                    "variants_processed": variants_processed,
                    "batches_processed": batch_idx,
                    "variants_with_consequences": variants_with_consequences,
                },
                "exception": e,
            }
        except Exception as e:
            logger.error(
                msg=f"Unexpected error processing batch {batch_idx + 1}: {str(e)}",
                extra=job_manager.logging_context(),
            )
            return {
                "status": "failed",
                "data": {
                    "variants_processed": variants_processed,
                    "batches_processed": batch_idx,
                    "variants_with_consequences": variants_with_consequences,
                },
                "exception": VEPProcessingError(f"Unexpected error processing batch {batch_idx + 1}: {str(e)}"),
            }

    job_manager.db.flush()

    job_manager.update_progress(
        100,
        100,
        f"Completed VEP functional consequence prediction for {variants_with_consequences}/{variants_processed} variants.",
    )
    logger.info(
        msg=f"Completed VEP prediction: {variants_with_consequences} variants with consequences, {variants_without_consequences} without",
        extra=job_manager.logging_context(),
    )

    return {
        "status": "ok",
        "data": {
            "variants_processed": variants_processed,
            "batches_processed": len(batches),
            "variants_with_consequences": variants_with_consequences,
            "variants_without_consequences": variants_without_consequences,
        },
        "exception": None,
    }

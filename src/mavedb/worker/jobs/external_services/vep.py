"""VEP functional consequence jobs for variant effect prediction.

This module handles the submission and processing of variant effect predictions
using the Ensembl VEP API.

The processing is asynchronous, requiring batch submission of HGVS strings
to the VEP API with fallback to Variant Recoder when necessary.
"""

import logging
from datetime import date

from sqlalchemy import select

from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationStatus
from mavedb.lib.utils import batched
from mavedb.lib.variants import get_hgvs_from_post_mapped
from mavedb.lib.vep import VEP_CONSEQUENCES, get_functional_consequence, run_variant_recoder
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

    # Setup annotation manager
    annotation_manager = AnnotationStatusManager(job_manager.db)

    # Extract HGVS strings and build batches of 200
    # hgvs_strings, variant_ids
    hgvs_and_mapped_variant_id_pairs: list[tuple[str, int]] = []

    for mapped_variant in mapped_variants:
        hgvs_string = get_hgvs_from_post_mapped(mapped_variant)  # type: ignore
        # TODO change above line to the one below once we pull in Ben's change that populates hgvs_assay_level during mapping job
        # hgvs_string = mapped_variant.hgvs_assay_level
        if not hgvs_string:
            annotation_manager.add_annotation(
                variant_id=mapped_variant.variant_id,
                annotation_type=AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
                status=AnnotationStatus.SKIPPED,
                annotation_data={
                    "job_run_id": job_manager.job_id,
                    "error_message": "Mapped variant does not have an associated HGVS string.",
                    "failure_category": "missing_hgvs",
                },
            )
            logger.debug("Mapped variant does not have an associated HGVS string.", extra=job_manager.logging_context())
            continue

        hgvs_and_mapped_variant_id_pairs.append((hgvs_string, mapped_variant.id))

    batches = batched(hgvs_and_mapped_variant_id_pairs, 200)

    job_manager.save_to_context({"vep_batches": len(batches)})
    logger.info(
        msg=f"Prepared {len(batches)} batches for VEP processing",
        extra=job_manager.logging_context(),
    )

    # Process each batch
    variants_processed = 0
    variants_with_consequences = 0
    variants_without_consequences = 0

    # Process each batch through VEP first
    all_consequences: dict[str, str | None] = {}
    all_missing_hgvs: set[str] = set()
    missing_hgvs_to_variant_ids: dict[str, list[int]] = {}

    for batch_idx, batch in enumerate(batches):
        try:
            logger.info(
                msg=f"Processing VEP batch {batch_idx + 1}/{len(batches)}",
                extra=job_manager.logging_context(),
            )

            hgvs_strings, mapped_variant_ids = map(list, zip(*batch))  # type: ignore

            # Get functional consequences from VEP
            consequences = get_functional_consequence(hgvs_strings)
            logger.debug(
                msg=f"Received consequences for {len(consequences)} variants in VEP batch {batch_idx + 1}",
                extra=job_manager.logging_context(),
            )

            # Collect all consequences and missing HGVS
            all_consequences.update(consequences)

            # Track missing HGVS and their associated variant IDs
            missing_hgvs = set(hgvs_strings) - set(consequences.keys())
            for hgvs, mapped_variant_id in zip(hgvs_strings, mapped_variant_ids):
                if hgvs in missing_hgvs:
                    all_missing_hgvs.add(hgvs)
                    if hgvs not in missing_hgvs_to_variant_ids:
                        missing_hgvs_to_variant_ids[hgvs] = []
                    missing_hgvs_to_variant_ids[hgvs].append(mapped_variant_id)

            # Update progress for VEP batches
            progress_pct = int((batch_idx + 1) / len(batches) * 33)  # Assume VEP is ~33% of work
            job_manager.update_progress(
                progress_pct,
                100,
                f"Processed initial VEP batch {batch_idx + 1}/{len(batches)}",
            )

            job_manager.save_to_context(
                {
                    "initial_vep_batches_processed": batch_idx + 1,
                    "missing_hgvs_count": len(all_missing_hgvs),
                }
            )

        except Exception as e:
            logger.error(
                msg=f"VEP processing error for batch {batch_idx + 1}: {str(e)}",
                extra=job_manager.logging_context(),
            )
            return {
                "status": "failed",
                "data": {
                    "initial_vep_batches_processed": batch_idx + 1,
                    "variant_recoder_batches_processed": 0,
                    "missing_hgvs_count": len(all_missing_hgvs),
                },
                "exception": e,
            }

    logger.info(
        msg=f"Completed initial VEP processing. {len(all_missing_hgvs)} variants require Variant Recoder fallback.",
        extra=job_manager.logging_context(),
    )

    # Process Variant Recoder if there are missing HGVS
    hgvs_to_genomic: dict[str, list[str]] = {}
    if all_missing_hgvs:
        logger.info(
            msg=f"Running Variant Recoder for {len(all_missing_hgvs)} HGVS strings",
            extra=job_manager.logging_context(),
        )

        recoder_batches = batched(list(all_missing_hgvs), 200)
        recoder_batch_list = list(recoder_batches)

        logger.debug(
            msg=f"Created {len(recoder_batch_list)} batches for Variant Recoder processing",
            extra=job_manager.logging_context(),
        )

        # Process each Variant Recoder batch
        for recoder_batch_idx, recoder_batch in enumerate(recoder_batch_list):
            try:
                logger.debug(
                    msg=f"Processing Variant Recoder batch {recoder_batch_idx + 1}/{len(recoder_batch_list)}",
                    extra=job_manager.logging_context(),
                )

                recoded_results = run_variant_recoder(recoder_batch)
                hgvs_to_genomic.update(recoded_results)

                logger.debug(
                    msg=f"Variant Recoder batch {recoder_batch_idx + 1} returned {len(recoded_results)} results",
                    extra=job_manager.logging_context(),
                )

                # Update progress for Variant Recoder batches
                progress_pct = 33 + int(
                    (recoder_batch_idx + 1) / len(recoder_batch_list) * 33
                )  # Recoder is ~33% of work
                job_manager.update_progress(
                    progress_pct,
                    100,
                    f"Processed Variant Recoder batch {recoder_batch_idx + 1}/{len(recoder_batch_list)}",
                )

                job_manager.save_to_context(
                    {
                        "variant_recoder_batches_processed": recoder_batch_idx + 1,
                        "recoded_variants_count": len(hgvs_to_genomic),
                    }
                )

            except Exception as e:
                logger.error(
                    msg=f"Variant Recoder error for batch {recoder_batch_idx + 1}: {str(e)}",
                    extra=job_manager.logging_context(),
                )
                # TODO consider updating the consequences that we do have first, before failing?
                # This failure is not expected because we have a built in retry
                return {
                    "status": "failed",
                    "data": {
                        "initial_vep_batches_processed": len(batches),
                        "variant_recoder_batches_processed": recoder_batch_idx + 1,
                        "missing_hgvs_count": len(all_missing_hgvs),
                    },
                    "exception": e,
                }

        logger.info(
            msg=f"Completed Variant Recoder processing. {len(hgvs_to_genomic)} variants successfully recoded.",
            extra=job_manager.logging_context(),
        )

        # Process recoded HGVS through VEP in batches of 200
        recoded_vep_batches = batched(list(hgvs_to_genomic.values()), 200)
        recoded_vep_batch_list = list(recoded_vep_batches)

        logger.debug(
            msg=f"Created {len(recoded_vep_batch_list)} batches for recoded HGVS VEP processing",
            extra=job_manager.logging_context(),
        )

        all_recoded_consequences: dict[str, str | None] = {}

        # Process each batch of recoded HGVS through VEP
        for recoded_vep_batch_idx, recoded_vep_batch in enumerate(recoded_vep_batch_list):
            try:
                logger.debug(
                    msg=f"Processing recoded HGVS VEP batch {recoded_vep_batch_idx + 1}/{len(recoded_vep_batch_list)}",
                    extra=job_manager.logging_context(),
                )

                recoded_vep_consequences = get_functional_consequence(recoded_vep_batch)
                all_recoded_consequences.update(recoded_vep_consequences)

                logger.debug(
                    msg=f"Received consequences for {len(recoded_vep_consequences)} recoded variants in VEP batch {recoded_vep_batch_idx + 1}",
                    extra=job_manager.logging_context(),
                )

                # Update progress for recoded VEP batches
                progress_pct = 66 + int(
                    (recoded_vep_batch_idx + 1) / len(recoded_vep_batch_list) * 33
                )  # Final VEP is ~33% of work
                job_manager.update_progress(
                    progress_pct,
                    100,
                    f"Processed recoded VEP batch {recoded_vep_batch_idx + 1}/{len(recoded_vep_batch_list)}",
                )

                job_manager.save_to_context(
                    {
                        "recoded_vep_batches_processed": recoded_vep_batch_idx + 1,
                        "recoded_consequences_count": len(all_recoded_consequences),
                    }
                )

            except Exception as e:
                logger.error(
                    msg=f"VEP processing error for recoded batch {recoded_vep_batch_idx + 1}: {str(e)}",
                    extra=job_manager.logging_context(),
                )
                return {
                    "status": "failed",
                    "data": {
                        "initial_vep_batches_processed": len(batches),
                        "variant_recoder_batches_processed": len(recoder_batch_list),
                        "recoded_vep_batches_processed": recoded_vep_batch_idx + 1,
                        "missing_hgvs_count": len(all_missing_hgvs),
                    },
                    "exception": e,
                }

        logger.info(
            msg=f"Completed recoded VEP processing. {len(all_recoded_consequences)} recoded consequences retrieved.",
            extra=job_manager.logging_context(),
        )

        # Now process all recoded results to assign most severe consequence to original HGVS
        for original_hgvs, recoded_hgvs_list in hgvs_to_genomic.items():
            # Collect all consequences for this original HGVS from all its recoded variants
            recoded_consequences_for_variant = []
            for recoded_hgvs in recoded_hgvs_list:
                consequence = all_recoded_consequences.get(recoded_hgvs)
                if consequence:
                    recoded_consequences_for_variant.append(consequence)
                    logger.debug(
                        msg=f"Found consequence '{consequence}' for recoded HGVS {recoded_hgvs} (original: {original_hgvs})",
                        extra=job_manager.logging_context(),
                    )

            # Select the most severe consequence based on VEP_CONSEQUENCES ordering
            if recoded_consequences_for_variant:
                most_severe = None
                for severe_consequence in VEP_CONSEQUENCES:
                    if severe_consequence in recoded_consequences_for_variant:
                        most_severe = severe_consequence
                        break

                if most_severe:
                    all_consequences[original_hgvs] = most_severe
                    logger.debug(
                        msg=f"Selected most severe consequence '{most_severe}' for {original_hgvs} from {recoded_consequences_for_variant}",
                        extra=job_manager.logging_context(),
                    )
            else:
                logger.warning(
                    msg=f"Could not retrieve functional consequences for any recoded variants of {original_hgvs}",
                    extra=job_manager.logging_context(),
                )

        # Handle variants that failed Variant Recoder
        recoder_missing_hgvs = all_missing_hgvs - set(hgvs_to_genomic.keys())
        for hgvs in recoder_missing_hgvs:
            variant_ids = missing_hgvs_to_variant_ids.get(hgvs, [])
            for variant_id in variant_ids:
                annotation_manager.add_annotation(
                    variant_id=variant_id,
                    annotation_type=AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
                    status=AnnotationStatus.FAILED,
                    annotation_data={
                        "job_run_id": job_manager.job_id,
                        "error_message": "Variant Recoder failed to recode HGVS string.",
                        "failure_category": "hgvs_not_processed_by_variant_recoder",
                    },
                )
                logger.debug(
                    msg=f"Recorded failure for variant {variant_id} (HGVS: {hgvs}): Variant Recoder failed",
                    extra=job_manager.logging_context(),
                )

    # get hgvs strings/mapped variant ids for anything that is missing a consequence and was not already marked as failed at variant recoder step, and mark annotation status as failed
    missing_hgvs = set(hgvs_strings) - set(consequences.keys()) - set(recoder_missing_hgvs)
    missing_mapped_variant_ids = [
        mapped_variant_id for hgvs, mapped_variant_id in zip(hgvs_strings, mapped_variant_ids) if hgvs in missing_hgvs
    ]

    for variant_id in missing_mapped_variant_ids:
        annotation_manager.add_annotation(
            variant_id=variant_id,
            annotation_type=AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
            status=AnnotationStatus.FAILED,
            annotation_data={
                "job_run_id": job_manager.job_id,
                "error_message": "VEP failed to retrieve functional consequence, even after running variant recoder.",
                "failure_category": "vep_failed",
            },
        )
        logger.debug(
            msg=f"Recorded failure for variant {variant_id} (HGVS: {hgvs}): VEP failed",
            extra=job_manager.logging_context(),
        )

    # Update mapped variants with consequences
    variants_processed = 0
    variants_with_consequences = 0
    variants_without_consequences = 0

    hgvs_strings, mapped_variant_ids = map(list, zip(*hgvs_and_mapped_variant_id_pairs))

    for hgvs_string in hgvs_strings:
        for mapped_variant in mapped_variants:
            if get_hgvs_from_post_mapped(mapped_variant) == hgvs_string:  # type: ignore
                consequence = all_consequences.get(hgvs_string)
                if consequence:
                    mapped_variant.vep_functional_consequence = consequence
                    mapped_variant.vep_access_date = date.today()
                    job_manager.db.add(mapped_variant)
                    variants_with_consequences += 1
                    logger.debug(
                        msg=f"Set consequence '{consequence}' for variant {mapped_variant.id} (HGVS: {hgvs_string})",
                        extra=job_manager.logging_context(),
                    )
                else:
                    variants_without_consequences += 1
                    logger.warning(
                        msg=f"Could not retrieve functional consequence for HGVS {hgvs_string}",
                        extra=job_manager.logging_context(),
                    )

                variants_processed += 1
                break

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
            "variants_with_consequences": variants_with_consequences,
            "variants_without_consequences": variants_without_consequences,
        },
        "exception": None,
    }

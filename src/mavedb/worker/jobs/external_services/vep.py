"""VEP functional consequence jobs for variant effect prediction.

This module handles the submission and processing of variant effect predictions
using the Ensembl VEP API.

The processing is asynchronous, requiring batch submission of HGVS strings
to the VEP API with fallback to Variant Recoder when necessary.
"""

import asyncio
import logging
from datetime import date

from sqlalchemy import select

from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.lib.utils import batched
from mavedb.lib.vep import VEP_CONSEQUENCES, get_functional_consequence, run_variant_recoder
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationFailureCategory, AnnotationStatus
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager

logger = logging.getLogger(__name__)

_VEP_BATCH_SIZE = 200
_RECODER_BATCH_SIZE = 25
_RECODER_CONCURRENCY = 5


@with_pipeline_management
async def populate_vep_for_score_set(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    """Populate VEP functional consequence predictions for all mapped variants in a ScoreSet.

    This function retrieves all mapped variants with a populated hgvs_assay_level field for a given
    ScoreSet and submits them to the Ensembl VEP API in configurable batches. It handles fallback
    to the Variant Recoder API for variants that cannot be processed by VEP directly.

    Job Parameters:
        - score_set_id (int): The ID of the ScoreSet containing mapped variants.
        - correlation_id (str): Correlation ID for tracing requests across services.

    Args:
        ctx (dict): The job context dictionary.
        job_id (int): The ID of the job being executed.
        job_manager (JobManager): Manager for job lifecycle and DB operations.

    Returns:
        JobExecutionOutcome: Outcome with counts of processed, successful, and failed variants.
    """
    job = job_manager.get_job()

    _job_required_params = ["score_set_id", "correlation_id"]
    validate_job_params(_job_required_params, job)

    # Safely ignore mypy warnings here, as params were checked above.
    score_set = job_manager.db.scalars(select(ScoreSet).where(ScoreSet.id == job.job_params["score_set_id"])).one()  # type: ignore
    correlation_id = job.job_params["correlation_id"]  # type: ignore

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
        logger.warning(
            msg=f"No mapped variants found for score set {score_set.urn}. Skipped VEP population.",
            extra=job_manager.logging_context(),
        )
        job_manager.db.flush()
        return JobExecutionOutcome.succeeded(
            data={
                "variants_processed": 0,
                "variants_with_consequences": 0,
                "variants_without_consequences": 0,
                "variants_recoder_failed": 0,
            }
        )

    job_manager.save_to_context({"total_variants_to_process": len(mapped_variants)})
    logger.info(
        msg=f"Found {len(mapped_variants)} mapped variants for VEP processing",
        extra=job_manager.logging_context(),
    )

    annotation_manager = AnnotationStatusManager(job_manager.db, job_run_id=job_manager.job_id)

    mapped_variants_by_id = {mv.id: mv for mv in mapped_variants}

    # Extract HGVS strings; skip and annotate variants that have none.
    hgvs_and_mapped_variant_id_pairs: list[tuple[str, int]] = []

    for mapped_variant in mapped_variants:
        if not mapped_variant.hgvs_assay_level:
            annotation_manager.add_annotation(
                variant_id=mapped_variant.variant_id,  # type: ignore
                annotation_type=AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
                status=AnnotationStatus.SKIPPED,
                failure_category=AnnotationFailureCategory.MISSING_IDENTIFIER,
                annotation_data={"error_message": "Mapped variant does not have an associated HGVS string."},
            )
            logger.debug("Mapped variant does not have an associated HGVS string.", extra=job_manager.logging_context())
            continue

        hgvs_and_mapped_variant_id_pairs.append((mapped_variant.hgvs_assay_level, mapped_variant.id))  # type: ignore

    batches = list(batched(hgvs_and_mapped_variant_id_pairs, _VEP_BATCH_SIZE))

    job_manager.save_to_context({"vep_batches": len(batches)})
    logger.debug(
        msg=f"Prepared {len(batches)} VEP batches ({_VEP_BATCH_SIZE} variants/batch)",
        extra=job_manager.logging_context(),
    )

    # --- Phase 1: Initial VEP pass ---
    all_consequences: dict[str, str | None] = {}
    all_missing_hgvs: set[str] = set()
    missing_hgvs_to_variant_ids: dict[str, list[int]] = {}

    for batch_idx, batch in enumerate(batches):
        try:
            logger.debug(
                msg=f"Processing VEP batch {batch_idx + 1}/{len(batches)}",
                extra=job_manager.logging_context(),
            )

            hgvs_strings, mapped_variant_ids = map(list, zip(*batch))  # type: ignore

            consequences = await get_functional_consequence(hgvs_strings)
            logger.debug(
                msg=f"Received consequences for {len(consequences)} variants in VEP batch {batch_idx + 1}",
                extra=job_manager.logging_context(),
            )

            all_consequences.update(consequences)

            missing_hgvs = set(hgvs_strings) - set(consequences.keys())
            for hgvs, mapped_variant_id in zip(hgvs_strings, mapped_variant_ids):
                if hgvs in missing_hgvs:
                    all_missing_hgvs.add(hgvs)
                    mv = mapped_variants_by_id[mapped_variant_id]
                    missing_hgvs_to_variant_ids.setdefault(hgvs, []).append(mv.variant_id)  # type: ignore

            progress_pct = int((batch_idx + 1) / len(batches) * 33)
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
            job_manager.db.flush()
            return JobExecutionOutcome.errored(
                exception=e,
                data={
                    "initial_vep_batches_processed": batch_idx + 1,
                    "variant_recoder_batches_processed": 0,
                    "missing_hgvs_count": len(all_missing_hgvs),
                },
            )

    logger.info(
        msg=f"Completed initial VEP processing. {len(all_missing_hgvs)} variants require Variant Recoder fallback.",
        extra=job_manager.logging_context(),
    )

    # --- Phase 2: Variant Recoder fallback for HGVS strings VEP could not resolve ---
    hgvs_to_genomic: dict[str, list[str]] = {}
    recoder_missing_hgvs: set[str] = set()

    if all_missing_hgvs:
        logger.info(
            msg=f"Running Variant Recoder for {len(all_missing_hgvs)} HGVS strings",
            extra=job_manager.logging_context(),
        )

        recoder_batch_list = list(batched(list(all_missing_hgvs), _RECODER_BATCH_SIZE))

        logger.debug(
            msg=f"Running {len(recoder_batch_list)} Variant Recoder batches with concurrency {_RECODER_CONCURRENCY}",
            extra=job_manager.logging_context(),
        )

        semaphore = asyncio.Semaphore(_RECODER_CONCURRENCY)

        async def _recoder_with_semaphore(batch: list[str], batch_idx: int, total: int) -> dict[str, list[str]]:
            async with semaphore:
                logger.debug(
                    msg=f"Starting Variant Recoder batch {batch_idx + 1}/{total} ({len(batch)} HGVS strings)",
                    extra=job_manager.logging_context(),
                )
                result = await run_variant_recoder(batch)
                logger.debug(
                    msg=f"Completed Variant Recoder batch {batch_idx + 1}/{total} ({len(result)} variants recoded)",
                    extra=job_manager.logging_context(),
                )
                return result

        total_recoder_batches = len(recoder_batch_list)
        recoder_results = await asyncio.gather(
            *[
                _recoder_with_semaphore(list(recoder_batch), idx, total_recoder_batches)
                for idx, recoder_batch in enumerate(recoder_batch_list)
            ],
            return_exceptions=True,
        )

        successful_batches = sum(1 for r in recoder_results if not isinstance(r, Exception))

        first_exception = next((r for r in recoder_results if isinstance(r, Exception)), None)
        if first_exception is not None:
            logger.error(
                msg=f"Variant Recoder error ({successful_batches}/{total_recoder_batches} batches succeeded): {str(first_exception)}",
                extra=job_manager.logging_context(),
            )
            job_manager.db.flush()
            return JobExecutionOutcome.errored(
                exception=first_exception,
                data={
                    "initial_vep_batches_processed": len(batches),
                    "variant_recoder_batches_processed": successful_batches,
                    "missing_hgvs_count": len(all_missing_hgvs),
                },
            )

        for result in recoder_results:
            hgvs_to_genomic.update(result)  # type: ignore[arg-type]

        job_manager.save_to_context(
            {
                "variant_recoder_batches_processed": len(recoder_batch_list),
                "recoded_variants_count": len(hgvs_to_genomic),
            }
        )
        job_manager.update_progress(
            66,
            100,
            f"Completed Variant Recoder for {len(recoder_batch_list)} batches ({len(hgvs_to_genomic)} variants recoded)",
        )
        logger.info(
            msg=f"Completed Variant Recoder processing. {len(hgvs_to_genomic)} variants successfully recoded.",
            extra=job_manager.logging_context(),
        )

        # --- Phase 3: VEP pass on the recoded genomic HGVS strings ---
        recoded_vep_batch_list = list(batched(list(hgvs_to_genomic.values()), _VEP_BATCH_SIZE))
        all_recoded_consequences: dict[str, str | None] = {}

        for recoded_vep_batch_idx, recoded_vep_batch in enumerate(recoded_vep_batch_list):
            try:
                logger.debug(
                    msg=f"Processing recoded HGVS VEP batch {recoded_vep_batch_idx + 1}/{len(recoded_vep_batch_list)}",
                    extra=job_manager.logging_context(),
                )

                recoded_vep_consequences = await get_functional_consequence(recoded_vep_batch)
                all_recoded_consequences.update(recoded_vep_consequences)

                progress_pct = 66 + int((recoded_vep_batch_idx + 1) / len(recoded_vep_batch_list) * 33)
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
                job_manager.db.flush()
                return JobExecutionOutcome.errored(
                    exception=e,
                    data={
                        "initial_vep_batches_processed": len(batches),
                        "variant_recoder_batches_processed": len(recoder_batch_list),
                        "recoded_vep_batches_processed": recoded_vep_batch_idx + 1,
                        "missing_hgvs_count": len(all_missing_hgvs),
                    },
                )

        logger.info(
            msg=f"Completed recoded VEP processing. {len(all_recoded_consequences)} recoded consequences retrieved.",
            extra=job_manager.logging_context(),
        )

        # Map most-severe consequence from recoded genomic HGVS back to the original HGVS.
        for original_hgvs, recoded_hgvs_list in hgvs_to_genomic.items():
            recoded_consequences_for_variant = [
                c for recoded_hgvs in recoded_hgvs_list if (c := all_recoded_consequences.get(recoded_hgvs))
            ]

            if recoded_consequences_for_variant:
                most_severe = next(
                    (c for c in VEP_CONSEQUENCES if c in recoded_consequences_for_variant),
                    None,
                )
                if most_severe:
                    all_consequences[original_hgvs] = most_severe
                    logger.debug(
                        msg=f"Selected most severe consequence '{most_severe}' for {original_hgvs}",
                        extra=job_manager.logging_context(),
                    )
            else:
                logger.debug(
                    msg=f"Could not retrieve functional consequences for any recoded variants of {original_hgvs}",
                    extra=job_manager.logging_context(),
                )

        recoder_missing_hgvs = all_missing_hgvs - set(hgvs_to_genomic.keys())

    # --- Phase 4: Annotate outcomes and update mapped variants in a single pass ---

    # HGVS strings that went through both VEP passes but still have no consequence.
    all_processed_hgvs = {h for h, _ in hgvs_and_mapped_variant_id_pairs}
    vep_failed_hgvs = all_processed_hgvs - set(all_consequences.keys()) - recoder_missing_hgvs

    variants_processed = 0
    variants_with_consequences = 0
    variants_without_consequences = 0
    variants_recoder_failed = 0

    for hgvs_string, mapped_variant_id in hgvs_and_mapped_variant_id_pairs:
        mapped_variant = mapped_variants_by_id.get(mapped_variant_id)  # type: ignore
        if mapped_variant is None:
            continue

        consequence = all_consequences.get(hgvs_string)
        if consequence:
            mapped_variant.vep_functional_consequence = consequence
            mapped_variant.vep_access_date = date.today()
            job_manager.db.add(mapped_variant)
            annotation_manager.add_annotation(
                variant_id=mapped_variant.variant_id,  # type: ignore
                annotation_type=AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
                status=AnnotationStatus.SUCCESS,
                annotation_data={"annotation_metadata": {"functional_consequence": consequence}},
            )
            variants_with_consequences += 1
            logger.debug(
                msg=f"Set consequence '{consequence}' for mapped variant {mapped_variant_id} (HGVS: {hgvs_string})",
                extra=job_manager.logging_context(),
            )
        elif hgvs_string in vep_failed_hgvs:
            annotation_manager.add_annotation(
                variant_id=mapped_variant.variant_id,  # type: ignore
                annotation_type=AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
                status=AnnotationStatus.FAILED,
                failure_category=AnnotationFailureCategory.EXTERNAL_REFERENCE_NOT_FOUND,
                annotation_data={
                    "error_message": "VEP could not determine a functional consequence for this variant, even after Variant Recoder fallback.",
                },
            )
            variants_without_consequences += 1
            logger.debug(
                msg=f"Recorded VEP failure for mapped_variant_id {mapped_variant_id} (HGVS: {hgvs_string})",
                extra=job_manager.logging_context(),
            )
        elif hgvs_string in recoder_missing_hgvs:
            annotation_manager.add_annotation(
                variant_id=mapped_variant.variant_id,  # type: ignore
                annotation_type=AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
                status=AnnotationStatus.FAILED,
                failure_category=AnnotationFailureCategory.EXTERNAL_REFERENCE_NOT_FOUND,
                annotation_data={
                    "error_message": "Variant Recoder could not recode this HGVS string to a genomic equivalent.",
                },
            )
            variants_recoder_failed += 1
            logger.debug(
                msg=f"Recorded Variant Recoder failure for mapped_variant_id {mapped_variant_id} (HGVS: {hgvs_string})",
                extra=job_manager.logging_context(),
            )
        else:
            annotation_manager.add_annotation(
                variant_id=mapped_variant.variant_id,  # type: ignore
                annotation_type=AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
                status=AnnotationStatus.FAILED,
                failure_category=AnnotationFailureCategory.UNKNOWN,
                annotation_data={
                    "error_message": "Variant was not classified by any VEP outcome branch. This is a bug.",
                },
            )
            variants_without_consequences += 1
            logger.warning(
                msg=f"Unexpected state: mapped_variant_id {mapped_variant_id} (HGVS: {hgvs_string}) was not classified by any outcome branch.",
                extra=job_manager.logging_context(),
            )

        variants_processed += 1

    annotation_manager.flush()
    job_manager.db.flush()

    job_manager.update_progress(
        100,
        100,
        f"Completed VEP functional consequence prediction for {variants_with_consequences}/{variants_processed} variants.",
    )
    logger.info(
        msg=f"Completed VEP prediction: {variants_with_consequences} with consequences, {variants_without_consequences} without, {variants_recoder_failed} recoder failed",
        extra=job_manager.logging_context(),
    )

    job_manager.db.flush()
    return JobExecutionOutcome.succeeded(
        data={
            "variants_processed": variants_processed,
            "variants_with_consequences": variants_with_consequences,
            "variants_without_consequences": variants_without_consequences,
            "variants_recoder_failed": variants_recoder_failed,
        }
    )

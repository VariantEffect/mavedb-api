"""ClinGen integration jobs for variant submission and linking.

This module contains jobs for submitting mapped variants to ClinGen services:
- ClinGen Allele Registry (CAR) for allele registration
- ClinGen Linked Data Hub (LDH) for data submission
- Variant linking and association management

These jobs enable integration with the ClinGen ecosystem for clinical
variant interpretation and data sharing.
"""

import asyncio
import functools
import logging

from sqlalchemy import select

from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from mavedb.lib.clingen.constants import (
    CAR_SUBMISSION_ENDPOINT,
    CLIN_GEN_SUBMISSION_ENABLED,
    DEFAULT_LDH_SUBMISSION_BATCH_SIZE,
    LDH_SUBMISSION_ENDPOINT,
)
from mavedb.lib.clingen.content_constructors import construct_ldh_submission
from mavedb.lib.clingen.services import (
    ClinGenAlleleRegistryService,
    ClinGenLdhService,
    get_allele_registry_associations,
)
from mavedb.lib.exceptions import LDHSubmissionFailureError
from mavedb.lib.variants import get_hgvs_from_post_mapped
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationStatus
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.managers.types import JobResultData

logger = logging.getLogger(__name__)


@with_pipeline_management
async def submit_score_set_mappings_to_car(ctx: dict, job_id: int, job_manager: JobManager) -> JobResultData:
    """
    Submit mapped variants for a score set to the ClinGen Allele Registry (CAR).

    This job registers mapped variants with CAR, assigns ClinGen Allele IDs (CAIDs),
    and updates the database with the results. Progress is tracked throughout the submission.

    Required job_params in the JobRun:
        - score_set_id (int): ID of the ScoreSet to process
        - correlation_id (str): Correlation ID for tracking

    Args:
        ctx (dict): Worker context containing DB and Redis connections
        job_manager (JobManager): Manager for job lifecycle and DB operations

    Side Effects:
        - Updates MappedVariant records with ClinGen Allele IDs
        - Submits data to ClinGen Allele Registry

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
            "function": "submit_score_set_mappings_to_car",
            "resource": score_set.urn,
            "correlation_id": correlation_id,
        }
    )
    job_manager.update_progress(0, 100, "Starting CAR mapped resource submission.")
    logger.info(msg="Started CAR mapped resource submission", extra=job_manager.logging_context())

    # Ensure we've enabled ClinGen submission
    if not CLIN_GEN_SUBMISSION_ENABLED:
        job_manager.update_progress(100, 100, "ClinGen submission is disabled. Skipping CAR submission.")
        logger.warning(
            msg="ClinGen submission is disabled via configuration, skipping submission of mapped variants to CAR.",
            extra=job_manager.logging_context(),
        )
        return {"status": "ok", "data": {}, "exception_details": None}

    # Check for CAR submission endpoint
    if not CAR_SUBMISSION_ENDPOINT:
        job_manager.update_progress(100, 100, "CAR submission endpoint not configured. Can't complete submission.")
        logger.warning(
            msg="ClinGen Allele Registry submission is disabled (no submission endpoint), unable to complete submission of mapped variants to CAR.",
            extra=job_manager.logging_context(),
        )
        raise ValueError("ClinGen Allele Registry submission endpoint is not configured.")

    # Fetch mapped variants with post-mapped data for the score set
    variant_post_mapped_objects = job_manager.db.execute(
        select(MappedVariant.id, MappedVariant.post_mapped)
        .join(Variant)
        .join(ScoreSet)
        .where(ScoreSet.urn == score_set.urn)
        .where(MappedVariant.post_mapped.is_not(None))
        .where(MappedVariant.current.is_(True))
    ).all()

    # Track total variants to submit
    job_manager.save_to_context({"total_variants_to_submit_car": len(variant_post_mapped_objects)})
    if not variant_post_mapped_objects:
        job_manager.update_progress(100, 100, "No mapped variants to submit to CAR. Skipped submission.")
        logger.warning(
            msg="No current mapped variants with post mapped metadata were found for this score set. Skipping CAR submission.",
            extra=job_manager.logging_context(),
        )
        return {"status": "ok", "data": {}, "exception_details": None}

    job_manager.update_progress(
        10, 100, f"Preparing {len(variant_post_mapped_objects)} mapped variants for CAR submission."
    )

    # Build HGVS strings for submission. Don't do duplicate submissions-- store mapped variant IDs by HGVS.
    variant_post_mapped_hgvs: dict[str, list[int]] = {}
    for mapped_variant_id, post_mapped in variant_post_mapped_objects:
        hgvs_for_post_mapped = get_hgvs_from_post_mapped(post_mapped)

        if not hgvs_for_post_mapped:
            logger.warning(
                msg=f"Could not construct a valid HGVS string for mapped variant {mapped_variant_id}. Skipping submission of this variant.",
                extra=job_manager.logging_context(),
            )
            continue

        if hgvs_for_post_mapped in variant_post_mapped_hgvs:
            variant_post_mapped_hgvs[hgvs_for_post_mapped].append(mapped_variant_id)
        else:
            variant_post_mapped_hgvs[hgvs_for_post_mapped] = [mapped_variant_id]

    job_manager.save_to_context({"unique_variants_to_submit_car": len(variant_post_mapped_hgvs)})
    job_manager.update_progress(15, 100, "Submitting mapped variants to CAR.")

    # Do submission
    car_service = ClinGenAlleleRegistryService(url=CAR_SUBMISSION_ENDPOINT)
    registered_alleles = car_service.dispatch_submissions(list(variant_post_mapped_hgvs.keys()))
    job_manager.update_progress(60, 100, "Processing registered alleles from CAR.")

    # Process registered alleles and update mapped variants
    linked_alleles = get_allele_registry_associations(list(variant_post_mapped_hgvs.keys()), registered_alleles)
    total = len(linked_alleles)
    processed = 0
    # Setup annotation manager
    annotation_manager = AnnotationStatusManager(job_manager.db)
    registered_mapped_variant_ids = []
    for hgvs_string, caid in linked_alleles.items():
        mapped_variant_ids = variant_post_mapped_hgvs[hgvs_string]
        registered_mapped_variant_ids.extend(mapped_variant_ids)
        mapped_variants = job_manager.db.scalars(
            select(MappedVariant).where(MappedVariant.id.in_(mapped_variant_ids))
        ).all()

        for mapped_variant in mapped_variants:
            mapped_variant.clingen_allele_id = caid
            job_manager.db.add(mapped_variant)

            annotation_manager.add_annotation(
                variant_id=mapped_variant.variant_id,  # type: ignore
                annotation_type=AnnotationType.CLINGEN_ALLELE_ID,
                version=None,
                status=AnnotationStatus.SUCCESS,
                annotation_data={
                    "success_data": {"clingen_allele_id": caid},
                },
                current=True,
            )

            processed += 1

            # Calculate progress: 50% + (processed/total_mapped)*50, rounded to nearest 5%
            if total % 20 == 0 or processed == total:
                progress = 50 + round((processed / total) * 45 / 5) * 5
                job_manager.update_progress(progress, 100, f"Processed {processed} of {total} registered alleles.")

    # For mapped variants which did not get a CAID, log failure annotation
    failed_submissions = set(obj[0] for obj in variant_post_mapped_objects) - set(registered_mapped_variant_ids)
    for mapped_variant_id in failed_submissions:
        mapped_variant = job_manager.db.scalars(
            select(MappedVariant).where(MappedVariant.id == mapped_variant_id)
        ).one()

        annotation_manager.add_annotation(
            variant_id=mapped_variant.variant_id,  # type: ignore
            annotation_type=AnnotationType.CLINGEN_ALLELE_ID,
            version=None,
            status=AnnotationStatus.FAILED,
            annotation_data={
                "error_message": "Failed to register variant with ClinGen Allele Registry.",
            },
            current=True,
        )

    # Finalize progress
    job_manager.update_progress(100, 100, "Completed CAR mapped resource submission.")
    job_manager.db.flush()
    logger.info(msg="Completed CAR mapped resource submission", extra=job_manager.logging_context())
    return {"status": "ok", "data": {}, "exception_details": None}


@with_pipeline_management
async def submit_score_set_mappings_to_ldh(ctx: dict, job_id: int, job_manager: JobManager) -> JobResultData:
    """
    Submit mapped variants for a score set to the ClinGen Linked Data Hub (LDH).

    This job submits mapped variant data to LDH for a given score set, handling authentication,
    submission batching, and error reporting. Progress and errors are logged and reported to Slack.

    Required job_params in the JobRun:
        - score_set_id (int): ID of the ScoreSet to process
        - correlation_id (str): Correlation ID for tracking

    Args:
        ctx (dict): Worker context containing DB and Redis connections
        job_manager (JobManager): Manager for job lifecycle and DB operations

    Side Effects:
        - Submits data to ClinGen Linked Data Hub

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
            "function": "submit_score_set_mappings_to_ldh",
            "resource": score_set.urn,
            "correlation_id": correlation_id,
        }
    )
    job_manager.update_progress(0, 100, "Starting LDH mapped resource submission.")
    logger.info(msg="Started LDH mapped resource submission", extra=job_manager.logging_context())

    # Connect to LDH service
    ldh_service = ClinGenLdhService(url=LDH_SUBMISSION_ENDPOINT)
    ldh_service.authenticate()

    # Fetch mapped variants with post-mapped data for the score set
    variant_objects = job_manager.db.execute(
        select(Variant, MappedVariant)
        .join(MappedVariant)
        .join(ScoreSet)
        .where(ScoreSet.urn == score_set.urn)
        .where(MappedVariant.post_mapped.is_not(None))
        .where(MappedVariant.current.is_(True))
    ).all()

    # Track total variants to submit
    job_manager.save_to_context({"total_variants_to_submit_ldh": len(variant_objects)})
    if not variant_objects:
        job_manager.update_progress(100, 100, "No mapped variants to submit to LDH. Skipping submission.")
        logger.warning(
            msg="No current mapped variants with post mapped metadata were found for this score set. Skipping LDH submission.",
            extra=job_manager.logging_context(),
        )
        return {"status": "ok", "data": {}, "exception_details": None}
    job_manager.update_progress(10, 100, f"Submitting {len(variant_objects)} mapped variants to LDH.")

    # Build submission content
    variant_content = []
    variant_for_urn = {}
    for variant, mapped_variant in variant_objects:
        variation = get_hgvs_from_post_mapped(mapped_variant.post_mapped)

        if not variation:
            logger.warning(
                msg=f"Could not construct a valid HGVS string for mapped variant {mapped_variant.id}. Skipping submission of this variant.",
                extra=job_manager.logging_context(),
            )
            continue

        variant_content.append((variation, variant, mapped_variant))
        variant_for_urn[variant.urn] = variant

    if not variant_content:
        job_manager.update_progress(100, 100, "No valid mapped variants to submit to LDH. Skipping submission.")
        logger.warning(
            msg="No valid mapped variants with post mapped metadata were found for this score set. Skipping LDH submission.",
            extra=job_manager.logging_context(),
        )
        return {"status": "ok", "data": {}, "exception_details": None}

    job_manager.save_to_context({"unique_variants_to_submit_ldh": len(variant_content)})
    job_manager.update_progress(30, 100, f"Dispatching submissions for {len(variant_content)} unique variants to LDH.")
    submission_content = construct_ldh_submission(variant_content)

    blocking = functools.partial(
        ldh_service.dispatch_submissions, submission_content, DEFAULT_LDH_SUBMISSION_BATCH_SIZE
    )
    loop = asyncio.get_running_loop()
    submission_successes, submission_failures = await loop.run_in_executor(ctx["pool"], blocking)
    job_manager.update_progress(90, 100, "Finalizing LDH mapped resource submission.")
    job_manager.save_to_context(
        {
            "ldh_submission_successes": len(submission_successes),
            "ldh_submission_failures": len(submission_failures),
        }
    )

    # TODO prior to finalizing: Verify typing of ClinGen submission responses. See https://reg.clinicalgenome.org/doc/AlleleRegistry_1.01.xx_api_v1.pdf
    annotation_manager = AnnotationStatusManager(job_manager.db)
    submitted_variant_urns = set()
    for success in submission_successes:
        logger.debug(
            msg=f"Successfully submitted mapped variant to LDH: {success}",
            extra=job_manager.logging_context(),
        )

        submitted_urn = success["data"]["entId"]
        submitted_variant = variant_for_urn[submitted_urn]

        annotation_manager.add_annotation(
            variant_id=submitted_variant.id,
            annotation_type=AnnotationType.LDH_SUBMISSION,
            version=None,
            status=AnnotationStatus.SUCCESS,
            annotation_data={
                "success_data": {"ldh_iri": success["data"]["ldhIri"], "ldh_id": success["data"]["ldhId"]},
            },
            current=True,
        )
        submitted_variant_urns.add(submitted_urn)

    # It isn't trivial to map individual failures back to their corresponding variants,
    # especially when submission occurred in batch. Save all failures generically here.
    # Note that failures may not be present in the submission failures list, but they are
    # guaranteed to be absent from the successes list.
    for failure_urn in set(variant_for_urn.keys()) - submitted_variant_urns:
        logger.error(
            msg=f"Failed to submit mapped variant to LDH: {failure_urn}",
            extra=job_manager.logging_context(),
        )

        failed_variant = variant_for_urn[failure_urn]

        annotation_manager.add_annotation(
            variant_id=failed_variant.id,
            annotation_type=AnnotationType.LDH_SUBMISSION,
            version=None,
            status=AnnotationStatus.FAILED,
            annotation_data={
                "error_message": "Failed to submit variant to ClinGen Linked Data Hub.",
            },
            current=True,
        )

    if submission_failures:
        logger.warning(
            msg=f"LDH mapped resource submission encountered {len(submission_failures)} failures.",
            extra=job_manager.logging_context(),
        )

        if not submission_successes:
            job_manager.update_progress(100, 100, "All mapped variant submissions to LDH failed.")
            error_message = f"All LDH submissions failed for score set {score_set.urn}."
            logger.error(
                msg=error_message,
                extra=job_manager.logging_context(),
            )

            # Return a failure state here rather than raising to indicate to the manager
            # we should still commit any successful annotations.
            return {
                "status": "failed",
                "data": {},
                "exception_details": {
                    "message": error_message,
                    "type": LDHSubmissionFailureError.__name__,
                    "traceback": None,
                },
            }

    logger.info(
        msg="Completed LDH mapped resource submission",
        extra=job_manager.logging_context(),
    )

    # Finalize progress
    job_manager.update_progress(
        100,
        100,
        f"Finalized LDH mapped resource submission ({len(submission_successes)} successes, {len(submission_failures)} failures).",
    )
    job_manager.db.flush()
    return {"status": "ok", "data": {}, "exception_details": None}

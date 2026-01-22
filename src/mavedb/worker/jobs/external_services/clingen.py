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

from mavedb.lib.clingen.constants import (
    CAR_SUBMISSION_ENDPOINT,
    DEFAULT_LDH_SUBMISSION_BATCH_SIZE,
    LDH_SUBMISSION_ENDPOINT,
)
from mavedb.lib.clingen.content_constructors import construct_ldh_submission
from mavedb.lib.clingen.services import (
    ClinGenAlleleRegistryService,
    ClinGenLdhService,
    clingen_allele_id_from_ldh_variation,
    get_allele_registry_associations,
    get_clingen_variation,
)
from mavedb.lib.variants import get_hgvs_from_post_mapped
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

    # Build HGVS strings for submission
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

    # Check for CAR submission endpoint
    if not CAR_SUBMISSION_ENDPOINT:
        job_manager.update_progress(100, 100, "CAR submission endpoint not configured. Skipping submission.")
        logger.warning(
            msg="ClinGen Allele Registry submission is disabled (no submission endpoint), skipping submission of mapped variants to CAR.",
            extra=job_manager.logging_context(),
        )
        raise ValueError("ClinGen Allele Registry submission endpoint is not configured.")

    # Do submission
    car_service = ClinGenAlleleRegistryService(url=CAR_SUBMISSION_ENDPOINT)
    registered_alleles = car_service.dispatch_submissions(list(variant_post_mapped_hgvs.keys()))
    job_manager.update_progress(50, 100, "Processing registered alleles from CAR.")

    # Process registered alleles and update mapped variants
    linked_alleles = get_allele_registry_associations(list(variant_post_mapped_hgvs.keys()), registered_alleles)
    processed = 0
    total = len(linked_alleles)
    for hgvs_string, caid in linked_alleles.items():
        mapped_variant_ids = variant_post_mapped_hgvs[hgvs_string]
        mapped_variants = job_manager.db.scalars(
            select(MappedVariant).where(MappedVariant.id.in_(mapped_variant_ids))
        ).all()

        # TODO: Track annotation progress.
        for mapped_variant in mapped_variants:
            mapped_variant.clingen_allele_id = caid
            job_manager.db.add(mapped_variant)
            processed += 1

            # Calculate progress: 50% + (processed/total_mapped)*50, rounded to nearest 5%
            if total % 20 == 0 or processed == total:
                progress = 50 + round((processed / total) * 50 / 5) * 5
                job_manager.update_progress(progress, 100, f"Processed {processed} of {total} registered alleles.")

    # Finalize progress
    job_manager.update_progress(100, 100, "Completed CAR mapped resource submission.")
    job_manager.db.commit()
    logger.info(msg="Completed CAR mapped resource submission", extra=job_manager.logging_context())
    return {"status": "ok", "data": {}, "exception_details": None}


@with_pipeline_management
async def submit_score_set_mappings_to_ldh(ctx: dict, job_manager: JobManager) -> JobResultData:
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
    for variant, mapped_variant in variant_objects:
        variation = get_hgvs_from_post_mapped(mapped_variant.post_mapped)

        if not variation:
            logger.warning(
                msg=f"Could not construct a valid HGVS string for mapped variant {mapped_variant.id}. Skipping submission of this variant.",
                extra=job_manager.logging_context(),
            )
            continue

        variant_content.append((variation, variant, mapped_variant))

    job_manager.save_to_context({"unique_variants_to_submit_ldh": len(variant_content)})
    job_manager.update_progress(30, 100, f"Dispatching submissions for {len(variant_content)} unique variants to LDH.")
    submission_content = construct_ldh_submission(variant_content)

    blocking = functools.partial(
        ldh_service.dispatch_submissions, submission_content, DEFAULT_LDH_SUBMISSION_BATCH_SIZE
    )
    loop = asyncio.get_running_loop()
    submission_successes, submission_failures = await loop.run_in_executor(ctx["pool"], blocking)
    job_manager.update_progress(90, 100, "Finalizing LDH mapped resource submission.")

    # TODO: Track submission successes and failures, add as annotation features.
    if submission_failures:
        job_manager.save_to_context({"ldh_submission_failures": len(submission_failures)})
        logger.error(
            msg=f"LDH mapped resource submission encountered {len(submission_failures)} failures.",
            extra=job_manager.logging_context(),
        )

    # Finalize progress
    job_manager.update_progress(100, 100, "Finalized LDH mapped resource submission.")
    job_manager.db.commit()
    return {"status": "ok", "data": {}, "exception_details": None}


def do_clingen_fetch(variant_urns):
    return [(variant_urn, get_clingen_variation(variant_urn)) for variant_urn in variant_urns]


@with_pipeline_management
async def link_clingen_variants(ctx: dict, job_manager: JobManager) -> JobResultData:
    """
    Link mapped variants to ClinGen Linked Data Hub (LDH) submissions.

    This job links mapped variant data to existing LDH data for a given score set. It fetches
    LDH variations for each mapped variant and updates the database accordingly. Progress
    and errors are logged throughout the process.

    Required job_params in the JobRun:
        - score_set_id (int): ID of the ScoreSet to process
        - correlation_id (str): Correlation ID for tracking

    Args:
        ctx (dict): Worker context containing DB and Redis connections
        job_manager (JobManager): Manager for job lifecycle and DB operations

    Side Effects:
        - Updates MappedVariant records with ClinGen Allele IDs from LDH objects

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
            "function": "link_clingen_variants",
            "resource": score_set.urn,
            "correlation_id": correlation_id,
        }
    )
    job_manager.update_progress(0, 100, "Starting LDH mapped resource linkage.")
    logger.info(msg="Started LDH mapped resource linkage", extra=job_manager.logging_context())

    # Fetch mapped variants with post-mapped data for the score set
    variant_urns = job_manager.db.scalars(
        select(Variant.urn)
        .join(MappedVariant)
        .join(ScoreSet)
        .where(ScoreSet.urn == score_set.urn, MappedVariant.current.is_(True), MappedVariant.post_mapped.is_not(None))
    ).all()
    num_variant_urns = len(variant_urns)

    job_manager.save_to_context({"total_variants_to_link_ldh": num_variant_urns})
    job_manager.update_progress(10, 100, f"Found {num_variant_urns} mapped variants to link to LDH submissions.")

    if not variant_urns:
        job_manager.update_progress(100, 100, "No mapped variants to link to LDH submissions. Skipping linkage.")
        logger.warning(
            msg="No current mapped variants with post mapped metadata were found for this score set. Skipping LDH linkage (nothing to do). A gnomAD linkage job will not be enqueued, as no variants will have a CAID.",
            extra=job_manager.logging_context(),
        )
        return {"status": "ok", "data": {}, "exception_details": None}

    logger.info(msg="Attempting to link mapped variants to LDH submissions.", extra=job_manager.logging_context())

    # TODO#372: Non-nullable variant urns.
    # Fetch linked data from LDH for each variant URN
    blocking = functools.partial(
        do_clingen_fetch,
        variant_urns,  # type: ignore
    )
    loop = asyncio.get_running_loop()
    linked_data = await loop.run_in_executor(ctx["pool"], blocking)

    linked_allele_ids = [
        (variant_urn, clingen_allele_id_from_ldh_variation(clingen_variation))
        for variant_urn, clingen_variation in linked_data
    ]
    job_manager.save_to_context({"ldh_variants_fetched": len(linked_allele_ids)})
    job_manager.update_progress(70, 100, "Fetched existing LDH variant data.")
    logger.info(msg="Fetched existing LDH variant data.", extra=job_manager.logging_context())

    # Link mapped variants to fetched LDH data
    linkage_failures = []
    for variant_urn, ldh_variation in linked_allele_ids:
        # XXX: Should we unlink variation if it is not found? Does this constitute a failure?
        if not ldh_variation:
            logger.warning(
                msg=f"Failed to link mapped variant {variant_urn} to LDH submission. No LDH variation found.",
                extra=job_manager.logging_context(),
            )
            linkage_failures.append(variant_urn)
            continue

        mapped_variant = job_manager.db.scalars(
            select(MappedVariant).join(Variant).where(Variant.urn == variant_urn, MappedVariant.current.is_(True))
        ).one_or_none()

        if not mapped_variant:
            logger.warning(
                msg=f"Failed to link mapped variant {variant_urn} to LDH submission. No mapped variant found.",
                extra=job_manager.logging_context(),
            )
            linkage_failures.append(variant_urn)
            continue

        mapped_variant.clingen_allele_id = ldh_variation
        job_manager.db.add(mapped_variant)

        # TODO: Track annotation progress. Given the new progress model, we can better understand what linked and what didn't and
        # can move away from the retry threshold model.

        # Calculate progress: 70% + (linked/total_variants)*30, rounded to nearest 5%
        if len(linked_allele_ids) % 20 == 0 or len(linked_allele_ids) == num_variant_urns:
            progress = 70 + round((len(linked_allele_ids) / num_variant_urns) * 30 / 5) * 5
            job_manager.update_progress(
                progress, 100, f"Linked {len(linked_allele_ids)} of {num_variant_urns} variants."
            )

    job_manager.save_to_context({"ldh_linkage_failures": len(linkage_failures)})
    if linkage_failures:
        logger.warning(
            msg=f"LDH mapped resource linkage encountered {len(linkage_failures)} failures.",
            extra=job_manager.logging_context(),
        )

    # Finalize progress
    job_manager.update_progress(100, 100, "Finalized LDH mapped resource linkage.")
    job_manager.db.commit()
    return {"status": "ok", "data": {}, "exception_details": None}

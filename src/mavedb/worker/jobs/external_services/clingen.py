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
from datetime import timedelta

from arq import ArqRedis
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.clingen.constants import (
    CAR_SUBMISSION_ENDPOINT,
    DEFAULT_LDH_SUBMISSION_BATCH_SIZE,
    LDH_SUBMISSION_ENDPOINT,
    LINKED_DATA_RETRY_THRESHOLD,
)
from mavedb.lib.clingen.content_constructors import construct_ldh_submission
from mavedb.lib.clingen.services import (
    ClinGenAlleleRegistryService,
    ClinGenLdhService,
    clingen_allele_id_from_ldh_variation,
    get_allele_registry_associations,
    get_clingen_variation,
)
from mavedb.lib.exceptions import LinkingEnqueueError, SubmissionEnqueueError
from mavedb.lib.logging.context import format_raised_exception_info_as_dict
from mavedb.lib.slack import send_slack_error, send_slack_message
from mavedb.lib.variants import get_hgvs_from_post_mapped
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.constants import ENQUEUE_BACKOFF_ATTEMPT_LIMIT, LINKING_BACKOFF_IN_SECONDS
from mavedb.worker.jobs.utils.job_state import setup_job_state
from mavedb.worker.jobs.utils.retry import enqueue_job_with_backoff

logger = logging.getLogger(__name__)


async def submit_score_set_mappings_to_car(ctx: dict, correlation_id: str, score_set_id: int):
    logging_context = {}
    score_set = None
    text = "Could not submit mappings to ClinGen Allele Registry for score set %s. Mappings for this score set should be submitted manually."
    try:
        db: Session = ctx["db"]
        redis: ArqRedis = ctx["redis"]
        score_set = db.scalars(select(ScoreSet).where(ScoreSet.id == score_set_id)).one()

        logging_context = setup_job_state(ctx, None, score_set.urn, correlation_id)
        logger.info(msg="Started CAR mapped resource submission", extra=logging_context)

        submission_urn = score_set.urn
        assert submission_urn, "A valid URN is needed to submit CAR objects for this score set."

        logging_context["current_car_submission_resource"] = submission_urn
        logger.debug(msg="Fetched score set metadata for CAR mapped resource submission.", extra=logging_context)

    except Exception as e:
        send_slack_error(e)
        if score_set:
            send_slack_message(text=text % score_set.urn)
        else:
            send_slack_message(text=text % score_set_id)

        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="CAR mapped resource submission encountered an unexpected error during setup. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": None}

    try:
        variant_post_mapped_objects = db.execute(
            select(MappedVariant.id, MappedVariant.post_mapped)
            .join(Variant)
            .join(ScoreSet)
            .where(ScoreSet.urn == score_set.urn)
            .where(MappedVariant.post_mapped.is_not(None))
            .where(MappedVariant.current.is_(True))
        ).all()

        if not variant_post_mapped_objects:
            logger.warning(
                msg="No current mapped variants with post mapped metadata were found for this score set. Skipping CAR submission.",
                extra=logging_context,
            )
            return {"success": True, "retried": False, "enqueued_job": None}

        variant_post_mapped_hgvs: dict[str, list[int]] = {}
        for mapped_variant_id, post_mapped in variant_post_mapped_objects:
            hgvs_for_post_mapped = get_hgvs_from_post_mapped(post_mapped)

            if not hgvs_for_post_mapped:
                logger.warning(
                    msg=f"Could not construct a valid HGVS string for mapped variant {mapped_variant_id}. Skipping submission of this variant.",
                    extra=logging_context,
                )
                continue

            if hgvs_for_post_mapped in variant_post_mapped_hgvs:
                variant_post_mapped_hgvs[hgvs_for_post_mapped].append(mapped_variant_id)
            else:
                variant_post_mapped_hgvs[hgvs_for_post_mapped] = [mapped_variant_id]

    except Exception as e:
        send_slack_error(e)
        send_slack_message(text=text % score_set.urn)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="LDH mapped resource submission encountered an unexpected error while attempting to construct post mapped HGVS strings. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": None}

    try:
        if not CAR_SUBMISSION_ENDPOINT:
            logger.warning(
                msg="ClinGen Allele Registry submission is disabled (no submission endpoint), skipping submission of mapped variants to CAR.",
                extra=logging_context,
            )
            return {"success": False, "retried": False, "enqueued_job": None}

        car_service = ClinGenAlleleRegistryService(url=CAR_SUBMISSION_ENDPOINT)
        registered_alleles = car_service.dispatch_submissions(list(variant_post_mapped_hgvs.keys()))
    except Exception as e:
        send_slack_error(e)
        send_slack_message(text=text % score_set.urn)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="LDH mapped resource submission encountered an unexpected error while attempting to authenticate to the LDH. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": None}

    try:
        linked_alleles = get_allele_registry_associations(list(variant_post_mapped_hgvs.keys()), registered_alleles)
        for hgvs_string, caid in linked_alleles.items():
            mapped_variant_ids = variant_post_mapped_hgvs[hgvs_string]
            mapped_variants = db.scalars(select(MappedVariant).where(MappedVariant.id.in_(mapped_variant_ids))).all()

            for mapped_variant in mapped_variants:
                mapped_variant.clingen_allele_id = caid
                db.add(mapped_variant)

        db.commit()

    except Exception as e:
        send_slack_error(e)
        send_slack_message(text=text % score_set.urn)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="LDH mapped resource submission encountered an unexpected error while attempting to authenticate to the LDH. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": None}

    new_job_id = None
    try:
        new_job = await redis.enqueue_job(
            "submit_score_set_mappings_to_ldh",
            correlation_id,
            score_set.id,
        )

        if new_job:
            new_job_id = new_job.job_id

            logging_context["submit_clingen_ldh_variants_job_id"] = new_job_id
            logger.info(msg="Queued a new ClinGen submission job.", extra=logging_context)

        else:
            raise SubmissionEnqueueError()

    except Exception as e:
        send_slack_error(e)
        send_slack_message(
            f"Could not submit mappings to LDH for score set {score_set.urn}. Mappings for this score set should be submitted manually."
        )
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="Mapped variant ClinGen submission encountered an unexpected error while attempting to enqueue a submission job. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": new_job_id}

    ctx["state"][ctx["job_id"]] = logging_context.copy()
    return {"success": True, "retried": False, "enqueued_job": new_job_id}


async def submit_score_set_mappings_to_ldh(ctx: dict, correlation_id: str, score_set_id: int):
    logging_context = {}
    score_set = None
    text = (
        "Could not submit mappings to LDH for score set %s. Mappings for this score set should be submitted manually."
    )
    try:
        db: Session = ctx["db"]
        redis: ArqRedis = ctx["redis"]
        score_set = db.scalars(select(ScoreSet).where(ScoreSet.id == score_set_id)).one()

        logging_context = setup_job_state(ctx, None, score_set.urn, correlation_id)
        logger.info(msg="Started LDH mapped resource submission", extra=logging_context)

        submission_urn = score_set.urn
        assert submission_urn, "A valid URN is needed to submit LDH objects for this score set."

        logging_context["current_ldh_submission_resource"] = submission_urn
        logger.debug(msg="Fetched score set metadata for ldh mapped resource submission.", extra=logging_context)

    except Exception as e:
        send_slack_error(e)
        if score_set:
            send_slack_message(text=text % score_set.urn)
        else:
            send_slack_message(text=text % score_set_id)

        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="LDH mapped resource submission encountered an unexpected error during setup. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": None}

    try:
        ldh_service = ClinGenLdhService(url=LDH_SUBMISSION_ENDPOINT)
        ldh_service.authenticate()
    except Exception as e:
        send_slack_error(e)
        send_slack_message(text=text % score_set.urn)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="LDH mapped resource submission encountered an unexpected error while attempting to authenticate to the LDH. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": None}

    try:
        variant_objects = db.execute(
            select(Variant, MappedVariant)
            .join(MappedVariant)
            .join(ScoreSet)
            .where(ScoreSet.urn == score_set.urn)
            .where(MappedVariant.post_mapped.is_not(None))
            .where(MappedVariant.current.is_(True))
        ).all()

        if not variant_objects:
            logger.warning(
                msg="No current mapped variants with post mapped metadata were found for this score set. Skipping LDH submission.",
                extra=logging_context,
            )
            return {"success": True, "retried": False, "enqueued_job": None}

        variant_content = []
        for variant, mapped_variant in variant_objects:
            variation = get_hgvs_from_post_mapped(mapped_variant.post_mapped)

            if not variation:
                logger.warning(
                    msg=f"Could not construct a valid HGVS string for mapped variant {mapped_variant.id}. Skipping submission of this variant.",
                    extra=logging_context,
                )
                continue

            variant_content.append((variation, variant, mapped_variant))

        submission_content = construct_ldh_submission(variant_content)

    except Exception as e:
        send_slack_error(e)
        send_slack_message(text=text % score_set.urn)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="LDH mapped resource submission encountered an unexpected error while attempting to construct submission objects. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": None}

    try:
        blocking = functools.partial(
            ldh_service.dispatch_submissions, submission_content, DEFAULT_LDH_SUBMISSION_BATCH_SIZE
        )
        loop = asyncio.get_running_loop()
        submission_successes, submission_failures = await loop.run_in_executor(ctx["pool"], blocking)

    except Exception as e:
        send_slack_error(e)
        send_slack_message(text=text % score_set.urn)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="LDH mapped resource submission encountered an unexpected error while dispatching submissions. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": None}

    try:
        assert not submission_failures, f"{len(submission_failures)} submissions failed to be dispatched to the LDH."
        logger.info(msg="Dispatched all variant mapping submissions to the LDH.", extra=logging_context)
    except AssertionError as e:
        send_slack_error(e)
        send_slack_message(
            text=f"{len(submission_failures)} submissions failed to be dispatched to the LDH for score set {score_set.urn}."
        )
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="LDH mapped resource submission failed to submit all mapping resources. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": None}

    new_job_id = None
    try:
        new_job = await redis.enqueue_job(
            "link_clingen_variants",
            correlation_id,
            score_set.id,
            1,
            _defer_by=timedelta(seconds=LINKING_BACKOFF_IN_SECONDS),
        )

        if new_job:
            new_job_id = new_job.job_id

            logging_context["link_clingen_variants_job_id"] = new_job_id
            logger.info(msg="Queued a new ClinGen linking job.", extra=logging_context)

        else:
            raise LinkingEnqueueError()

    except Exception as e:
        send_slack_error(e)
        send_slack_message(text=text % score_set.urn)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="LDH mapped resource submission encountered an unexpected error while attempting to enqueue a linking job. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": new_job_id}

    return {"success": True, "retried": False, "enqueued_job": new_job_id}


def do_clingen_fetch(variant_urns):
    return [(variant_urn, get_clingen_variation(variant_urn)) for variant_urn in variant_urns]


async def link_clingen_variants(ctx: dict, correlation_id: str, score_set_id: int, attempt: int) -> dict:
    logging_context = {}
    score_set = None
    text = "Could not link mappings to LDH for score set %s. Mappings for this score set should be linked manually."
    try:
        db: Session = ctx["db"]
        redis: ArqRedis = ctx["redis"]
        score_set = db.scalars(select(ScoreSet).where(ScoreSet.id == score_set_id)).one()

        logging_context = setup_job_state(ctx, None, score_set.urn, correlation_id)
        logging_context["linkage_retry_threshold"] = LINKED_DATA_RETRY_THRESHOLD
        logging_context["attempt"] = attempt
        logging_context["max_attempts"] = ENQUEUE_BACKOFF_ATTEMPT_LIMIT
        logger.info(msg="Started LDH mapped resource linkage", extra=logging_context)

        submission_urn = score_set.urn
        assert submission_urn, "A valid URN is needed to link LDH objects for this score set."

        logging_context["current_ldh_linking_resource"] = submission_urn
        logger.debug(msg="Fetched score set metadata for ldh mapped resource linkage.", extra=logging_context)

    except Exception as e:
        send_slack_error(e)
        if score_set:
            send_slack_message(text=text % score_set.urn)
        else:
            send_slack_message(text=text % score_set_id)

        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="LDH mapped resource linkage encountered an unexpected error during setup. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": None}

    try:
        variant_urns = db.scalars(
            select(Variant.urn)
            .join(MappedVariant)
            .join(ScoreSet)
            .where(
                ScoreSet.urn == score_set.urn, MappedVariant.current.is_(True), MappedVariant.post_mapped.is_not(None)
            )
        ).all()
        num_variant_urns = len(variant_urns)

        logging_context["variants_to_link_ldh"] = num_variant_urns

        if not variant_urns:
            logger.warning(
                msg="No current mapped variants with post mapped metadata were found for this score set. Skipping LDH linkage (nothing to do). A gnomAD linkage job will not be enqueued, as no variants will have a CAID.",
                extra=logging_context,
            )

            return {"success": True, "retried": False, "enqueued_job": None}

        logger.info(
            msg="Found current mapped variants with post mapped metadata for this score set. Attempting to link them to LDH submissions.",
            extra=logging_context,
        )

    except Exception as e:
        send_slack_error(e)
        send_slack_message(text=text % score_set.urn)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="LDH mapped resource linkage encountered an unexpected error while attempting to build linkage urn list. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": None}

    try:
        logger.info(msg="Attempting to link mapped variants to LDH submissions.", extra=logging_context)

        # TODO#372: Non-nullable variant urns.
        blocking = functools.partial(
            do_clingen_fetch,
            variant_urns,  # type: ignore
        )
        loop = asyncio.get_running_loop()
        linked_data = await loop.run_in_executor(ctx["pool"], blocking)

    except Exception as e:
        send_slack_error(e)
        send_slack_message(text=text % score_set.urn)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="LDH mapped resource linkage encountered an unexpected error while attempting to link LDH submissions. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": None}

    try:
        linked_allele_ids = [
            (variant_urn, clingen_allele_id_from_ldh_variation(clingen_variation))
            for variant_urn, clingen_variation in linked_data
        ]

        linkage_failures = []
        for variant_urn, ldh_variation in linked_allele_ids:
            # XXX: Should we unlink variation if it is not found? Does this constitute a failure?
            if not ldh_variation:
                logger.warning(
                    msg=f"Failed to link mapped variant {variant_urn} to LDH submission. No LDH variation found.",
                    extra=logging_context,
                )
                linkage_failures.append(variant_urn)
                continue

            mapped_variant = db.scalars(
                select(MappedVariant).join(Variant).where(Variant.urn == variant_urn, MappedVariant.current.is_(True))
            ).one_or_none()

            if not mapped_variant:
                logger.warning(
                    msg=f"Failed to link mapped variant {variant_urn} to LDH submission. No mapped variant found.",
                    extra=logging_context,
                )
                linkage_failures.append(variant_urn)
                continue

            mapped_variant.clingen_allele_id = ldh_variation
            db.add(mapped_variant)

        db.commit()

    except Exception as e:
        db.rollback()

        send_slack_error(e)
        send_slack_message(text=text % score_set.urn)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="LDH mapped resource linkage encountered an unexpected error while attempting to link LDH submissions. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": None}

    try:
        num_linkage_failures = len(linkage_failures)
        ratio_failed_linking = round(num_linkage_failures / num_variant_urns, 3)
        logging_context["linkage_failure_rate"] = ratio_failed_linking
        logging_context["linkage_failures"] = num_linkage_failures
        logging_context["linkage_successes"] = num_variant_urns - num_linkage_failures

        assert (
            len(linked_allele_ids) == num_variant_urns
        ), f"{num_variant_urns - len(linked_allele_ids)} appear to not have been attempted to be linked."

        job_succeeded = False
        if not linkage_failures:
            logger.info(
                msg="Successfully linked all mapped variants to LDH submissions.",
                extra=logging_context,
            )

            job_succeeded = True

        elif ratio_failed_linking < LINKED_DATA_RETRY_THRESHOLD:
            logger.warning(
                msg="Linkage failures exist, but did not exceed the retry threshold.",
                extra=logging_context,
            )
            send_slack_message(
                text=f"Failed to link {len(linkage_failures)} mapped variants to LDH submissions for score set {score_set.urn}."
                f"The retry threshold was not exceeded and this job will not be retried. URNs failed to link: {', '.join(linkage_failures)}."
            )

            job_succeeded = True

    except Exception as e:
        send_slack_error(e)
        send_slack_message(text=text % score_set.urn)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="LDH mapped resource linkage encountered an unexpected error while attempting to finalize linkage. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": None}

    if job_succeeded:
        gnomad_linking_job_id = None
        try:
            new_job = await redis.enqueue_job(
                "link_gnomad_variants",
                correlation_id,
                score_set.id,
            )

            if new_job:
                gnomad_linking_job_id = new_job.job_id

                logging_context["link_gnomad_variants_job_id"] = gnomad_linking_job_id
                logger.info(msg="Queued a new gnomAD linking job.", extra=logging_context)

            else:
                raise LinkingEnqueueError()

        except Exception as e:
            job_succeeded = False

            send_slack_error(e)
            send_slack_message(text=text % score_set.urn)
            logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
            logger.error(
                msg="LDH mapped resource linkage encountered an unexpected error while attempting to enqueue a gnomAD linking job. GnomAD variants should be linked manually for this score set. This job will not be retried.",
                extra=logging_context,
            )
        finally:
            return {"success": job_succeeded, "retried": False, "enqueued_job": gnomad_linking_job_id}

    # If we reach this point, we should consider the job failed (there were failures which exceeded our retry threshold).
    new_job_id = None
    max_retries_exceeded = None
    try:
        new_job_id, max_retries_exceeded, backoff_time = await enqueue_job_with_backoff(
            ctx["redis"], "variant_mapper_manager", attempt, LINKING_BACKOFF_IN_SECONDS, correlation_id
        )

        logging_context["backoff_limit_exceeded"] = max_retries_exceeded
        logging_context["backoff_deferred_in_seconds"] = backoff_time
        logging_context["backoff_job_id"] = new_job_id

    except Exception as e:
        send_slack_error(e)
        send_slack_message(text=text % score_set.urn)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.critical(
            msg="LDH mapped resource linkage encountered an unexpected error while attempting to retry a failed linkage job. This job will not be retried.",
            extra=logging_context,
        )
    else:
        if new_job_id and not max_retries_exceeded:
            logger.info(
                msg="After a failure condition while linking mapped variants to LDH submissions, another linkage job was queued.",
                extra=logging_context,
            )
            send_slack_message(
                text=f"Failed to link {len(linkage_failures)} ({ratio_failed_linking * 100}% of total mapped variants for {score_set.urn})."
                f"This job was successfully retried. This was attempt {attempt}. Retry will occur in {backoff_time} seconds. URNs failed to link: {', '.join(linkage_failures)}."
            )
        elif new_job_id is None and not max_retries_exceeded:
            logger.error(
                msg="After a failure condition while linking mapped variants to LDH submissions, another linkage job was unable to be queued.",
                extra=logging_context,
            )
            send_slack_message(
                text=f"Failed to link {len(linkage_failures)} ({ratio_failed_linking} of total mapped variants for {score_set.urn})."
                f"This job could not be retried due to an unexpected issue while attempting to enqueue another linkage job. This was attempt {attempt}. URNs failed to link: {', '.join(linkage_failures)}."
            )
        else:
            logger.error(
                msg="After a failure condition while linking mapped variants to LDH submissions, the maximum retries for this job were exceeded. The reamining linkage failures will not be retried.",
                extra=logging_context,
            )
            send_slack_message(
                text=f"Failed to link {len(linkage_failures)} ({ratio_failed_linking} of total mapped variants for {score_set.urn})."
                f"The retry threshold was exceeded and this job will not be retried. URNs failed to link: {', '.join(linkage_failures)}."
            )

    finally:
        return {
            "success": False,
            "retried": (not max_retries_exceeded and new_job_id is not None),
            "enqueued_job": new_job_id,
        }

"""Variant mapping jobs using VRS (Variant Representation Specification).

This module handles the mapping of variants to standardized genomic coordinates
using the VRS mapping service. It includes queue management, retry logic,
and coordination with downstream services like ClinGen and UniProt.
"""

import asyncio
import functools
import logging
from contextlib import asynccontextmanager
from datetime import date, timedelta
from typing import Any

from arq import ArqRedis
from arq.jobs import Job, JobStatus
from sqlalchemy import cast, null, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session

from mavedb.data_providers.services import vrs_mapper
from mavedb.lib.clingen.constants import CLIN_GEN_SUBMISSION_ENABLED
from mavedb.lib.exceptions import (
    MappingEnqueueError,
    NonexistentMappingReferenceError,
    NonexistentMappingResultsError,
    SubmissionEnqueueError,
    UniProtIDMappingEnqueueError,
)
from mavedb.lib.logging.context import format_raised_exception_info_as_dict
from mavedb.lib.mapping import ANNOTATION_LAYERS
from mavedb.lib.slack import send_slack_error, send_slack_message
from mavedb.lib.uniprot.constants import UNIPROT_ID_MAPPING_ENABLED
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.constants import MAPPING_BACKOFF_IN_SECONDS, MAPPING_CURRENT_ID_NAME, MAPPING_QUEUE_NAME
from mavedb.worker.jobs.utils.job_state import setup_job_state
from mavedb.worker.jobs.utils.retry import enqueue_job_with_backoff

logger = logging.getLogger(__name__)


@asynccontextmanager
async def mapping_in_execution(redis: ArqRedis, job_id: str):
    await redis.set(MAPPING_CURRENT_ID_NAME, job_id)
    try:
        yield
    finally:
        await redis.set(MAPPING_CURRENT_ID_NAME, "")


async def variant_mapper_manager(ctx: dict, correlation_id: str, updater_id: int, attempt: int = 1) -> dict:
    logging_context = {}
    mapping_job_id = None
    mapping_job_status = None
    queued_score_set = None
    try:
        redis: ArqRedis = ctx["redis"]
        db: Session = ctx["db"]

        logging_context = setup_job_state(ctx, updater_id, None, correlation_id)
        logging_context["attempt"] = attempt
        logger.debug(msg="Variant mapping manager began execution", extra=logging_context)

        queue_length = await redis.llen(MAPPING_QUEUE_NAME)  # type: ignore
        queued_id = await redis.rpop(MAPPING_QUEUE_NAME)  # type: ignore
        logging_context["variant_mapping_queue_length"] = queue_length

        # Setup the job id cache if it does not already exist.
        if not await redis.exists(MAPPING_CURRENT_ID_NAME):
            await redis.set(MAPPING_CURRENT_ID_NAME, "")

        if not queued_id:
            logger.debug(msg="No mapping jobs exist in the queue.", extra=logging_context)
            return {"success": True, "enqueued_job": None}
        else:
            queued_id = queued_id.decode("utf-8")
            queued_score_set = db.scalars(select(ScoreSet).where(ScoreSet.id == queued_id)).one()

            logging_context["upcoming_mapping_resource"] = queued_score_set.urn
            logger.debug(msg="Found mapping job(s) still in queue.", extra=logging_context)

        mapping_job_id = await redis.get(MAPPING_CURRENT_ID_NAME)
        if mapping_job_id:
            mapping_job_id = mapping_job_id.decode("utf-8")
            mapping_job_status = (await Job(job_id=mapping_job_id, redis=redis).status()).value

        logging_context["existing_mapping_job_status"] = mapping_job_status
        logging_context["existing_mapping_job_id"] = mapping_job_id

    except Exception as e:
        send_slack_error(e)

        # Attempt to remove this item from the mapping queue.
        try:
            await redis.lrem(MAPPING_QUEUE_NAME, 1, queued_id)  # type: ignore
            logger.warning(msg="Removed un-queueable score set from the queue.", extra=logging_context)
        except Exception:
            pass

        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(msg="Variant mapper manager encountered an unexpected error during setup.", extra=logging_context)

        return {"success": False, "enqueued_job": None}

    new_job = None
    new_job_id = None
    try:
        if not mapping_job_id or mapping_job_status in (JobStatus.not_found, JobStatus.complete):
            logger.debug(msg="No mapping jobs are running, queuing a new one.", extra=logging_context)

            new_job = await redis.enqueue_job(
                "map_variants_for_score_set", correlation_id, queued_score_set.id, updater_id, attempt
            )

        if new_job:
            new_job_id = new_job.job_id

            logging_context["new_mapping_job_id"] = new_job_id
            logger.info(msg="Queued a new mapping job.", extra=logging_context)

            return {"success": True, "enqueued_job": new_job_id}

        logger.info(
            msg="A mapping job is already running, or a new job was unable to be enqueued. Deferring mapping by 5 minutes.",
            extra=logging_context,
        )

        new_job = await redis.enqueue_job(
            "variant_mapper_manager",
            correlation_id,
            updater_id,
            attempt,
            _defer_by=timedelta(minutes=5),
        )

        if new_job:
            # Ensure this score set remains in the front of the queue.
            queued_id = await redis.rpush(MAPPING_QUEUE_NAME, queued_score_set.id)  # type: ignore
            new_job_id = new_job.job_id

            logging_context["new_mapping_manager_job_id"] = new_job_id
            logger.info(msg="Deferred a new mapping manager job.", extra=logging_context)

            # Our persistent Redis queue and ARQ's execution rules ensure that even if the worker is stopped and not restarted
            # before the deferred time, these deferred jobs will still run once able.
            return {"success": True, "enqueued_job": new_job_id}

        raise MappingEnqueueError()

    except Exception as e:
        send_slack_error(e)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="Variant mapper manager encountered an unexpected error while enqueing a mapping job. This job will not be retried.",
            extra=logging_context,
        )

        db.rollback()

        # We shouldn't rely on the passed score set id matching the score set we are operating upon.
        if not queued_score_set:
            return {"success": False, "enqueued_job": new_job_id}

        # Attempt to remove this item from the mapping queue.
        try:
            await redis.lrem(MAPPING_QUEUE_NAME, 1, queued_id)  # type: ignore
            logger.warning(msg="Removed un-queueable score set from the queue.", extra=logging_context)
        except Exception:
            pass

        score_set_exc = db.scalars(select(ScoreSet).where(ScoreSet.id == queued_score_set.id)).one_or_none()
        if score_set_exc:
            score_set_exc.mapping_state = MappingState.failed
            score_set_exc.mapping_errors = "Unable to queue a new mapping job or defer score set mapping."
            db.add(score_set_exc)
        db.commit()

        return {"success": False, "enqueued_job": new_job_id}


async def map_variants_for_score_set(
    ctx: dict, correlation_id: str, score_set_id: int, updater_id: int, attempt: int = 1
) -> dict:
    async with mapping_in_execution(redis=ctx["redis"], job_id=ctx["job_id"]):
        logging_context = {}
        score_set = None
        try:
            db: Session = ctx["db"]
            redis: ArqRedis = ctx["redis"]
            score_set = db.scalars(select(ScoreSet).where(ScoreSet.id == score_set_id)).one()

            logging_context = setup_job_state(ctx, updater_id, score_set.urn, correlation_id)
            logging_context["attempt"] = attempt
            logger.info(msg="Started variant mapping", extra=logging_context)

            score_set.mapping_state = MappingState.processing
            score_set.mapping_errors = null()
            db.add(score_set)
            db.commit()

            mapping_urn = score_set.urn
            assert mapping_urn, "A valid URN is needed to map this score set."

            logging_context["current_mapping_resource"] = mapping_urn
            logging_context["mapping_state"] = score_set.mapping_state
            logger.debug(msg="Fetched score set metadata for mapping job.", extra=logging_context)

            # Do not block Worker event loop during mapping, see: https://arq-docs.helpmanual.io/#synchronous-jobs.
            vrs = vrs_mapper()
            blocking = functools.partial(vrs.map_score_set, mapping_urn)
            loop = asyncio.get_running_loop()

        except Exception as e:
            send_slack_error(e)
            logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
            logger.error(
                msg="Variant mapper encountered an unexpected error during setup. This job will not be retried.",
                extra=logging_context,
            )

            db.rollback()
            if score_set:
                score_set.mapping_state = MappingState.failed
                score_set.mapping_errors = {"error_message": "Encountered an internal server error during mapping"}
                db.add(score_set)
            db.commit()

            return {"success": False, "retried": False, "enqueued_jobs": []}

        mapping_results = None
        try:
            mapping_results = await loop.run_in_executor(ctx["pool"], blocking)
            logger.debug(msg="Done mapping variants.", extra=logging_context)

        except Exception as e:
            db.rollback()
            score_set.mapping_errors = {
                "error_message": f"Encountered an internal server error during mapping. Mapping will be automatically retried up to 5 times for this score set (attempt {attempt}/5)."
            }
            db.add(score_set)
            db.commit()

            send_slack_error(e)
            logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
            logger.warning(
                msg="Variant mapper encountered an unexpected error while mapping variants. This job will be retried.",
                extra=logging_context,
            )

            new_job_id = None
            max_retries_exceeded = None
            try:
                await redis.lpush(MAPPING_QUEUE_NAME, score_set.id)  # type: ignore
                new_job_id, max_retries_exceeded, backoff_time = await enqueue_job_with_backoff(
                    redis, "variant_mapper_manager", attempt, MAPPING_BACKOFF_IN_SECONDS, correlation_id, updater_id
                )
                # If we fail to enqueue a mapping manager for this score set, evict it from the queue.
                if new_job_id is None:
                    await redis.lpop(MAPPING_QUEUE_NAME, score_set.id)  # type: ignore

                logging_context["backoff_limit_exceeded"] = max_retries_exceeded
                logging_context["backoff_deferred_in_seconds"] = backoff_time
                logging_context["backoff_job_id"] = new_job_id

            except Exception as backoff_e:
                score_set.mapping_state = MappingState.failed
                score_set.mapping_errors = {"error_message": "Encountered an internal server error during mapping"}
                db.add(score_set)
                db.commit()
                send_slack_error(backoff_e)
                logging_context = {**logging_context, **format_raised_exception_info_as_dict(backoff_e)}
                logger.critical(
                    msg="While attempting to re-enqueue a mapping job that exited in error, another exception was encountered. This score set will not be mapped.",
                    extra=logging_context,
                )
            else:
                if new_job_id and not max_retries_exceeded:
                    score_set.mapping_state = MappingState.queued
                    db.add(score_set)
                    db.commit()
                    logger.info(
                        msg="After encountering an error while mapping variants, another mapping job was queued.",
                        extra=logging_context,
                    )
                elif new_job_id is None and not max_retries_exceeded:
                    score_set.mapping_state = MappingState.failed
                    score_set.mapping_errors = {"error_message": "Encountered an internal server error during mapping"}
                    db.add(score_set)
                    db.commit()
                    logger.error(
                        msg="After encountering an error while mapping variants, another mapping job was unable to be queued. This score set will not be mapped.",
                        extra=logging_context,
                    )
                else:
                    score_set.mapping_state = MappingState.failed
                    score_set.mapping_errors = {"error_message": "Encountered an internal server error during mapping"}
                    db.add(score_set)
                    db.commit()
                    logger.error(
                        msg="After encountering an error while mapping variants, the maximum retries for this job were exceeded. This score set will not be mapped.",
                        extra=logging_context,
                    )
            finally:
                return {
                    "success": False,
                    "retried": (not max_retries_exceeded and new_job_id is not None),
                    "enqueued_jobs": [job for job in [new_job_id] if job],
                }

        try:
            if mapping_results:
                mapped_scores = mapping_results.get("mapped_scores")
                if not mapped_scores:
                    # if there are no mapped scores, the score set failed to map.
                    score_set.mapping_state = MappingState.failed
                    score_set.mapping_errors = {"error_message": mapping_results.get("error_message")}
                else:
                    reference_metadata = mapping_results.get("reference_sequences")
                    if not reference_metadata:
                        raise NonexistentMappingReferenceError()

                    for target_gene_identifier in reference_metadata:
                        target_gene = next(
                            (
                                target_gene
                                for target_gene in score_set.target_genes
                                if target_gene.name == target_gene_identifier
                            ),
                            None,
                        )
                        if not target_gene:
                            raise ValueError(
                                f"Target gene {target_gene_identifier} not found in database for score set {score_set.urn}."
                            )
                        # allow for multiple annotation layers
                        pre_mapped_metadata: dict[str, Any] = {}
                        post_mapped_metadata: dict[str, Any] = {}
                        excluded_pre_mapped_keys = {"sequence"}

                        gene_info = reference_metadata[target_gene_identifier].get("gene_info")
                        if gene_info:
                            target_gene.mapped_hgnc_name = gene_info.get("hgnc_symbol")
                            post_mapped_metadata["hgnc_name_selection_method"] = gene_info.get("selection_method")

                        for annotation_layer in reference_metadata[target_gene_identifier]["layers"]:
                            layer_premapped = reference_metadata[target_gene_identifier]["layers"][
                                annotation_layer
                            ].get("computed_reference_sequence")
                            if layer_premapped:
                                pre_mapped_metadata[ANNOTATION_LAYERS[annotation_layer]] = {
                                    k: layer_premapped[k]
                                    for k in set(list(layer_premapped.keys())) - excluded_pre_mapped_keys
                                }
                            layer_postmapped = reference_metadata[target_gene_identifier]["layers"][
                                annotation_layer
                            ].get("mapped_reference_sequence")
                            if layer_postmapped:
                                post_mapped_metadata[ANNOTATION_LAYERS[annotation_layer]] = layer_postmapped
                        target_gene.pre_mapped_metadata = cast(pre_mapped_metadata, JSONB)
                        target_gene.post_mapped_metadata = cast(post_mapped_metadata, JSONB)

                    total_variants = 0
                    successful_mapped_variants = 0
                    for mapped_score in mapped_scores:
                        total_variants += 1
                        variant_urn = mapped_score.get("mavedb_id")
                        variant = db.scalars(select(Variant).where(Variant.urn == variant_urn)).one()

                        # there should only be one current mapped variant per variant id, so update old mapped variant to current = false
                        existing_mapped_variant = (
                            db.query(MappedVariant)
                            .filter(MappedVariant.variant_id == variant.id, MappedVariant.current.is_(True))
                            .one_or_none()
                        )

                        if existing_mapped_variant:
                            existing_mapped_variant.current = False
                            db.add(existing_mapped_variant)

                        if mapped_score.get("pre_mapped") and mapped_score.get("post_mapped"):
                            successful_mapped_variants += 1

                        mapped_variant = MappedVariant(
                            pre_mapped=mapped_score.get("pre_mapped", null()),
                            post_mapped=mapped_score.get("post_mapped", null()),
                            variant_id=variant.id,
                            modification_date=date.today(),
                            mapped_date=mapping_results["mapped_date_utc"],
                            vrs_version=mapped_score.get("vrs_version", null()),
                            mapping_api_version=mapping_results["dcd_mapping_version"],
                            error_message=mapped_score.get("error_message", null()),
                            current=True,
                        )
                        db.add(mapped_variant)

                    if successful_mapped_variants == 0:
                        score_set.mapping_state = MappingState.failed
                        score_set.mapping_errors = {"error_message": "All variants failed to map"}
                    elif successful_mapped_variants < total_variants:
                        score_set.mapping_state = MappingState.incomplete
                    else:
                        score_set.mapping_state = MappingState.complete

                    logging_context["mapped_variants_inserted_db"] = len(mapped_scores)
                    logging_context["variants_successfully_mapped"] = successful_mapped_variants
                    logging_context["mapping_state"] = score_set.mapping_state.name
                    logging_context["mapping_errors"] = score_set.mapping_errors
                    logger.info(msg="Inserted mapped variants into db.", extra=logging_context)

            else:
                raise NonexistentMappingResultsError()

            db.add(score_set)
            db.commit()

        except Exception as e:
            db.rollback()
            score_set.mapping_errors = {
                "error_message": f"Encountered an unexpected error while parsing mapped variants. Mapping will be automatically retried up to 5 times for this score set (attempt {attempt}/5)."
            }
            db.add(score_set)
            db.commit()

            send_slack_error(e)
            logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
            logger.warning(
                msg="An unexpected error occurred during variant mapping. This job will be attempted again.",
                extra=logging_context,
            )

            new_job_id = None
            max_retries_exceeded = None
            try:
                await redis.lpush(MAPPING_QUEUE_NAME, score_set.id)  # type: ignore
                new_job_id, max_retries_exceeded, backoff_time = await enqueue_job_with_backoff(
                    redis, "variant_mapper_manager", attempt, MAPPING_BACKOFF_IN_SECONDS, correlation_id, updater_id
                )
                # If we fail to enqueue a mapping manager for this score set, evict it from the queue.
                if new_job_id is None:
                    await redis.lpop(MAPPING_QUEUE_NAME, score_set.id)  # type: ignore

                logging_context["backoff_limit_exceeded"] = max_retries_exceeded
                logging_context["backoff_deferred_in_seconds"] = backoff_time
                logging_context["backoff_job_id"] = new_job_id

            except Exception as backoff_e:
                score_set.mapping_state = MappingState.failed
                score_set.mapping_errors = {"error_message": "Encountered an internal server error during mapping"}
                send_slack_error(backoff_e)
                logging_context = {**logging_context, **format_raised_exception_info_as_dict(backoff_e)}
                logger.critical(
                    msg="While attempting to re-enqueue a mapping job that exited in error, another exception was encountered. This score set will not be mapped.",
                    extra=logging_context,
                )
            else:
                if new_job_id and not max_retries_exceeded:
                    score_set.mapping_state = MappingState.queued
                    logger.info(
                        msg="After encountering an error while parsing mapped variants, another mapping job was queued.",
                        extra=logging_context,
                    )
                elif new_job_id is None and not max_retries_exceeded:
                    score_set.mapping_state = MappingState.failed
                    score_set.mapping_errors = {"error_message": "Encountered an internal server error during mapping"}
                    logger.error(
                        msg="After encountering an error while parsing mapped variants, another mapping job was unable to be queued. This score set will not be mapped.",
                        extra=logging_context,
                    )
                else:
                    score_set.mapping_state = MappingState.failed
                    score_set.mapping_errors = {"error_message": "Encountered an internal server error during mapping"}
                    logger.error(
                        msg="After encountering an error while parsing mapped variants, the maximum retries for this job were exceeded. This score set will not be mapped.",
                        extra=logging_context,
                    )
            finally:
                db.add(score_set)
                db.commit()
                return {
                    "success": False,
                    "retried": (not max_retries_exceeded and new_job_id is not None),
                    "enqueued_jobs": [job for job in [new_job_id] if job],
                }

    new_uniprot_job_id = None
    try:
        if UNIPROT_ID_MAPPING_ENABLED:
            new_job = await redis.enqueue_job(
                "submit_uniprot_mapping_jobs_for_score_set",
                score_set.id,
                correlation_id,
            )

            if new_job:
                new_uniprot_job_id = new_job.job_id

                logging_context["submit_uniprot_mapping_job_id"] = new_uniprot_job_id
                logger.info(msg="Queued a new UniProt mapping job.", extra=logging_context)

            else:
                raise UniProtIDMappingEnqueueError()
        else:
            logger.warning(
                msg="UniProt ID mapping is disabled, skipped submission of UniProt mapping jobs.",
                extra=logging_context,
            )

    except Exception as e:
        send_slack_error(e)
        send_slack_message(
            f"Could not enqueue UniProt mapping job for score set {score_set.urn}. UniProt mappings for this score set should be submitted manually."
        )
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="Mapped variant UniProt submission encountered an unexpected error while attempting to enqueue a mapping job. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_jobs": [job for job in [new_uniprot_job_id] if job]}

    new_clingen_job_id = None
    try:
        if CLIN_GEN_SUBMISSION_ENABLED:
            new_job = await redis.enqueue_job(
                "submit_score_set_mappings_to_car",
                correlation_id,
                score_set.id,
            )

            if new_job:
                new_clingen_job_id = new_job.job_id

                logging_context["submit_clingen_variants_job_id"] = new_clingen_job_id
                logger.info(msg="Queued a new ClinGen submission job.", extra=logging_context)

            else:
                raise SubmissionEnqueueError()
        else:
            logger.warning(
                msg="ClinGen submission is disabled, skipped submission of mapped variants to CAR and LDH.",
                extra=logging_context,
            )

    except Exception as e:
        send_slack_error(e)
        send_slack_message(
            f"Could not submit mappings to CAR and/or LDH mappings for score set {score_set.urn}. Mappings for this score set should be submitted manually."
        )
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="Mapped variant ClinGen submission encountered an unexpected error while attempting to enqueue a submission job. This job will not be retried.",
            extra=logging_context,
        )

        return {
            "success": False,
            "retried": False,
            "enqueued_jobs": [job for job in [new_uniprot_job_id, new_clingen_job_id] if job],
        }

    ctx["state"][ctx["job_id"]] = logging_context.copy()
    return {
        "success": True,
        "retried": False,
        "enqueued_jobs": [job for job in [new_uniprot_job_id, new_clingen_job_id] if job],
    }

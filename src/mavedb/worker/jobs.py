import asyncio
import functools
import logging
from contextlib import asynccontextmanager
from datetime import date, timedelta
from typing import Any, Optional

import pandas as pd
from arq import ArqRedis
from arq.jobs import Job, JobStatus
from cdot.hgvs.dataproviders import RESTDataProvider
from sqlalchemy import cast, delete, null, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session

from mavedb.data_providers.services import vrs_mapper
from mavedb.db.view import refresh_all_mat_views
from mavedb.lib.clingen.constants import (
    DEFAULT_LDH_SUBMISSION_BATCH_SIZE,
    LDH_SUBMISSION_ENDPOINT,
    LINKED_DATA_RETRY_THRESHOLD,
    CLIN_GEN_SUBMISSION_ENABLED,
)
from mavedb.lib.clingen.content_constructors import construct_ldh_submission
from mavedb.lib.clingen.linked_data_hub import (
    ClinGenLdhService,
    get_clingen_variation,
    clingen_allele_id_from_ldh_variation,
)
from mavedb.lib.exceptions import (
    MappingEnqueueError,
    SubmissionEnqueueError,
    LinkingEnqueueError,
    NonexistentMappingReferenceError,
    NonexistentMappingResultsError,
)
from mavedb.lib.logging.context import format_raised_exception_info_as_dict
from mavedb.lib.mapping import ANNOTATION_LAYERS
from mavedb.lib.score_sets import (
    columns_for_dataset,
    create_variants,
    create_variants_data,
)
from mavedb.lib.slack import send_slack_error, send_slack_message
from mavedb.lib.validation.dataframe.dataframe import (
    validate_and_standardize_dataframe_pair,
)
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.variants import hgvs_from_mapped_variant
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.published_variant import PublishedVariantsMV
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.models.variant import Variant

logger = logging.getLogger(__name__)

MAPPING_QUEUE_NAME = "vrs_mapping_queue"
MAPPING_CURRENT_ID_NAME = "vrs_mapping_current_job_id"
BACKOFF_LIMIT = 5
MAPPING_BACKOFF_IN_SECONDS = 15
LINKING_BACKOFF_IN_SECONDS = 15 * 60


####################################################################################################
#  Job utilities
####################################################################################################


def setup_job_state(
    ctx, invoker: Optional[int], resource: Optional[str], correlation_id: Optional[str]
) -> dict[str, Any]:
    ctx["state"][ctx["job_id"]] = {
        "application": "mavedb-worker",
        "user": invoker,
        "resource": resource,
        "correlation_id": correlation_id,
    }
    return ctx["state"][ctx["job_id"]]


async def enqueue_job_with_backoff(
    redis: ArqRedis, job_name: str, attempt: int, backoff: int, *args
) -> tuple[Optional[str], bool, Any]:
    new_job_id = None
    limit_reached = attempt > BACKOFF_LIMIT
    if not limit_reached:
        limit_reached = True
        backoff = backoff * (2**attempt)
        attempt = attempt + 1

        # NOTE: for jobs supporting backoff, `attempt` should be the final argument.
        new_job = await redis.enqueue_job(
            job_name,
            *args,
            attempt,
            _defer_by=timedelta(seconds=backoff),
        )

        if new_job:
            new_job_id = new_job.job_id

    return (new_job_id, not limit_reached, backoff)


####################################################################################################
#  Creating variants
####################################################################################################


async def create_variants_for_score_set(
    ctx, correlation_id: str, score_set_id: int, updater_id: int, scores: pd.DataFrame, counts: pd.DataFrame
):
    """
    Create variants for a score set. Intended to be run within a worker.
    On any raised exception, ensure ProcessingState of score set is set to `failed` prior
    to exiting.
    """
    logging_context = {}
    try:
        db: Session = ctx["db"]
        hdp: RESTDataProvider = ctx["hdp"]
        redis: ArqRedis = ctx["redis"]
        score_set = db.scalars(select(ScoreSet).where(ScoreSet.id == score_set_id)).one()

        logging_context = setup_job_state(ctx, updater_id, score_set.urn, correlation_id)
        logger.info(msg="Began processing of score set variants.", extra=logging_context)

        updated_by = db.scalars(select(User).where(User.id == updater_id)).one()

        score_set.modified_by = updated_by
        score_set.processing_state = ProcessingState.processing
        score_set.mapping_state = MappingState.pending_variant_processing
        logging_context["processing_state"] = score_set.processing_state.name
        logging_context["mapping_state"] = score_set.mapping_state.name

        db.add(score_set)
        db.commit()
        db.refresh(score_set)

        if not score_set.target_genes:
            logger.warning(
                msg="No targets are associated with this score set; could not create variants.",
                extra=logging_context,
            )
            raise ValueError("Can't create variants when score set has no targets.")

        validated_scores, validated_counts = validate_and_standardize_dataframe_pair(
            scores, counts, score_set.target_genes, hdp
        )

        score_set.dataset_columns = {
            "score_columns": columns_for_dataset(validated_scores),
            "count_columns": columns_for_dataset(validated_counts),
        }

        # Delete variants after validation occurs so we don't overwrite them in the case of a bad update.
        if score_set.variants:
            existing_variants = db.scalars(select(Variant.id).where(Variant.score_set_id == score_set.id)).all()
            db.execute(delete(MappedVariant).where(MappedVariant.variant_id.in_(existing_variants)))
            db.execute(delete(Variant).where(Variant.id.in_(existing_variants)))
            logging_context["deleted_variants"] = score_set.num_variants
            score_set.num_variants = 0

            logger.info(msg="Deleted existing variants from score set.", extra=logging_context)

            db.commit()
            db.refresh(score_set)

        variants_data = create_variants_data(validated_scores, validated_counts, None)
        create_variants(db, score_set, variants_data)

    # Validation errors arise from problematic user data. These should be inserted into the database so failures can
    # be persisted to them.
    except ValidationError as e:
        db.rollback()
        score_set.processing_state = ProcessingState.failed
        score_set.processing_errors = {"exception": str(e), "detail": e.triggering_exceptions}
        score_set.mapping_state = MappingState.not_attempted

        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logging_context["processing_state"] = score_set.processing_state.name
        logging_context["mapping_state"] = score_set.mapping_state.name
        logging_context["created_variants"] = 0
        logger.warning(msg="Encountered a validation error while processing variants.", extra=logging_context)

        return {"success": False}

    # NOTE: Since these are likely to be internal errors, it makes less sense to add them to the DB and surface them to the end user.
    # Catch all non-system exiting exceptions.
    except Exception as e:
        db.rollback()
        score_set.processing_state = ProcessingState.failed
        score_set.processing_errors = {"exception": str(e), "detail": []}
        score_set.mapping_state = MappingState.not_attempted

        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logging_context["processing_state"] = score_set.processing_state.name
        logging_context["mapping_state"] = score_set.mapping_state.name
        logging_context["created_variants"] = 0
        logger.warning(msg="Encountered an internal exception while processing variants.", extra=logging_context)

        send_slack_error(err=e)
        return {"success": False}

    # Catch all other exceptions. The exceptions caught here were intented to be system exiting.
    except BaseException as e:
        db.rollback()
        score_set.processing_state = ProcessingState.failed
        score_set.mapping_state = MappingState.not_attempted
        db.commit()

        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logging_context["processing_state"] = score_set.processing_state.name
        logging_context["mapping_state"] = score_set.mapping_state.name
        logging_context["created_variants"] = 0
        logger.error(
            msg="Encountered an unhandled exception while creating variants for score set.", extra=logging_context
        )

        # Don't raise BaseExceptions so we may emit canonical logs (TODO: Perhaps they are so problematic we want to raise them anyway).
        return {"success": False}

    else:
        score_set.processing_state = ProcessingState.success
        score_set.processing_errors = null()

        logging_context["created_variants"] = score_set.num_variants
        logging_context["processing_state"] = score_set.processing_state.name
        logger.info(msg="Finished creating variants in score set.", extra=logging_context)

        await redis.lpush(MAPPING_QUEUE_NAME, score_set.id)  # type: ignore
        await redis.enqueue_job("variant_mapper_manager", correlation_id, updater_id)
        score_set.mapping_state = MappingState.queued
    finally:
        db.add(score_set)
        db.commit()
        db.refresh(score_set)
        logger.info(msg="Committed new variants to score set.", extra=logging_context)

    ctx["state"][ctx["job_id"]] = logging_context.copy()
    return {"success": True}


####################################################################################################
#  Mapping variants
####################################################################################################


@asynccontextmanager
async def mapping_in_execution(redis: ArqRedis, job_id: str):
    await redis.set(MAPPING_CURRENT_ID_NAME, job_id)
    try:
        yield
    finally:
        await redis.set(MAPPING_CURRENT_ID_NAME, "")


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

            return {"success": False, "retried": False}

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
                return {"success": False, "retried": (not max_retries_exceeded and new_job_id is not None)}

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
                        pre_mapped_metadata = {}
                        post_mapped_metadata = {}
                        excluded_pre_mapped_keys = {"sequence"}
                        for annotation_layer in reference_metadata[target_gene_identifier]:
                            layer_premapped = reference_metadata[target_gene_identifier][annotation_layer].get(
                                "computed_reference_sequence"
                            )
                            if layer_premapped:
                                pre_mapped_metadata[ANNOTATION_LAYERS[annotation_layer]] = {
                                    k: layer_premapped[k]
                                    for k in set(list(layer_premapped.keys())) - excluded_pre_mapped_keys
                                }
                            layer_postmapped = reference_metadata[target_gene_identifier][annotation_layer].get(
                                "mapped_reference_sequence"
                            )
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
                return {"success": False, "retried": (not max_retries_exceeded and new_job_id is not None)}

    new_job_id = None
    try:
        if CLIN_GEN_SUBMISSION_ENABLED:
            new_job = await redis.enqueue_job(
                "submit_score_set_mappings_to_ldh",
                correlation_id,
                score_set.id,
            )

            if new_job:
                new_job_id = new_job.job_id

                logging_context["submit_clingen_variants_job_id"] = new_job_id
                logger.info(msg="Queued a new ClinGen submission job.", extra=logging_context)

            else:
                raise SubmissionEnqueueError()
        else:
            logger.warning(
                msg="ClinGen submission is disabled, skipped submission of mapped variants to LDH.",
                extra=logging_context,
            )

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


####################################################################################################
#  Materialized Views
####################################################################################################


# TODO#405: Refresh materialized views within an executor.
async def refresh_materialized_views(ctx: dict):
    logging_context = setup_job_state(ctx, None, None, None)
    logger.debug(msg="Began refresh materialized views.", extra=logging_context)
    refresh_all_mat_views(ctx["db"])
    logger.debug(msg="Done refreshing materialized views.", extra=logging_context)
    return {"success": True}


async def refresh_published_variants_view(ctx: dict, correlation_id: str):
    logging_context = setup_job_state(ctx, None, None, correlation_id)
    logger.debug(msg="Began refresh of published variants materialized view.", extra=logging_context)
    PublishedVariantsMV.refresh(ctx["db"])
    logger.debug(msg="Done refreshing of published variants materialized view.", extra=logging_context)
    return {"success": True}


####################################################################################################
#  ClinGen resource creation / linkage
####################################################################################################


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
            for variation in hgvs_from_mapped_variant(mapped_variant):
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
        score_set = db.scalars(select(ScoreSet).where(ScoreSet.id == score_set_id)).one()

        logging_context = setup_job_state(ctx, None, score_set.urn, correlation_id)
        logging_context["linkage_retry_threshold"] = LINKED_DATA_RETRY_THRESHOLD
        logging_context["attempt"] = attempt
        logging_context["max_attempts"] = BACKOFF_LIMIT
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

        logging_context["variants_to_link_ldh"] = submission_urn

        if not variant_urns:
            logger.warning(
                msg="No current mapped variants with post mapped metadata were found for this score set. Skipping LDH linkage (nothing to do).",
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

        if not linkage_failures:
            logger.info(
                msg="Successfully linked all mapped variants to LDH submissions.",
                extra=logging_context,
            )
            return {"success": True, "retried": False, "enqueued_job": None}

        if ratio_failed_linking < LINKED_DATA_RETRY_THRESHOLD:
            logger.warning(
                msg="Linkage failures exist, but did not exceed the retry threshold.",
                extra=logging_context,
            )
            send_slack_message(
                text=f"Failed to link {len(linkage_failures)} mapped variants to LDH submissions for score set {score_set.urn}."
                f"The retry threshold was not exceeded and this job will not be retried. URNs failed to link: {', '.join(linkage_failures)}."
            )
            return {"success": True, "retried": False, "enqueued_job": None}

    except Exception as e:
        send_slack_error(e)
        send_slack_message(text=text % score_set.urn)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="LDH mapped resource linkage encountered an unexpected error while attempting to finalize linkage. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": None}

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
                text=f"Failed to link {len(linkage_failures)} ({ratio_failed_linking*100}% of total mapped variants for {score_set.urn})."
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

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
from mavedb.lib.exceptions import MappingEnqueueError, NonexistentMappingReferenceError, NonexistentMappingResultsError
from mavedb.lib.logging.context import format_raised_exception_info_as_dict
from mavedb.lib.score_sets import (
    columns_for_dataset,
    create_variants,
    create_variants_data,
)
from mavedb.lib.slack import send_slack_message
from mavedb.lib.validation.dataframe import (
    validate_and_standardize_dataframe_pair,
)
from mavedb.lib.validation.exceptions import ValidationError
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
BACKOFF_IN_SECONDS = 15


@asynccontextmanager
async def mapping_in_execution(redis: ArqRedis, job_id: str):
    await redis.set(MAPPING_CURRENT_ID_NAME, job_id)
    try:
        yield
    finally:
        await redis.set(MAPPING_CURRENT_ID_NAME, "")


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
    redis: ArqRedis, job_name: str, attempt: int, *args
) -> tuple[Optional[str], bool, Any]:
    new_job_id = None
    backoff = None
    limit_reached = attempt > BACKOFF_LIMIT
    if not limit_reached:
        limit_reached = True
        backoff = BACKOFF_IN_SECONDS * (2**attempt)
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

        send_slack_message(err=e)
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
            send_slack_message(e)
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

            send_slack_message(e)
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
                    redis, "variant_mapper_manager", attempt, correlation_id, updater_id
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
                send_slack_message(backoff_e)
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
                    # TODO(VariantEffect/dcd-mapping2#2) after adding multi target mapping support:
                    # this assumes single-target mapping, will need to be changed to support multi-target mapping
                    # just in case there are multiple target genes in the db for a score set (this point shouldn't be reached
                    # while we only support single-target mapping), match up the target sequence with the one in the computed genomic reference sequence.
                    # TODO(VariantEffect/dcd-mapping2#3) after adding accession-based score set mapping support:
                    # this also assumes that the score set is based on a target sequence, not a target accession

                    computed_genomic_ref = mapping_results.get("computed_genomic_reference_sequence")
                    mapped_genomic_ref = mapping_results.get("mapped_genomic_reference_sequence")
                    computed_protein_ref = mapping_results.get("computed_protein_reference_sequence")
                    mapped_protein_ref = mapping_results.get("mapped_protein_reference_sequence")

                    if computed_genomic_ref:
                        target_sequence = computed_genomic_ref["sequence"]  # noqa: F841
                    elif computed_protein_ref:
                        target_sequence = computed_protein_ref["sequence"]  # noqa: F841
                    else:
                        raise NonexistentMappingReferenceError()

                    # TODO(VariantEffect/dcd_mapping2#2): Handle variant mappings for score sets with more than 1 target.
                    target_gene = score_set.target_genes[0]

                    excluded_pre_mapped_keys = {"sequence"}
                    if computed_genomic_ref and mapped_genomic_ref:
                        pre_mapped_metadata = computed_genomic_ref
                        target_gene.pre_mapped_metadata = cast(
                            {
                                "genomic": {
                                    k: pre_mapped_metadata[k]
                                    for k in set(list(pre_mapped_metadata.keys())) - excluded_pre_mapped_keys
                                }
                            },
                            JSONB,
                        )
                        target_gene.post_mapped_metadata = cast({"genomic": mapped_genomic_ref}, JSONB)
                    elif computed_protein_ref and mapped_protein_ref:
                        pre_mapped_metadata = computed_protein_ref
                        target_gene.pre_mapped_metadata = cast(
                            {
                                "protein": {
                                    k: pre_mapped_metadata[k]
                                    for k in set(list(pre_mapped_metadata.keys())) - excluded_pre_mapped_keys
                                }
                            },
                            JSONB,
                        )
                        target_gene.post_mapped_metadata = cast({"protein": mapped_protein_ref}, JSONB)
                    else:
                        raise NonexistentMappingReferenceError()

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

            send_slack_message(e)
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
                    redis, "variant_mapper_manager", attempt, correlation_id, updater_id
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
                send_slack_message(backoff_e)
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

    ctx["state"][ctx["job_id"]] = logging_context.copy()
    return {"success": True, "retried": False}


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
        send_slack_message(e)

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
        send_slack_message(e)
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

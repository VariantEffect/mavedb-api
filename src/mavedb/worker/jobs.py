import asyncio
import functools
import logging
import requests
from datetime import timedelta, date
from typing import Optional

import pandas as pd
from arq import ArqRedis
from arq.jobs import Job, JobStatus
from cdot.hgvs.dataproviders import RESTDataProvider
from sqlalchemy import cast, delete, select, null
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session

from mavedb.lib.score_sets import (
    columns_for_dataset,
    create_variants,
    create_variants_data,
)
from mavedb.lib.logging.context import format_raised_exception_info_as_dict
from mavedb.lib.slack import send_slack_message
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.dataframe import (
    validate_and_standardize_dataframe_pair,
)
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_sequence import TargetSequence
from mavedb.models.user import User
from mavedb.models.variant import Variant
from mavedb.data_providers.services import vrs_mapper

logger = logging.getLogger(__name__)

MAPPING_QUEUE_NAME = "vrs_mapping_queue"
MAPPING_CURRENT_ID_NAME = "vrs_mapping_current_job_id"

def setup_job_state(ctx, invoker: int, resource: str, correlation_id: str):
    ctx["state"][ctx["job_id"]] = {
        "application": "mavedb-worker",
        "user": invoker,
        "resource": resource,
        "correlation_id": correlation_id,
    }
    return ctx["state"][ctx["job_id"]]


async def create_variants_for_score_set(
    ctx, correlation_id: str, score_set_urn: str, updater_id: int, scores: pd.DataFrame, counts: pd.DataFrame
):
    """
    Create variants for a score set. Intended to be run within a worker.
    On any raised exception, ensure ProcessingState of score set is set to `failed` prior
    to exiting.
    """
    logging_context = {}
    try:
        logging_context = setup_job_state(ctx, updater_id, score_set_urn, correlation_id)
        logger.info(msg="Began processing of score set variants.", extra=logging_context)

        db: Session = ctx["db"]
        hdp: RESTDataProvider = ctx["hdp"]
        redis: ArqRedis = ctx["redis"]

        score_set = db.scalars(select(ScoreSet).where(ScoreSet.urn == score_set_urn)).one()
        updated_by = db.scalars(select(User).where(User.id == updater_id)).one()

        score_set.modified_by = updated_by
        score_set.processing_state = ProcessingState.processing
        logging_context["processing_state"] = score_set.processing_state.name

        db.add(score_set)
        db.commit()
        db.refresh(score_set)

        if not score_set.target_genes:
            logger.warning(
                msg="No targets are associated with this score set; could not create variants.",
                extra=logging_context,
            )
            raise ValueError("Can't create variants when score set has no targets.")

        if score_set.variants:
            db.execute(delete(Variant).where(Variant.score_set_id == score_set.id))
            logging_context["deleted_variants"] = score_set.num_variants
            score_set.num_variants = 0

            logger.info(msg="Deleted existing variants from score set.", extra=logging_context)

            db.commit()
            db.refresh(score_set)

        validated_scores, validated_counts = validate_and_standardize_dataframe_pair(
            scores, counts, score_set.target_genes, hdp
        )

        score_set.dataset_columns = {
            "score_columns": columns_for_dataset(validated_scores),
            "count_columns": columns_for_dataset(validated_counts),
        }

        variants_data = create_variants_data(validated_scores, validated_counts, None)
        create_variants(db, score_set, variants_data)

    # Validation errors arise from problematic user data. These should be inserted into the database so failures can
    # be persisted to them.
    except ValidationError as e:
        db.rollback()
        score_set.processing_state = ProcessingState.failed
        score_set.processing_errors = {"exception": str(e), "detail": e.triggering_exceptions}

        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logging_context["processing_state"] = score_set.processing_state.name
        logger.warning(msg="Encountered a validation error while processing variants.", extra=logging_context)

    # NOTE: Since these are likely to be internal errors, it makes less sense to add them to the DB and surface them to the end user.
    # Catch all non-system exiting exceptions.
    except Exception as e:
        db.rollback()
        score_set.processing_state = ProcessingState.failed
        score_set.processing_errors = {"exception": str(e), "detail": []}

        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logging_context["processing_state"] = score_set.processing_state.name
        logger.warning(msg="Encountered an internal exception while processing variants.", extra=logging_context)

        send_slack_message(err=e)

    # Catch all other exceptions and raise them. The exceptions caught here will be system exiting.
    except BaseException as e:
        db.rollback()
        score_set.processing_state = ProcessingState.failed
        db.commit()

        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logging_context["processing_state"] = score_set.processing_state.name
        logger.error(
            msg="Encountered an unhandled exception while creating variants for score set.", extra=logging_context
        )

        raise e

    else:
        score_set.processing_state = ProcessingState.success
        score_set.processing_errors = null()

        logging_context["created_variants"] = score_set.num_variants
        logging_context["processing_state"] = score_set.processing_state.name
        logger.info(msg="Finished creating variants in score set.", extra=logging_context)

        await redis.lpush(MAPPING_QUEUE_NAME, score_set_urn)  # type: ignore
        await redis.enqueue_job("variant_mapper_manager", correlation_id, score_set_urn, updater_id)
    finally:
        db.add(score_set)
        db.commit()
        db.refresh(score_set)
        logger.info(msg="Committed new variants to score set.", extra=logging_context)

    ctx["state"][ctx["job_id"]] = logging_context.copy()
    return score_set.processing_state.name


async def map_variants_for_score_set(
    ctx: dict, correlation_id: str, score_set_urn: str, updater_id: int
):
    logging_context = {}
    try:
        db: Session = ctx["db"]
        redis: ArqRedis = ctx["redis"]

        logging_context = setup_job_state(ctx, updater_id, score_set_urn, correlation_id)
        logger.info(msg="Started variant mapping", extra=logging_context)

        # Do not block Worker event loop during mapping, see: https://arq-docs.helpmanual.io/#synchronous-jobs.
        vrs = vrs_mapper()
        blocking = functools.partial(vrs.map_score_set, score_set_urn)
        loop = asyncio.get_running_loop()

        score_set = db.scalars(select(ScoreSet).where(ScoreSet.urn == score_set_urn)).one()
        score_set.mapping_state = MappingState.processing
        score_set.mapping_errors = null()

        try:
            mapping_results = await loop.run_in_executor(ctx["pool"], blocking)
        except requests.exceptions.HTTPError as e:
            score_set.mapping_state = MappingState.failed
            score_set.mapping_errors = {
                "error_message": "Encountered an internal server error during mapping. Mapping will be automatically retried for this score set."
            }
            logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
            logging_context["mapping_state"] = score_set.mapping_state.name
            logger.critical(
                msg="Encountered an exception while mapping variants",
                extra=logging_context
            )
            send_slack_message(err=e)
            # put back in queue, since this is an internal error rather than a problem with the score set
            await redis.lpush(MAPPING_QUEUE_NAME, score_set_urn)  # type: ignore
            await redis.enqueue_job("variant_mapper_manager", correlation_id, score_set_urn, updater_id)
            db.commit()
            return

        logger.debug("Done mapping variants.")

        if mapping_results:
            if not mapping_results["mapped_scores"]:
                # if there are no mapped scores, the score set failed to map.
                score_set.mapping_state = MappingState.failed
                score_set.mapping_errors = mapping_results # TODO check that this gets inserted as json correctly
            else:
                # TODO after adding multi target mapping support:
                # this assumes single-target mapping, will need to be changed to support multi-target mapping
                # just in case there are multiple target genes in the db for a score set (this point shouldn't be reached
                # while we only support single-target mapping), match up the target sequence with the one in the computed genomic reference sequence.
                # TODO after adding accession-based score set mapping support:
                # this also assumes that the score set is based on a target sequence, not a target accession

                if mapping_results["computed_genomic_reference_sequence"]:
                    target_sequence = mapping_results["computed_genomic_reference_sequence"]["sequence"]
                elif mapping_results["computed_protein_reference_sequence"]:
                    target_sequence = mapping_results["computed_protein_reference_sequence"]["sequence"]
                else:
                    score_set.mapping_state = MappingState.failed
                    score_set.mapping_errors = {
                        "error_message": "Encountered an unexpected error during mapping. Mapping will be automatically retried for this score set."
                    }
                    # TODO create error here and send to slack
                    logging_context["mapping_state"] = score_set.mapping_state.name
                    logger.error(msg="No target sequence metadata provided by mapping job", extra=logging_context)
                    # put back in queue, since this is an internal error rather than a problem with the score set
                    await redis.lpush(MAPPING_QUEUE_NAME, score_set_urn)  # type: ignore
                    await redis.enqueue_job("variant_mapper_manager", correlation_id, score_set_urn, updater_id)
                    db.commit()
                    return

                target_gene = db.scalars(select(TargetGene)
                    .join(ScoreSet)
                    .join(TargetSequence)
                    .where(
                        ScoreSet.urn == str(score_set_urn),
                        TargetSequence.sequence == target_sequence,
                    )
                ).one()

                excluded_pre_mapped_keys = {"sequence"}
                if mapping_results["computed_genomic_reference_sequence"] and mapping_results["mapped_genomic_reference_sequence"]:
                    pre_mapped_metadata = mapping_results["computed_genomic_reference_sequence"]
                    target_gene.pre_mapped_metadata = cast({
                        "genomic": {
                            k: pre_mapped_metadata[k] for k in set(list(pre_mapped_metadata.keys())) - excluded_pre_mapped_keys
                        }
                    }, JSONB)
                    target_gene.post_mapped_metadata = cast({"genomic": mapping_results["mapped_genomic_reference_sequence"]}, JSONB)
                elif mapping_results["computed_protein_reference_sequence"] and mapping_results["mapped_protein_reference_sequence"]:
                    pre_mapped_metadata = mapping_results["computed_protein_reference_sequence"]
                    target_gene.pre_mapped_metadata = cast({
                        "protein": {
                            k: pre_mapped_metadata[k] for k in set(list(pre_mapped_metadata.keys())) - excluded_pre_mapped_keys
                        }
                    }, JSONB)
                    target_gene.post_mapped_metadata = cast({"protein": mapping_results["mapped_protein_reference_sequence"]}, JSONB)
                else:
                    score_set.mapping_state = MappingState.failed
                    score_set.mapping_errors = {
                        "error_message": "Encountered an unexpected error during mapping. Mapping will be automatically retried for this score set."
                    }
                    # TODO create error here and send to slack
                    logging_context["mapping_state"] = score_set.mapping_state.name
                    logger.error(
                        msg="No mapped reference sequence metadata provided by mapping job",
                        extra=logging_context,
                    )
                     # put back in queue, since this is an internal error rather than a problem with the score set
                    await redis.lpush(MAPPING_QUEUE_NAME, score_set_urn)  # type: ignore
                    await redis.enqueue_job("variant_mapper_manager", correlation_id, score_set_urn, updater_id)
                    db.commit()
                    return       

                total_variants = 0
                successful_mapped_variants = 0
                for mapped_score in mapping_results["mapped_scores"]:
                    total_variants += 1
                    variant_urn = mapped_score["mavedb_id"]
                    variant = db.scalars(select(Variant).where(Variant.urn == variant_urn)).one()

                    # TODO check with team that this is desired behavior
                    # (another possible behavior would be to always set current to true if there is no other 'current' for this variant id)
                    if mapped_score["pre_mapped"] and mapped_score["post_mapped"]:
                        current = True
                        successful_mapped_variants += 1
                    else:
                        current = False

                    # there should only be one current mapped variant per variant id, so update old mapped variant to current = false
                    if current:
                        db.query(MappedVariant).filter(MappedVariant.variant_id == variant.id).update({"current": False})

                    mapped_variant = MappedVariant(
                        pre_mapped=mapped_score["pre_mapped"] if mapped_score["pre_mapped"] else None,
                        post_mapped=mapped_score["post_mapped"] if mapped_score["post_mapped"] else None,
                        variant_id=variant.id,
                        modification_date=date.today(),
                        mapped_date=mapping_results["mapped_date_utc"],
                        vrs_version=mapped_score["vrs_version"] if mapped_score["vrs_version"] else None,
                        mapping_api_version=mapping_results["dcd_mapping_version"],
                        error_message=mapped_score["error_message"] if mapped_score["error_message"] else None,
                        current=current,
                    )
                    db.add(mapped_variant)  

                if successful_mapped_variants == 0:
                    score_set.mapping_state = MappingState.failed
                    score_set.mapping_errors = {"error_message": "All variants failed to map"}
                elif successful_mapped_variants < total_variants:
                    score_set.mapping_state = MappingState.incomplete
                else:
                    score_set.mapping_state = MappingState.complete 

                logging_context["mapped_variants_inserted_db"] = len(mapping_results['mapped_scores'])
                logging_context["mapping_state"] = score_set.mapping_state.name
                logger.info(msg="Inserted mapped variants into db.", extra=logging_context)

        else:
            score_set.mapping_state = MappingState.failed
            score_set.mapping_errors = {
                "error_message": "Encountered an unexpected error during mapping. Mapping will be automatically retried for this score set."
            }
            # TODO create error here and send to slack
            logging_context["mapping_state"] = score_set.mapping_state.name
            logger.critical(
                msg="No mapping job output for score set, but no HTTPError encountered.",
                extra=logging_context,
            )
            # put back in queue, since this is an internal error rather than a problem with the score set
            await redis.lpush(MAPPING_QUEUE_NAME, score_set_urn)  # type: ignore
            await redis.enqueue_job("variant_mapper_manager", correlation_id, score_set_urn, updater_id)
            db.commit()
            return

        db.commit()
    except Exception as e:
        # score set selection is performed in try statement, so don't update the db if this outer except statement is reached
        send_slack_message(e)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logging_context["mapping_state"] = MappingState.failed.name
        logger.error(
            msg="An unexpected error occurred during variant mapping.",
            extra=logging_context
        )
        # put back in queue, since this is an internal error rather than a problem with the score set
        await redis.lpush(MAPPING_QUEUE_NAME, score_set_urn)  # type: ignore
        await redis.enqueue_job("variant_mapper_manager", correlation_id, score_set_urn, updater_id)
        db.rollback()
        return
        


async def variant_mapper_manager(
    ctx: dict, correlation_id: str, score_set_urn: str, updater_id: int
) -> Optional[Job]:
    logging_context = {}
    try:
        logging_context = setup_job_state(ctx, updater_id, score_set_urn, correlation_id)
        logger.debug(msg="Variant mapping manager began execution", extra=logging_context)
        redis: ArqRedis = ctx["redis"]

        queue_length = await redis.llen(MAPPING_QUEUE_NAME)  # type:ignore
        if queue_length == 0:
            logger.debug(msg="No mapping jobs exist in the queue.", extra=logging_context)
            return None

        logging_context["variant_map_queue_length"] = queue_length
        logger.debug(msg="Found mapping job(s) in queue", extra=logging_context)

        if await redis.exists(MAPPING_CURRENT_ID_NAME):
            mapping_job_id = await redis.get(MAPPING_CURRENT_ID_NAME)
            if mapping_job_id:
                mapping_job_id = mapping_job_id.decode('utf-8')
                job_status = await Job(job_id=mapping_job_id, redis=redis).status()

        if not mapping_job_id or job_status is JobStatus.not_found or job_status is JobStatus.complete:
            logger.info(msg="No mapping jobs are running, queuing a new one.", extra=logging_context)
            queued_urn = await redis.rpop(MAPPING_QUEUE_NAME)  # type:ignore
            queued_urn = queued_urn.decode('utf-8')
            # NOTE: the score_set_urn provided to this function is only used for logging context;
            # get the urn from the queue and pass that urn to map_variants_for_score_set
            new_job = await redis.enqueue_job("map_variants_for_score_set", correlation_id, queued_urn, updater_id)
            if new_job: # for mypy, since enqueue_job can return None
                new_job_id = new_job.job_id
            await redis.set(MAPPING_CURRENT_ID_NAME, new_job_id)
            return new_job
        else:
            logger.debug(
                msg="A mapping job is already running, deferring mapping by 5 minutes.",
                extra=logging_context,
            )

            # Our persistent Redis queue and ARQ's execution rules ensure that even if the worker is stopped and not restarted
            # before the deferred time, these deferred jobs will still run once able.
            return await redis.enqueue_job("variant_mapper_manager", _defer_by=timedelta(minutes=5))
    except Exception as e:
        send_slack_message(e)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="An unexpected error occurred during variant mapper management.",
            extra=logging_context
        )
        return None

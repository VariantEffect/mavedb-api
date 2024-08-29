import asyncio
import functools
import logging
import requests
from datetime import timedelta, date
from typing import Optional, Union

import pandas as pd
from arq import ArqRedis
from arq.jobs import Job, JobStatus
from cdot.hgvs.dataproviders import RESTDataProvider
from sqlalchemy import delete, select, null
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
        await redis.enqueue_job("variant_mapper_manager")
    finally:
        db.add(score_set)
        db.commit()
        db.refresh(score_set)
        logger.info(msg="Committed new variants to score set.", extra=logging_context)

    ctx["state"][ctx["job_id"]] = logging_context.copy()
    return score_set.processing_state.name


async def map_variants_for_score_set(ctx, score_set_urn: str):
    db: Session = ctx["db"]

    logger.info(f"Started variant mapping for score set: {score_set_urn}")

    # Do not block Worker event loop during mapping, see: https://arq-docs.helpmanual.io/#synchronous-jobs.
    vrs = vrs_mapper()
    blocking = functools.partial(vrs.map_score_set, score_set_urn)
    loop = asyncio.get_running_loop()

    score_set = db.scalars(select(ScoreSet).where(ScoreSet.urn == score_set_urn)).one()
    score_set.mapping_state = ProcessingState.processing

    try:
        mapping_results = await loop.run_in_executor(ctx["pool"], blocking)
    except requests.exceptions.HTTPError as e:
        score_set.mapping_state = ProcessingState.failed
        score_set.mapping_errors = {
            "error_message": "Encountered an internal server error during mapping. Mapping will be automatically retried for this score set."
        }
        logger.critical(
            f"Encountered an exception while mapping variants for {score_set_urn}",
            exc_info=e,
        )
        send_slack_message(err=e)
        # TODO put back in queue?
        return

    logger.debug("Done mapping variants.")

    # pseudo code
    # if mapping results:
    #     check whether there are mapped scores.
    #     if there are not mapped scores, there was a score-set-wide error:
    #         this means that nothing will be inserted into the mapped variants table
    #             or into the target genes table.
    #         insert a failed status and the reason into the scoresets table.
    #     else (if there are mapped scores):
    #         insert the pre- and post-mapped sequence metadata into the target genes table
    #         for each mapped score:
    #             if there are both pre and post mapped objects, set current to True. else, current is False.
    #             if current is True, set rows with same variant id to current=False.
    #             insert the mapped variant as below, using the value of current decided above.
    #         keep track of how many mapped scores were set to current. if 100%, then set mapping_state in scoresets table to complete.
    #         if not 100%, then set mapping_state in scoresets table to incomplete. (not sure about this, because variants like '=' might fail... so could be misleading...)
    #         if 0%, then set score set status to failed and error message is that none of the variants mapped

    # else (if not mapping results):
    #     this would be a problem. there should either be an httperror or something returned from the api.
    #     so throw a critical error if this else is hit.

    if mapping_results:
        if not mapping_results["mapped_scores"]:
            # if there are no mapped scores, the score set failed to map.
            score_set.mapping_state = ProcessingState.failed
            score_set.mapping_errors = mapping_results # TODO check that this gets inserted as json correctly
        else:
            # TODO this assumes single-target mapping, will need to be changed to support multi-target mapping
            # just in case there are multiple target genes in the db for a score set (this point shouldn't be reached
            # while we only support single-target mapping), match up the target sequence with the one in the computed genomic reference sequence.
            # TODO this also assumes that the score set is based on a target sequence, not a target accession

            #target_genes = db.scalars(select(TargetGene).join(ScoreSet).where(ScoreSet.urn == score_set_urn)).all()
            if mapping_results["computed_genomic_reference_sequence"]:
                target_sequence = mapping_results["computed_genomic_reference_sequence"]["sequence"]
            elif mapping_results["computed_protein_reference_sequence"]:
                target_sequence = mapping_results["computed_protein_reference_sequence"]["sequence"]
            else:
                score_set.mapping_state = ProcessingState.failed
                score_set.mapping_errors = {
                    "error_message": "Encountered an unexpected error during mapping. Mapping will be automatically retried for this score set."
                }
                logger.error("No target sequence metadata provided by mapping job", exc_info=1)
            # TODO assumes that no hgvs_nt strings were supplied if target sequence is protein. this is currently true
            # but is it guaranteed?
            target_gene = db.scalars(select(TargetGene)
                .join(ScoreSet)
                .join(TargetSequence)
                .where(
                    ScoreSet.urn == score_set_urn,
                    TargetSequence.sequence == target_sequence,
                )
            )

            # TODO may want to append to json rather than replace?
            # TODO cast to jsonb?
            if mapping_results["computed_genomic_reference_sequence"] and mapping_results["mapped_genomic_reference_sequence"]:
                target_gene.pre_mapped_metadata = {"genomic": mapping_results["computed_genomic_reference_sequence"]}
                target_gene.post_mapped_metadata = {"genomic": mapping_results["mapped_genomic_reference_sequence"]}
            elif mapping_results["computed_protein_reference_sequence"] and mapping_results["mapped_protein_reference_sequence"]:
                target_gene.pre_mapped_metadata = {"protein": mapping_results["computed_protein_reference_sequence"]}
                target_gene.post_mapped_metadata = {"protein": mapping_results["mapped_protein_reference_sequence"]}
            else:
                score_set.mapping_state = ProcessingState.failed
                score_set.mapping_errors = {
                    "error_message": "Encountered an unexpected error during mapping. Mapping will be automatically retried for this score set."
                }
                logger.error("No mapped reference sequence metadata provided by mapping job", exc_info=1)               

            total_variants = 0
            successful_mapped_variants = 0
            for mapped_score in mapping_results["mapped_scores"]:
                total_variants += 1
                variant_urn = mapped_score["mavedb_id"]
                variant = db.scalars(select(Variant).where(Variant.urn == variant_urn)).one()

                # TODO if there is an existing mapped variant, then only set this one to current if pre and post mapped objects both exist.
                # or should we always set current to false if pre and post mapped objects aren't both successful, even if there is no existing mapped variant?
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
                    mapped_date=mapped_score["mapped_date_utc"],
                    vrs_version=mapping_results["vrs_version"],
                    mapping_api_version=mapping_results["dcd_mapping_version"],
                    error_message=mapping_results["error_message"] if mapping_results["error_message"] else None,
                    current=current,
                )
                db.add(mapped_variant)  

            logger.info(f"Inserted {len(mapping_results['mapped_scores'])} mapped variants.")

            if successful_mapped_variants == 0:
                score_set.mapping_state = ProcessingState.failed
                score_set.mapping_errors = {"error_message": "All variants failed to map"}
            elif successful_mapped_variants < total_variants:
                score_set.mapping_state = ProcessingState.incomplete
            else:
                score_set.mapping_state = ProcessingState.complete  

    else:
        score_set.mapping_state = ProcessingState.failed
        score_set.mapping_errors = {
            "error_message": "Encountered an unexpected error during mapping. Mapping will be automatically retried for this score set."
        }
        logger.critical(
            f"No mapping job output for {score_set_urn}, but no HTTPError encountered.",
            exc_info=1,
        )
        return

    db.commit()


async def variant_mapper_manager(ctx: dict) -> Optional[Job]:
    logger.debug("Variant mapping manager began execution")
    redis: ArqRedis = ctx["redis"]

    queue_length = await redis.llen(MAPPING_QUEUE_NAME)  # type:ignore
    if queue_length == 0:
        logger.debug("No mapping jobs exist in the queue.")
        return None

    logger.debug(f"{queue_length} mapping job(s) are queued.")

    job = Job(job_id="vrs_map", redis=redis)
    if await job.status() is JobStatus.not_found:
        logger.info("No mapping jobs are running, queuing a new one.")
        queued_urn = await redis.rpop(MAPPING_QUEUE_NAME)  # type:ignore
        return await redis.enqueue_job("map_variants_for_score_set", queued_urn, _job_id="vrs_map")
    else:
        logger.debug("A mapping job is already running, deferring mapping by 5 minutes.")

        # Our persistent Redis queue and ARQ's execution rules ensure that even if the worker is stopped and not restarted
        # before the deferred time, these deferred jobs will still run once able.
        return await redis.enqueue_job("variant_mapper_manager", _defer_by=timedelta(minutes=5))
